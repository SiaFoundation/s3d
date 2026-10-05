package sia

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

const (
	// transferSampleInterval is how often the cumulative transfer counters are
	// sampled to derive a rate.
	transferSampleInterval = time.Second

	// transferRateWindow is the period the reported transfer rates cover.
	transferRateWindow = 5 * time.Second

	numSamples = int(transferRateWindow / transferSampleInterval)
)

type (
	rateSample struct {
		at    time.Time
		total int64
	}

	// transferCounter tracks a cumulative byte total and the rate it grew at
	// over the rate window.
	transferCounter struct {
		total atomic.Int64

		mu      sync.Mutex
		rate    int64
		samples [numSamples]rateSample
	}

	// activeUpload tracks an upload group being streamed from the local buffer
	// to Sia.
	activeUpload struct {
		group uploadGroup

		sent       atomic.Int64
		finalizing atomic.Bool
	}

	// transferStats tracks the request bodies streaming in from S3 clients, the
	// buffered data streaming out to Sia, and the upload groups currently in
	// flight.
	transferStats struct {
		ingress transferCounter
		upload  transferCounter

		mu            sync.Mutex
		ingressActive int64
		active        []*activeUpload
	}
)

func (c *transferCounter) Write(p []byte) (int, error) {
	c.total.Add(int64(len(p)))
	return len(p), nil
}

func (u *activeUpload) Write(p []byte) (int, error) {
	u.sent.Add(int64(len(p)))
	return len(p), nil
}

// seed primes every sample in the rate window with the current total at now.
func (c *transferCounter) seed(now time.Time) {
	total := c.total.Load()

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.samples {
		c.samples[i] = rateSample{at: now, total: total}
	}
}

// sample records the current total at now and updates the rate.
func (c *transferCounter) sample(now time.Time) {
	total := c.total.Load()

	c.mu.Lock()
	defer c.mu.Unlock()
	oldest := c.samples[0]
	copy(c.samples[:], c.samples[1:])
	c.samples[len(c.samples)-1] = rateSample{at: now, total: total}

	elapsed := now.Sub(oldest.at)
	if elapsed <= 0 {
		return
	}
	c.rate = int64(float64(total-oldest.total) / elapsed.Seconds())
}

// trackIngress wraps r so the bytes read from it count towards S3 ingress. The
// stream counts as active until the returned function is called.
func (t *transferStats) trackIngress(r io.Reader) (io.Reader, func()) {
	t.mu.Lock()
	t.ingressActive++
	t.mu.Unlock()

	return io.TeeReader(r, &t.ingress), func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.ingressActive--
	}
}

// snapshot returns the cumulative total and the rate over the window.
func (c *transferCounter) snapshot() (total, rate int64) {
	total = c.total.Load()

	c.mu.Lock()
	defer c.mu.Unlock()
	return total, c.rate
}

func (t *transferStats) beginUpload(group uploadGroup) *activeUpload {
	u := &activeUpload{group: group}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = append(t.active, u)
	return u
}

func (t *transferStats) endUpload(u *activeUpload) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = slices.DeleteFunc(t.active, func(a *activeUpload) bool { return a == u })
}

// finalize records that every object in the group has been handed to the Sia
// uploader and the group is now being finalized.
func (u *activeUpload) finalize() {
	u.finalizing.Store(true)
}

// trackUpload wraps r so the bytes read from it count towards both u's
// progress and the total handed to the Sia uploader.
func (t *transferStats) trackUpload(r io.Reader, u *activeUpload) io.Reader {
	return io.TeeReader(r, io.MultiWriter(&t.upload, u))
}

// snapshot returns the transfer stats as they stand, with active uploads in
// the order they started.
func (t *transferStats) snapshot() s3.TransferStats {
	var stats s3.TransferStats
	stats.IngressBytes, stats.IngressRate = t.ingress.snapshot()
	stats.UploadBytes, stats.UploadRate = t.upload.snapshot()

	t.mu.Lock()
	defer t.mu.Unlock()
	stats.IngressActive = t.ingressActive
	for _, u := range t.active {
		stats.ActiveUploads = append(stats.ActiveUploads, s3.ActiveUpload{
			Label:      uploadLabel(u.group),
			Objects:    int64(len(u.group.objects)),
			Size:       u.group.totalSize,
			Sent:       u.sent.Load(),
			Finalizing: u.finalizing.Load(),
		})
	}
	return stats
}

func uploadLabel(group uploadGroup) string {
	if len(group.objects) == 1 {
		obj := group.objects[0]
		return obj.Bucket + "/" + obj.Name
	}
	return fmt.Sprintf("%d objects", len(group.objects))
}

// statsLoop samples the transfer counters every transferSampleInterval.
func (s *Sia) statsLoop(ctx context.Context) {
	t := time.NewTicker(transferSampleInterval)
	defer t.Stop()

	start := time.Now()
	s.transfer.ingress.seed(start)
	s.transfer.upload.seed(start)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.transfer.ingress.sample(now)
			s.transfer.upload.sample(now)
		}
	}
}

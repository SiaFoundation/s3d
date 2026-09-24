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

	// transferRateSamples is the number of samples spanning the rate window.
	transferRateSamples = int(transferRateWindow / transferSampleInterval)
)

type (
	// rateSample is a cumulative byte total observed at a point in time.
	rateSample struct {
		at    time.Time
		total int64
	}

	// transferCounter tracks a cumulative byte total and the rate it grew at
	// over the rate window.
	transferCounter struct {
		total atomic.Int64
		rate  atomic.Int64

		// samples holds the totals observed over the rate window, oldest
		// first. Only the sampling loop touches it.
		samples [transferRateSamples]rateSample
	}

	// activeUpload tracks an upload group being streamed from the local buffer
	// to Sia.
	activeUpload struct {
		label   string
		objects int
		size    int64

		sent       atomic.Int64
		finalizing atomic.Bool
	}

	// transferStats tracks live transfer activity: request bodies streaming in
	// from S3 clients, buffered data streaming out to Sia, and the upload
	// groups currently in flight.
	transferStats struct {
		ingress transferCounter
		upload  transferCounter

		ingressActive atomic.Int64

		mu     sync.Mutex
		active []*activeUpload
	}

	// countingReader counts the bytes read from r.
	countingReader struct {
		r     io.Reader
		count func(int64)
	}

	// countingReadCloser counts the bytes read from a reader and closes it.
	countingReadCloser struct {
		countingReader
		io.Closer
	}
)

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.count(int64(n))
	}
	return n, err
}

func (c *transferCounter) add(n int64) {
	c.total.Add(n)
}

// seed fills the rate window so a rate can be derived before it has been
// sampled a full window's worth of times.
func (c *transferCounter) seed(now time.Time) {
	total := c.total.Load()
	for i := range c.samples {
		c.samples[i] = rateSample{at: now, total: total}
	}
}

// sample records the current total and derives the rate from the oldest sample
// still inside the window.
func (c *transferCounter) sample(now time.Time) {
	oldest := c.samples[0]
	total := c.total.Load()
	copy(c.samples[:], c.samples[1:])
	c.samples[len(c.samples)-1] = rateSample{at: now, total: total}

	elapsed := now.Sub(oldest.at)
	if elapsed <= 0 {
		return
	}
	c.rate.Store(int64(float64(total-oldest.total) / elapsed.Seconds()))
}

func (t *transferStats) seed(now time.Time) {
	t.ingress.seed(now)
	t.upload.seed(now)
}

func (t *transferStats) sample(now time.Time) {
	t.ingress.sample(now)
	t.upload.sample(now)
}

// trackIngress wraps r so the bytes read from it count towards S3 ingress. The
// stream counts as active until the returned function is called, which must
// happen exactly once.
func (t *transferStats) trackIngress(r io.Reader) (io.Reader, func()) {
	t.ingressActive.Add(1)
	return &countingReader{r: r, count: t.ingress.add}, func() {
		t.ingressActive.Add(-1)
	}
}

// beginUpload registers group as streaming to Sia.
func (t *transferStats) beginUpload(group uploadGroup) *activeUpload {
	u := &activeUpload{
		label:   uploadLabel(group),
		objects: len(group.objects),
		size:    group.totalSize,
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = append(t.active, u)
	return u
}

// endUpload removes u from the uploads streaming to Sia.
func (t *transferStats) endUpload(u *activeUpload) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = slices.DeleteFunc(t.active, func(a *activeUpload) bool { return a == u })
}

// trackUpload wraps rc so the bytes read from it count towards both u's
// progress and the total handed to the Sia uploader.
func (t *transferStats) trackUpload(rc io.ReadCloser, u *activeUpload) io.ReadCloser {
	return &countingReadCloser{
		countingReader: countingReader{
			r: rc,
			count: func(n int64) {
				t.upload.add(n)
				u.sent.Add(n)
			},
		},
		Closer: rc,
	}
}

// snapshot returns the transfer stats as they stand, with active uploads in the
// order they started.
func (t *transferStats) snapshot() s3.TransferStats {
	stats := s3.TransferStats{
		IngressActive: t.ingressActive.Load(),
		IngressBytes:  t.ingress.total.Load(),
		IngressRate:   t.ingress.rate.Load(),
		UploadBytes:   t.upload.total.Load(),
		UploadRate:    t.upload.rate.Load(),
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	stats.UploadActive = int64(len(t.active))
	if len(t.active) == 0 {
		return stats
	}
	stats.ActiveUploads = make([]s3.ActiveUpload, 0, len(t.active))
	for _, u := range t.active {
		stats.ActiveUploads = append(stats.ActiveUploads, s3.ActiveUpload{
			Label:      u.label,
			Objects:    u.objects,
			Size:       u.size,
			Sent:       u.sent.Load(),
			Finalizing: u.finalizing.Load(),
		})
	}
	return stats
}

// uploadLabel describes the contents of an upload group.
func uploadLabel(group uploadGroup) string {
	if len(group.objects) == 1 {
		obj := group.objects[0]
		return obj.Bucket + "/" + obj.Name
	}
	return fmt.Sprintf("%d objects", len(group.objects))
}

// statsLoop samples the transfer counters so the reported rates track recent
// activity.
func (s *Sia) statsLoop(ctx context.Context) {
	t := time.NewTicker(transferSampleInterval)
	defer t.Stop()

	s.transfer.seed(time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.transfer.sample(now)
		}
	}
}

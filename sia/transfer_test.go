package sia

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
)

func TestTransferCounterRate(t *testing.T) {
	start := time.Now()
	sampleAt := func(i int) time.Time {
		return start.Add(time.Duration(i) * transferSampleInterval)
	}

	var c transferCounter
	c.seed(start)

	// a steady 1 MiB per interval reads back as a 1 MiB/s rate, both while the
	// window is still filling and once it has
	for i := 1; i <= 2*transferRateSamples; i++ {
		c.add(1 << 20)
		c.sample(sampleAt(i))
		if got := c.rate.Load(); got != 1<<20 {
			t.Fatalf("expected a 1 MiB/s rate after %d samples, got %d", i, got)
		}
	}

	// once the transfer stops the rate decays to zero as the window clears
	for i := 2*transferRateSamples + 1; i <= 3*transferRateSamples; i++ {
		c.sample(sampleAt(i))
	}
	if got := c.rate.Load(); got != 0 {
		t.Fatalf("expected the rate to decay to 0, got %d", got)
	}
	if got, want := c.total.Load(), int64(2*transferRateSamples)<<20; got != want {
		t.Fatalf("expected a %d byte total, got %d", want, got)
	}
}

func TestTrackIngress(t *testing.T) {
	var stats transferStats
	r, done := stats.trackIngress(bytes.NewReader(make([]byte, 1024)))
	if got := stats.ingressActive.Load(); got != 1 {
		t.Fatalf("expected 1 active ingress stream, got %d", got)
	}

	// bytes are counted as they are read so the rate tracks a body still in
	// flight, not just completed requests
	buf := make([]byte, 256)
	for i := 1; i <= 4; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatal(err)
		} else if got, want := stats.ingress.total.Load(), int64(i*256); got != want {
			t.Fatalf("expected %d ingress bytes after %d reads, got %d", want, i, got)
		}
	}

	done()
	if got := stats.ingressActive.Load(); got != 0 {
		t.Fatalf("expected no active ingress streams, got %d", got)
	}
}

func TestActiveUploadProgress(t *testing.T) {
	var stats transferStats
	single := uploadGroup{
		objects: []objects.ObjectForUpload{{
			Bucket: "bucket",
			Name:   "large.img",
			Length: 2048,
		}},
		totalSize: 2048,
	}
	batch := uploadGroup{
		objects: []objects.ObjectForUpload{
			{Bucket: "bucket", Name: "a", Length: 10},
			{Bucket: "bucket", Name: "b", Length: 20},
		},
		totalSize: 30,
	}

	active := stats.beginUpload(single)
	rc := stats.trackUpload(io.NopCloser(bytes.NewReader(make([]byte, 768))), active)
	if _, err := io.Copy(io.Discard, rc); err != nil {
		t.Fatal(err)
	} else if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	active.finalizing.Store(true)
	batched := stats.beginUpload(batch)

	snap := stats.snapshot()
	if snap.UploadActive != 2 {
		t.Fatalf("expected 2 active Sia uploads, got %d", snap.UploadActive)
	} else if snap.UploadBytes != 768 {
		t.Fatalf("expected 768 bytes handed to the uploader, got %d", snap.UploadBytes)
	} else if len(snap.ActiveUploads) != 2 {
		t.Fatalf("expected 2 reported uploads, got %d", len(snap.ActiveUploads))
	}

	// uploads are reported in the order they started
	first, second := snap.ActiveUploads[0], snap.ActiveUploads[1]
	if first.Label != "bucket/large.img" {
		t.Fatalf("unexpected label for the first upload %q", first.Label)
	} else if first.Sent != 768 || first.Size != 2048 {
		t.Fatalf("expected 768 of 2048 bytes sent, got %d of %d", first.Sent, first.Size)
	} else if !first.Finalizing {
		t.Fatal("expected the first upload to be finalizing")
	} else if second.Label != "2 objects" {
		t.Fatalf("unexpected label for the batched upload %q", second.Label)
	} else if second.Objects != 2 || second.Size != 30 {
		t.Fatalf("expected 2 objects totalling 30 bytes, got %d totalling %d", second.Objects, second.Size)
	} else if second.Sent != 0 || second.Finalizing {
		t.Fatal("expected the batched upload to have made no progress")
	}

	stats.endUpload(active)
	stats.endUpload(batched)
	if snap := stats.snapshot(); snap.UploadActive != 0 || snap.ActiveUploads != nil {
		t.Fatalf("expected no active uploads, got %d", snap.UploadActive)
	}
}

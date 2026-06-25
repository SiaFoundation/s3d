package s3

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// readOnly wraps a reader so it does NOT satisfy io.WriterTo, forcing io.Copy
// down the Read path (bytes.Reader would otherwise divert to WriteTo).
type readOnly struct{ r io.Reader }

func (r readOnly) Read(p []byte) (int, error) { return r.r.Read(p) }

// failingReader returns its data and then fails with err, mimicking a backend
// whose body read fails partway through. It does not implement io.WriterTo.
type failingReader struct {
	data []byte
	err  error
	off  int
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

// failingWriter accepts up to limit bytes and then fails with err, mimicking a
// client connection that drops mid-response.
type failingWriter struct {
	limit   int
	written int
	err     error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.written >= w.limit {
		return 0, w.err
	}
	n := len(p)
	if w.written+n > w.limit {
		n = w.limit - w.written
		w.written += n
		return n, w.err
	}
	w.written += n
	return n, nil
}

// writerToSpy records whether the io.WriterTo fast path was taken.
type writerToSpy struct {
	r    *bytes.Reader
	used bool
}

func (w *writerToSpy) Read(p []byte) (int, error) { return w.r.Read(p) }
func (w *writerToSpy) WriteTo(dst io.Writer) (int64, error) {
	w.used = true
	return w.r.WriteTo(dst)
}

// copyBody mirrors how getObject classifies the failure: io.Copy through the
// wrapper, then attribute by inspecting Err.
func copyBody(dst io.Writer, src io.Reader) (copyErr, readErr error) {
	body := newErrTrackingReader(src)
	_, copyErr = io.Copy(dst, body)
	return copyErr, body.Err()
}

func TestErrTrackingReader(t *testing.T) {
	backendErr := errors.New("backend exploded")
	clientErr := errors.New("connection reset by peer")
	// larger than io.Copy's buffer so the Read path iterates repeatedly
	payload := bytes.Repeat([]byte("abcd"), 32*1024)

	t.Run("successful copy", func(t *testing.T) {
		var got bytes.Buffer
		copyErr, readErr := copyBody(&got, readOnly{bytes.NewReader(payload)})
		if copyErr != nil || readErr != nil {
			t.Fatalf("copyErr=%v readErr=%v, want both nil", copyErr, readErr)
		}
		if !bytes.Equal(got.Bytes(), payload) {
			t.Fatalf("copied %d bytes, want %d", got.Len(), len(payload))
		}
	})

	t.Run("read error is recorded by the wrapper", func(t *testing.T) {
		copyErr, readErr := copyBody(&bytes.Buffer{}, &failingReader{data: []byte("hello"), err: backendErr})
		if !errors.Is(readErr, backendErr) {
			t.Fatalf("readErr=%v, want %v", readErr, backendErr)
		}
		if !errors.Is(copyErr, backendErr) {
			t.Fatalf("copyErr=%v, want io.Copy to surface the read error", copyErr)
		}
	})

	t.Run("write error leaves the wrapper's Err nil", func(t *testing.T) {
		// readOnly forces the Read path; the writer fails partway through
		copyErr, readErr := copyBody(&failingWriter{limit: 10 * 1024, err: clientErr}, readOnly{bytes.NewReader(payload)})
		if copyErr == nil {
			t.Fatal("expected io.Copy to fail on write")
		}
		if readErr != nil {
			t.Fatalf("readErr=%v, want nil (write side failed, not read)", readErr)
		}
	})

	t.Run("WriterTo fast path is preserved and leaves Err nil", func(t *testing.T) {
		spy := &writerToSpy{r: bytes.NewReader(payload)}
		var got bytes.Buffer
		copyErr, readErr := copyBody(&got, spy)
		if !spy.used {
			t.Fatal("expected io.Copy to use the WriterTo fast path")
		}
		if copyErr != nil || readErr != nil {
			t.Fatalf("copyErr=%v readErr=%v, want both nil", copyErr, readErr)
		}
		if !bytes.Equal(got.Bytes(), payload) {
			t.Fatalf("copied %d bytes, want %d", got.Len(), len(payload))
		}
	})

	t.Run("WriterTo write failure attributed to writer", func(t *testing.T) {
		spy := &writerToSpy{r: bytes.NewReader(payload)}
		copyErr, readErr := copyBody(&failingWriter{limit: 10 * 1024, err: clientErr}, spy)
		if !spy.used {
			t.Fatal("expected io.Copy to use the WriterTo fast path")
		}
		if readErr != nil {
			t.Fatalf("readErr=%v, want nil", readErr)
		}
		if copyErr == nil {
			t.Fatal("expected a copy error from the failing writer")
		}
	})

	t.Run("EOF is not recorded as an error", func(t *testing.T) {
		copyErr, readErr := copyBody(&bytes.Buffer{}, &failingReader{data: payload, err: io.EOF})
		if copyErr != nil || readErr != nil {
			t.Fatalf("copyErr=%v readErr=%v, want both nil", copyErr, readErr)
		}
	})
}

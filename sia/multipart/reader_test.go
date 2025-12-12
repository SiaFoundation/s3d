package multipart

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"lukechampine.com/frand"
)

func TestReader(t *testing.T) {
	dir := t.TempDir()

	writePart := func(partNumber int, data []byte) string {
		t.Helper()

		var uuid [8]byte
		frand.Read(uuid[:])
		filename := fmt.Sprintf("%x.part", uuid[:])

		path := filepath.Join(dir, strconv.Itoa(partNumber), filename)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		} else if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}

		return filename
	}

	// prepare reader
	p1 := bytes.Repeat([]byte{'a', 'b', 'c'}, 100)
	p2 := bytes.Repeat([]byte{'d', 'e', 'f'}, 100)
	r := NewReader(Upload{
		Parts: []Part{
			{PartNumber: 1, Filename: writePart(1, p1), Size: int64(len(p1)), MD5: md5.Sum(p1)},
			{PartNumber: 2, Filename: writePart(2, p2), Size: int64(len(p2)), MD5: md5.Sum(p2)},
		},
	}, dir)
	defer r.Close()

	// read all data
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}

	// assert data
	if !bytes.Equal(got, append(p1, p2...)) {
		t.Fatalf("unexpected data: %q", got)
	}

	// assert MD5
	expected := md5.Sum(append(p1, p2...))
	if got := r.MD5Sum(); got != expected {
		t.Fatalf("unexpected MD5: %x, want %x", got, expected)
	}

	// assert size
	if gotSize := r.Size(); gotSize != int64(len(p1)+len(p2)) {
		t.Fatalf("unexpected size: %d, want %d", gotSize, len(p1)+len(p2))
	}

	// recreate reader
	r = NewReader(Upload{
		Parts: []Part{
			{PartNumber: 1, Filename: writePart(1, p1), Size: int64(len(p1)), MD5: md5.Sum(p1)},
			{PartNumber: 2, Filename: writePart(2, p2), Size: int64(len(p2)), MD5: md5.Sum(p2)},
		},
	}, dir)
	defer r.Close()

	// read in random chunks
	var all []byte
	for {
		buf := make([]byte, frand.Intn(100)+1)
		n, err := r.Read(buf)
		all = append(all, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
	}

	// assert data
	if !bytes.Equal(all, append(p1, p2...)) {
		t.Fatalf("unexpected data: %q", all)
	}

	// assert [ErrBadDigest] on part MD5 mismatch
	r = NewReader(Upload{
		Parts: []Part{
			{PartNumber: 3, Filename: writePart(3, []byte("x")), Size: 1, MD5: md5.Sum([]byte("y"))},
		},
	}, dir)
	defer r.Close()
	if _, err := io.ReadAll(r); !errors.Is(err, s3errs.ErrBadDigest) {
		t.Fatalf("expected ErrBadDigest, got %v", err)
	}
}

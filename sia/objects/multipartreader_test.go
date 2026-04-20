package objects

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

func TestMultipartReader(t *testing.T) {
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
	r, err := NewReader(dir, []Part{
		{PartNumber: 1, Filename: writePart(1, p1), Size: int64(len(p1)), ContentMD5: md5.Sum(p1)},
		{PartNumber: 2, Filename: writePart(2, p2), Size: int64(len(p2)), ContentMD5: md5.Sum(p2)},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
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

	// recreate reader
	r, err = NewReader(dir, []Part{
		{PartNumber: 1, Filename: writePart(1, p1), Size: int64(len(p1)), ContentMD5: md5.Sum(p1)},
		{PartNumber: 2, Filename: writePart(2, p2), Size: int64(len(p2)), ContentMD5: md5.Sum(p2)},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
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

	// assert MD5 is validated
	r, err = NewReader(dir, []Part{
		{PartNumber: 3, Filename: writePart(3, []byte("x")), Size: 1, ContentMD5: md5.Sum([]byte("y"))},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); !errors.Is(err, s3errs.ErrBadDigest) {
		t.Fatalf("expected ErrBadDigest, got %v", err)
	}

	// assert size is validated
	wrongSizeFile := writePart(5, []byte("actual data"))
	r, err = NewReader(dir, []Part{
		{PartNumber: 5, Filename: wrongSizeFile, Size: 999, ContentMD5: md5.Sum([]byte("actual data"))},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); err == nil || !bytes.Contains([]byte(err.Error()), []byte("size mismatch")) {
		t.Fatalf("expected size mismatch error, got %v", err)
	}

	// assert part file must exist
	r, err = NewReader(dir, []Part{{PartNumber: 6, Filename: "nonexistent.file", Size: 10, ContentMD5: md5.Sum([]byte("irrelevant"))}}, 0)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected file open error, got %v", err)
	}
}

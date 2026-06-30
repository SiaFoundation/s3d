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

	// offset tests use 3 parts of known sizes
	op1 := bytes.Repeat([]byte{'A'}, 100)
	op2 := bytes.Repeat([]byte{'B'}, 200)
	op3 := bytes.Repeat([]byte{'C'}, 150)
	fullData := append(append(op1, op2...), op3...)
	offsetParts := []Part{
		{PartNumber: 10, Filename: writePart(10, op1), Size: int64(len(op1)), ContentMD5: md5.Sum(op1)},
		{PartNumber: 11, Filename: writePart(11, op2), Size: int64(len(op2)), ContentMD5: md5.Sum(op2)},
		{PartNumber: 12, Filename: writePart(12, op3), Size: int64(len(op3)), ContentMD5: md5.Sum(op3)},
	}

	// offset within the first part
	r, err = NewReader(dir, offsetParts, 50)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if !bytes.Equal(got, fullData[50:]) {
		t.Fatalf("offset 50: expected %d bytes, got %d", len(fullData)-50, len(got))
	}

	// offset exactly at a part boundary (skip first part entirely)
	r, err = NewReader(dir, offsetParts, 100)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if !bytes.Equal(got, fullData[100:]) {
		t.Fatalf("offset 100: expected %d bytes, got %d", len(fullData)-100, len(got))
	}

	// offset that skips the first part and lands within the second
	r, err = NewReader(dir, offsetParts, 250)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if !bytes.Equal(got, fullData[250:]) {
		t.Fatalf("offset 250: expected %d bytes, got %d", len(fullData)-250, len(got))
	}

	// offset that skips all parts (at the very end)
	r, err = NewReader(dir, offsetParts, int64(len(fullData)))
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("offset at end: expected 0 bytes, got %d", len(got))
	}

	// zero offset returns all data
	r, err = NewReader(dir, offsetParts, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if !bytes.Equal(got, fullData) {
		t.Fatalf("offset 0: expected %d bytes, got %d", len(fullData), len(got))
	}

	// WriteTo streams all remaining parts
	r, err = NewReader(dir, offsetParts, 0)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	n, err := r.WriteTo(&buf)
	r.Close()
	if err != nil {
		t.Fatalf("unexpected WriteTo error: %v", err)
	} else if n != int64(len(fullData)) {
		t.Fatalf("WriteTo: expected %d bytes, got %d", len(fullData), n)
	} else if !bytes.Equal(buf.Bytes(), fullData) {
		t.Fatalf("WriteTo: unexpected data: %q", buf.Bytes())
	}

	// WriteTo honors an offset within the first part
	r, err = NewReader(dir, offsetParts, 50)
	if err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	n, err = r.WriteTo(&buf)
	r.Close()
	if err != nil {
		t.Fatalf("WriteTo offset 50: unexpected error: %v", err)
	} else if n != int64(len(fullData)-50) {
		t.Fatalf("WriteTo offset 50: expected %d bytes, got %d", len(fullData)-50, n)
	} else if !bytes.Equal(buf.Bytes(), fullData[50:]) {
		t.Fatalf("WriteTo offset 50: unexpected data: %q", buf.Bytes())
	}

	// WriteTo continues from where a preceding Read left off
	r, err = NewReader(dir, offsetParts, 0)
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 30)
	if _, err := io.ReadFull(r, head); err != nil {
		t.Fatalf("read head: %v", err)
	}
	buf.Reset()
	n, err = r.WriteTo(&buf)
	r.Close()
	if err != nil {
		t.Fatalf("WriteTo after Read: unexpected error: %v", err)
	} else if n != int64(len(fullData)-30) {
		t.Fatalf("WriteTo after Read: expected %d bytes, got %d", len(fullData)-30, n)
	} else if !bytes.Equal(append(head, buf.Bytes()...), fullData) {
		t.Fatalf("WriteTo after Read: unexpected data: %q", append(head, buf.Bytes()...))
	}

	// LimitReader.WriteTo stops at the limit within the first part
	r, err = NewReader(dir, offsetParts, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	n, err = io.Copy(&buf, LimitReader(r, 60))
	r.Close()
	if err != nil {
		t.Fatalf("LimitReader WriteTo: unexpected error: %v", err)
	} else if n != 60 {
		t.Fatalf("LimitReader WriteTo: expected 60 bytes, got %d", n)
	} else if !bytes.Equal(buf.Bytes(), fullData[:60]) {
		t.Fatalf("LimitReader WriteTo: unexpected data: %q", buf.Bytes())
	}

	// LimitReader.WriteTo spans parts, combined with an offset
	r, err = NewReader(dir, offsetParts, 50)
	if err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	n, err = io.Copy(&buf, LimitReader(r, 200))
	r.Close()
	if err != nil {
		t.Fatalf("LimitReader WriteTo span: unexpected error: %v", err)
	} else if n != 200 {
		t.Fatalf("LimitReader WriteTo span: expected 200 bytes, got %d", n)
	} else if !bytes.Equal(buf.Bytes(), fullData[50:250]) {
		t.Fatalf("LimitReader WriteTo span: unexpected data: %q", buf.Bytes())
	}

	// LimitReader caps a limit longer than the remaining data
	r, err = NewReader(dir, offsetParts, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	n, err = io.Copy(&buf, LimitReader(r, int64(len(fullData))+100))
	r.Close()
	if err != nil {
		t.Fatalf("LimitReader WriteTo overshoot: unexpected error: %v", err)
	} else if n != int64(len(fullData)) {
		t.Fatalf("LimitReader WriteTo overshoot: expected %d bytes, got %d", len(fullData), n)
	} else if !bytes.Equal(buf.Bytes(), fullData) {
		t.Fatalf("LimitReader WriteTo overshoot: unexpected data: %q", buf.Bytes())
	}

	// LimitReader.Read honors the same bound as WriteTo
	r, err = NewReader(dir, offsetParts, 50)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(LimitReader(r, 200))
	r.Close()
	if err != nil {
		t.Fatalf("LimitReader Read: unexpected error: %v", err)
	} else if !bytes.Equal(got, fullData[50:250]) {
		t.Fatalf("LimitReader Read: unexpected data: %q", got)
	}
}

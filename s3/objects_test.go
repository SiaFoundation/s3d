package s3_test

import (
	"bytes"
	"crypto/md5"
	"io"
	"testing"

	"github.com/SiaFoundation/s3d/s3/internal/testutil"
	"lukechampine.com/frand"
)

func TestGetObject(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	// prepare a bucket
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// add object to backend
	data := frand.Bytes(100)
	object := "bar"
	err := s3Tester.AddObject(bucket, object, data, make(map[string]string))
	if err != nil {
		t.Fatal(err)
	}

	// retrieve object
	obj, err := s3Tester.GetObject(t.Context(), bucket, object)
	if err != nil {
		t.Fatal(err)
	}
	hash := md5.Sum(data)
	if content, err := io.ReadAll(obj.Body); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(content, data) {
		t.Fatal("data mismatch")
	} else if !bytes.Equal(obj.Hash, hash[:]) {
		t.Fatal("hash mismatch")
	} else if obj.Size != int64(len(data)) {
		t.Fatalf("size mismatch: expected %d, got %d", len(data), obj.Size)
	}
}

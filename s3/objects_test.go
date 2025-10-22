package s3_test

import (
	"bytes"
	"crypto/md5"
	"io"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/internal/testutil"
	"lukechampine.com/frand"
)

func TestGetAndHeadObject(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	// prepare a bucket
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// add object to backend
	data := frand.Bytes(100)
	hash := md5.Sum(data)
	object := "bar"
	err := s3Tester.AddObject(bucket, object, data, make(map[string]string))
	if err != nil {
		t.Fatal(err)
	}

	assertObject := func(obj *s3.Object, head bool) {
		t.Helper()

		if !bytes.Equal(obj.Hash, hash[:]) {
			t.Fatal("hash mismatch", obj.Hash, hash[:])
		} else if obj.Size != int64(len(data)) {
			t.Fatalf("size mismatch: expected %d, got %d", len(data), obj.Size)
		}

		if !head {
			if content, err := io.ReadAll(obj.Body); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(content, data) {
				t.Fatal("data mismatch")
			}
		}
	}

	// GET the object
	obj, err := s3Tester.GetObject(t.Context(), bucket, object)
	if err != nil {
		t.Fatal(err)
	}
	assertObject(obj, false)

	// HEAD the object
	obj, err = s3Tester.HeadObject(t.Context(), bucket, object)
	if err != nil {
		t.Fatal(err)
	}
	assertObject(obj, true)
}

package sia_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"lukechampine.com/frand"
)

func TestPutObject(t *testing.T) {
	test := func(t *testing.T, s3Tester *testutil.S3Tester) {
		data := frand.Bytes(100)
		hash := md5.Sum(data)

		// prepare a bucket
		bucket := "foo"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// prepare the object to upload
		object := "bar"
		metadata := map[string]string{
			"foo": "bar",
		}

		// upload the object
		resp, err := s3Tester.PutObject(t.Context(), bucket, object, bytes.NewReader(data), metadata)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(resp, hash[:]) {
			t.Fatalf("hash mismatch: expected %x, got %x", hash, resp)
		}

		// download the object and verify it
		obj, err := s3Tester.GetObject(t.Context(), bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		} else if obj.ContentMD5 != hash {
			t.Fatal("hash mismatch", obj.ContentMD5, hash[:])
		} else if obj.Size != int64(len(data)) {
			t.Fatalf("size mismatch: expected %d, got %d", len(data), obj.Size)
		} else if !reflect.DeepEqual(obj.Metadata, metadata) {
			t.Fatal("metadata mismatch", obj.Metadata)
		}

		// upload with a key that is too large
		_, err = s3Tester.PutObject(t.Context(), bucket, hex.EncodeToString(frand.Bytes(s3.KeySizeLimit)), bytes.NewReader(data), nil)
		testutil.AssertS3Error(t, s3errs.ErrKeyTooLongError, err)

		// upload with metadata that is too large
		_, err = s3Tester.PutObject(t.Context(), bucket, "too-much-meta", bytes.NewReader(data), map[string]string{
			"too-much": hex.EncodeToString(frand.Bytes(s3.MetadataSizeLimit)),
		})
		testutil.AssertS3Error(t, s3errs.ErrMetadataTooLarge, err)

		// upload to a bucket that doesn't exist
		_, err = s3Tester.PutObject(t.Context(), "nonexistent", object, bytes.NewReader(data), metadata)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

		// upload to a bucket that we don't own
		otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
		_, err = otherTester.PutObject(t.Context(), bucket, object, bytes.NewReader(data), metadata)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	}

	t.Run("http", func(t *testing.T) {
		s3Tester := NewTester(t)
		test(t, s3Tester)
	})

	t.Run("https", func(t *testing.T) {
		s3Tester := NewTester(t, testutil.WithTLS())
		test(t, s3Tester)
	})
}

package sia_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"lukechampine.com/frand"
)

func TestGetAndHeadObject(t *testing.T) {
	now := time.Now().UTC().Add(-time.Second)
	s3Tester := NewTester(t)

	// prepare a bucket
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// add object to backend
	data := frand.Bytes(100)
	hash := md5.Sum(data)
	object := "bar"
	metadata := map[string]string{
		"x-amz-meta-foo": "bar",
	}
	err := s3Tester.AddObject(bucket, object, data, metadata)
	if err != nil {
		t.Fatal(err)
	}

	assertObject := func(t *testing.T, obj *s3.Object, head bool, rnge *s3.ObjectRangeRequest) {
		t.Helper()

		var start, end int64
		if rnge == nil {
			start = 0
			end = int64(len(data)) - 1
		} else if rnge.FromEnd {
			start = int64(len(data)) - rnge.Start
			end = rnge.End
		} else {
			start = rnge.Start
			end = rnge.End
		}

		if end == -1 {
			end = int64(len(data)) - 1
		}

		expected := data[start : end+1]
		if obj.ContentMD5 != hash {
			t.Fatal("hash mismatch", obj.ContentMD5, hash[:])
		} else if obj.Size != int64(len(data)) {
			t.Fatalf("size mismatch: expected %d, got %d", len(data), obj.Size)
		} else if obj.LastModified.Before(now) {
			t.Fatal("last modified not set", obj.LastModified)
		}

		// NOTE: The S3 client trims away the x-amz-meta- prefix when returning user
		// metadata. Since we added the object to the store directly rather than
		// the client, we set x-amz-meta-foo in the store but check for foo
		// here.
		if obj.Metadata["foo"] != "bar" {
			t.Fatal("metadata mismatch", obj.Metadata)
		}

		if !head {
			if content, err := io.ReadAll(obj.Body); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(content, expected) {
				t.Fatal("data mismatch", len(content), len(expected))
			}
		}
	}

	tests := []struct {
		name string
		rnge *s3.ObjectRangeRequest
	}{
		{
			name: "NoRange",
			rnge: nil,
		},
		{
			name: "FullRange",
			rnge: &s3.ObjectRangeRequest{
				Start: 0,
				End:   99,
			},
		},
		{
			name: "PartialRange",
			rnge: &s3.ObjectRangeRequest{
				Start: 33,
				End:   66,
			},
		},
		{
			name: "OpenEndedRange",
			rnge: &s3.ObjectRangeRequest{
				Start: 33,
				End:   -1,
			},
		},
		{
			name: "ReversedRange",
			rnge: &s3.ObjectRangeRequest{
				Start:   10,
				End:     99,
				FromEnd: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// HEAD Object
			obj, err := s3Tester.HeadObject(t.Context(), bucket, object, tc.rnge)
			if err != nil {
				t.Fatal(err)
			}
			assertObject(t, obj, true, tc.rnge)

			// GET Object
			obj, err = s3Tester.GetObject(t.Context(), bucket, object, tc.rnge)
			if err != nil {
				t.Fatal(err)
			}
			assertObject(t, obj, false, tc.rnge)
		})
	}
}

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

func TestCopyObject(t *testing.T) {
	s3Tester := NewTester(t)
	data := frand.Bytes(100)
	hash := md5.Sum(data)

	// prepare a bucket
	srcBucket := "srcbucket"
	if err := s3Tester.CreateBucket(t.Context(), srcBucket); err != nil {
		t.Fatal(err)
	}

	// prepare the object to upload
	srcObject := "srcobject"
	metadata := map[string]string{
		"foo": "bar",
	}

	// upload the object
	resp, err := s3Tester.PutObject(t.Context(), srcBucket, srcObject, bytes.NewReader(data), metadata)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(resp, hash[:]) {
		t.Fatalf("hash mismatch: expected %x, got %x", hash, resp)
	}

	var dstBucket, dstObject = "dstbucket", "dstobject"

	// copying before creating the destination bucket should fail
	_, err = s3Tester.CopyObject(t.Context(), srcBucket, srcObject, dstBucket, dstObject, types.MetadataDirectiveCopy, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// create destination bucket and try again
	if err := s3Tester.CreateBucket(t.Context(), dstBucket); err != nil {
		t.Fatal(err)
	}
	dstMeta := map[string]string{
		"baz": "qux",
	}
	copyTime := time.Now().UTC()
	time.Sleep(time.Second)
	etag, err := s3Tester.CopyObject(t.Context(), srcBucket, srcObject, dstBucket, dstObject, types.MetadataDirectiveCopy, dstMeta)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(etag, hash[:]) {
		t.Fatalf("etag mismatch: expected %x, got %x", hash, etag)
	}

	// fetch the copied object and verify it
	obj, err := s3Tester.GetObject(t.Context(), dstBucket, dstObject, nil)
	if err != nil {
		t.Fatal(err)
	} else if fetched, err := io.ReadAll(obj.Body); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, fetched) {
		t.Fatal("data mismatch")
	} else if len(obj.Metadata) != 2 || obj.Metadata["foo"] != "bar" || obj.Metadata["baz"] != "qux" {
		t.Fatalf("metadata mismatch: %+v", obj.Metadata)
	} else if obj.ContentMD5 != hash {
		t.Fatal("hash mismatch", obj.ContentMD5, hash[:])
	} else if obj.LastModified.Before(copyTime) {
		t.Fatalf("last modified mismatch: expected after %v, got %v", copyTime, obj.LastModified)
	}

	// copy an object that doesn't exist
	_, err = s3Tester.CopyObject(t.Context(), srcBucket, "nonexistent", dstBucket, dstBucket, types.MetadataDirectiveCopy, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)

	// copy an object to the same bucket and key, adding additional metadata
	additionalMeta := map[string]string{
		"new-key": "new-value",
	}
	etag, err = s3Tester.CopyObject(t.Context(), dstBucket, dstObject, dstBucket, dstObject, types.MetadataDirectiveReplace, additionalMeta)
	if err != nil {
		t.Fatal(err)
	}
	obj, err = s3Tester.GetObject(t.Context(), dstBucket, dstObject, nil)
	if err != nil {
		t.Fatal(err)
	} else if fetched, err := io.ReadAll(obj.Body); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, fetched) {
		t.Fatal("data mismatch")
	} else if len(obj.Metadata) != 1 || obj.Metadata["new-key"] != "new-value" {
		t.Fatalf("metadata mismatch: %+v", obj.Metadata)
	} else if obj.ContentMD5 != hash {
		t.Fatal("hash mismatch", obj.ContentMD5, hash[:])
	} else if obj.LastModified.Before(copyTime) {
		t.Fatalf("last modified mismatch: expected after %v, got %v", copyTime, obj.LastModified)
	}
}

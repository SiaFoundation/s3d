package sia_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap/zaptest"
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

		// upload an empty object
		emptyObject := "empty.txt"
		emptyMD5 := md5.Sum([]byte{})
		resp, err = s3Tester.PutObject(t.Context(), bucket, emptyObject, bytes.NewReader([]byte{}), metadata)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(resp, emptyMD5[:]) {
			t.Fatalf("empty object hash mismatch: expected %x, got %x", emptyMD5, resp)
		}

		// assert fetching the object works and the body is empty
		obj, err = s3Tester.GetObject(t.Context(), bucket, emptyObject, nil)
		if err != nil {
			t.Fatal(err)
		} else if obj.ContentMD5 != emptyMD5 {
			t.Fatal("empty object hash mismatch", obj.ContentMD5, emptyMD5)
		} else if obj.Size != 0 {
			t.Fatalf("empty object size mismatch: expected 0, got %d", obj.Size)
		} else if !reflect.DeepEqual(obj.Metadata, metadata) {
			t.Fatal("empty object metadata mismatch", obj.Metadata)
		}
		body, err := io.ReadAll(obj.Body)
		if err != nil {
			t.Fatal(err)
		} else if len(body) != 0 {
			t.Fatalf("expected empty body, got %d bytes", len(body))
		}
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
	_, err = s3Tester.CopyObject(t.Context(), srcBucket, "nonexistent", dstBucket, "someDstObj", types.MetadataDirectiveCopy, nil)
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

func TestListObjects(t *testing.T) {
	s3Tester := NewTester(t)

	// prepare a bucket
	bucket := "testbucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects with different prefixes
	objects := []string{
		"file1.txt",
		"file2.txt",
		"folder1/file1.txt",
		"folder1/file2.txt",
		"folder1/subfolder/file1.txt",
		"folder2/file1.txt",
	}
	for _, obj := range objects {
		_, err := s3Tester.PutObject(t.Context(), bucket, obj, bytes.NewReader([]byte("test")), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name            string
		prefix          *string
		delimiter       *string
		maxKeys         int64
		expectedKeys    []string
		expectedPrefix  []string
		expectedTrunc   bool
		expectedKeysCnt int32
	}{
		{
			name:            "list all objects",
			prefix:          nil,
			delimiter:       nil,
			maxKeys:         100,
			expectedKeys:    []string{"file1.txt", "file2.txt", "folder1/file1.txt", "folder1/file2.txt", "folder1/subfolder/file1.txt", "folder2/file1.txt"},
			expectedPrefix:  nil,
			expectedTrunc:   false,
			expectedKeysCnt: 6,
		},
		{
			name:            "list with delimiter at root",
			prefix:          nil,
			delimiter:       aws.String("/"),
			maxKeys:         100,
			expectedKeys:    []string{"file1.txt", "file2.txt"},
			expectedPrefix:  []string{"folder1/", "folder2/"},
			expectedTrunc:   false,
			expectedKeysCnt: 4,
		},
		{
			name:            "list with prefix",
			prefix:          aws.String("folder1/"),
			delimiter:       nil,
			maxKeys:         100,
			expectedKeys:    []string{"folder1/file1.txt", "folder1/file2.txt", "folder1/subfolder/file1.txt"},
			expectedPrefix:  nil,
			expectedTrunc:   false,
			expectedKeysCnt: 3,
		},
		{
			name:            "list with prefix and delimiter",
			prefix:          aws.String("folder1/"),
			delimiter:       aws.String("/"),
			maxKeys:         100,
			expectedKeys:    []string{"folder1/file1.txt", "folder1/file2.txt"},
			expectedPrefix:  []string{"folder1/subfolder/"},
			expectedTrunc:   false,
			expectedKeysCnt: 3,
		},
		{
			name:            "list with maxKeys truncation",
			prefix:          nil,
			delimiter:       nil,
			maxKeys:         2,
			expectedKeys:    []string{"file1.txt", "file2.txt"},
			expectedPrefix:  nil,
			expectedTrunc:   true,
			expectedKeysCnt: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := s3Tester.ListObjectsV2(t.Context(), bucket, tc.prefix, tc.delimiter, s3.ListObjectsPage{
				MaxKeys: tc.maxKeys,
			})
			if err != nil {
				t.Fatal(err)
			}

			// check key count
			if *resp.KeyCount != tc.expectedKeysCnt {
				t.Fatalf("expected %d keys, got %d", tc.expectedKeysCnt, *resp.KeyCount)
			}

			// check truncation
			if resp.IsTruncated == nil || *resp.IsTruncated != tc.expectedTrunc {
				t.Fatalf("expected IsTruncated=%v, got %v", tc.expectedTrunc, resp.IsTruncated)
			}

			// check contents
			if len(resp.Contents) != len(tc.expectedKeys) {
				t.Fatalf("expected %d objects, got %d", len(tc.expectedKeys), len(resp.Contents))
			}
			for i, obj := range resp.Contents {
				if *obj.Key != tc.expectedKeys[i] {
					t.Fatalf("expected key %q, got %q", tc.expectedKeys[i], *obj.Key)
				}
			}

			// check common prefixes
			if len(resp.CommonPrefixes) != len(tc.expectedPrefix) {
				t.Fatalf("expected %d common prefixes, got %d", len(tc.expectedPrefix), len(resp.CommonPrefixes))
			}
			for i, prefix := range resp.CommonPrefixes {
				if *prefix.Prefix != tc.expectedPrefix[i] {
					t.Fatalf("expected prefix %q, got %q", tc.expectedPrefix[i], *prefix.Prefix)
				}
			}
		})
	}

	// test pagination with continuation token
	t.Run("pagination", func(t *testing.T) {
		// first request
		resp, err := s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{
			MaxKeys: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !*resp.IsTruncated {
			t.Fatal("expected truncated response")
		}
		if len(resp.Contents) != 2 {
			t.Fatalf("expected 2 objects, got %d", len(resp.Contents))
		}

		// continue from where we left off
		resp, err = s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{
			Marker:  resp.NextContinuationToken,
			MaxKeys: 100,
		})
		if err != nil {
			t.Fatal(err)
		}
		if *resp.IsTruncated {
			t.Fatal("expected non-truncated response")
		}
		if len(resp.Contents) != 4 {
			t.Fatalf("expected 4 remaining objects, got %d", len(resp.Contents))
		}
	})

	// test list objects with and without owner
	t.Run("owner", func(t *testing.T) {
		resp, err := s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{
			MaxKeys:    1,
			FetchOwner: aws.Bool(true),
		})
		if err != nil {
			t.Fatal(err)
		} else if len(resp.Contents) == 0 {
			t.Fatal("expected at least one object")
		}

		obj := resp.Contents[0]
		if obj.Owner == nil {
			t.Fatal("expected owner to be set")
		} else if obj.Owner.ID == nil || *obj.Owner.ID == "" {
			t.Fatal("expected owner ID to be set")
		} else if obj.Owner.DisplayName == nil || *obj.Owner.DisplayName == "" {
			t.Fatal("expected owner DisplayName to be set")
		}

		resp, err = s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{
			MaxKeys:    1,
			FetchOwner: aws.Bool(false),
		})
		if err != nil {
			t.Fatal(err)
		} else if len(resp.Contents) == 0 {
			t.Fatal("expected at least one object")
		}

		obj = resp.Contents[0]
		if obj.Owner != nil {
			t.Fatal("expected owner to be nil")
		}
	})

	// test listing from non-existent bucket
	t.Run("nonexistent bucket", func(t *testing.T) {
		_, err := s3Tester.ListObjectsV2(t.Context(), "nonexistent", nil, nil, s3.ListObjectsPage{})
		testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)
	})
}

func TestDeleteObjects(t *testing.T) {
	s3Tester := NewTester(t)

	// prepare a bucket
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
	const etag = "d41d8cd98f00b204e9800998ecf8427e"
	keys := []string{"1", "2", "3", "4", "5"}
	for _, key := range keys {
		_, err := s3Tester.PutObject(t.Context(), bucket, key, bytes.NewReader([]byte{}), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// delete from nonexistent bucket
	_, err := s3Tester.DeleteObjects(t.Context(), "nonexistent", testutil.ObjectIdentifiers(keys...), nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	assertDeleted := func(t *testing.T, key string, deleted types.DeletedObject) {
		t.Helper()
		if *deleted.Key != key {
			t.Fatalf("expected deleted key %v, got %v", key, *deleted.Key)
		}
	}

	// attempt to delete an object with wrong conditions
	resp, err := s3Tester.DeleteObjects(t.Context(), bucket, []types.ObjectIdentifier{
		{
			Key:  aws.String("1"),
			ETag: aws.String("wrong"),
		},
		{
			Key:              aws.String("1"),
			LastModifiedTime: aws.Time(time.Now().Add(-time.Hour)),
		},
		{
			Key:  aws.String("1"),
			Size: aws.Int64(147),
		},
	}, nil) // delete object 1
	if err != nil {
		t.Fatal(err)
	} else if len(resp.Deleted) != 0 {
		t.Fatalf("expected 0 deleted object, got %d", len(resp.Deleted))
	} else if len(resp.Errors) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(resp.Errors))
	}
	for _, delErr := range resp.Errors {
		if delErr.Code == nil || *delErr.Code != s3errs.ErrPreconditionFailed.Code {
			t.Fatalf("expected PreconditionFailed error, got %v", *delErr.Code)
		}
	}

	// delete object 1 with correct conditions
	o1, err := s3Tester.HeadObject(t.Context(), bucket, "1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = s3Tester.DeleteObjects(t.Context(), bucket, []types.ObjectIdentifier{
		{
			Key:              aws.String("1"),
			ETag:             aws.String(`"` + etag + `"`),
			LastModifiedTime: aws.Time(o1.LastModified),
			Size:             aws.Int64(o1.Size),
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(resp.Deleted) != 1 {
		t.Fatal("expected 1 deleted object, got", len(resp.Deleted))
	}

	// delete a few objects, including one that doesn't exist
	delKeys := []string{"2", "4", "nonexistent"}
	resp, err = s3Tester.DeleteObjects(t.Context(), bucket, testutil.ObjectIdentifiers(delKeys...), nil)
	if err != nil {
		t.Fatal(err)
	} else if len(resp.Deleted) != 3 {
		t.Fatalf("expected 3 deleted objects, got %d", len(resp.Deleted))
	}
	assertDeleted(t, "2", resp.Deleted[0])
	assertDeleted(t, "4", resp.Deleted[1])
	assertDeleted(t, "nonexistent", resp.Deleted[2])

	// verify deleted objects are gone and others remain
	// TODO: Re-enable once ListObjectsV2 is implemented
	//	objs, err := s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	if *objs.KeyCount != 3 {
	//		t.Fatalf("expected 3 remaining objects, got %d", objs.KeyCount)
	//	} else if *objs.Contents[0].Key != "1" || *objs.Contents[1].Key != "3" || *objs.Contents[2].Key != "5" {
	//		t.Fatalf("remaining objects mismatch: %+v", objs.Contents)
	//	}

	// delete the remaining ones using 'quiet' mode
	resp, err = s3Tester.DeleteObjects(t.Context(), bucket, testutil.ObjectIdentifiers("3", "5"), aws.Bool(true))
	if err != nil {
		t.Fatal(err)
	} else if len(resp.Deleted) != 0 {
		t.Fatalf("expected 0 deleted objects in quiet mode, got %d", len(resp.Deleted))
	} else if len(resp.Errors) != 0 {
		t.Fatalf("expected 0 errors in quiet mode, got %d", len(resp.Errors))
	}

	// verify deleted objects are gone and others remain
	// TODO: Re-enable once ListObjectsV2 is implemented
	// objs, err = s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	// if err != nil {
	// 	t.Fatal(err)
	// } else if objs.KeyCount != nil {
	// 	t.Fatalf("expected 0 remaining objects, got %d", *objs.KeyCount)
	// }
}

func TestObjectMetadataCache(t *testing.T) {
	memSDK := NewMemorySDK()
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	s3Tester := NewCustomTester(t, dir, store, memSDK, log)

	// prepare a bucket
	const bucket = "bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// upload a non-empty object via PutObject - metadata will be cached from upload
	data := frand.Bytes(64)

	const object = "object"
	_, err = s3Tester.PutObject(t.Context(), bucket, object, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}

	memSDK.objectCallCount = 0
	t.Run("uses cached metadata", func(t *testing.T) {
		// first GET should use cached metadata from upload
		_, err := s3Tester.GetObject(t.Context(), bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		}
		if memSDK.objectCallCount != 0 {
			t.Fatalf("expected 0 calls to SDK.Object when using fresh cache, got %d", memSDK.objectCallCount)
		}

		// second GET should still use cached metadata without calling indexer
		_, err = s3Tester.GetObject(t.Context(), bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		}
		if memSDK.objectCallCount != 0 {
			t.Fatalf("expected 0 calls to SDK.Object on second GET, got %d", memSDK.objectCallCount)
		}
	})

	t.Run("expired cache triggers refresh", func(t *testing.T) {
		accessKeyID := testutil.AccessKeyID
		obj, err := store.GetObject(&accessKeyID, bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		}
		// set retrieval time to 25 hours ago (past the 24-hour cache lifetime)
		obj.CachedAt = time.Now().Add(-25 * time.Hour)
		if err := store.PutObject(accessKeyID, bucket, object, obj, true); err != nil {
			t.Fatal(err)
		}

		// reset counter
		memSDK.objectCallCount = 0

		// GET with expired cache should call SDK.Object to refresh
		_, err = s3Tester.GetObject(t.Context(), bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		}
		if memSDK.objectCallCount != 1 {
			t.Fatalf("expected 1 call to SDK.Object when cache expired, got %d", memSDK.objectCallCount)
		}

		// subsequent GET should use refreshed cache without calling indexer
		memSDK.objectCallCount = 0
		_, err = s3Tester.GetObject(t.Context(), bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		}
		if memSDK.objectCallCount != 0 {
			t.Fatalf("expected 0 calls to SDK.Object when using refreshed cache, got %d", memSDK.objectCallCount)
		}
	})

	t.Run("falls back to stale cache on indexer failure", func(t *testing.T) {
		// expire the cache again
		accessKeyID := testutil.AccessKeyID
		storedObj, err := store.GetObject(&accessKeyID, bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		}
		storedObj.CachedAt = time.Now().Add(-25 * time.Hour)
		if err := store.PutObject(accessKeyID, bucket, object, storedObj, true); err != nil {
			t.Fatal(err)
		}

		// make SDK.Object return an error to simulate indexer failure
		memSDK.fail = true
		memSDK.objectCallCount = 0

		// GET with expired cache and indexer failure should fall back to stale cache
		obj, err := s3Tester.GetObject(t.Context(), bucket, object, nil)
		if err != nil {
			t.Fatal("expected download to succeed with stale cache, got error:", err)
		}
		if memSDK.objectCallCount != 1 {
			t.Fatalf("expected 1 failed call to SDK.Object, got %d", memSDK.objectCallCount)
		}

		// verify body can still be read from stale cache
		body, err := io.ReadAll(obj.Body)
		if err != nil {
			t.Fatal("failed to read body with stale cache:", err)
		}
		if !bytes.Equal(body, data) {
			t.Fatal("body mismatch when using stale cache")
		}

		memSDK.fail = false
	})

	t.Run("empty objects skip cache", func(t *testing.T) {
		memSDK.objectCallCount = 0

		// upload an empty object
		const emptyObject = "empty"
		_, err = s3Tester.PutObject(t.Context(), bucket, emptyObject, bytes.NewReader(nil), nil)
		if err != nil {
			t.Fatal(err)
		}

		// verify empty object has no cached metadata
		accessKeyID := testutil.AccessKeyID
		obj, err := store.GetObject(&accessKeyID, bucket, emptyObject, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !obj.CachedAt.IsZero() {
			t.Fatal("expected zero CachedAt for empty object")
		}

		// GET empty object should succeed without calling SDK.Object
		memSDK.objectCallCount = 0
		resp, err := s3Tester.GetObject(t.Context(), bucket, emptyObject, nil)
		if err != nil {
			t.Fatal(err)
		}
		if memSDK.objectCallCount != 0 {
			t.Fatalf("expected 0 calls to SDK.Object for empty object GET, got %d", memSDK.objectCallCount)
		}

		// verify empty body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if len(body) != 0 {
			t.Fatalf("expected empty body, got %d bytes", len(body))
		}
	})
}

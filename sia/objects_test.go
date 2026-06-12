package sia_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	stypes "go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestGetAndHeadObject(t *testing.T) {
	now := time.Now().UTC().Add(-time.Second)
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
		s3Tester := testutil.NewTester(t)
		test(t, s3Tester)
	})

	t.Run("https", func(t *testing.T) {
		s3Tester := testutil.NewTester(t, testutil.WithTLS())
		test(t, s3Tester)
	})

	t.Run("upload", func(t *testing.T) {
		log := zaptest.NewLogger(t)
		dir := t.TempDir()
		store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })

		if err := store.CreateUser(testutil.Owner); err != nil {
			t.Fatal(err)
		} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
			t.Fatal(err)
		}

		memSDK := testutil.NewMemorySDK()
		memSDK.SetSlabSize(24)
		backend, err := sia.New(t.Context(), memSDK, store, dir,
			sia.WithLogger(log))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { backend.Close() })
		s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

		const bucket = "upload-bucket"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// upload a small object that qualifies for the upload loop, sized to nearly fill a slab
		data := []byte("hello upload via put!!!")
		_, err = s3Tester.PutObject(t.Context(), bucket, "pending", bytes.NewReader(data), nil)
		if err != nil {
			t.Fatal(err)
		}

		// verify the object is on disk
		obj, err := store.GetObject(testutil.AccessKeyID, bucket, "pending", nil)
		if err != nil {
			t.Fatal(err)
		}
		if obj.FileName == nil {
			t.Fatal("expected filename to be set for pending object")
		}
		uploadPath := filepath.Join(dir, sia.UploadsDirectory, *obj.FileName)
		if _, err := os.Stat(uploadPath); err != nil {
			t.Fatal("expected upload file to exist on disk:", err)
		}

		// verify GetObject serves correct data from disk
		getObj, err := s3Tester.GetObject(t.Context(), bucket, "pending", nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(getObj.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, data) {
			t.Fatal("data mismatch before upload")
		}

		// run the upload loop
		backend.UploadObjects(t.Context())

		// verify the object is now on Sia
		obj, err = store.GetObject(testutil.AccessKeyID, bucket, "pending", nil)
		if err != nil {
			t.Fatal(err)
		}
		if obj.FileName != nil {
			t.Fatal("expected filename to be nil after upload")
		}
		if obj.SiaObject == nil {
			t.Fatal("expected sia object to be set after upload")
		}

		// verify upload file is removed from disk
		if _, err := os.Stat(uploadPath); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("expected upload file to be removed after upload")
		}

		// verify GetObject still serves correct data from Sia
		getObj, err = s3Tester.GetObject(t.Context(), bucket, "pending", nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err = io.ReadAll(getObj.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, data) {
			t.Fatal("data mismatch after upload")
		}

		// mock insufficient remaining storage
		blocked := frand.Bytes(24)
		memSDK.SetRemainingStorage(uint64(len(blocked) - 1))

		// add the pending object
		if _, err := s3Tester.PutObject(t.Context(), bucket, "blocked", bytes.NewReader(blocked), nil); err != nil {
			t.Fatal(err)
		}
		blockedObj, err := store.GetObject(testutil.AccessKeyID, bucket, "blocked", nil)
		if err != nil {
			t.Fatal(err)
		} else if blockedObj.FileName == nil {
			t.Fatal("expected filename to be set for blocked object")
		}
		blockedPath := filepath.Join(dir, sia.UploadsDirectory, *blockedObj.FileName)

		// upload objects
		backend.UploadObjects(t.Context())

		// assert blocked object was not uploaded
		blockedObj, err = store.GetObject(testutil.AccessKeyID, bucket, "blocked", nil)
		if err != nil {
			t.Fatal(err)
		} else if blockedObj.FileName == nil {
			t.Fatal("expected blocked object to remain on disk")
		} else if blockedObj.SiaObject != nil {
			t.Fatal("expected blocked object to not have a sia object")
		} else if _, err := os.Stat(blockedPath); err != nil {
			t.Fatal("expected blocked upload file to still exist:", err)
		}

		// free up storage and try again
		memSDK.SetRemainingStorage(uint64(len(blocked)))
		backend.UploadObjects(t.Context())

		// assert object was uploaded
		blockedObj, err = store.GetObject(testutil.AccessKeyID, bucket, "blocked", nil)
		if err != nil {
			t.Fatal(err)
		} else if blockedObj.FileName != nil {
			t.Fatal("expected filename to be nil after upload")
		} else if blockedObj.SiaObject == nil {
			t.Fatal("expected sia object to be set after upload")
		} else if _, err := os.Stat(blockedPath); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("expected blocked upload file to be removed after upload")
		}

		// mock account call failure
		memSDK.SetAccountErr(errors.New("account failure"))
		backend.UploadObjects(t.Context())
		memSDK.SetAccountErr(nil)

		blockedObj, err = store.GetObject(testutil.AccessKeyID, bucket, "blocked", nil)
		if err != nil {
			t.Fatal(err)
		} else if blockedObj.FileName != nil {
			t.Fatal("expected filename to be nil after upload despite account error")
		} else if blockedObj.SiaObject == nil {
			t.Fatal("expected sia object to be set after upload despite account error")
		} else if _, err := os.Stat(blockedPath); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("expected blocked upload file to be removed after upload")
		}
	})
}

func TestCopyObject(t *testing.T) {
	s3Tester := testutil.NewTester(t)
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
	s3Tester := testutil.NewTester(t)

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
	s3Tester := testutil.NewTester(t)

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
	objs, err := s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	if err != nil {
		t.Fatal(err)
	} else if *objs.KeyCount != 2 {
		t.Fatalf("expected 2 remaining objects, got %d", *objs.KeyCount)
	} else if *objs.Contents[0].Key != "3" || *objs.Contents[1].Key != "5" {
		t.Fatalf("remaining objects mismatch: %+v", objs.Contents)
	}

	// delete the remaining ones using 'quiet' mode
	resp, err = s3Tester.DeleteObjects(t.Context(), bucket, testutil.ObjectIdentifiers("3", "5"), aws.Bool(true))
	if err != nil {
		t.Fatal(err)
	} else if len(resp.Deleted) != 0 {
		t.Fatalf("expected 0 deleted objects in quiet mode, got %d", len(resp.Deleted))
	} else if len(resp.Errors) != 0 {
		t.Fatalf("expected 0 errors in quiet mode, got %d", len(resp.Errors))
	}

	// verify all objects are gone
	objs, err = s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	if err != nil {
		t.Fatal(err)
	} else if len(objs.Contents) != 0 {
		t.Fatalf("expected 0 remaining objects, got %d", len(objs.Contents))
	}
}

func TestSyncMetadata(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	}

	siaBackend, err := sia.New(t.Context(), memSDK, store, dir, sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	s3Tester := testutil.NewTester(t, testutil.WithBackend(siaBackend))

	const bucket = "bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// upload an object and simulate the background upload to Sia
	data := frand.Bytes(64)
	_, err = s3Tester.PutObject(t.Context(), bucket, "obj", bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	siaObj, err := memSDK.Upload(t.Context(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	obj, err := store.GetObject(testutil.AccessKeyID, bucket, "obj", nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName == nil {
		t.Fatal("expected pending upload to have a filename")
	}
	sealed := memSDK.SealObject(siaObj)
	if err := store.MarkObjectUploaded(bucket, "obj", obj.ContentMD5, sealed); err != nil {
		t.Fatal(err)
	}

	// record the original sealed object
	origObj, err := store.GetObject(testutil.AccessKeyID, bucket, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	origSealed := *origObj.SiaObject

	// inject a deleted event followed by a matching update event
	eventTime := time.Now().Truncate(time.Second)
	deletedKey := stypes.Hash256{1, 2, 3}
	memSDK.SetEvents([]sdk.ObjectEvent{
		{
			Key:       deletedKey,
			Deleted:   true,
			UpdatedAt: eventTime,
		},
		{
			Key:       sealed.ID(),
			UpdatedAt: eventTime.Add(time.Second),
			Object:    &siaObj,
		},
	})
	siaBackend.SyncMetadata(t.Context())

	// cursor should advance to the last event
	cursor, err := store.ObjectsCursor()
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.After.Equal(eventTime.Add(time.Second)) {
		t.Fatalf("expected cursor at %v, got %v", eventTime.Add(time.Second), cursor.After)
	}
	if cursor.Key != sealed.ID() {
		t.Fatalf("expected cursor key %v, got %v", sealed.ID(), cursor.Key)
	}

	// the object's sia_object should have been re-sealed by the sync
	objAfter, err := store.GetObject(testutil.AccessKeyID, bucket, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(*objAfter.SiaObject, origSealed) {
		t.Fatal("sia_object should have been updated by sync")
	}
	if objAfter.SiaObject.ID != sealed.ID() {
		t.Fatal("sia_object ID should not change after sync")
	}

	// a second sync with no new events should be a no-op
	siaBackend.SyncMetadata(t.Context())
	cursor2, err := store.ObjectsCursor()
	if err != nil {
		t.Fatal(err)
	}
	if cursor2 != cursor {
		t.Fatal("cursor should not change on no-op sync")
	}
}

func TestCopyAndDeleteObject(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	s3Tester := testutil.NewCustomTester(t, dir, store, testutil.NewMemorySDK(), log)

	const bucket = "bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// upload an object
	data := frand.Bytes(256)
	_, err = s3Tester.PutObject(t.Context(), bucket, "src", bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}

	// find the file on disk
	uploadsDir := filepath.Join(dir, sia.UploadsDirectory)
	entries, err := os.ReadDir(uploadsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 upload file, got %d", len(entries))
	}
	uploadFile := entries[0].Name()

	// copy src -> dst
	_, err = s3Tester.CopyObject(t.Context(), bucket, "src", bucket, "dst", types.MetadataDirectiveCopy, nil)
	if err != nil {
		t.Fatal(err)
	}

	// delete the source
	if err := s3Tester.DeleteObject(t.Context(), bucket, "src"); err != nil {
		t.Fatal(err)
	}

	// file should still exist on disk (dst still references it)
	if _, err := os.Stat(filepath.Join(uploadsDir, uploadFile)); err != nil {
		t.Fatalf("expected upload file to still exist after deleting src, got: %v", err)
	}

	// download the destination and verify contents
	obj, err := s3Tester.GetObject(t.Context(), bucket, "dst", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded data does not match original")
	}

	// delete the destination
	if err := s3Tester.DeleteObject(t.Context(), bucket, "dst"); err != nil {
		t.Fatal(err)
	}

	// file should now be gone from disk
	if _, err := os.Stat(filepath.Join(uploadsDir, uploadFile)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected upload file to be deleted, got: %v", err)
	}
}

// TestOverwritePendingObjectCleansUpFile asserts that overwriting a pending
// object removes its previous file from the uploads directory, across the
// PutObject, CopyObject, and CompleteMultipartUpload paths. It also asserts
// that a file shared with another object (via CopyObject) is not removed.
func TestOverwritePendingObjectCleansUpFile(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	s3Tester := testutil.NewCustomTester(t, dir, store, testutil.NewMemorySDK(), log)

	const bucket = "bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	uploadsDir := filepath.Join(dir, sia.UploadsDirectory)
	pendingFilename := func(t *testing.T, object string) string {
		t.Helper()
		obj, err := store.GetObject(testutil.AccessKeyID, bucket, object, nil)
		if err != nil {
			t.Fatal(err)
		} else if obj.FileName == nil {
			t.Fatalf("expected %q to have a pending filename", object)
		}
		return *obj.FileName
	}
	assertRemoved := func(t *testing.T, filename string) {
		t.Helper()
		if _, err := os.Stat(filepath.Join(uploadsDir, filename)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("expected pending file %q to be removed, got: %v", filename, err)
		}
	}
	assertExists := func(t *testing.T, filename string) {
		t.Helper()
		if _, err := os.Stat(filepath.Join(uploadsDir, filename)); err != nil {
			t.Fatalf("expected pending file %q to exist, got: %v", filename, err)
		}
	}

	// PutObject over a pending object removes the previous file
	if _, err := s3Tester.PutObject(t.Context(), bucket, "put", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}
	putFile := pendingFilename(t, "put")
	if _, err := s3Tester.PutObject(t.Context(), bucket, "put", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}
	assertRemoved(t, putFile)

	// CopyObject onto a pending object removes the destination's previous
	// file while preserving the source's file
	if _, err := s3Tester.PutObject(t.Context(), bucket, "copy-src", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s3Tester.PutObject(t.Context(), bucket, "copy-dst", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}
	srcFile := pendingFilename(t, "copy-src")
	dstFile := pendingFilename(t, "copy-dst")
	if _, err := s3Tester.CopyObject(t.Context(), bucket, "copy-src", bucket, "copy-dst", types.MetadataDirectiveCopy, nil); err != nil {
		t.Fatal(err)
	}
	assertRemoved(t, dstFile)
	assertExists(t, srcFile)

	// overwriting the source of a copy must not delete the shared file
	// while the destination still references it
	if _, err := s3Tester.PutObject(t.Context(), bucket, "copy-src", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}
	assertExists(t, srcFile)

	// CompleteMultipartUpload over a pending object removes the previous file
	if _, err := s3Tester.PutObject(t.Context(), bucket, "mp", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}
	mpFile := pendingFilename(t, "mp")
	mu, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, "mp", nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadID := *mu.UploadId
	up, err := s3Tester.UploadPart(t.Context(), bucket, "mp", uploadID, 1, bytes.Repeat([]byte("a"), int(s3.MinUploadPartSize)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3Tester.CompleteMultipartUpload(t.Context(), bucket, "mp", uploadID, []types.CompletedPart{
		{PartNumber: aws.Int32(1), ETag: up.ETag},
	}); err != nil {
		t.Fatal(err)
	}
	assertRemoved(t, mpFile)

	// PutObject over a completed multipart object removes the upload
	// directory and clears object_parts so no stale rows are left pointing
	// at deleted files
	mpDir := pendingFilename(t, "mp")
	if _, err := s3Tester.PutObject(t.Context(), bucket, "mp", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
		t.Fatal(err)
	}
	assertRemoved(t, mpDir)
	parts, err := store.ObjectPartsByName(bucket, "mp")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 0 {
		t.Fatalf("expected object_parts to be empty after overwrite, got %d", len(parts))
	}
}

func TestDiskUsageLimit(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	}

	const limit = 500
	memSDK := testutil.NewMemorySDK()
	memSDK.SetSlabSize(100)
	backend, err := sia.New(t.Context(), memSDK, store, dir,
		sia.WithUploadDisabled(),
		sia.WithDiskUsageLimit(limit),
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const bucket = "bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// uploads under the limit succeed even if they push usage over it
	_, err = s3Tester.PutObject(t.Context(), bucket, "a", bytes.NewReader(frand.Bytes(400)), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s3Tester.PutObject(t.Context(), bucket, "b", bytes.NewReader(frand.Bytes(200)), nil)
	if err != nil {
		t.Fatal(err)
	}

	// CopyObject does not create new disk data so it must not block
	copyCtx, copyCancel := context.WithTimeout(t.Context(), time.Second)
	defer copyCancel()
	if _, err := s3Tester.CopyObject(copyCtx, bucket, "a", bucket, "a-copy", types.MetadataDirectiveCopy, nil); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := s3Tester.PutObject(t.Context(), bucket, "c", bytes.NewReader(frand.Bytes(10)), nil)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("expected upload to block while usage exceeds the limit, got %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	backend.UploadObjects(t.Context())

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upload to resume")
	}
}

func TestDiskUsageLimitOngoingMultipartUpload(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	}

	const limit = 20
	backend, err := sia.New(t.Context(), testutil.NewMemorySDK(), store, dir,
		sia.WithUploadDisabled(),
		sia.WithDiskUsageLimit(limit),
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const (
		bucket = "bucket"
		object = "multipart"
	)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// put a source object for the UploadPartCopy below
	srcData := frand.Bytes(limit / 2)
	if _, err := s3Tester.PutObject(t.Context(), bucket, "src", bytes.NewReader(srcData), nil); err != nil {
		t.Fatal(err)
	}

	resp, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}

	// first part pushes usage to the limit
	if _, err := s3Tester.UploadPart(t.Context(), bucket, object, *resp.UploadId, 1, frand.Bytes(limit/2)); err != nil {
		t.Fatal(err)
	}

	// subsequent parts of the same upload are allowed to exceed the limit
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := s3Tester.UploadPart(ctx, bucket, object, *resp.UploadId, 2, frand.Bytes(limit/2)); err != nil {
		t.Fatal(err)
	}

	// UploadPartCopy should also be allowed to exceed the limit for
	// an ongoing multipart upload
	if _, err := s3Tester.UploadPartCopy(ctx, bucket, "src", bucket, object, *resp.UploadId, 3, &s3.ObjectRange{Start: 0, Length: int64(len(srcData))}); err != nil {
		t.Fatal(err)
	}
}

func TestDiskUsageLimitOverwriteCleanup(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	}

	const limit = 200
	backend, err := sia.New(t.Context(), testutil.NewMemorySDK(), store, dir,
		sia.WithUploadDisabled(),
		sia.WithDiskUsageLimit(limit),
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))

	const bucket = "bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// without overwrite cleanup, the third put would block at the limit
	for range 3 {
		if _, err := s3Tester.PutObject(t.Context(), bucket, "obj", bytes.NewReader(frand.Bytes(100)), nil); err != nil {
			t.Fatal(err)
		}
	}

	// only the current pending object should remain on disk
	entries, err := os.ReadDir(filepath.Join(dir, sia.UploadsDirectory))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected 1 file in uploads directory, got %d: %v", len(entries), names)
	}

	// re-open the database to assert the persisted disk usage
	store2, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store2.Close() })
	usage, err := store2.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage != 100 {
		t.Fatalf("expected persisted disk usage 100, got %d", usage)
	}
}

func TestDeleteObjectUnpin(t *testing.T) {
	memSDK := testutil.NewMemorySDK()
	memSDK.SetSlabSize(32)
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateUser(testutil.Owner); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey(testutil.Owner, testutil.AccessKeyID, testutil.SecretAccessKey); err != nil {
		t.Fatal(err)
	}

	siaBackend, err := sia.New(t.Context(), memSDK, store, dir, sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { siaBackend.Close() })

	s3Tester := testutil.NewTester(t, testutil.WithBackend(siaBackend))

	const bucket = "bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// upload object A
	data := frand.Bytes(64)
	_, err = s3Tester.PutObject(t.Context(), bucket, "A", bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	siaBackend.UploadObjects(t.Context())

	// verify SDK has the object pinned
	if memSDK.ObjectCount() != 1 {
		t.Fatalf("expected 1 pinned object, got %d", memSDK.ObjectCount())
	}

	// copy A to B (shares same sia_object_id)
	_, err = s3Tester.CopyObject(t.Context(), bucket, "A", bucket, "B", types.MetadataDirectiveCopy, nil)
	if err != nil {
		t.Fatal(err)
	}

	// delete A - should NOT unpin since B still references the same sia_object_id
	if err := s3Tester.DeleteObject(t.Context(), bucket, "A"); err != nil {
		t.Fatal(err)
	}
	siaBackend.ProcessOrphans(t.Context())
	if memSDK.ObjectCount() != 1 {
		t.Fatalf("expected 1 pinned object after deleting A (B still references it), got %d", memSDK.ObjectCount())
	}

	// delete B - should unpin since no references remain
	if err := s3Tester.DeleteObject(t.Context(), bucket, "B"); err != nil {
		t.Fatal(err)
	}
	siaBackend.ProcessOrphans(t.Context())
	if memSDK.ObjectCount() != 0 {
		t.Fatalf("expected 0 pinned objects after deleting B, got %d", memSDK.ObjectCount())
	}

	// test empty object deletion does not attempt to unpin
	_, err = s3Tester.PutObject(t.Context(), bucket, "empty", bytes.NewReader(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	siaBackend.ProcessOrphans(t.Context())
	if memSDK.ObjectCount() != 0 {
		t.Fatalf("expected 0 pinned objects for empty object, got %d", memSDK.ObjectCount())
	}
	if err := s3Tester.DeleteObject(t.Context(), bucket, "empty"); err != nil {
		t.Fatal(err)
	}
	siaBackend.ProcessOrphans(t.Context())
	if memSDK.ObjectCount() != 0 {
		t.Fatalf("expected 0 pinned objects after deleting empty object, got %d", memSDK.ObjectCount())
	}

	// test PutObject overwrite unpins old object
	data2 := frand.Bytes(64)
	_, err = s3Tester.PutObject(t.Context(), bucket, "C", bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	siaBackend.UploadObjects(t.Context())
	if memSDK.ObjectCount() != 1 {
		t.Fatalf("expected 1 pinned object, got %d", memSDK.ObjectCount())
	}
	// overwrite with different data (different sia_object_id)
	_, err = s3Tester.PutObject(t.Context(), bucket, "C", bytes.NewReader(data2), nil)
	if err != nil {
		t.Fatal(err)
	}
	siaBackend.UploadObjects(t.Context())
	siaBackend.ProcessOrphans(t.Context())
	// old object should be unpinned, new one pinned
	if memSDK.ObjectCount() != 1 {
		t.Fatalf("expected 1 pinned object after overwrite, got %d", memSDK.ObjectCount())
	}
}

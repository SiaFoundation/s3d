package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

		// upload to a bucket that we don't own
		otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
		_, err = otherTester.PutObject(t.Context(), bucket, object, bytes.NewReader(data), metadata)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	}

	t.Run("http", func(t *testing.T) {
		backend := testutil.NewMemoryBackend(
			testutil.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
			testutil.WithKeyPair("foo", "bar"),
		)
		s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))
		test(t, s3Tester)
	})

	t.Run("https", func(t *testing.T) {
		backend := testutil.NewMemoryBackend(
			testutil.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
			testutil.WithKeyPair("foo", "bar"),
		)
		s3Tester := testutil.NewTester(t, testutil.WithTLS(), testutil.WithBackend(backend))
		test(t, s3Tester)
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
	_, err = s3Tester.CopyObject(t.Context(), srcBucket, srcObject, dstBucket, dstObject, nil)
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
	etag, err := s3Tester.CopyObject(t.Context(), srcBucket, srcObject, dstBucket, dstObject, dstMeta)
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
	_, err = s3Tester.CopyObject(t.Context(), srcBucket, "nonexistent", dstBucket, dstBucket, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)

	// copy an object to the same bucket and key, adding additional metadata
	additionalMeta := map[string]string{
		"new-key": "new-value",
	}
	etag, err = s3Tester.CopyObject(t.Context(), dstBucket, dstObject, dstBucket, dstObject, additionalMeta)
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
	} else if len(obj.Metadata) != 3 || obj.Metadata["foo"] != "bar" || obj.Metadata["baz"] != "qux" || obj.Metadata["new-key"] != "new-value" {
		t.Fatalf("metadata mismatch: %+v", obj.Metadata)
	} else if obj.ContentMD5 != hash {
		t.Fatal("hash mismatch", obj.ContentMD5, hash[:])
	} else if obj.LastModified.Before(copyTime) {
		t.Fatalf("last modified mismatch: expected after %v, got %v", copyTime, obj.LastModified)
	}
}

func TestRangeRequest(t *testing.T) {
	for idx, tc := range []struct {
		inst, inend  int64
		rev          bool
		sz           int64
		outst, outln int64
		fail         bool
	}{
		{inst: 0, inend: -1, sz: 5, outst: 0, outln: 5},
		{inst: 0, inend: 5, sz: 10, outst: 0, outln: 6},
		{inst: 0, inend: 0, sz: 4, outst: 0, outln: 1},
		{inst: 1, inend: 5, sz: 10, outst: 1, outln: 5},
		{inst: 1, inend: 5, sz: 3, outst: 1, outln: 2},
		{inst: 5, inend: 7, sz: 6, outst: 5, outln: 1},

		{rev: true, inend: 10, sz: 10, outst: 0, outln: 10},
		{rev: true, inend: 5, sz: 10, outst: 5, outln: 5},

		{fail: true, inst: 0, inend: 0, sz: 0},
		{fail: true, inst: 1, inend: 1, sz: 1},
		{fail: true, inst: 10, inend: 15, sz: 10},
		{fail: true, inst: 40, inend: 50, sz: 11},
		{fail: true, rev: true, inend: 20, sz: 10},
		{fail: true, rev: true, inend: 11, sz: 10},
		{fail: true, rev: true, inend: 0, sz: 10}, // zero suffix-length is not satisfiable
	} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			orr := s3.ObjectRangeRequest{Start: tc.inst, End: tc.inend, FromEnd: tc.rev}

			rng, err := orr.Range(tc.sz)
			if tc.fail != (err != nil) {
				t.Fatal("failure expected:", tc.fail, "found:", err)
			}
			if !tc.fail {
				if rng.Start != tc.outst {
					t.Fatal("unexpected start:", rng.Start, "expected:", tc.outst)
				}
				if rng.Length != tc.outln {
					t.Fatal("unexpected length:", rng.Length, "expected:", tc.outln)
				}
			}
		})
	}
}

func TestListObjects(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	// prepare a bucket
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
	const etag = "d41d8cd98f00b204e9800998ecf8427e"
	keys := []string{"foo", "foo/baz", "foo/bar"}
	for _, key := range keys {
		_, err := s3Tester.PutObject(t.Context(), bucket, key, bytes.NewReader([]byte{}), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	ptr := func(s string) *string {
		return &s
	}

	toBase64 := func(s *string) *string {
		if s == nil {
			return nil
		}
		return ptr(base64.URLEncoding.EncodeToString([]byte(*s)))
	}

	assertCommonPrefixesEqual := func(t *testing.T, expected []string, actual []types.CommonPrefix) {
		t.Helper()
		if len(expected) != len(actual) {
			t.Fatalf("expected %d common prefixes, got %d", len(expected), len(actual))
		}
		for i := range expected {
			if expected[i] != *actual[i].Prefix {
				t.Fatalf("expected common prefix %v, got %v", expected[i], actual[i])
			}
		}
	}

	assertMarkersEqual := func(t *testing.T, isBase64 bool, expected, actual *string) {
		t.Helper()
		if expected == nil && actual == nil {
			return
		} else if (expected == nil) != (actual == nil) {
			t.Fatalf("expected marker %v, got %v", expected, actual)
		}
		if isBase64 {
			var decoded []byte
			var err error
			decoded, err = base64.URLEncoding.DecodeString(*actual)
			if err != nil {
				t.Fatalf("failed to decode expected marker %v: %v", *expected, err)
			}
			*actual = string(decoded)
		}
		if *expected != *actual {
			t.Fatalf("expected marker %v, got %v", *expected, *actual)
		}
	}

	tests := []struct {
		name       string
		prefix     *string
		delimiter  *string
		marker     *string
		nextMarker *string
		maxKeys    int64

		truncated      bool
		objects        []string
		commonPrefixes []string
	}{
		{
			name:    "All",
			objects: []string{"foo", "foo/bar", "foo/baz"},
		},
		{
			name:       "MaxKeys",
			maxKeys:    2,
			objects:    []string{"foo", "foo/bar"},
			truncated:  true,
			nextMarker: ptr("foo/bar"),
		},
		{
			name:    "Marker",
			marker:  aws.String("foo/bar"),
			objects: []string{"foo/baz"},
		},
		{
			name:    "Prefix",
			prefix:  ptr("foo/b"),
			objects: []string{"foo/bar", "foo/baz"},
		},
		{
			name:           "Delimiter",
			delimiter:      ptr("/"),
			objects:        []string{"foo"},
			commonPrefixes: []string{"foo/"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("ListObjectsV2", func(t *testing.T) {
				resp, err := s3Tester.ListObjectsV2(t.Context(), bucket, tc.prefix, tc.delimiter, s3.ListObjectsPage{
					Marker:  toBase64(tc.marker),
					MaxKeys: tc.maxKeys,
				})
				if err != nil {
					t.Fatal(err)
				} else if len(resp.Contents) != len(tc.objects) {
					t.Fatalf("expected %d objects, got %d", len(tc.objects), len(resp.Contents))
				} else if *resp.IsTruncated != tc.truncated {
					t.Fatalf("expected truncated=%v, got %v", tc.truncated, *resp.IsTruncated)
				}
				for i := range tc.objects {
					if *resp.Contents[i].Key != tc.objects[i] {
						t.Fatalf("expected object %v, got %v", tc.objects[i], *resp.Contents[i].Key)
					} else if *resp.Contents[i].ETag != etag {
						t.Fatalf("expected ETag %q, got %q", etag, *resp.Contents[i].ETag)
					}
				}
				assertCommonPrefixesEqual(t, tc.commonPrefixes, resp.CommonPrefixes)
				assertMarkersEqual(t, true, tc.nextMarker, resp.NextContinuationToken)
			})

			t.Run("ListObjectVersions", func(t *testing.T) {
				resp, err := s3Tester.ListObjectVersions(t.Context(), bucket, tc.prefix, tc.delimiter, s3.ListObjectsPage{
					Marker:  tc.marker,
					MaxKeys: tc.maxKeys,
				})
				if err != nil {
					t.Fatal(err)
				} else if len(resp.Versions) != len(tc.objects) {
					t.Fatalf("expected %d objects, got %d", len(tc.objects), len(resp.Versions))
				} else if *resp.IsTruncated != tc.truncated {
					t.Fatalf("expected truncated=%v, got %v", tc.truncated, *resp.IsTruncated)
				}
				for i := range tc.objects {
					if *resp.Versions[i].Key != tc.objects[i] {
						t.Fatalf("expected object %v, got %v", tc.objects[i], *resp.Versions[i].Key)
					}
				}
				assertCommonPrefixesEqual(t, tc.commonPrefixes, resp.CommonPrefixes)
				assertMarkersEqual(t, false, tc.nextMarker, resp.NextKeyMarker)
			})
		})
	}
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
	_, err := s3Tester.DeleteObjects(t.Context(), "nonexistent", keys, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	assertDeleted := func(t *testing.T, key string, deleted types.DeletedObject) {
		t.Helper()
		if *deleted.Key != key {
			t.Fatalf("expected deleted key %v, got %v", key, *deleted.Key)
		}
	}

	// delete a few objects, including one that doesn't exist
	delKeys := []string{"2", "4", "nonexistent"}
	resp, err := s3Tester.DeleteObjects(t.Context(), bucket, delKeys, nil)
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
	}
	if *objs.KeyCount != 3 {
		t.Fatalf("expected 3 remaining objects, got %d", objs.KeyCount)
	} else if *objs.Contents[0].Key != "1" || *objs.Contents[1].Key != "3" || *objs.Contents[2].Key != "5" {
		t.Fatalf("remaining objects mismatch: %+v", objs.Contents)
	}

	// delete the remaining ones using 'quiet' mode
	remainingKeys := []string{"1", "3", "5"}
	resp, err = s3Tester.DeleteObjects(t.Context(), bucket, remainingKeys, aws.Bool(true))
	if err != nil {
		t.Fatal(err)
	} else if len(resp.Deleted) != 0 {
		t.Fatalf("expected 0 deleted objects in quiet mode, got %d", len(resp.Deleted))
	} else if len(resp.Errors) != 0 {
		t.Fatalf("expected 0 errors in quiet mode, got %d", len(resp.Errors))
	}

	// verify deleted objects are gone and others remain
	objs, err = s3Tester.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	if err != nil {
		t.Fatal(err)
	} else if objs.KeyCount != nil {
		t.Fatalf("expected 0 remaining objects, got %d", *objs.KeyCount)
	}
}

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

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
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
		if !bytes.Equal(obj.Hash, hash[:]) {
			t.Fatal("hash mismatch", obj.Hash, hash[:])
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
	s3Tester := testutil.NewTester(t)

	// prepare a bucket
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// prepare the object to upload
	data := frand.Bytes(100)
	hash := md5.Sum(data)
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
	} else if !bytes.Equal(obj.Hash, hash[:]) {
		t.Fatal("hash mismatch", obj.Hash, hash[:])
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
	otherTester := s3Tester.AddAccessKey(t, "foo", "bar")
	_, err = otherTester.PutObject(t.Context(), bucket, object, bytes.NewReader(data), metadata)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
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
				assertMarkersEqual(t, false, tc.nextMarker, resp.NextKeyMarker)
			})
		})
	}
}

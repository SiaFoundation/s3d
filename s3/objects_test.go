package s3_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
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

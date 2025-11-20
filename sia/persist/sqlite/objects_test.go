package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap/zaptest"
)

func TestListObjects(t *testing.T) {
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "hostd.sqlite3")

	store, err := OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket("", bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
	keys := []string{"foo", "foo/baz", "foo/bar"}
	obj := sdk.Object{}
	sealed := obj.Seal(types.GeneratePrivateKey())
	id := obj.ID()

	etag := s3.FormatETag(id[:])
	for _, key := range keys {
		err := store.PutObject("", bucket, key, sealed)
		if err != nil {
			t.Fatal(err)
		}
	}

	ptr := func(s string) *string {
		return &s
	}
	val := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}

	assertCommonPrefixesEqual := func(t *testing.T, expected []string, actual []s3.CommonPrefix) {
		t.Helper()
		if len(expected) != len(actual) {
			t.Fatalf("expected %d common prefixes, got %d", len(expected), len(actual))
		}
		for i := range expected {
			if expected[i] != actual[i].Prefix {
				t.Fatalf("expected common prefix %v, got %v", expected[i], actual[i])
			}
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
			maxKeys: 100,
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
			marker:  ptr("foo/bar"),
			objects: []string{"foo/baz"},
			maxKeys: 100,
		},
		{
			name:    "Prefix",
			prefix:  ptr("foo/b"),
			objects: []string{"foo/bar", "foo/baz"},
			maxKeys: 100,
		},
		{
			name:           "Delimiter",
			delimiter:      ptr("/"),
			objects:        []string{"foo"},
			commonPrefixes: []string{"foo/"},
			maxKeys:        100,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:       val(tc.prefix),
				HasPrefix:    tc.prefix != nil,
				Delimiter:    val(tc.delimiter),
				HasDelimiter: tc.delimiter != nil,
			}, s3.ListObjectsPage{
				Marker:  tc.marker,
				MaxKeys: tc.maxKeys,
			})
			if err != nil {
				t.Fatal(err)
			} else if len(resp.Contents) != len(tc.objects) {
				t.Fatalf("expected %d objects, got %d", len(tc.objects), len(resp.Contents))
			} else if resp.IsTruncated != tc.truncated {
				t.Fatalf("expected truncated=%v, got %v", tc.truncated, resp.IsTruncated)
			}
			for i := range tc.objects {
				if resp.Contents[i].Key != tc.objects[i] {
					t.Fatalf("expected object %v, got %v", tc.objects[i], resp.Contents[i].Key)
				} else if resp.Contents[i].ETag != etag {
					t.Fatalf("expected ETag %q, got %q", etag, resp.Contents[i].ETag)
				}
			}
			assertCommonPrefixesEqual(t, tc.commonPrefixes, resp.CommonPrefixes)
			if expectedMarker := val(tc.nextMarker); expectedMarker != resp.NextMarker {
				t.Fatalf("expected marker %v, got %v", expectedMarker, resp.NextMarker)
			}
		})
	}
}

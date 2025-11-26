package sqlite

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestListObjects(t *testing.T) {
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "s3d.sqlite3")

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
	contentMD5 := [16]byte(frand.Bytes(16))

	etag := s3.FormatETag(contentMD5[:])
	for _, key := range keys {
		err := store.PutObject("", bucket, key, contentMD5, sealed)
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

func TestListObjectsMatch(t *testing.T) {
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "s3d.sqlite3")

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
	keys := []string{"foo/baz", "foo/bar", "😊/д"}
	obj := sdk.Object{}
	sealed := obj.Seal(types.GeneratePrivateKey())
	contentMD5 := [16]byte(frand.Bytes(16))

	etag := s3.FormatETag(contentMD5[:])
	for _, key := range keys {
		err := store.PutObject("", bucket, key, contentMD5, sealed)
		if err != nil {
			t.Fatal(err)
		}
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

	ptr := func(s string) *string {
		return &s
	}
	val := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}

	for idx, tc := range []struct {
		prefix         *string
		delim          *string
		objects        []string
		commonPrefixes []string
	}{
		{prefix: ptr("foo"), objects: []string{"foo/bar", "foo/baz"}},
		{prefix: ptr("foo/"), objects: []string{"foo/bar", "foo/baz"}},
		{prefix: ptr("foo/ba"), objects: []string{"foo/bar", "foo/baz"}},
		{prefix: ptr("foo/bar"), objects: []string{"foo/bar"}},
		{prefix: ptr("foo//ba"), objects: []string{"foo/bar", "foo/baz"}},
		{prefix: ptr("foo//bar"), objects: []string{"foo/bar"}},
		{prefix: ptr("😊"), objects: []string{"😊/д"}},

		{prefix: ptr("FOO"), objects: []string{"foo/bar", "foo/baz"}},
		{prefix: ptr("FOO/"), objects: []string{"foo/bar", "foo/baz"}},
		{prefix: ptr("foo/BA"), objects: []string{"foo/bar", "foo/baz"}},
		{prefix: ptr("foo/BAR"), objects: []string{"foo/bar"}},

		{prefix: ptr("foo"), delim: ptr("/"), commonPrefixes: []string{"foo/"}},
		{prefix: ptr("aaa"), delim: ptr("/")},

		{prefix: ptr("FOO"), delim: ptr("/"), commonPrefixes: []string{"foo/"}},
		{prefix: ptr("FOO"), delim: ptr("//"), commonPrefixes: []string{"foo/"}},
		{prefix: ptr("aaa"), delim: ptr("/")},

		{delim: ptr("/"), commonPrefixes: []string{"foo/", "😊/"}},
		{delim: ptr("//"), commonPrefixes: []string{"foo/", "😊/"}},
		{prefix: ptr("foo"), delim: ptr("/bar"), objects: []string{"foo/baz"}, commonPrefixes: []string{"foo/"}},
		{prefix: ptr("foo"), delim: ptr("/BAR"), objects: []string{"foo/baz"}, commonPrefixes: []string{"foo/"}},
	} {
		t.Run(fmt.Sprint(idx), func(t *testing.T) {
			resp, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:       val(tc.prefix),
				HasPrefix:    tc.prefix != nil,
				Delimiter:    val(tc.delim),
				HasDelimiter: tc.delim != nil,
			}, s3.ListObjectsPage{MaxKeys: 100})
			if err != nil {
				t.Fatal(err)
			} else if len(resp.Contents) != len(tc.objects) {
				t.Fatalf("expected %d objects, got %d", len(tc.objects), len(resp.Contents))
			}
			for i := range tc.objects {
				if resp.Contents[i].Key != tc.objects[i] {
					t.Fatalf("expected object %v, got %v", tc.objects[i], resp.Contents[i].Key)
				} else if resp.Contents[i].ETag != etag {
					t.Fatalf("expected ETag %q, got %q", etag, resp.Contents[i].ETag)
				}
			}
			assertCommonPrefixesEqual(t, tc.commonPrefixes, resp.CommonPrefixes)
		})
	}
}

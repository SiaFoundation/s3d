package sqlite

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
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
	obj := sdk.Object{}
	contentMD5 := [16]byte(frand.Bytes(16))
	etag := s3.FormatETag(contentMD5[:])

	var largeDirectoryKeys []string
	for i := 0; i < 200; i++ {
		largeDirectoryKeys = append(largeDirectoryKeys, fmt.Sprintf("large-directory/%d", i))
	}
	slices.Sort(largeDirectoryKeys)

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

	type testCase struct {
		name       string
		prefix     string
		delimiter  string
		marker     string
		nextMarker string
		maxKeys    int64

		truncated      bool
		objects        []string
		commonPrefixes []string
	}

	tests := []struct {
		keys  []string
		cases []testCase
	}{
		{
			keys: []string{"foo", "foo/bar", "foo/baz"},
			cases: []testCase{
				{
					name:    "All",
					objects: []string{"foo", "foo/bar", "foo/baz"},
					maxKeys: 100,
				},
				{
					name:       "MaxKeys",
					objects:    []string{"foo", "foo/bar"},
					truncated:  true,
					nextMarker: "foo/bar",
					maxKeys:    2,
				},
				{
					name:    "Marker",
					marker:  "foo/bar",
					objects: []string{"foo/baz"},
					maxKeys: 100,
				},
				{
					name:    "Prefix",
					prefix:  "foo/b",
					objects: []string{"foo/bar", "foo/baz"},
					maxKeys: 100,
				},
				{
					name:           "Delimiter",
					delimiter:      "/",
					objects:        []string{"foo"},
					commonPrefixes: []string{"foo/"},
					maxKeys:        100,
				},
			},
		},
		{
			keys: []string{"a/file1", "a/sub/file2", "a/sub/file3", "b/file4"},
			cases: []testCase{
				{
					name:       "Page1",
					prefix:     "a/",
					delimiter:  "/",
					maxKeys:    1,
					objects:    []string{"a/file1"},
					truncated:  true,
					nextMarker: "a/file1",
				},
				{
					name:           "Page2",
					prefix:         "a/",
					delimiter:      "/",
					marker:         "a/file1",
					maxKeys:        2,
					commonPrefixes: []string{"a/sub/"},
				}},
		},
		{
			keys: largeDirectoryKeys,
			cases: []testCase{
				{
					name:    "Large all",
					objects: largeDirectoryKeys,
					maxKeys: 500,
				},
				{
					name:       "Large truncated",
					objects:    largeDirectoryKeys[:100],
					truncated:  true,
					nextMarker: largeDirectoryKeys[99],
					maxKeys:    100,
				},
			},
		},
	}

	for _, tt := range tests {
		_, err := store.db.Exec(`DELETE FROM objects`)
		if err != nil {
			t.Fatal(err)
		}

		for _, key := range tt.keys {
			err := store.PutObject("", bucket, key, &objects.Object{
				ID:         obj.ID(),
				ContentMD5: contentMD5,
				Size:       0,
				UpdatedAt:  time.Now(),
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		for _, tc := range tt.cases {
			t.Run(tc.name, func(t *testing.T) {
				resp, err := store.ListObjects(nil, bucket, s3.Prefix{
					Prefix:       tc.prefix,
					HasPrefix:    tc.prefix != "",
					Delimiter:    tc.delimiter,
					HasDelimiter: tc.delimiter != "",
				}, s3.ListObjectsPage{
					Marker: func() *string {
						if tc.marker == "" {
							return nil
						}
						v := tc.marker
						return &v
					}(),
					MaxKeys: tc.maxKeys,
				})
				if err != nil {
					t.Fatal(err)
				} else if len(resp.Contents) != len(tc.objects) {
					t.Fatalf("expected %d objects, got %d", len(tc.objects), len(resp.Contents))
				} else if resp.IsTruncated != tc.truncated {
					t.Fatalf("expected truncated=%v, got %v", tc.truncated, resp.IsTruncated)
				} else if tc.nextMarker != resp.NextMarker {
					t.Fatalf("expected marker %v, got %v", tc.nextMarker, resp.NextMarker)
				}

				for i := range tc.objects {
					if resp.Contents[i].Key != tc.objects[i] {
						t.Fatalf("expected object %v, got %v", tc.objects[i], resp.Contents[i].Key)
					}
					if resp.Contents[i].ETag != etag {
						t.Fatalf("expected ETag %q, got %q", etag, resp.Contents[i].ETag)
					}
				}
				assertCommonPrefixesEqual(t, tc.commonPrefixes, resp.CommonPrefixes)
			})
		}
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
	keys := []string{"a//b", "foo/baz", "foo/bar", "😊/д"}
	obj := sdk.Object{}
	contentMD5 := [16]byte(frand.Bytes(16))
	etag := s3.FormatETag(contentMD5[:])

	for _, key := range keys {
		err := store.PutObject("", bucket, key, &objects.Object{
			ID:         obj.ID(),
			ContentMD5: contentMD5,
			Size:       0,
			UpdatedAt:  time.Now(),
		})
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

	for idx, tc := range []struct {
		prefix         string
		delim          string
		objects        []string
		commonPrefixes []string
	}{
		{prefix: "a", objects: []string{"a//b"}},
		{prefix: "a/", objects: []string{"a//b"}},
		{prefix: "a//", objects: []string{"a//b"}},

		{prefix: "foo", objects: []string{"foo/bar", "foo/baz"}},
		{prefix: "foo/", objects: []string{"foo/bar", "foo/baz"}},
		{prefix: "foo/ba", objects: []string{"foo/bar", "foo/baz"}},
		{prefix: "foo/bar", objects: []string{"foo/bar"}},
		{prefix: "foo//ba"},
		{prefix: "😊", objects: []string{"😊/д"}},

		{prefix: "FOO"},
		{prefix: "FOO/"},
		{prefix: "foo/BA"},
		{prefix: "foo/BAR"},

		{prefix: "foo", delim: "/", commonPrefixes: []string{"foo/"}},
		{prefix: "aaa", delim: "/"},

		{prefix: "FOO", delim: "/"},
		{prefix: "FOO", delim: "//"},
		{prefix: "aaa", delim: "/"},

		{delim: "/", commonPrefixes: []string{"a/", "foo/", "😊/"}},
		{prefix: "", delim: "/", commonPrefixes: []string{"a/", "foo/", "😊/"}},
	} {
		t.Run(fmt.Sprint(idx), func(t *testing.T) {
			resp, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:       tc.prefix,
				HasPrefix:    tc.prefix != "",
				Delimiter:    tc.delim,
				HasDelimiter: tc.delim != "",
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

func randomPath(minLength, maxLength, maxDepth int, alphabet []rune, delimiter string) string {
	length := frand.Intn(maxLength-minLength+1) + minLength

	runes := make([]rune, length)
	for i := range runes {
		runes[i] = alphabet[frand.Intn(len(alphabet))]
	}

	if delimiter == "" {
		return string(runes)
	}

	key := string(runes)
	depth := frand.Intn(maxDepth)
	for i := 1; i < length && depth > 0; i++ {
		if frand.Intn(2) == 0 {
			key = key[:i] + delimiter + key[i:]
			i++
			depth--
		}
	}

	return key
}

func TestListObjectsWalk(t *testing.T) {
	const (
		numKeys   = 10000
		maxKeys   = 100
		maxDepth  = 4
		minLength = 4
		maxLength = 10
	)

	var (
		alphabet  = []rune("ÎmNotÂfraid!%_")
		delimiter = "%"
	)

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
	obj := sdk.Object{}
	contentMD5 := [16]byte(frand.Bytes(16))

	keysSeen := make(map[string]struct{})
	keysAll := make(map[string]struct{})
	for range numKeys {
		key := randomPath(minLength, maxLength, maxDepth, alphabet, delimiter)
		keysAll[key] = struct{}{}
		err := store.PutObject("", bucket, key, &objects.Object{
			ID:         obj.ID(),
			ContentMD5: contentMD5,
			Size:       0,
			UpdatedAt:  time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	type page struct {
		prefix    string
		keyMarker string
	}

	seen := make(map[string]struct{})
	stack := []page{{}}
	for len(stack) > 0 {
		// pop from stack
		n := len(stack) - 1
		pg := stack[n]
		stack = stack[:n]

		// fetch page
		res, err := store.ListObjects(nil, bucket, s3.Prefix{
			Prefix:       pg.prefix,
			HasPrefix:    pg.prefix != "",
			Delimiter:    delimiter,
			HasDelimiter: true,
		}, s3.ListObjectsPage{
			MaxKeys: maxKeys,
			Marker: func() *string {
				if pg.keyMarker != "" {
					return &pg.keyMarker
				}
				return nil
			}(),
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, obj := range res.Contents {
			keysSeen[obj.Key] = struct{}{}
		}

		// push subdirectories
		for _, cp := range res.CommonPrefixes {
			if _, ok := seen[cp.Prefix]; ok {
				t.Fatalf("already seen common prefix %q", cp)
			}
			seen[cp.Prefix] = struct{}{}
			stack = append(stack, page{prefix: cp.Prefix})
		}

		// re-enqueue if truncated
		if res.IsTruncated {
			stack = append(stack, page{
				prefix:    pg.prefix,
				keyMarker: res.NextMarker,
			})
		}
	}

	for key := range keysAll {
		if _, ok := keysSeen[key]; !ok {
			t.Logf("missing %s", key)
		}
	}
	if len(keysAll) != len(keysSeen) {
		t.Fatalf("didn't see some keys: %d vs %d", len(keysAll), len(keysSeen))
	}
}

func BenchmarkListObjects(b *testing.B) {
	const (
		// number of root level directories
		dir1 = 1000
		// number of second level directories
		dir2 = 10
		// number of third level directories
		dir3 = 10
		// number of fourth level files
		dir4 = 10
	)

	log := zaptest.NewLogger(b)
	fp := filepath.Join(b.TempDir(), "s3d.sqlite3")

	store, err := OpenDatabase(fp, log)
	if err != nil {
		b.Fatal(err)
	}

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket("", bucket); err != nil {
		b.Fatal(err)
	}

	obj := sdk.Object{}
	sealed := obj.Seal(types.GeneratePrivateKey())

	err = store.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		objID := sealed.ID()
		contentMD5 := [16]byte(frand.Bytes(16))

		var size uint64
		for _, slab := range sealed.Slabs {
			size += uint64(slab.Length)
		}

		now := time.Now()
		for i := 0; i < dir1; i++ {
			layer1 := fmt.Sprint(i)
			for j := 0; j < dir2; j++ {
				layer2 := filepath.Join(layer1, fmt.Sprint(j))
				for k := 0; k < dir3; k++ {
					layer3 := filepath.Join(layer2, fmt.Sprint(k))
					for l := 0; l < dir4; l++ {
						idx := i*dir1 + j*dir2 + k*dir3 + l*dir4
						if (idx % 10000) == 0 {
							b.Log(idx)
						}

						name := strconv.Itoa(idx)
						layer4 := filepath.Join(layer3, name)

						_, err = tx.Exec(`
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, bid, layer4, sqlHash256(objID), sqlMD5(contentMD5), []byte{}, size, sqlTime(now))
					}
				}
			}
		}
		return err
	})
	if err != nil {
		b.Fatal(err)
	}

	if _, err := store.db.Exec(`VACUUM;`); err != nil {
		b.Fatal(err)
	} else if _, err := store.db.Exec(`ANALYZE;`); err != nil {
		b.Fatal(err)
	}

	const maxKeys = 1000
	b.Run("no_delimiter_no_prefix", func(b *testing.B) {
		for b.Loop() {
			result, err := store.ListObjects(nil, bucket, s3.Prefix{}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})

	b.Run("root_delimiter", func(b *testing.B) {
		for b.Loop() {
			result, err := store.ListObjects(nil, bucket, s3.Prefix{
				Delimiter:    "/",
				HasDelimiter: true,
			}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})

	b.Run("random_without_delimiter", func(b *testing.B) {
		for b.Loop() {
			result, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:    fmt.Sprintf("%d/%d/", frand.Intn(dir1), frand.Intn(dir2)),
				HasPrefix: true,
			}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})

	b.Run("random_with_root_delimiter", func(b *testing.B) {
		for b.Loop() {
			result, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:       fmt.Sprintf("%d/%d/", frand.Intn(dir1), frand.Intn(dir2)),
				HasPrefix:    true,
				Delimiter:    "/",
				HasDelimiter: true,
			}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})

	b.Run("folder_bottom_delimiter", func(b *testing.B) {
		for b.Loop() {
			result, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:       "0/0/0",
				HasPrefix:    true,
				Delimiter:    "/",
				HasDelimiter: true,
			}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})

	b.Run("folder_delimiter", func(b *testing.B) {
		for b.Loop() {
			result, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:       "0/",
				HasPrefix:    true,
				Delimiter:    "/",
				HasDelimiter: true,
			}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})
}

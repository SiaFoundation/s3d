package sqlite

import (
	"errors"
	"testing"

	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestGetObject(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
		multipart   = "test-multipart"
	)

	var (
		objID     = frand.Entropy256()
		objMD5    = frand.Entropy128()
		objMeta   = map[string]string{"foo": "bar"}
		objLength = frand.Intn(10) + 1

		multipartID       = frand.Entropy256()
		multipartMD5      = frand.Entropy128()
		multipartUploadID = s3.NewUploadID()
		multipartMeta     = map[string]string{"baz": "qux"}
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create object
	err := store.PutObject(accessKeyID, bucket, object, &objects.Object{
		ID:         objID,
		Meta:       objMeta,
		ContentMD5: objMD5,
		Length:     int64(objLength),
	}, true)
	if err != nil {
		t.Fatal(err)
	}

	// create multipart object
	err = store.CreateMultipartUpload(bucket, multipart, multipartUploadID, multipartMeta)
	if err != nil {
		t.Fatal(err)
	}
	// add parts
	part1MD5 := frand.Entropy128()
	part2MD5 := frand.Entropy128()
	if _, err := store.AddMultipartPart(bucket, multipart, multipartUploadID, "part-1", 1, part1MD5, s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMultipartPart(bucket, multipart, multipartUploadID, "part-2", 2, part2MD5, 2); err != nil {
		t.Fatal(err)
	}
	// complete
	totalSize := int64(s3.MinUploadPartSize + 2)
	err = store.CompleteMultipartUpload(bucket, multipart, multipartUploadID, multipartID, multipartMD5, totalSize)
	if err != nil {
		t.Fatal(err)
	}

	// get object without part number
	obj, err := store.GetObject(aws.String(accessKeyID), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.ID != objID {
		t.Fatalf("expected object ID %v, got %v", objID, obj.ID)
	} else if obj.Length != int64(objLength) {
		t.Fatalf("expected object length %d, got %d", objLength, obj.Length)
	} else if obj.ContentMD5 != objMD5 {
		t.Fatalf("expected object MD5 %v, got %v", objMD5, obj.ContentMD5)
	} else if len(obj.Meta) != len(objMeta) || obj.Meta["foo"] != "bar" {
		t.Fatalf("expected object metadata %v, got %v", objMeta, obj.Meta)
	}

	// get object with part number 1
	objPart1, err := store.GetObject(aws.String(accessKeyID), bucket, object, aws.Int32(1))
	if err != nil {
		t.Fatal(err)
	} else if objPart1.ID != objID {
		t.Fatalf("expected object ID %v, got %v", objID, objPart1.ID)
	} else if objPart1.Offset != 0 {
		t.Fatalf("expected object offset 0, got %d", objPart1.Offset)
	} else if objPart1.Length != int64(objLength) {
		t.Fatalf("expected object length %d, got %d", objLength, objPart1.Length)
	} else if objPart1.ContentMD5 != objMD5 {
		t.Fatalf("expected object MD5 %v, got %v", objMD5, objPart1.ContentMD5)
	} else if len(objPart1.Meta) != len(objMeta) || objPart1.Meta["foo"] != "bar" {
		t.Fatalf("expected object metadata %v, got %v", objMeta, objPart1.Meta)
	}

	// get multipart object with part number 2
	multipartPart2, err := store.GetObject(aws.String(accessKeyID), bucket, multipart, aws.Int32(2))
	if err != nil {
		t.Fatal(err)
	} else if multipartPart2.ID != multipartID {
		t.Fatalf("expected object ID %v, got %v", multipartID, multipartPart2.ID)
	} else if multipartPart2.Offset != int64(s3.MinUploadPartSize) {
		t.Fatalf("expected object offset %d, got %d", s3.MinUploadPartSize, multipartPart2.Offset)
	} else if multipartPart2.Length != 2 {
		t.Fatalf("expected object length %d, got %d", 2, multipartPart2.Length)
	} else if multipartPart2.ContentMD5 != part2MD5 {
		t.Fatalf("expected object MD5 %v, got %v", part2MD5, multipartPart2.ContentMD5)
	} else if len(multipartPart2.Meta) != len(multipartMeta) || multipartPart2.Meta["baz"] != "qux" {
		t.Fatalf("expected object metadata %v, got %v", multipartMeta, multipartPart2.Meta)
	}

	// get object with invalid part number
	_, err = store.GetObject(aws.String(accessKeyID), bucket, object, aws.Int32(3))
	if !errors.Is(err, s3errs.ErrInvalidPart) {
		t.Fatal("unexpected error", err)
	}
}

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
	etag := s3.FormatETag(contentMD5[:], 0)

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
			// regression: NextMarker can't contain \xFF when it is a
			// common prefix, the \xFF skip is applied on re-entry via
			// adjustMarkerForCommonPrefix
			keys: []string{"a", "b/1", "b/2", "c"},
			cases: []testCase{
				{
					name:           "CommonPrefixMarkerPage1",
					delimiter:      "/",
					maxKeys:        2,
					objects:        []string{"a"},
					commonPrefixes: []string{"b/"},
					truncated:      true,
					nextMarker:     "b/",
				},
				{
					name:      "CommonPrefixMarkerPage2",
					delimiter: "/",
					marker:    "b/",
					maxKeys:   2,
					objects:   []string{"c"},
				},
			},
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
				Length:     int64(frand.Intn(1000)) + 1,
			}, true)
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

				if tc.nextMarker != resp.NextMarker {
					t.Fatalf("expected marker %v, got %v", tc.nextMarker, resp.NextMarker)
				}
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
	etag := s3.FormatETag(contentMD5[:], 0)

	for _, key := range keys {
		err := store.PutObject("", bucket, key, &objects.Object{
			ID:         obj.ID(),
			ContentMD5: contentMD5,
			Length:     int64(frand.Intn(1000)) + 1,
		}, true)
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

	keysAll := make(map[string]struct{})
	for range numKeys {
		key := randomPath(minLength, maxLength, maxDepth, alphabet, delimiter)
		err := store.PutObject("", bucket, key, &objects.Object{
			ID:         obj.ID(),
			ContentMD5: contentMD5,
			Length:     int64(frand.Intn(1000)) + 1,
		}, true)
		if err != nil {
			t.Fatal(err)
		}
		keysAll[key] = struct{}{}
	}

	type page struct {
		prefix    string
		keyMarker string
	}

	seen := make(map[string]struct{})
	stack := []page{{}}
	var visited int
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
			HasDelimiter: delimiter != "",
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
		visited += len(res.Contents)

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

	if len(keysAll) != visited {
		t.Fatalf("expected to visit %d uploads, visited %d", len(keysAll), visited)
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
			INSERT INTO objects (bucket_id, name, object_id, content_md5, metadata, size, updated_at, sia_object, cached_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, bid, layer4, sqlHash256(objID), sqlMD5(contentMD5), []byte{}, size, sqlTime(now), sqlSiaObject(sealed.SealedObject), sqlTime(now))
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
			var marker *string
			for {
				result, err := store.ListObjects(nil, bucket, s3.Prefix{
					Delimiter:    "/",
					HasDelimiter: true,
				}, s3.ListObjectsPage{MaxKeys: maxKeys, Marker: marker})
				if err != nil {
					b.Fatal(err)
				} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
					b.Fatal("no results")
				}
				if !result.IsTruncated {
					break
				}
				marker = &result.NextMarker
			}
		}
	})

	b.Run("random_without_delimiter", func(b *testing.B) {
		for b.Loop() {
			var prefix string
			switch frand.Intn(3) {
			case 0:
				prefix = fmt.Sprintf("%d/", frand.Intn(dir1))
			case 1:
				prefix = fmt.Sprintf("%d/%d/", frand.Intn(dir1), frand.Intn(dir2))
			case 2:
				prefix = fmt.Sprintf("%d/%d/%d/", frand.Intn(dir1), frand.Intn(dir2), frand.Intn(dir3))
			}
			result, err := store.ListObjects(nil, bucket, s3.Prefix{
				Prefix:    prefix,
				HasPrefix: true,
			}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})

	b.Run("random_with_delimiter", func(b *testing.B) {
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

func TestOrphanedObjects(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	objID := frand.Entropy256()

	// no orphans initially
	orphans, err := store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans, got %d", len(orphans))
	}

	// put first object
	if err := store.PutObject(accessKeyID, bucket, "a", &objects.Object{
		ID:         objID,
		ContentMD5: frand.Entropy128(),
		Length:     1,
	}, true); err != nil {
		t.Fatal(err)
	}

	// copy object to a second key
	if _, err := store.CopyObject(bucket, "a", bucket, "b", nil, false); err != nil {
		t.Fatal(err)
	}

	// delete first object - references still exist, nothing orphaned
	if err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans with remaining reference, got %d", len(orphans))
	}

	// delete second object - last reference gone, should be orphaned
	if err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "b"}); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != objID {
		t.Fatalf("expected orphan %v, got %v", objID, orphans)
	}

	// remove orphan
	if err := store.RemoveOrphanedObject(objID); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans after removal, got %d", len(orphans))
	}
}

func TestPutObjectOrphan(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	oldID := frand.Entropy256()
	newID := frand.Entropy256()

	// put initial object - no orphans
	if err := store.PutObject(accessKeyID, bucket, "obj", &objects.Object{
		ID:         oldID,
		ContentMD5: frand.Entropy128(),
		Length:     1,
	}, true); err != nil {
		t.Fatal(err)
	}

	orphans, err := store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("first put should not orphan anything")
	}

	// overwrite with a different object_id - old ID should be orphaned
	if err := store.PutObject(accessKeyID, bucket, "obj", &objects.Object{
		ID:         newID,
		ContentMD5: frand.Entropy128(),
		Length:     1,
	}, true); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != oldID {
		t.Fatalf("expected orphaned ID %v, got %v", oldID, orphans)
	}

	// clean up orphan
	if err := store.RemoveOrphanedObject(oldID); err != nil {
		t.Fatal(err)
	}

	// overwrite with same object_id should not orphan
	if err := store.PutObject(accessKeyID, bucket, "obj", &objects.Object{
		ID:         newID,
		ContentMD5: frand.Entropy128(),
		Length:     2,
	}, true); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("overwrite with same ID should not orphan")
	}
}

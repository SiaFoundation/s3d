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
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestGetObject(t *testing.T) {
	var (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
		multipart   = "test-multipart"
	)

	var (
		objMD5    = frand.Entropy128()
		objMeta   = map[string]string{"foo": "bar"}
		objLength = frand.Intn(10) + 1

		objSealKey = types.GeneratePrivateKey()
		objSdkObj  = sdk.Object{}
		objSealed  = objSdkObj.Seal(objSealKey)

		multipartSealKey = types.GeneratePrivateKey()
		multipartSdkObj  = sdk.Object{}
		multipartSealed  = multipartSdkObj.Seal(multipartSealKey)

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
	err := store.PutObject(accessKeyID, bucket, object, objMD5, objMeta, int64(objLength), new(string), true)
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
	err = store.CompleteMultipartUpload(bucket, multipart, multipartUploadID, multipartMD5, totalSize)
	if err != nil {
		t.Fatal(err)
	}

	// get object without part number
	obj, err := store.GetObject(aws.String(accessKeyID), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.SiaObject != nil {
		t.Fatalf("expected nil SiaObject, got %v", obj.SiaObject)
	} else if obj.Length != int64(objLength) {
		t.Fatalf("expected object length %d, got %d", objLength, obj.Length)
	} else if obj.ContentMD5 != objMD5 {
		t.Fatalf("expected object MD5 %v, got %v", objMD5, obj.ContentMD5)
	} else if len(obj.Meta) != len(objMeta) || obj.Meta["foo"] != "bar" {
		t.Fatalf("expected object metadata %v, got %v", objMeta, obj.Meta)
	}

	// mark the object as uploaded
	if err := store.MarkObjectUploaded(bucket, object, obj.ContentMD5, objSealed); err != nil {
		t.Fatal(err)
	}

	// re-fetch and verify the sia_object_id is now set
	obj, err = store.GetObject(&accessKeyID, bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.SiaObject == nil || obj.SiaObject.ID != objSealed.ID() {
		t.Fatalf("expected object ID %v, got %v", objSealed.ID(), obj.SiaObject)
	}

	// get object with part number 1
	objPart1, err := store.GetObject(&accessKeyID, bucket, object, aws.Int32(1))
	if err != nil {
		t.Fatal(err)
	} else if objPart1.Offset != 0 {
		t.Fatalf("expected object offset 0, got %d", objPart1.Offset)
	} else if objPart1.Length != int64(objLength) {
		t.Fatalf("expected object length %d, got %d", objLength, objPart1.Length)
	} else if objPart1.ContentMD5 != objMD5 {
		t.Fatalf("expected object MD5 %v, got %v", objMD5, objPart1.ContentMD5)
	} else if len(objPart1.Meta) != len(objMeta) || objPart1.Meta["foo"] != "bar" {
		t.Fatalf("expected object metadata %v, got %v", objMeta, objPart1.Meta)
	}

	// mark multipart object as uploaded
	if err := store.MarkObjectUploaded(bucket, multipart, multipartMD5, multipartSealed); err != nil {
		t.Fatal(err)
	}

	// get multipart object with part number 2
	mpID := multipartSealed.ID()
	multipartPart2, err := store.GetObject(aws.String(accessKeyID), bucket, multipart, aws.Int32(2))
	if err != nil {
		t.Fatal(err)
	} else if multipartPart2.SiaObject == nil || multipartPart2.SiaObject.ID != mpID {
		t.Fatalf("expected object ID %v, got %v", mpID, multipartPart2.SiaObject.ID)
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

func TestGetObjectPartFields(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		name        = "multipart-obj"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	uploadID := s3.NewUploadID()
	if err := store.CreateMultipartUpload(bucket, name, uploadID, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMultipartPart(bucket, name, uploadID, "part-1", 1, frand.Entropy128(), s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMultipartPart(bucket, name, uploadID, "part-2", 2, frand.Entropy128(), 64); err != nil {
		t.Fatal(err)
	}
	contentMD5 := frand.Entropy128()
	if err := store.CompleteMultipartUpload(bucket, name, uploadID, contentMD5, s3.MinUploadPartSize+64); err != nil {
		t.Fatal(err)
	}

	// pending multipart: fetching part 1 should populate FileName
	aki := accessKeyID
	obj, err := store.GetObject(&aki, bucket, name, aws.Int32(1))
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName == nil {
		t.Fatal("expected FileName to be set for pending multipart part")
	} else if obj.SiaObject != nil {
		t.Fatal("expected nil SiaObject for pending multipart part")
	}

	// mark as uploaded to Sia
	sealKey := types.GeneratePrivateKey()
	sdkObj := sdk.NewEmptyObject()
	sealed := sdkObj.Seal(sealKey)
	if err := store.MarkObjectUploaded(bucket, name, contentMD5, sealed); err != nil {
		t.Fatal(err)
	}

	// after upload: fetching part 2 should populate SiaObject
	obj, err = store.GetObject(&aki, bucket, name, aws.Int32(2))
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName != nil {
		t.Fatal("expected nil FileName after upload")
	} else if obj.SiaObject == nil {
		t.Fatal("expected SiaObject to be set after upload")
	} else if obj.SiaObject.ID != sealed.ID() {
		t.Fatalf("expected SiaObject ID %v, got %v", sealed.ID(), obj.SiaObject.ID)
	}
}

func TestListObjects(t *testing.T) {
	log := zaptest.NewLogger(t)
	fp := filepath.Join(t.TempDir(), "s3d.sqlite3")

	store, err := OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket("", bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
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
			err := store.PutObject("", bucket, key, contentMD5, nil, int64(frand.Intn(1000))+1, new(string), true)
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
	t.Cleanup(func() { store.Close() })

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket("", bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
	keys := []string{"a//b", "foo/baz", "foo/bar", "😊/д"}
	contentMD5 := [16]byte(frand.Bytes(16))
	etag := s3.FormatETag(contentMD5[:], 0)

	for _, key := range keys {
		err := store.PutObject("", bucket, key, contentMD5, nil, int64(frand.Intn(1000))+1, new(string), true)
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
	t.Cleanup(func() { store.Close() })

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket("", bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
	contentMD5 := [16]byte(frand.Bytes(16))

	keysAll := make(map[string]struct{})
	for range numKeys {
		key := randomPath(minLength, maxLength, maxDepth, alphabet, delimiter)
		err := store.PutObject("", bucket, key, contentMD5, nil, int64(frand.Intn(1000))+1, new(string), true)
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
	b.Cleanup(func() { store.Close() })

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
			INSERT INTO objects (bucket_id, name, sia_object_id, content_md5, metadata, size, updated_at, sia_object)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, bid, layer4, sqlHash256(objID), sqlMD5(contentMD5), []byte{}, size, sqlTime(now), sqlSiaObject(sealed))
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

	sealObj := sdk.Object{}
	sealed := sealObj.Seal(types.GeneratePrivateKey())
	objID := sealed.ID()

	// no orphans initially
	orphans, err := store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans, got %d", len(orphans))
	}

	// put first object and mark it uploaded
	md5a := frand.Entropy128()
	if err := store.PutObject(accessKeyID, bucket, "a", md5a, nil, 1, new(string), true); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, "a", md5a, sealed); err != nil {
		t.Fatal(err)
	}

	// copy object to a second key
	if _, err := store.CopyObject(bucket, "a", bucket, "b", nil, false); err != nil {
		t.Fatal(err)
	}

	// delete first object - references still exist, nothing orphaned
	if _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans with remaining reference, got %d", len(orphans))
	}

	// delete second object - last reference gone, should be orphaned
	if _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "b"}); err != nil {
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

	oldObj := (sdk.Object{})
	oldSealed := oldObj.Seal(types.GeneratePrivateKey())
	newObj := sdk.Object{}
	newSealed := newObj.Seal(types.GeneratePrivateKey())

	// put initial object and mark it uploaded
	md5old := frand.Entropy128()
	if err := store.PutObject(accessKeyID, bucket, "obj", md5old, nil, 1, new(string), true); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, "obj", md5old, oldSealed); err != nil {
		t.Fatal(err)
	}

	orphans, err := store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatal("first put should not orphan anything")
	}

	// overwrite with a different sia_object_id - old ID should be orphaned
	md5new := frand.Entropy128()
	if err := store.PutObject(accessKeyID, bucket, "obj", md5new, nil, 1, new(string), true); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != oldSealed.ID() {
		t.Fatalf("expected orphaned ID %v, got %v", oldSealed.ID(), orphans)
	}

	// clean up orphan
	if err := store.RemoveOrphanedObject(oldSealed.ID()); err != nil {
		t.Fatal(err)
	}

	// mark new upload and overwrite again with same sia_object_id - should not orphan
	if err := store.MarkObjectUploaded(bucket, "obj", md5new, newSealed); err != nil {
		t.Fatal(err)
	}
	if err := store.PutObject(accessKeyID, bucket, "obj", frand.Entropy128(), nil, 2, new(string), true); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 || orphans[0] != newSealed.ID() {
		t.Fatalf("expected orphaned ID %v, got %v", newSealed.ID(), orphans)
	}
}

func TestObjectsCursor(t *testing.T) {
	store := initTestDB(t, zaptest.NewLogger(t))

	// cursor should start at epoch zero
	cursor, err := store.ObjectsCursor()
	if err != nil {
		t.Fatal(err)
	}
	if cursor.After.Unix() != 0 {
		t.Fatalf("expected unix epoch cursor time, got %v", cursor.After)
	}
	if cursor.Key != (types.Hash256{}) {
		t.Fatalf("expected zero cursor key, got %v", cursor.Key)
	}

	// set cursor and verify it persists
	now := time.Now().Truncate(time.Second)
	key := types.Hash256{1, 2, 3}
	if err := store.SetObjectsCursor(slabs.Cursor{After: now, Key: key}); err != nil {
		t.Fatal(err)
	}
	cursor, err = store.ObjectsCursor()
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.After.Equal(now) {
		t.Fatalf("expected cursor at %v, got %v", now, cursor.After)
	}
	if cursor.Key != key {
		t.Fatalf("expected cursor key %v, got %v", key, cursor.Key)
	}

	// overwrite with a new cursor
	later := now.Add(5 * time.Minute)
	key2 := types.Hash256{4, 5, 6}
	if err := store.SetObjectsCursor(slabs.Cursor{After: later, Key: key2}); err != nil {
		t.Fatal(err)
	}
	cursor, err = store.ObjectsCursor()
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.After.Equal(later) {
		t.Fatalf("expected cursor at %v, got %v", later, cursor.After)
	}
	if cursor.Key != key2 {
		t.Fatalf("expected cursor key %v, got %v", key2, cursor.Key)
	}
}

func TestUpdateSiaObject(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	sealKey := types.GeneratePrivateKey()
	sdkObj := sdk.NewEmptyObject()
	sealed := sdkObj.Seal(sealKey)

	// updating a non-existent object should return false
	updated, err := store.UpdateSiaObject(objects.SiaObject{ID: sealed.ID(), Sealed: sealed})
	if err != nil {
		t.Fatal(err)
	} else if updated {
		t.Fatal("expected no update for non-existent object")
	}

	// put and mark an object as uploaded
	contentMD5 := frand.Entropy128()
	if err := store.PutObject(accessKeyID, bucket, "obj", contentMD5, nil, 1, new(string), true); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, "obj", contentMD5, sealed); err != nil {
		t.Fatal(err)
	}

	// verify the stored sia_object matches
	aki := accessKeyID
	before, err := store.GetObject(&aki, bucket, "obj", nil)
	if err != nil {
		t.Fatal(err)
	} else if before.SiaObject == nil {
		t.Fatal("expected sia_object to be set")
	} else if before.SiaObject.ID != sealed.ID() {
		t.Fatal("unexpected object ID")
	}

	// updating with a matching object ID should succeed
	updated, err = store.UpdateSiaObject(objects.SiaObject{ID: sealed.ID(), Sealed: sealed})
	if err != nil {
		t.Fatal(err)
	} else if !updated {
		t.Fatal("expected update for existing object")
	}

	// after removing the object, updating should return false again
	if _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "obj"}); err != nil {
		t.Fatal(err)
	}
	updated, err = store.UpdateSiaObject(objects.SiaObject{ID: sealed.ID(), Sealed: sealed})
	if err != nil {
		t.Fatal(err)
	} else if updated {
		t.Fatal("expected no update after object deleted")
	}
}

func TestMarkObjectUploaded(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create a pending upload
	fileName := "test-file.obj"
	contentMD5 := frand.Entropy128()
	if err := store.PutObject(accessKeyID, bucket, object, contentMD5, nil, 100, &fileName, true); err != nil {
		t.Fatal(err)
	}

	sdkObj := sdk.Object{}
	sealed := sdkObj.Seal(types.GeneratePrivateKey())

	// marking with a different content MD5 should return ErrObjectModified
	wrongMD5 := frand.Entropy128()
	err := store.MarkObjectUploaded(bucket, object, wrongMD5, sealed)
	if !errors.Is(err, objects.ErrObjectModified) {
		t.Fatalf("expected ErrObjectModified, got %v", err)
	}

	// marking with the correct content MD5 should succeed
	if err := store.MarkObjectUploaded(bucket, object, contentMD5, sealed); err != nil {
		t.Fatal(err)
	}

	// verify the object is now on Sia
	akid := accessKeyID
	obj, err := store.GetObject(&akid, bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName != nil {
		t.Fatal("expected filename to be cleared after upload")
	} else if obj.SiaObject == nil {
		t.Fatal("expected sia_object to be set after upload")
	}

	// marking again should return ErrObjectNotFound since the object is
	// already uploaded
	sdkObj2 := sdk.Object{}
	sealed2 := sdkObj2.Seal(types.GeneratePrivateKey())
	err = store.MarkObjectUploaded(bucket, object, contentMD5, sealed2)
	if !errors.Is(err, objects.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestObjectsForUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// insert three pending uploads with filenames, varying sizes
	for _, tc := range []struct {
		name   string
		size   int64
		fnName string
	}{
		{"small", 10, "small.obj"},
		{"medium", 500, "medium.obj"},
		{"large", 1000, "large.obj"},
	} {
		fn := tc.fnName
		if err := store.PutObject(accessKeyID, bucket, tc.name, frand.Entropy128(), nil, tc.size, &fn, true); err != nil {
			t.Fatal(err)
		}
	}

	// insert an object that has been uploaded to Sia, so filename is cleared
	fn := "uploaded.obj"
	uploadedMD5 := frand.Entropy128()
	if err := store.PutObject(accessKeyID, bucket, "uploaded", uploadedMD5, nil, 200, &fn, true); err != nil {
		t.Fatal(err)
	}
	sealObj := sdk.Object{}
	sealed := sealObj.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "uploaded", uploadedMD5, sealed); err != nil {
		t.Fatal(err)
	}

	// insert a completed multipart upload with 2 parts
	uid := s3.NewUploadID()
	if err := store.CreateMultipartUpload(bucket, "multipart", uid, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMultipartPart(bucket, "multipart", uid, "p1", 1, frand.Entropy128(), s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMultipartPart(bucket, "multipart", uid, "p2", 2, frand.Entropy128(), 50); err != nil {
		t.Fatal(err)
	}
	mpSize := int64(s3.MinUploadPartSize + 50)
	if err := store.CompleteMultipartUpload(bucket, "multipart", uid, frand.Entropy128(), mpSize); err != nil {
		t.Fatal(err)
	}

	objects, err := store.ObjectsForUpload()
	if err != nil {
		t.Fatal(err)
	}

	// "uploaded" should be excluded since its filename was cleared
	if len(objects) != 4 {
		t.Fatalf("expected 4 objects, got %d", len(objects))
	}

	// verify descending size order
	for i := 1; i < len(objects); i++ {
		if objects[i].Length > objects[i-1].Length {
			t.Fatalf("expected descending order, but %s (%d) > %s (%d)",
				objects[i].Name, objects[i].Length, objects[i-1].Name, objects[i-1].Length)
		}
	}

	// verify the multipart property is set correctly
	for _, obj := range objects {
		if obj.Name == "multipart" {
			if !obj.Multipart {
				t.Fatalf("expected multipart object, got non-multipart")
			}
			if obj.Length != mpSize {
				t.Fatalf("expected multipart size %d, got %d", mpSize, obj.Length)
			}
		} else if obj.Multipart {
			t.Fatalf("expected non-multipart object, got multipart for %s", obj.Name)
		}
	}

	// verify all returned objects have a filename set
	for _, obj := range objects {
		if obj.Filename == "" {
			t.Fatalf("expected non-empty filename for %s", obj.Name)
		}
	}
}

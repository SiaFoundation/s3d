package sqlite

import (
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"fmt"
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

func TestDiskUsage(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// initially zero
	usage, err := store.DiskUsage()
	if err != nil {
		t.Fatal(err)
	} else if usage != 0 {
		t.Fatalf("expected 0 disk usage, got %d", usage)
	}

	// add pending objects
	fn := "a.obj"
	if _, _, err := store.PutObject(accessKeyID, bucket, "a", frand.Entropy128(), nil, 100, &fn); err != nil {
		t.Fatal(err)
	}
	fn = "b.obj"
	if _, _, err := store.PutObject(accessKeyID, bucket, "b", frand.Entropy128(), nil, 250, &fn); err != nil {
		t.Fatal(err)
	}

	usage, err = store.DiskUsage()
	if err != nil {
		t.Fatal(err)
	} else if usage != 350 {
		t.Fatalf("expected 350, got %d", usage)
	}

	// add in-progress multipart parts
	uid := s3.NewUploadID()
	if err := store.CreateMultipartUpload(accessKeyID, bucket, "multipart", uid, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddMultipartPart(accessKeyID, bucket, "multipart", uid, "p1", 1, frand.Entropy128(), 500); err != nil {
		t.Fatal(err)
	}

	usage, err = store.DiskUsage()
	if err != nil {
		t.Fatal(err)
	} else if usage != 850 {
		t.Fatalf("expected 850, got %d", usage)
	}

	// copy "a" - shared filename should not double-count
	if _, _, err := store.CopyObject(accessKeyID, bucket, "a", s3.NoVersion(), bucket, "a-copy", nil, false); err != nil {
		t.Fatal(err)
	}

	usage, err = store.DiskUsage()
	if err != nil {
		t.Fatal(err)
	} else if usage != 850 {
		t.Fatalf("expected 850 (shared filename not double-counted), got %d", usage)
	}

	// uploaded but not yet pinned objects still hold their file on disk
	contentMD5 := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, "uploaded", contentMD5, nil, 200, new(string)); err != nil {
		t.Fatal(err)
	}
	sealObj := sdk.Object{}
	sealed := sealObj.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "uploaded", "", contentMD5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	usage, err = store.DiskUsage()
	if err != nil {
		t.Fatal(err)
	} else if usage != 1050 {
		t.Fatalf("expected 1050 (uploaded-but-not-pinned still counts), got %d", usage)
	}

	// once pinned, the file is released and no longer counts
	if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
		t.Fatal(err)
	}

	usage, err = store.DiskUsage()
	if err != nil {
		t.Fatal(err)
	} else if usage != 850 {
		t.Fatalf("expected 850 (pinned object released), got %d", usage)
	}
}

func TestAllFilenames(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zaptest.NewLogger(t))

	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert empty store returns no filenames
	filenames, err := store.AllFilenames()
	if err != nil {
		t.Fatal(err)
	} else if len(filenames) != 0 {
		t.Fatal("expected no filenames", len(filenames))
	}

	// add a pending upload
	fn := "regular.obj"
	if _, _, err := store.PutObject(accessKeyID, bucket, "obj1", frand.Entropy128(), nil, 100, &fn); err != nil {
		t.Fatal(err)
	}

	// assert regular upload filename is returned
	filenames, err = store.AllFilenames()
	if err != nil {
		t.Fatal(err)
	} else if len(filenames) != 1 {
		t.Fatal("expected 1 filename", len(filenames))
	} else if !slices.Contains(filenames, "regular.obj") {
		t.Fatal("expected regular.obj in filenames")
	}

	// add an in-progress multipart upload
	uid := s3.NewUploadID()
	if err := store.CreateMultipartUpload(testAccessKeyID, bucket, "mp1", uid, nil); err != nil {
		t.Fatal(err)
	}

	// assert multipart upload ID is included
	filenames, err = store.AllFilenames()
	if err != nil {
		t.Fatal(err)
	} else if len(filenames) != 2 {
		t.Fatal("expected 2 filenames", len(filenames))
	} else if !slices.Contains(filenames, uid.String()) {
		t.Fatal("expected multipart upload ID in filenames")
	}

	// mark the regular upload as uploaded to Sia
	obj := sdk.Object{}
	sealed := obj.Seal(types.GeneratePrivateKey())
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, "obj1", md5, nil, 100, &fn); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, "obj1", "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// before pinning, the filename remains so the file on disk is preserved as
	// a backup until the pin succeeds
	filenames, err = store.AllFilenames()
	if err != nil {
		t.Fatal(err)
	} else if len(filenames) != 2 {
		t.Fatal("expected 2 filenames before pinning", len(filenames))
	} else if !slices.Contains(filenames, "regular.obj") {
		t.Fatal("expected regular.obj to still be referenced before pinning")
	}

	// once pinned, the filename is cleared
	if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
		t.Fatal(err)
	}
	filenames, err = store.AllFilenames()
	if err != nil {
		t.Fatal(err)
	} else if len(filenames) != 1 {
		t.Fatal("expected 1 filename after pinning", len(filenames))
	} else if !slices.Contains(filenames, uid.String()) {
		t.Fatal("expected multipart upload ID in filenames")
	}

	// complete the multipart upload
	if _, _, err := store.AddMultipartPart(accessKeyID, bucket, "mp1", uid, "p1", 1, frand.Entropy128(), s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	mpMD5 := frand.Entropy128()
	if _, _, err := store.CompleteMultipartUpload(accessKeyID, bucket, "mp1", uid, mpMD5, s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}

	// assert completed multipart object has a filename
	filenames, err = store.AllFilenames()
	if err != nil {
		t.Fatal(err)
	} else if len(filenames) != 1 {
		t.Fatal("expected 1 filename after completing multipart", len(filenames))
	} else if !slices.Contains(filenames, uid.String()) {
		t.Fatal("expected upload ID as filename for completed multipart")
	}

	// mark the completed multipart as uploaded to Sia and then pinned so the
	// filename is released
	mpObj := sdk.Object{}
	mpSealed := mpObj.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "mp1", "", mpMD5, mpSealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(mpSealed.ID()); err != nil {
		t.Fatal(err)
	}

	// assert no filenames remain
	filenames, err = store.AllFilenames()
	if err != nil {
		t.Fatal(err)
	} else if len(filenames) != 0 {
		t.Fatal("expected no filenames after upload", len(filenames))
	}
}

func TestGetObject(t *testing.T) {
	var (
		accessKeyID = testAccessKeyID
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
	_, _, err := store.PutObject(accessKeyID, bucket, object, objMD5, objMeta, int64(objLength), new(string))
	if err != nil {
		t.Fatal(err)
	}

	// create multipart object
	err = store.CreateMultipartUpload(accessKeyID, bucket, multipart, multipartUploadID, multipartMeta)
	if err != nil {
		t.Fatal(err)
	}
	// add parts
	part1MD5 := frand.Entropy128()
	part2MD5 := frand.Entropy128()
	if _, _, err := store.AddMultipartPart(accessKeyID, bucket, multipart, multipartUploadID, "part-1", 1, part1MD5, s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddMultipartPart(accessKeyID, bucket, multipart, multipartUploadID, "part-2", 2, part2MD5, 2); err != nil {
		t.Fatal(err)
	}
	// complete
	totalSize := int64(s3.MinUploadPartSize + 2)
	_, _, err = store.CompleteMultipartUpload(accessKeyID, bucket, multipart, multipartUploadID, multipartMD5, totalSize)
	if err != nil {
		t.Fatal(err)
	}

	// get object without part number
	obj, err := store.GetObject(testAccessKeyID, bucket, object, s3.NoVersion(), nil)
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
	if err := store.MarkObjectUploaded(bucket, object, "", obj.ContentMD5, objSealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// re-fetch and verify the sia_object_id is now set
	obj, err = store.GetObject(testAccessKeyID, bucket, object, s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.SiaObject == nil || obj.SiaObject.ID != objSealed.ID() {
		t.Fatalf("expected object ID %v, got %v", objSealed.ID(), obj.SiaObject)
	}

	// get object with part number 1
	objPart1, err := store.GetObject(testAccessKeyID, bucket, object, s3.NoVersion(), aws.Int32(1))
	if err != nil {
		t.Fatal(err)
	} else if objPart1.SiaObject == nil || objPart1.SiaObject.ID != objSealed.ID() {
		t.Fatalf("expected object ID %v, got %v", objSealed.ID(), objPart1.SiaObject.ID)
	} else if objPart1.PartsCount != 0 {
		t.Fatalf("expected parts count 0, got %d", objPart1.PartsCount)
	} else if objPart1.Offset != 0 {
		t.Fatalf("expected object offset 0, got %d", objPart1.Offset)
	} else if objPart1.Length != int64(objLength) {
		t.Fatalf("expected object length %d, got %d", objLength, objPart1.Length)
	} else if objPart1.Size != int64(objLength) {
		t.Fatalf("expected object size %d, got %d", objLength, objPart1.Size)
	} else if objPart1.ContentMD5 != objMD5 {
		t.Fatalf("expected object MD5 %v, got %v", objMD5, objPart1.ContentMD5)
	} else if len(objPart1.Meta) != len(objMeta) || objPart1.Meta["foo"] != "bar" {
		t.Fatalf("expected object metadata %v, got %v", objMeta, objPart1.Meta)
	} else if objPart1.SiaObject == nil {
		t.Fatal("expected sia object to be set")
	}

	// mark multipart object as uploaded
	if err := store.MarkObjectUploaded(bucket, multipart, "", multipartMD5, multipartSealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// get multipart object with part number 2
	mpID := multipartSealed.ID()
	multipartPart2, err := store.GetObject(testAccessKeyID, bucket, multipart, s3.NoVersion(), aws.Int32(2))
	if err != nil {
		t.Fatal(err)
	} else if multipartPart2.SiaObject == nil || multipartPart2.SiaObject.ID != mpID {
		t.Fatalf("expected object ID %v, got %v", mpID, multipartPart2.SiaObject.ID)
	} else if multipartPart2.Offset != int64(s3.MinUploadPartSize) {
		t.Fatalf("expected object offset %d, got %d", s3.MinUploadPartSize, multipartPart2.Offset)
	} else if multipartPart2.Length != 2 {
		t.Fatalf("expected object length %d, got %d", 2, multipartPart2.Length)
	} else if multipartPart2.Size != totalSize {
		t.Fatalf("expected object size %d, got %d", totalSize, multipartPart2.Size)
	} else if multipartPart2.ContentMD5 != part2MD5 {
		t.Fatalf("expected object MD5 %v, got %v", part2MD5, multipartPart2.ContentMD5)
	} else if len(multipartPart2.Meta) != len(multipartMeta) || multipartPart2.Meta["baz"] != "qux" {
		t.Fatalf("expected object metadata %v, got %v", multipartMeta, multipartPart2.Meta)
	} else if multipartPart2.SiaObject == nil {
		t.Fatal("expected sia object to be set")
	}

	// get object with invalid part number
	_, err = store.GetObject(testAccessKeyID, bucket, object, s3.NoVersion(), aws.Int32(3))
	if !errors.Is(err, s3errs.ErrInvalidPart) {
		t.Fatal("unexpected error", err)
	}
}

func TestGetObjectPartFields(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
		name        = "multipart-obj"
	)

	store := initTestDB(t, zaptest.NewLogger(t))

	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	uploadID := s3.NewUploadID()
	if err := store.CreateMultipartUpload(accessKeyID, bucket, name, uploadID, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddMultipartPart(accessKeyID, bucket, name, uploadID, "part-1", 1, frand.Entropy128(), s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddMultipartPart(accessKeyID, bucket, name, uploadID, "part-2", 2, frand.Entropy128(), 64); err != nil {
		t.Fatal(err)
	}
	contentMD5 := frand.Entropy128()
	if _, _, err := store.CompleteMultipartUpload(accessKeyID, bucket, name, uploadID, contentMD5, s3.MinUploadPartSize+64); err != nil {
		t.Fatal(err)
	}

	// pending multipart: fetching part 1 should populate FileName
	obj, err := store.GetObject(accessKeyID, bucket, name, s3.NoVersion(), aws.Int32(1))
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
	if err := store.MarkObjectUploaded(bucket, name, "", contentMD5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// after upload: SiaObject is populated and FileName remains until pinning
	obj, err = store.GetObject(accessKeyID, bucket, name, s3.NoVersion(), aws.Int32(2))
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName == nil {
		t.Fatal("expected FileName to remain set until pinning")
	} else if obj.SiaObject == nil {
		t.Fatal("expected SiaObject to be set after upload")
	} else if obj.SiaObject.ID != sealed.ID() {
		t.Fatalf("expected SiaObject ID %v, got %v", sealed.ID(), obj.SiaObject.ID)
	}

	// after pinning: FileName is cleared
	if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
		t.Fatal(err)
	}
	obj, err = store.GetObject(accessKeyID, bucket, name, s3.NoVersion(), aws.Int32(2))
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName != nil {
		t.Fatal("expected nil FileName after pinning")
	} else if obj.SiaObject == nil {
		t.Fatal("expected SiaObject to remain set after pinning")
	}
}

func TestListObjects(t *testing.T) {
	const accessKeyID = testAccessKeyID

	store := initTestDB(t, zaptest.NewLogger(t))

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
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
			_, _, err := store.PutObject(testAccessKeyID, bucket, key, contentMD5, nil, int64(frand.Intn(1000))+1, new(string))
			if err != nil {
				t.Fatal(err)
			}
		}

		for _, tc := range tt.cases {
			t.Run(tc.name, func(t *testing.T) {
				resp, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
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
	const accessKeyID = testAccessKeyID

	store := initTestDB(t, zaptest.NewLogger(t))

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
	keys := []string{"a//b", "foo/baz", "foo/bar", "😊/д"}
	contentMD5 := [16]byte(frand.Bytes(16))
	etag := s3.FormatETag(contentMD5[:], 0)

	for _, key := range keys {
		_, _, err := store.PutObject(testAccessKeyID, bucket, key, contentMD5, nil, int64(frand.Intn(1000))+1, new(string))
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
			resp, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
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
		accessKeyID = testAccessKeyID
		numKeys     = 10000
		maxKeys     = 100
		maxDepth    = 4
		minLength   = 4
		maxLength   = 10
	)

	var (
		alphabet  = []rune("ÎmNotÂfraid!%_")
		delimiter = "%"
	)

	store := initTestDB(t, zaptest.NewLogger(t))

	// prepare a bucket
	bucket := "foo"
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// upload a few objects
	contentMD5 := [16]byte(frand.Bytes(16))

	keysAll := make(map[string]struct{})
	for range numKeys {
		key := randomPath(minLength, maxLength, maxDepth, alphabet, delimiter)
		_, _, err := store.PutObject(testAccessKeyID, bucket, key, contentMD5, nil, int64(frand.Intn(1000))+1, new(string))
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
		res, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
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
		accessKeyID = testAccessKeyID
		// number of root level directories
		dir1 = 1000
		// number of second level directories
		dir2 = 10
		// number of third level directories
		dir3 = 10
		// number of fourth level files
		dir4 = 10
	)

	store := initTestDB(b, zaptest.NewLogger(b))

	obj := sdk.Object{}
	sealed := obj.Seal(types.GeneratePrivateKey())
	objID := sealed.ID()
	contentMD5 := [16]byte(frand.Bytes(16))

	var size uint64
	for _, slab := range sealed.Slabs {
		size += uint64(slab.Length)
	}
	size = max(size, 1)

	populateBucket := func(bucket, delimiter string) {
		if err := store.CreateBucket(accessKeyID, bucket); err != nil {
			b.Fatal(err)
		}
		err := store.transaction(func(tx *txn) error {
			bid, err := bucketIDByName(tx, bucket)
			if err != nil {
				return err
			}
			now := time.Now()
			for i := 0; i < dir1; i++ {
				layer1 := fmt.Sprint(i)
				for j := 0; j < dir2; j++ {
					layer2 := layer1 + delimiter + fmt.Sprint(j)
					for k := 0; k < dir3; k++ {
						layer3 := layer2 + delimiter + fmt.Sprint(k)
						for l := 0; l < dir4; l++ {
							idx := i*dir1 + j*dir2 + k*dir3 + l*dir4
							if (idx % 10000) == 0 {
								b.Log(idx)
							}

							name := strconv.Itoa(idx)
							key := layer3 + delimiter + name

							_, err = tx.Exec(`
			INSERT INTO objects (bucket_id, name, seq, sia_object_id, content_md5, metadata, size, updated_at, sia_object)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, bid, key, 1, sqlHash256(objID), sqlMD5(contentMD5), []byte{}, size, sqlTime(now), sqlSiaObject(sealed))
						}
					}
				}
			}
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	populateBucket("slash", "/")
	populateBucket("backslash", `\`)

	if _, err := store.db.Exec(`VACUUM;`); err != nil {
		b.Fatal(err)
	} else if _, err := store.db.Exec(`ANALYZE;`); err != nil {
		b.Fatal(err)
	}

	const maxKeys = 1000

	b.Run("no_delimiter_no_prefix", func(b *testing.B) {
		for b.Loop() {
			result, err := store.ListObjects(testAccessKeyID, "slash", s3.Prefix{}, s3.ListObjectsPage{MaxKeys: maxKeys})
			if err != nil {
				b.Fatal(err)
			} else if (len(result.Contents) + len(result.CommonPrefixes)) == 0 {
				b.Fatal("no results")
			}
		}
	})

	benchmarkListObjects := func(b *testing.B, bucket, delimiter string) {
		b.Run("root_delimiter", func(b *testing.B) {
			for b.Loop() {
				var marker *string
				for {
					result, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
						Delimiter:    delimiter,
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
					prefix = fmt.Sprintf("%d"+delimiter, frand.Intn(dir1))
				case 1:
					prefix = fmt.Sprintf("%d"+delimiter+"%d"+delimiter, frand.Intn(dir1), frand.Intn(dir2))
				case 2:
					prefix = fmt.Sprintf("%d"+delimiter+"%d"+delimiter+"%d"+delimiter, frand.Intn(dir1), frand.Intn(dir2), frand.Intn(dir3))
				}
				result, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
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
				result, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
					Prefix:       fmt.Sprintf("%d"+delimiter+"%d"+delimiter, frand.Intn(dir1), frand.Intn(dir2)),
					HasPrefix:    true,
					Delimiter:    delimiter,
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
				result, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
					Prefix:       "0" + delimiter + "0" + delimiter + "0",
					HasPrefix:    true,
					Delimiter:    delimiter,
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
				result, err := store.ListObjects(testAccessKeyID, bucket, s3.Prefix{
					Prefix:       "0" + delimiter,
					HasPrefix:    true,
					Delimiter:    delimiter,
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

	b.Run("slash", func(b *testing.B) {
		benchmarkListObjects(b, "slash", "/")
	})
	b.Run("backslash", func(b *testing.B) {
		benchmarkListObjects(b, "backslash", `\`)
	})
}

func TestOrphanedObjects(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
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

	// put first object, mark it uploaded, then pin it
	md5a := frand.Entropy128()
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "a", md5a, nil, 1, new(string)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, "a", "", md5a, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
		t.Fatal(err)
	}

	// copy object to a second key
	if _, _, err := store.CopyObject(testAccessKeyID, bucket, "a", s3.NoVersion(), bucket, "b", nil, false); err != nil {
		t.Fatal(err)
	}

	// delete first object - references still exist, nothing orphaned
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "a"}); err != nil {
		t.Fatal(err)
	}

	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans with remaining reference, got %d", len(orphans))
	}

	// delete second object - last reference gone, should be orphaned
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "b"}); err != nil {
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
		accessKeyID = testAccessKeyID
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

	// put initial object, mark it uploaded, then pin it
	md5old := frand.Entropy128()
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "obj", md5old, nil, 1, new(string)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkObjectUploaded(bucket, "obj", "", md5old, oldSealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(oldSealed.ID()); err != nil {
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
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "obj", md5new, nil, 1, new(string)); err != nil {
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

	// mark + pin a new upload, then overwrite: pinned data orphans
	// correctly when its last reference disappears.
	if err := store.MarkObjectUploaded(bucket, "obj", "", md5new, newSealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if _, err := store.MarkObjectPinned(newSealed.ID()); err != nil {
		t.Fatal(err)
	} else if _, _, err := store.PutObject(testAccessKeyID, bucket, "obj", frand.Entropy128(), nil, 2, new(string)); err != nil {
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

func TestUpdateSiaObjects(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zaptest.NewLogger(t))

	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// empty slice should be a no-op
	updated, err := store.UpdateSiaObjects(nil)
	if err != nil {
		t.Fatal(err)
	} else if updated != 0 {
		t.Fatal("expected 0 updates for empty slice", updated)
	}

	// update a non-existent object, should return 0 updates
	updated, err = store.UpdateSiaObjects([]objects.SiaObject{{ID: frand.Entropy256(), Sealed: sdk.SealedObject{}}})
	if err != nil {
		t.Fatal(err)
	} else if updated != 0 {
		t.Fatal("expected 0 updates for non-existent object", updated)
	}

	// addTestObject adds an object to the store
	addTestObject := func(key string) sdk.SealedObject {
		t.Helper()

		obj := newTestObject()
		sealed := obj.Seal(types.GeneratePrivateKey())
		contentMD5 := frand.Entropy128()
		if _, _, err := store.PutObject(testAccessKeyID, bucket, key, contentMD5, nil, 1, new(string)); err != nil {
			t.Fatal(err)
		} else if err := store.MarkObjectUploaded(bucket, key, "", contentMD5, sealed, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		return sealed
	}

	// add two objects to the store
	sealed1 := addTestObject("obj1")
	sealed2 := addTestObject("obj2")
	missing := newTestObject()

	// batch update objects, should only update 2 objects
	updated, err = store.UpdateSiaObjects([]objects.SiaObject{
		{ID: sealed1.ID(), Sealed: sealed1},
		{ID: sealed2.ID(), Sealed: sealed2},
		{ID: missing.ID(), Sealed: missing.Seal(types.GeneratePrivateKey())},
	})
	if err != nil {
		t.Fatal(err)
	} else if updated != 2 {
		t.Fatal("expected 2 updates", updated)
	}

	// delete the first object
	if _, _, _, err := store.DeleteObject(testAccessKeyID, bucket, s3.ObjectID{Key: "obj1"}); err != nil {
		t.Fatal(err)
	}

	// batch update objects, should only update 1 object
	now := time.Now().Round(time.Second)
	sealed2.UpdatedAt = now
	updated, err = store.UpdateSiaObjects([]objects.SiaObject{
		{ID: sealed1.ID(), Sealed: sealed1},
		{ID: sealed2.ID(), Sealed: sealed2},
		{ID: missing.ID(), Sealed: missing.Seal(types.GeneratePrivateKey())},
	})
	if err != nil {
		t.Fatal(err)
	} else if updated != 1 {
		t.Fatal("expected 1 update", updated)
	}

	// verify the second object was updated
	obj, err := store.GetObject(testAccessKeyID, bucket, "obj2", s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.SiaObject == nil {
		t.Fatal("expected sia_object to be set")
	} else if !obj.SiaObject.Sealed.UpdatedAt.Equal(now) {
		t.Fatalf("expected UpdatedAt %v, got %v", now, obj.SiaObject.Sealed.UpdatedAt)
	}
}

func TestMarkObjectUploaded(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
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
	if _, _, err := store.PutObject(testAccessKeyID, bucket, object, contentMD5, nil, 100, &fileName); err != nil {
		t.Fatal(err)
	}

	sdkObj := sdk.Object{}
	sealed := sdkObj.Seal(types.GeneratePrivateKey())

	// marking with a different content MD5 should return ErrObjectModified
	wrongMD5 := frand.Entropy128()
	err := store.MarkObjectUploaded(bucket, object, "", wrongMD5, sealed, time.Now().Add(time.Hour))
	if !errors.Is(err, objects.ErrObjectModified) {
		t.Fatalf("expected ErrObjectModified, got %v", err)
	}

	// marking with the correct content MD5 should succeed
	if err := store.MarkObjectUploaded(bucket, object, "", contentMD5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// verify the object is now on Sia but its filename is preserved on
	// disk pending the pin
	obj, err := store.GetObject(testAccessKeyID, bucket, object, s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName == nil || *obj.FileName != fileName {
		t.Fatalf("expected filename %q to remain until pinning, got %v", fileName, obj.FileName)
	} else if obj.SiaObject == nil {
		t.Fatal("expected sia_object to be set after upload")
	}

	// marking again should return ErrObjectNotFound since the object is
	// already uploaded
	sdkObj2 := sdk.Object{}
	sealed2 := sdkObj2.Seal(types.GeneratePrivateKey())
	err = store.MarkObjectUploaded(bucket, object, "", contentMD5, sealed2, time.Now().Add(time.Hour))
	if !errors.Is(err, objects.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	// pinning the object releases the file
	orphans, err := store.MarkObjectPinned(sealed.ID())
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 {
		t.Fatalf("expected 1 orphan, got %d", len(orphans))
	} else if orphans[0].Filename != fileName {
		t.Fatalf("expected orphan filename %q, got %q", fileName, orphans[0].Filename)
	} else if orphans[0].Size != 100 {
		t.Fatalf("expected orphan size 100, got %d", orphans[0].Size)
	}

	obj, err = store.GetObject(testAccessKeyID, bucket, object, s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName != nil {
		t.Fatal("expected filename to be cleared after pinning")
	} else if obj.SiaObject == nil {
		t.Fatal("expected sia_object to remain set after pinning")
	}
}

func TestObjectsForUpload(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
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
		if _, _, err := store.PutObject(testAccessKeyID, bucket, tc.name, frand.Entropy128(), nil, tc.size, &fn); err != nil {
			t.Fatal(err)
		}
	}

	// insert an object that has been uploaded to Sia, so filename is cleared
	fn := "uploaded.obj"
	uploadedMD5 := frand.Entropy128()
	if _, _, err := store.PutObject(testAccessKeyID, bucket, "uploaded", uploadedMD5, nil, 200, &fn); err != nil {
		t.Fatal(err)
	}
	sealObj := sdk.Object{}
	sealed := sealObj.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "uploaded", "", uploadedMD5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// insert a completed multipart upload with 2 parts
	uid := s3.NewUploadID()
	if err := store.CreateMultipartUpload(testAccessKeyID, bucket, "multipart", uid, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddMultipartPart(testAccessKeyID, bucket, "multipart", uid, "p1", 1, frand.Entropy128(), s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AddMultipartPart(testAccessKeyID, bucket, "multipart", uid, "p2", 2, frand.Entropy128(), 50); err != nil {
		t.Fatal(err)
	}
	mpSize := int64(s3.MinUploadPartSize + 50)
	if _, _, err := store.CompleteMultipartUpload(testAccessKeyID, bucket, "multipart", uid, frand.Entropy128(), mpSize); err != nil {
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

func TestUploadStats(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	assertStats := func(expected s3.UploadStats) {
		t.Helper()
		stats, err := store.UploadStats()
		if err != nil {
			t.Fatal(err)
		} else if stats != expected {
			t.Fatalf("expected %+v, got %+v", expected, stats)
		}
	}

	// no stats initially
	assertStats(s3.UploadStats{})

	// add two pending uploads
	if _, _, err := store.PutObject(accessKeyID, bucket, "obj1", frand.Entropy128(), nil, 100, new(string)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.PutObject(accessKeyID, bucket, "obj2", frand.Entropy128(), nil, 250, new(string)); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects: 2,
		PendingSize:    350,
	})

	// mark a third object as uploaded to Sia
	contentMD5 := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, "obj3", contentMD5, nil, 500, new(string)); err != nil {
		t.Fatal(err)
	}
	sealObj := sdk.Object{}
	sealed := sealObj.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "obj3", "", contentMD5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// uploaded-but-not-pinned objects are tracked separately from pending uploads
	assertStats(s3.UploadStats{
		PendingObjects:  2,
		PendingSize:     350,
		UploadedObjects: 1,
		UploadedSize:    500,
		UnpinnedObjects: 1,
	})

	// pinning the uploaded object clears it from the unpinned count
	if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects:  2,
		PendingSize:     350,
		UploadedObjects: 1,
		UploadedSize:    500,
	})

	// create an in-progress multipart upload
	uid := s3.NewUploadID()
	if err := store.CreateMultipartUpload(accessKeyID, bucket, "mp1", uid, nil); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects:   2,
		PendingSize:      350,
		UploadedObjects:  1,
		UploadedSize:     500,
		MultipartUploads: 1,
	})

	// delete uploaded object to create an orphan
	if _, _, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "obj3"}); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects:   2,
		PendingSize:      350,
		OrphanedObjects:  1,
		MultipartUploads: 1,
	})

	// clean up the orphan
	if err := store.RemoveOrphanedObject(sealed.ID()); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects:   2,
		PendingSize:      350,
		MultipartUploads: 1,
	})

	// complete the multipart upload, turning it into a pending object
	if _, _, err := store.AddMultipartPart(accessKeyID, bucket, "mp1", uid, "p1", 1, frand.Entropy128(), 500); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CompleteMultipartUpload(accessKeyID, bucket, "mp1", uid, frand.Entropy128(), 500); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects: 3,
		PendingSize:    850,
	})

	// copy a pending object, adding another pending object
	if _, _, err := store.CopyObject(accessKeyID, bucket, "obj1", s3.NoVersion(), bucket, "copy1", nil, true); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects: 4,
		PendingSize:    950,
	})

	// overwrite a pending object with a smaller one
	if _, _, err := store.PutObject(accessKeyID, bucket, "obj2", frand.Entropy128(), nil, 70, new(string)); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects: 4,
		PendingSize:    770,
	})

	// delete a pending object
	if _, _, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "obj1"}); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects: 3,
		PendingSize:    670,
	})

	// creating then aborting a multipart upload leaves the counters unchanged
	uid2 := s3.NewUploadID()
	if err := store.CreateMultipartUpload(accessKeyID, bucket, "mp2", uid2, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AbortMultipartUpload(accessKeyID, bucket, "mp2", uid2); err != nil {
		t.Fatal(err)
	}

	assertStats(s3.UploadStats{
		PendingObjects: 3,
		PendingSize:    670,
	})
}

func TestMarkObjectPinned(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
		name        = "obj"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// pinning an object without an unpinned_objects row or any references
	// records it in orphaned_objects so the pin is eventually reverted
	missingID := types.Hash256(frand.Entropy256())
	if orphans, err := store.MarkObjectPinned(missingID); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no file orphans, got %+v", orphans)
	}
	if orphaned, err := store.OrphanedObjects(10); err != nil {
		t.Fatal(err)
	} else if len(orphaned) != 1 || orphaned[0] != missingID {
		t.Fatalf("expected %v in orphaned_objects, got %v", missingID, orphaned)
	}
	if err := store.RemoveOrphanedObject(missingID); err != nil {
		t.Fatal(err)
	}

	fileName := "obj.upload"
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, name, md5, nil, 42, &fileName); err != nil {
		t.Fatal(err)
	}
	sdkObj := sdk.Object{}
	sealed := sdkObj.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, name, "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	orphans, err := store.MarkObjectPinned(sealed.ID())
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 1 {
		t.Fatalf("expected 1 orphan, got %d", len(orphans))
	} else if orphans[0].Filename != fileName {
		t.Fatalf("expected orphan %q, got %q", fileName, orphans[0].Filename)
	} else if orphans[0].Size != 42 {
		t.Fatalf("expected size 42, got %d", orphans[0].Size)
	}

	// the unpinned_objects row is gone but the object is still referenced,
	// so a second call is a no-op and must not orphan the object
	if orphans, err := store.MarkObjectPinned(sealed.ID()); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no file orphans on second pin, got %+v", orphans)
	}
	if orphaned, err := store.OrphanedObjects(10); err != nil {
		t.Fatal(err)
	} else if len(orphaned) != 0 {
		t.Fatalf("expected no orphaned objects after second pin, got %v", orphaned)
	}

	// a shared filename is not returned for cleanup because another object
	// still references it
	otherMD5 := frand.Entropy128()
	otherMD52 := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, "a", otherMD5, nil, 10, &fileName); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.PutObject(accessKeyID, bucket, "b", otherMD52, nil, 10, &fileName); err != nil {
		t.Fatal(err)
	}
	sdkObjA := sdk.Object{}
	sealedA := sdkObjA.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "a", "", otherMD5, sealedA, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	orphans, err = store.MarkObjectPinned(sealedA.ID())
	if err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphan when filename is shared, got %+v", orphans)
	}

	// simulate a delete racing the pin: the object is deleted while the
	// indexer pin is in flight, removing the unpinned_objects row; the id
	// must end up in orphaned_objects so the pin gets reverted, and marking
	// pinned afterwards must succeed without disturbing that
	fnC := "c.upload"
	md5C := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, "c", md5C, nil, 7, &fnC); err != nil {
		t.Fatal(err)
	}
	sdkObjC := newTestObject()
	sealedC := sdkObjC.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "c", "", md5C, sealedC, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "c"}); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.MarkObjectPinned(sealedC.ID()); err != nil {
		t.Fatal(err)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no file orphans, got %+v", orphans)
	}
	if orphaned, err := store.OrphanedObjects(10); err != nil {
		t.Fatal(err)
	} else if len(orphaned) != 1 || orphaned[0] != sealedC.ID() {
		t.Fatalf("expected %v in orphaned_objects, got %v", sealedC.ID(), orphaned)
	}
}

func TestObjectsForPinning(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// empty store: no due rows, no next attempt
	due, err := store.ObjectsForPinning(time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("expected 0 due rows, got %d", len(due))
	}
	if _, ok, err := store.NextPinningAttempt(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected no next attempt when empty")
	}

	// add three uploaded-but-not-pinned objects; use newTestObject so each
	// gets a distinct sia_object_id (and therefore a distinct
	// unpinned_objects row)
	upload := func(name string) sdk.SealedObject {
		t.Helper()
		fn := name + ".upload"
		md5 := frand.Entropy128()
		if _, _, err := store.PutObject(accessKeyID, bucket, name, md5, nil, 1, &fn); err != nil {
			t.Fatal(err)
		}
		o := newTestObject()
		sealed := o.Seal(types.GeneratePrivateKey())
		if err := store.MarkObjectUploaded(bucket, name, "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		return sealed
	}

	sealedA := upload("a")
	sealedB := upload("b")
	sealedC := upload("c")

	// the default next_attempt_at is 0 (epoch), so all three are due now
	due, err = store.ObjectsForPinning(time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	} else if len(due) != 3 {
		t.Fatalf("expected 3 due rows, got %d", len(due))
	}

	// limit caps the result
	due, err = store.ObjectsForPinning(time.Now(), 2)
	if err != nil {
		t.Fatal(err)
	} else if len(due) != 2 {
		t.Fatalf("expected 2 due rows with limit, got %d", len(due))
	}

	// the SiaObject is populated correctly via the join with objects
	all, err := store.ObjectsForPinning(time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	var foundA *objects.UnpinnedObject
	for i, uo := range all {
		if uo.SiaObject.ID == sealedA.ID() {
			foundA = &all[i]
		}
	}
	if foundA == nil {
		t.Fatal("expected sealed A's id in due rows")
	}

	// push "a" into the future; only "b" and "c" remain due
	future := time.Now().Add(time.Hour)
	if err := store.RescheduleUnpinnedObject(sealedA.ID(), future); err != nil {
		t.Fatal(err)
	}
	due, err = store.ObjectsForPinning(time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	} else if len(due) != 2 {
		t.Fatalf("expected 2 due rows after rescheduling, got %d", len(due))
	}
	for _, uo := range due {
		if uo.SiaObject.ID == sealedA.ID() {
			t.Fatal("did not expect 'a' to be due after rescheduling")
		}
	}

	// after pinning "b" and "c", only the future row remains and
	// NextPinningAttempt reports its time
	if _, err := store.MarkObjectPinned(sealedB.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkObjectPinned(sealedC.ID()); err != nil {
		t.Fatal(err)
	}
	due, err = store.ObjectsForPinning(time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("expected 0 due rows after pinning, got %d", len(due))
	}
	next, ok, err := store.NextPinningAttempt()
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected next attempt to be set")
	} else if !next.Equal(future.Truncate(time.Second)) {
		t.Fatalf("expected next attempt %v, got %v", future.Truncate(time.Second), next)
	}

	// rescheduling a non-existent row returns ErrObjectNotFound
	if err := store.RescheduleUnpinnedObject(types.Hash256(frand.Entropy256()), time.Now()); !errors.Is(err, objects.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestScheduleObjectForReupload(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
		name        = "obj"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// no row to demote
	if err := store.ScheduleObjectForReupload(types.Hash256(frand.Entropy256())); !errors.Is(err, objects.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	fileName := "obj.upload"
	md5 := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, name, md5, nil, 7, &fileName); err != nil {
		t.Fatal(err)
	}
	sdkObj := sdk.Object{}
	sealed := sdkObj.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, name, "", md5, sealed, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// demote the row
	if err := store.ScheduleObjectForReupload(sealed.ID()); err != nil {
		t.Fatal(err)
	}

	// the unpinned_objects row is gone
	if _, ok, err := store.NextPinningAttempt(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected no remaining unpinned rows")
	}

	// the object reappears in the upload queue with sia_object_id cleared
	obj, err := store.GetObject(accessKeyID, bucket, name, s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.FileName == nil || *obj.FileName != fileName {
		t.Fatalf("expected filename %q to be preserved, got %v", fileName, obj.FileName)
	} else if obj.SiaObject != nil {
		t.Fatal("expected sia_object to be cleared after re-upload schedule")
	}

	uploads, err := store.ObjectsForUpload()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, u := range uploads {
		if u.Bucket == bucket && u.Name == name {
			found = true
		}
	}
	if !found {
		t.Fatal("expected demoted object to appear in upload queue")
	}

	// the prior upload is orphaned: a pin attempt may have succeeded in the
	// indexer without MarkObjectPinned committing, so the old id is
	// conservatively routed through the orphan path (unpinning never-pinned
	// data is a no-op)
	orphans, err := store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 || orphans[0] != sealed.ID() {
		t.Fatalf("expected demoted sealed ID %v to be orphaned, got %v", sealed.ID(), orphans)
	}
	if err := store.RemoveOrphanedObject(sealed.ID()); err != nil {
		t.Fatal(err)
	}

	// simulate a delete racing the demotion: the object is deleted after the
	// pin loop fetched the expired row but before it demotes it; the delete
	// removes the unpinned_objects row and orphans the id, so the demotion
	// must return ErrObjectNotFound (tolerated by the pin loop) and leave
	// the orphan record in place
	fileName2 := "obj2.upload"
	md52 := frand.Entropy128()
	if _, _, err := store.PutObject(accessKeyID, bucket, "obj2", md52, nil, 7, &fileName2); err != nil {
		t.Fatal(err)
	}
	sdkObj2 := newTestObject()
	sealed2 := sdkObj2.Seal(types.GeneratePrivateKey())
	if err := store.MarkObjectUploaded(bucket, "obj2", "", md52, sealed2, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "obj2"}); err != nil {
		t.Fatal(err)
	}
	if err := store.ScheduleObjectForReupload(sealed2.ID()); !errors.Is(err, objects.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
	orphans, err = store.OrphanedObjects(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 || orphans[0] != sealed2.ID() {
		t.Fatalf("expected deleted sealed ID %v to be orphaned, got %v", sealed2.ID(), orphans)
	}
}

func TestVersioning(t *testing.T) {
	const accessKeyID = testAccessKeyID

	store := initTestDB(t, zaptest.NewLogger(t))

	t.Run("BucketConfiguration", func(t *testing.T) {
		const bucket = "config"
		if err := store.CreateBucket(accessKeyID, bucket); err != nil {
			t.Fatal(err)
		}

		// an unconfigured bucket reports no status
		if status, err := store.GetBucketVersioning(accessKeyID, bucket); err != nil {
			t.Fatal(err)
		} else if status != "" {
			t.Fatalf("expected empty status, got %q", status)
		}

		// status round-trips through put/get
		for _, want := range []string{s3.VersioningStatusEnabled, s3.VersioningStatusSuspended} {
			if err := store.PutBucketVersioning(accessKeyID, bucket, want); err != nil {
				t.Fatal(err)
			}
			if got, err := store.GetBucketVersioning(accessKeyID, bucket); err != nil {
				t.Fatal(err)
			} else if got != want {
				t.Fatalf("expected status %q, got %q", want, got)
			}
		}
	})

	t.Run("EnabledLifecycle", func(t *testing.T) {
		const (
			bucket = "enabled"
			key    = "key"
		)
		if err := store.CreateBucket(accessKeyID, bucket); err != nil {
			t.Fatal(err)
		} else if err := store.PutBucketVersioning(accessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}

		put := func() string {
			t.Helper()
			v, _, err := store.PutObject(accessKeyID, bucket, key, frand.Entropy128(), nil, 1, new(string))
			if err != nil {
				t.Fatal(err)
			}
			return v
		}

		// two writes produce two distinct, non-empty version IDs
		v1 := put()
		v2 := put()
		if v1 == "" || v2 == "" {
			t.Fatalf("expected non-empty version IDs, got %q and %q", v1, v2)
		} else if v1 == v2 {
			t.Fatalf("expected distinct version IDs, both were %q", v1)
		}

		// the current version is the latest write, and the object reports as versioned
		if obj, err := store.GetObject(accessKeyID, bucket, key, s3.NoVersion(), nil); err != nil {
			t.Fatal(err)
		} else if obj.VersionID != v2 {
			t.Fatalf("expected current version %q, got %q", v2, obj.VersionID)
		} else if !obj.Versioned {
			t.Fatal("expected object to be reported as versioned")
		}

		// each version is independently retrievable
		if obj, err := store.GetObject(accessKeyID, bucket, key, s3.SpecificVersion(v1), nil); err != nil {
			t.Fatal(err)
		} else if obj.VersionID != v1 {
			t.Fatalf("expected version %q, got %q", v1, obj.VersionID)
		}

		// an unknown version is reported as missing
		if _, err := store.GetObject(accessKeyID, bucket, key, s3.SpecificVersion("does-not-exist"), nil); !errors.Is(err, s3errs.ErrNoSuchVersion) {
			t.Fatalf("expected ErrNoSuchVersion, got %v", err)
		}

		// a simple delete inserts a delete marker that becomes the current version
		marker, isDeleteMarker, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: key})
		if err != nil {
			t.Fatal(err)
		} else if !isDeleteMarker {
			t.Fatal("expected a delete marker on simple delete")
		} else if marker == "" {
			t.Fatal("expected a delete marker version ID")
		}
		if obj, err := store.GetObject(accessKeyID, bucket, key, s3.NoVersion(), nil); err != nil {
			t.Fatal(err)
		} else if !obj.IsDeleteMarker {
			t.Fatal("expected current version to be a delete marker")
		}

		// deleting the delete marker by version restores the previous version
		if _, _, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: key, VersionID: &marker}); err != nil {
			t.Fatal(err)
		}
		if obj, err := store.GetObject(accessKeyID, bucket, key, s3.NoVersion(), nil); err != nil {
			t.Fatal(err)
		} else if obj.IsDeleteMarker {
			t.Fatal("expected the object to be restored")
		} else if obj.VersionID != v2 {
			t.Fatalf("expected restored current version %q, got %q", v2, obj.VersionID)
		}

		// permanently deleting a specific version removes only that version
		if _, _, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: key, VersionID: &v1}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.GetObject(accessKeyID, bucket, key, s3.SpecificVersion(v1), nil); !errors.Is(err, s3errs.ErrNoSuchVersion) {
			t.Fatalf("expected ErrNoSuchVersion for deleted version, got %v", err)
		}
		if obj, err := store.GetObject(accessKeyID, bucket, key, s3.NoVersion(), nil); err != nil {
			t.Fatal(err)
		} else if obj.VersionID != v2 {
			t.Fatalf("expected remaining version %q to stay current, got %q", v2, obj.VersionID)
		}
	})

	t.Run("SuspendedNullVersion", func(t *testing.T) {
		const (
			bucket = "suspended"
			key    = "key"
		)
		if err := store.CreateBucket(accessKeyID, bucket); err != nil {
			t.Fatal(err)
		}

		// create a non-null version while enabled
		if err := store.PutBucketVersioning(accessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		v1, _, err := store.PutObject(accessKeyID, bucket, key, frand.Entropy128(), nil, 1, new(string))
		if err != nil {
			t.Fatal(err)
		} else if v1 == "" {
			t.Fatal("expected a version ID while enabled")
		}

		// suspended writes report no version and reuse the null version in place
		if err := store.PutBucketVersioning(accessKeyID, bucket, s3.VersioningStatusSuspended); err != nil {
			t.Fatal(err)
		}
		if v, _, err := store.PutObject(accessKeyID, bucket, key, frand.Entropy128(), nil, 1, new(string)); err != nil {
			t.Fatal(err)
		} else if v != "" {
			t.Fatalf("expected no version ID while suspended, got %q", v)
		}
		if _, _, err := store.PutObject(accessKeyID, bucket, key, frand.Entropy128(), nil, 1, new(string)); err != nil {
			t.Fatal(err)
		}

		// only the null version and the original non-null version remain
		result, err := store.ListObjectVersions(accessKeyID, bucket, s3.Prefix{}, s3.ListObjectVersionsPage{MaxKeys: 100})
		if err != nil {
			t.Fatal(err)
		} else if len(result.Versions) != 2 {
			t.Fatalf("expected 2 versions (null + original), got %d", len(result.Versions))
		}
		var nullCount int
		for _, v := range result.Versions {
			if v.VersionID == "" {
				nullCount++
			}
		}
		if nullCount != 1 {
			t.Fatalf("expected exactly one null version, got %d", nullCount)
		}

		// the null version is current and addressable as the empty version ID
		if obj, err := store.GetObject(accessKeyID, bucket, key, s3.NoVersion(), nil); err != nil {
			t.Fatal(err)
		} else if obj.VersionID != "" {
			t.Fatalf("expected the null version to be current, got %q", obj.VersionID)
		}
		if _, err := store.GetObject(accessKeyID, bucket, key, s3.SpecificVersion(""), nil); err != nil {
			t.Fatalf("expected the null version to be addressable, got %v", err)
		}
		// the original non-null version is still retrievable
		if obj, err := store.GetObject(accessKeyID, bucket, key, s3.SpecificVersion(v1), nil); err != nil {
			t.Fatal(err)
		} else if obj.VersionID != v1 {
			t.Fatalf("expected version %q, got %q", v1, obj.VersionID)
		}
	})
}

func TestListObjectVersions(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "versioned"
	)

	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	} else if err := store.PutBucketVersioning(accessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	put := func(key string) string {
		t.Helper()
		v, _, err := store.PutObject(accessKeyID, bucket, key, frand.Entropy128(), nil, 1, new(string))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	// three versions of "key" (newest last) plus a single-version "other"
	kv1 := put("key")
	kv2 := put("key")
	kv3 := put("key")
	ov1 := put("other")

	t.Run("Ordering", func(t *testing.T) {
		result, err := store.ListObjectVersions(accessKeyID, bucket, s3.Prefix{}, s3.ListObjectVersionsPage{MaxKeys: 100})
		if err != nil {
			t.Fatal(err)
		}

		// key ascending, then newest version first within a key
		want := []struct {
			key      string
			version  string
			isLatest bool
		}{
			{"key", kv3, true},
			{"key", kv2, false},
			{"key", kv1, false},
			{"other", ov1, true},
		}
		if len(result.Versions) != len(want) {
			t.Fatalf("expected %d versions, got %d", len(want), len(result.Versions))
		}
		for i, w := range want {
			got := result.Versions[i]
			if got.Key != w.key || got.VersionID != w.version {
				t.Fatalf("version %d: expected %s/%s, got %s/%s", i, w.key, w.version, got.Key, got.VersionID)
			} else if got.IsLatest != w.isLatest {
				t.Fatalf("version %d (%s/%s): expected IsLatest=%v, got %v", i, w.key, w.version, w.isLatest, got.IsLatest)
			}
		}
	})

	t.Run("VersionIDMarkerResumesMidKey", func(t *testing.T) {
		// resuming within "key" after its newest version yields the remaining two
		// versions of "key" followed by "other"
		result, err := store.ListObjectVersions(accessKeyID, bucket, s3.Prefix{}, s3.ListObjectVersionsPage{
			KeyMarker:       aws.String("key"),
			VersionIDMarker: aws.String(kv3),
			MaxKeys:         100,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"key/" + kv2, "key/" + kv1, "other/" + ov1}
		if len(result.Versions) != len(want) {
			t.Fatalf("expected %d versions, got %d", len(want), len(result.Versions))
		}
		for i, w := range want {
			if got := result.Versions[i].Key + "/" + result.Versions[i].VersionID; got != w {
				t.Fatalf("version %d: expected %s, got %s", i, w, got)
			}
		}
	})

	t.Run("Pagination", func(t *testing.T) {
		// a small max-keys forces truncation; the (key, version) cursor must
		// round-trip without dropping or repeating a version
		var got []string
		seen := map[string]bool{}
		page := s3.ListObjectVersionsPage{MaxKeys: 2}
		for {
			result, err := store.ListObjectVersions(accessKeyID, bucket, s3.Prefix{}, page)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range result.Versions {
				id := v.Key + "/" + v.VersionID
				if seen[id] {
					t.Fatalf("version returned twice: %s", id)
				}
				seen[id] = true
				got = append(got, id)
			}
			if !result.IsTruncated {
				break
			}
			// all versions here are non-null, so the wire-encoded markers equal
			// the internal IDs and can be fed straight back
			km, vm := result.NextKeyMarker, result.NextVersionIDMarker
			page.KeyMarker, page.VersionIDMarker = &km, &vm
		}
		want := []string{"key/" + kv3, "key/" + kv2, "key/" + kv1, "other/" + ov1}
		if len(got) != len(want) {
			t.Fatalf("expected %d versions, got %d", len(want), len(got))
		}
		for i, w := range want {
			if got[i] != w {
				t.Fatalf("version %d: expected %s, got %s", i, w, got[i])
			}
		}
	})

	t.Run("MaxKeysZero", func(t *testing.T) {
		result, err := store.ListObjectVersions(accessKeyID, bucket, s3.Prefix{}, s3.ListObjectVersionsPage{MaxKeys: 0})
		if err != nil {
			t.Fatal(err)
		} else if len(result.Versions) != 0 {
			t.Fatalf("expected no versions, got %d", len(result.Versions))
		} else if result.IsTruncated {
			t.Fatal("expected IsTruncated=false for max-keys=0")
		}
	})

	t.Run("DeleteMarkerListed", func(t *testing.T) {
		const bucket = "delete-marker"
		if err := store.CreateBucket(accessKeyID, bucket); err != nil {
			t.Fatal(err)
		} else if err := store.PutBucketVersioning(accessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		v, _, err := store.PutObject(accessKeyID, bucket, "k", frand.Entropy128(), nil, 1, new(string))
		if err != nil {
			t.Fatal(err)
		}
		marker, _, _, err := store.DeleteObject(accessKeyID, bucket, s3.ObjectID{Key: "k"})
		if err != nil {
			t.Fatal(err)
		}

		result, err := store.ListObjectVersions(accessKeyID, bucket, s3.Prefix{}, s3.ListObjectVersionsPage{MaxKeys: 100})
		if err != nil {
			t.Fatal(err)
		} else if len(result.Versions) != 2 {
			t.Fatalf("expected 2 versions, got %d", len(result.Versions))
		}
		// newest first: the delete marker is the current version
		dm, obj := result.Versions[0], result.Versions[1]
		if !dm.IsDeleteMarker || dm.VersionID != marker || !dm.IsLatest {
			t.Fatalf("expected current delete marker %q, got %+v", marker, dm)
		} else if obj.IsDeleteMarker || obj.VersionID != v {
			t.Fatalf("expected underlying object version %q, got %+v", v, obj)
		}
	})

	t.Run("DelimiterRollsUpCommonPrefixes", func(t *testing.T) {
		const bucket = "delimiter"
		if err := store.CreateBucket(accessKeyID, bucket); err != nil {
			t.Fatal(err)
		} else if err := store.PutBucketVersioning(accessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"dir/a", "dir/b", "top"} {
			if _, _, err := store.PutObject(accessKeyID, bucket, k, frand.Entropy128(), nil, 1, new(string)); err != nil {
				t.Fatal(err)
			}
		}

		result, err := store.ListObjectVersions(accessKeyID, bucket, s3.Prefix{
			Delimiter:    "/",
			HasDelimiter: true,
		}, s3.ListObjectVersionsPage{MaxKeys: 100})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.CommonPrefixes) != 1 || result.CommonPrefixes[0].Prefix != "dir/" {
			t.Fatalf("expected common prefix dir/, got %+v", result.CommonPrefixes)
		}
		if len(result.Versions) != 1 || result.Versions[0].Key != "top" {
			t.Fatalf("expected only top-level key 'top', got %+v", result.Versions)
		}
	})
}

func newTestObject() sdk.Object {
	obj := sdk.NewEmptyObject()
	ss := []slabs.SlabSlice{{EncryptionKey: frand.Entropy256(), Length: 1}}
	v := reflect.ValueOf(&obj).Elem()
	f := v.FieldByName("slabs")
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(ss))
	return obj
}

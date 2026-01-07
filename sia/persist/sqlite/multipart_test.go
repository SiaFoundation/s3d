package sqlite

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestCreateMultipartUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	store := initTestDB(t, zap.NewNop())

	// assert [s3errs.ErrNoSuchBucket] for unknown bucket - then create it
	if err := store.CreateMultipartUpload(bucket, object, s3.NewUploadID(), nil); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatal(err)
	} else if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create multipart upload
	uid1 := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid1, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.assertCount(1, "multipart_uploads")

	// abort the multipart upload
	if err := store.AbortMultipartUpload(bucket, object, uid1); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "multipart_uploads")
}

func TestAddMultipartPart(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
		location    = "part-location"
	)

	var contentMD5 [16]byte
	frand.Read(contentMD5[:])

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if _, err := store.AddMultipartPart(bucket, object, s3.NewUploadID(), location, 1, contentMD5, 0); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add a part (assert no error on duplicate part addition)
	prev, err := store.AddMultipartPart(bucket, object, uid, location, 1, contentMD5, 0)
	if err != nil {
		t.Fatal(err)
	} else if prev != "" {
		t.Fatal("expected empty previous filename for first part upload", prev)
	}

	prev, err = store.AddMultipartPart(bucket, object, uid, location, 1, contentMD5, 0)
	if err != nil {
		t.Fatal(err)
	} else if prev == "" || prev != location {
		t.Fatal("expected previous filename to be returned on part overwrite", prev)
	}

	store.assertCount(1, "parts")
}

func TestAbortMultipartUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
		filename    = "part-filename"
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if err := store.AbortMultipartUpload(bucket, object, s3.NewUploadID()); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	var contentMD5 [16]byte
	frand.Read(contentMD5[:])

	// add a part
	if _, err := store.AddMultipartPart(bucket, object, uid, filename, 1, contentMD5, 0); err != nil {
		t.Fatal(err)
	}

	// abort the upload
	if err := store.AbortMultipartUpload(bucket, object, uid); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "multipart_uploads")
	store.assertCount(0, "parts")

	// assert [s3errs.ErrNoSuchUpload] for aborted upload
	if err := store.AbortMultipartUpload(bucket, object, uid); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}
}

func TestHasMultipartUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if err := store.HasMultipartUpload(bucket, object, s3.NewUploadID()); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	// assert no error for existing upload
	if err := store.HasMultipartUpload(bucket, object, uid); err != nil {
		t.Fatal(err)
	}
}

func TestListParts(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// assert [s3errs.ErrNoSuchUpload] for unknown upload ID
	if _, err := store.ListParts(bucket, object, s3.NewUploadID(), 0, 1000); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add finalized parts
	const totalParts = 5
	for i := 1; i <= totalParts; i++ {
		_, err := store.AddMultipartPart(bucket, object, uid, "", i, frand.Entropy128(), int64(frand.Uint64n(100)+1))
		if err != nil {
			t.Fatal(err)
		}
	}

	// list parts
	result, err := store.ListParts(bucket, object, uid, 0, 1000)
	if err != nil {
		t.Fatal(err)
	} else if result.IsTruncated {
		t.Fatal("expected non-truncated result")
	} else if result.NextPartNumberMarker != "" {
		t.Fatal("expected empty NextPartNumberMarker")
	} else if int64(len(result.Parts)) != totalParts {
		t.Fatalf("expected %d parts, got %d", totalParts, len(result.Parts))
	}
	for i, p := range result.Parts {
		expectedPartNumber := i + 1
		if p.PartNumber != expectedPartNumber {
			t.Fatalf("part %d: expected part number %d, got %d", i, expectedPartNumber, p.PartNumber)
		}
	}

	// paginate through parts
	var partNumberMarker int
	for partNumberMarker < totalParts {
		result, err := store.ListParts(bucket, object, uid, partNumberMarker, 1)
		if err != nil {
			t.Fatal(err)
		} else if !result.IsTruncated && partNumberMarker < totalParts-1 {
			t.Fatal("expected truncated result")
		} else if result.IsTruncated && partNumberMarker == totalParts-1 {
			t.Fatal("expected non-truncated result")
		} else if int64(len(result.Parts)) != 1 {
			t.Fatalf("expected 1 part, got %d", len(result.Parts))
		} else if result.Parts[0].PartNumber != partNumberMarker+1 {
			t.Fatalf("expected part number %d, got %d", partNumberMarker+1, result.Parts[0].PartNumber)
		}
		partNumberMarker = result.Parts[0].PartNumber
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	uid := s3.NewUploadID()
	err := store.CreateMultipartUpload(bucket, object, uid, nil)
	if err != nil {
		t.Fatal(err)
	}

	partMD5A := frand.Entropy128()
	partMD5B := frand.Entropy128()
	parts := []objects.Part{
		{PartNumber: 1, Filename: "part-1", Size: s3.MinUploadPartSize, ContentMD5: partMD5A},
		{PartNumber: 2, Filename: "part-2", Size: 5, ContentMD5: partMD5B}, // Last part can be any size
	}

	// add parts to the upload
	for _, part := range parts {
		if _, err := store.AddMultipartPart(bucket, object, uid, part.Filename, part.PartNumber, part.ContentMD5, part.Size); err != nil {
			t.Fatal(err)
		}
	}

	objID := frand.Entropy256()
	contentMD5 := frand.Entropy128()
	totalSize := s3.MinUploadPartSize + 5 // part1 + part2
	if err := store.CompleteMultipartUpload(bucket, object, uid, objID, contentMD5, int64(totalSize)); err != nil {
		t.Fatal(err)
	}

	store.assertCount(0, "multipart_uploads")
	store.assertCount(len(parts), "parts")

	rows, err := store.db.Query(`SELECT part_number, content_md5, offset, content_length FROM parts WHERE object_bucket_id IS NOT NULL ORDER BY part_number`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var offsets []int64
	for rows.Next() {
		var partNumber int
		var length int64
		var offset int64
		var contentMD5 sqlMD5
		if err := rows.Scan(&partNumber, &contentMD5, &offset, &length); err != nil {
			t.Fatal(err)
		}
		idx := partNumber - 1
		if idx < 0 || idx >= len(parts) {
			t.Fatalf("unexpected part number %d", partNumber)
		}
		if parts[idx].Size != int64(length) {
			t.Fatalf("expected length %d, got %d", parts[idx].Size, length)
		}
		if parts[idx].ContentMD5 != [16]byte(contentMD5) {
			t.Fatalf("expected MD5 %x, got %x", parts[idx].ContentMD5, contentMD5)
		}
		offsets = append(offsets, offset)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != len(parts) {
		t.Fatalf("expected %d parts, got %d", len(parts), len(offsets))
	}
	if offsets[0] != 0 || offsets[1] != parts[0].Size {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
}

func TestListMultipartUploads(t *testing.T) {
	store := initTestDB(t, zap.NewNop())

	setupBucket := func(keys []string) (string, map[string][]s3.UploadID) {
		t.Helper()

		entropy := frand.Entropy128()
		bucket := hex.EncodeToString(entropy[:8])
		if err := store.CreateBucket("", bucket); err != nil {
			t.Fatal(err)
		}

		uids := make(map[string][]s3.UploadID)
		for _, key := range keys {
			uid := s3.NewUploadID()
			err := store.CreateMultipartUpload(bucket, key, uid, nil)
			if err != nil {
				t.Fatal(err)
			}
			uids[key] = append(uids[key], uid)
			sort.Slice(uids[key], func(i, j int) bool {
				return bytes.Compare(uids[key][i][:], uids[key][j][:]) < 0
			})
		}

		return bucket, uids
	}

	assertKeys := func(t *testing.T, cIdx int, got []s3.MultipartUploadInfo, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("case %d: expected %d uploads, got %d", cIdx, len(want), len(got))
		}
		for i := range got {
			if got[i].Key != want[i] {
				t.Fatalf("case %d: expected key %q at index %d, got %q", cIdx, want[i], i, got[i].Key)
			}
		}
	}

	assertCommonPrefixes := func(t *testing.T, cIdx int, got []string, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("case %d: expected %d common prefixes, got %d", cIdx, len(want), len(got))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("case %d: expected common prefix %q at index %d, got %q", cIdx, want[i], i, got[i])
			}
		}
	}

	const (
		noDelim    = ""
		slashDelim = "/"
		otherDelim = "X"
	)

	// no delimiter tests
	t.Run(fmt.Sprintf("%s_delimiter_none", t.Name()), func(t *testing.T) {
		bucket, uids := setupBucket([]string{
			"foo",
			"foo", // multiple uploads for same key
			"foo/bar",
			"ůťḟ８string",
			"caseSensitive",
		})
		orderedKeys := []string{"caseSensitive", "foo", "foo", "foo/bar", "ůťḟ８string"}

		cases := []struct {
			prefix         string
			keyMarker      string
			uploadIDMarker s3.UploadID
			expectedKeys   []string
		}{
			// no filters
			{expectedKeys: orderedKeys},
			// prefix filters
			{prefix: "caseS", expectedKeys: []string{"caseSensitive"}},
			{prefix: "cases", expectedKeys: nil},
			{prefix: "ůť", expectedKeys: []string{"ůťḟ８string"}},
			{prefix: "utf8", expectedKeys: nil},
			// key markers
			{keyMarker: "caseSensitive", expectedKeys: []string{"foo", "foo", "foo/bar", "ůťḟ８string"}},
			{keyMarker: "foo", expectedKeys: []string{"foo/bar", "ůťḟ８string"}},
			{keyMarker: "foo/bar", expectedKeys: []string{"ůťḟ８string"}},
			{keyMarker: "ůťḟ８string", expectedKeys: nil},
			// combined prefix and key markers
			{prefix: "f", keyMarker: "foo", expectedKeys: []string{"foo/bar"}},
			{prefix: "f", keyMarker: "foo/bar", expectedKeys: nil},
			// uploadID markers
			{keyMarker: "foo", uploadIDMarker: uids["foo"][0], expectedKeys: []string{"foo", "foo/bar", "ůťḟ８string"}},
			{keyMarker: "foo", uploadIDMarker: uids["foo"][1], expectedKeys: []string{"foo/bar", "ůťḟ８string"}},
			{keyMarker: "", uploadIDMarker: uids["foo"][1], expectedKeys: orderedKeys}, // uploadID marker ignored without key marker
		}
		for i, tc := range cases {
			prefix := s3.Prefix{
				HasPrefix:    tc.prefix != "",
				Prefix:       tc.prefix,
				HasDelimiter: noDelim != "",
				Delimiter:    noDelim,
			}
			page := s3.ListMultipartUploadsPage{
				KeyMarker:      tc.keyMarker,
				UploadIDMarker: tc.uploadIDMarker,
				MaxUploads:     10,
			}
			result, err := store.ListMultipartUploads(bucket, prefix, page)
			if err != nil {
				t.Fatal(err)
			}
			assertKeys(t, i, result.Uploads, tc.expectedKeys)
			assertCommonPrefixes(t, i, result.CommonPrefixes, nil)
		}

		// paginate through results
		var prevKeyMarker string
		var prevUploadIDMarker s3.UploadID
		for i := range orderedKeys {
			prefix := s3.Prefix{HasDelimiter: false}
			page := s3.ListMultipartUploadsPage{
				KeyMarker:      prevKeyMarker,
				UploadIDMarker: prevUploadIDMarker,
				MaxUploads:     1,
			}
			result, err := store.ListMultipartUploads(bucket, prefix, page)
			if err != nil {
				t.Fatal(err)
			} else if !result.IsTruncated && i != len(orderedKeys)-1 {
				t.Fatal("expected truncated result")
			} else if len(result.Uploads) != 1 {
				t.Fatalf("expected 1 upload, got %d", len(result.Uploads))
			}

			assertKeys(t, i, result.Uploads, orderedKeys[i:i+1])
			assertCommonPrefixes(t, i, result.CommonPrefixes, nil)

			prevKeyMarker = result.NextKeyMarker
			prevUploadIDMarker = result.NextUploadIDMarker
		}
	})

	// slash delimiter tests
	t.Run(fmt.Sprintf("%s_delimiter_slash", t.Name()), func(t *testing.T) {
		bucket, _ := setupBucket([]string{
			"foo",
			"foo/bar",
			"foo/bar/baz",
			"foo/baz/",
			"ůťḟ８/string",
			"ůťḟ８/another",
			"caseSensitive",
			"double//slash",
		})

		cases := []struct {
			prefix           string
			keyMarker        string
			uploadIDMarker   s3.UploadID
			maxUploads       int64
			expectedKeys     []string
			expectedPrefixes []string
		}{
			// no filters
			{expectedKeys: []string{"caseSensitive", "foo"}, expectedPrefixes: []string{"double/", "foo/", "ůťḟ８/"}},
			{expectedKeys: []string{"caseSensitive"}, expectedPrefixes: []string{"double/"}, maxUploads: 2},
			{expectedKeys: []string{"caseSensitive", "foo"}, expectedPrefixes: []string{"double/"}, maxUploads: 3},
			// prefix filters
			{prefix: "f", expectedKeys: []string{"foo"}, expectedPrefixes: []string{"foo/"}},
			{prefix: "foo/", expectedKeys: []string{"foo/bar"}, expectedPrefixes: []string{"foo/bar/", "foo/baz/"}},
			{prefix: "foo/bar", expectedKeys: []string{"foo/bar"}, expectedPrefixes: []string{"foo/bar/"}},
			{prefix: "ůť", expectedKeys: nil, expectedPrefixes: []string{"ůťḟ８/"}},
			{prefix: "ůťḟ８/", expectedKeys: []string{"ůťḟ８/another", "ůťḟ８/string"}, expectedPrefixes: nil},
			// key markers
			{keyMarker: "caseSensitive", expectedKeys: []string{"foo"}, expectedPrefixes: []string{"double/", "foo/", "ůťḟ８/"}},
			{keyMarker: "ůťḟ８/another", expectedKeys: nil, expectedPrefixes: nil},
			// combined prefix and key markers
			{prefix: "f", keyMarker: "caseSensitive", expectedKeys: []string{"foo"}, expectedPrefixes: []string{"foo/"}},
			{prefix: "foo/", keyMarker: "foo/bar", expectedKeys: nil, expectedPrefixes: []string{"foo/bar/", "foo/baz/"}},
			{prefix: "ůťḟ８/", keyMarker: "ůťḟ８/another", expectedKeys: []string{"ůťḟ８/string"}, expectedPrefixes: nil},
		}

		for i, tc := range cases {
			if tc.maxUploads == 0 {
				tc.maxUploads = 10
			}
			prefix := s3.Prefix{
				HasPrefix:    tc.prefix != "",
				Prefix:       tc.prefix,
				HasDelimiter: true,
				Delimiter:    slashDelim,
			}
			page := s3.ListMultipartUploadsPage{
				KeyMarker:      tc.keyMarker,
				UploadIDMarker: tc.uploadIDMarker,
				MaxUploads:     tc.maxUploads,
			}
			result, err := store.ListMultipartUploads(bucket, prefix, page)
			if err != nil {
				t.Fatal(err)
			}
			assertKeys(t, i, result.Uploads, tc.expectedKeys)
			assertCommonPrefixes(t, i, result.CommonPrefixes, tc.expectedPrefixes)
		}

		// paginate through results of the same bucket to ensure we advance past common prefixes
		prefix := s3.Prefix{HasDelimiter: true, Delimiter: slashDelim}
		first, err := store.ListMultipartUploads(bucket, prefix, s3.ListMultipartUploadsPage{
			MaxUploads: 1,
		})
		if err != nil {
			t.Fatal(err)
		} else if !first.IsTruncated {
			t.Fatal("unexpected")
		} else if len(first.Uploads) != 1 || first.Uploads[0].Key != "caseSensitive" {
			t.Fatalf("expected caseSensitive, got %+v", first.Uploads)
		}

		second, err := store.ListMultipartUploads(bucket, prefix, s3.ListMultipartUploadsPage{
			KeyMarker:      first.NextKeyMarker,
			UploadIDMarker: first.NextUploadIDMarker,
			MaxUploads:     1,
		})
		if err != nil {
			t.Fatal(err)
		} else if !second.IsTruncated {
			t.Fatal("unexpected")
		} else if len(second.CommonPrefixes) != 1 || second.CommonPrefixes[0] != "double/" {
			t.Fatalf("expected common prefix double/, got %+v", second.CommonPrefixes)
		}

		third, err := store.ListMultipartUploads(bucket, prefix, s3.ListMultipartUploadsPage{
			KeyMarker:      second.NextKeyMarker,
			UploadIDMarker: second.NextUploadIDMarker,
			MaxUploads:     1,
		})
		if err != nil {
			t.Fatal(err)
		} else if len(third.Uploads) != 1 || third.Uploads[0].Key != "foo" {
			t.Fatalf("expected third page to advance past common prefix and return upload foo, got %+v", third.Uploads)
		}
	})

	// other delimiter tests
	t.Run(fmt.Sprintf("%s_delimiter_other", t.Name()), func(t *testing.T) {
		bucket, _ := setupBucket([]string{
			"foo",
			"fooXbar",
			"fooXbazX",
			"ůťḟ８Xstring",
			"ůťḟ８Xanother",
			"caseSensitive",
			"doubleXXslash",
		})

		cases := []struct {
			prefix           string
			keyMarker        string
			uploadIDMarker   s3.UploadID
			expectedKeys     []string
			expectedPrefixes []string
		}{
			// no filters
			{expectedKeys: []string{"caseSensitive", "foo"}, expectedPrefixes: []string{"doubleX", "fooX", "ůťḟ８X"}},
			// prefix filters
			{prefix: "f", expectedKeys: []string{"foo"}, expectedPrefixes: []string{"fooX"}},
			{prefix: "fooX", expectedKeys: []string{"fooXbar"}, expectedPrefixes: []string{"fooXbazX"}},
			{prefix: "ůť", expectedKeys: nil, expectedPrefixes: []string{"ůťḟ８X"}},
			{prefix: "ůťḟ８X", expectedKeys: []string{"ůťḟ８Xanother", "ůťḟ８Xstring"}, expectedPrefixes: nil},
			// key markers
			{keyMarker: "caseSensitive", expectedKeys: []string{"foo"}, expectedPrefixes: []string{"doubleX", "fooX", "ůťḟ８X"}},
			{keyMarker: "ůťḟ８Xanother", expectedKeys: nil, expectedPrefixes: nil},
			// combined prefix and key markers
			{prefix: "f", keyMarker: "caseSensitive", expectedKeys: []string{"foo"}, expectedPrefixes: []string{"fooX"}},
			{prefix: "fooX", keyMarker: "fooXbar", expectedKeys: nil, expectedPrefixes: []string{"fooXbazX"}},
			{prefix: "ůťḟ８X", keyMarker: "ůťḟ８Xanother", expectedKeys: []string{"ůťḟ８Xstring"}, expectedPrefixes: nil},
		}

		for i, tc := range cases {
			prefix := s3.Prefix{
				HasPrefix:    tc.prefix != "",
				Prefix:       tc.prefix,
				HasDelimiter: true,
				Delimiter:    otherDelim,
			}
			page := s3.ListMultipartUploadsPage{
				KeyMarker:      tc.keyMarker,
				UploadIDMarker: tc.uploadIDMarker,
				MaxUploads:     10,
			}
			result, err := store.ListMultipartUploads(bucket, prefix, page)
			if err != nil {
				t.Fatal(err)
			}
			assertKeys(t, i, result.Uploads, tc.expectedKeys)
			assertCommonPrefixes(t, i, result.CommonPrefixes, tc.expectedPrefixes)
		}
	})

	// walk large keyspace
	t.Run(fmt.Sprintf("%s_walk_large_keyspace", t.Name()), func(t *testing.T) {
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

		var keys []string
		for range numKeys {
			keys = append(keys, randomPath(minLength, maxLength, maxDepth, alphabet, delimiter))
		}
		bucket, _ := setupBucket(keys)

		type page struct {
			prefix         string
			keyMarker      string
			uploadIDMarker s3.UploadID
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
			prefix := s3.Prefix{
				HasPrefix:    pg.prefix != "",
				Prefix:       pg.prefix,
				HasDelimiter: true,
				Delimiter:    delimiter,
			}
			listPage := s3.ListMultipartUploadsPage{
				KeyMarker:      pg.keyMarker,
				UploadIDMarker: pg.uploadIDMarker,
				MaxUploads:     maxKeys,
			}
			res, err := store.ListMultipartUploads(bucket, prefix, listPage)
			if err != nil {
				t.Fatal(err)
			}
			visited += len(res.Uploads)

			// push subdirectories
			for _, cp := range res.CommonPrefixes {
				if _, ok := seen[cp]; ok {
					t.Fatalf("already seen common prefix %q", cp)
				}
				seen[cp] = struct{}{}
				stack = append(stack, page{prefix: cp})
			}

			// re-enqueue if truncated
			if res.IsTruncated {
				stack = append(stack, page{
					prefix:         pg.prefix,
					keyMarker:      res.NextKeyMarker,
					uploadIDMarker: res.NextUploadIDMarker,
				})
			}
		}

		if visited != numKeys {
			t.Fatalf("expected to visit %d uploads, visited %d", numKeys, visited)
		}
	})
}

func BenchmarkListMultipartUploads(b *testing.B) {
	const (
		numKeys     = 1_000_000
		maxKeys     = 1000
		maxDepth    = 4
		minLength   = 4
		maxLength   = 10
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		slashDelim  = "/"
	)

	var (
		alphabet = []rune("abcde")
		start    = time.Now()
	)

	// create bucket
	store := initTestDB(b, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, "test-bucket"); err != nil {
		b.Fatal(err)
	}

	// create multipart uploads
	err := store.transaction(func(tx *txn) error {
		bid, err := bucketID(tx, bucket)
		if err != nil {
			return err
		}

		now := time.Now()
		for range numKeys {
			key := randomPath(minLength, maxLength, maxDepth, alphabet, slashDelim)
			uploadID := s3.NewUploadID()
			_, err = tx.Exec(`
				INSERT INTO multipart_uploads (bucket_id, name, upload_id, metadata, created_at)
				VALUES ($1, $2, $3, $4, $5)
			`, bid, key, sqlUploadID(uploadID), "{}", sqlTime(now))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	// optimize database for benchmarking
	_, err1 := store.db.Exec(`VACUUM;`)
	_, err2 := store.db.Exec(`ANALYZE;`)
	if err1 != nil || err2 != nil {
		b.Fatal("failed to optimize database for benchmarking")
	}

	b.Logf("setup took %s, starting benchmarks...", time.Since(start))

	// benchmark no-delimiter with random prefix
	b.Run("no-delim", func(b *testing.B) {
		var consec bool
		for b.Loop() {
			prefix := randomPath(1, minLength, maxDepth, alphabet, "")
			prefixArg := s3.Prefix{
				HasPrefix: prefix != "",
				Prefix:    prefix,
			}
			listPage := s3.ListMultipartUploadsPage{
				MaxUploads: maxKeys,
			}
			res, err := store.ListMultipartUploads(bucket, prefixArg, listPage)
			if err != nil {
				b.Fatal(err)
			} else if len(res.Uploads)+len(res.CommonPrefixes) == 0 {
				if consec {
					b.Fatalf("no results for two consecutive iterations, last prefix: %q", prefix)
				}
				consec = true
			} else {
				consec = false
			}
		}
	})

	// benchmark / delimiter with random prefix
	b.Run("slash-delim-no-prefix", func(b *testing.B) {
		for b.Loop() {
			prefix := s3.Prefix{
				HasDelimiter: true,
				Delimiter:    slashDelim,
			}
			listPage := s3.ListMultipartUploadsPage{
				MaxUploads: maxKeys,
			}
			res, err := store.ListMultipartUploads(bucket, prefix, listPage)
			if err != nil {
				b.Fatal(err)
			} else if len(res.Uploads)+len(res.CommonPrefixes) == 0 {
				b.Fatal("no results for empty prefix")
			}
		}
	})

	// benchmark / delimiter with random prefix
	b.Run("slash-delim-rand-prefix", func(b *testing.B) {
		var consec bool
		for b.Loop() {
			prefix := randomPath(1, minLength, maxDepth, alphabet, slashDelim)
			prefixArg := s3.Prefix{
				HasPrefix:    prefix != "",
				Prefix:       prefix,
				HasDelimiter: true,
				Delimiter:    slashDelim,
			}
			listPage := s3.ListMultipartUploadsPage{
				MaxUploads: maxKeys,
			}
			res, err := store.ListMultipartUploads(bucket, prefixArg, listPage)
			if err != nil {
				b.Fatal(err)
			} else if len(res.Uploads)+len(res.CommonPrefixes) == 0 {
				if consec {
					b.Fatalf("no results for two consecutive iterations, last prefix: %q", prefix)
				}
				consec = true
			} else {
				consec = false
			}
		}
	})

	// benchmark / delimiter with random prefix and paging
	b.Run("slash-delim-paging", func(b *testing.B) {
		var paged int
		var prevKeyMarker, prefix string
		var prevUploadIDMarker s3.UploadID
		for b.Loop() {
			prefixArg := s3.Prefix{
				HasPrefix:    prefix != "",
				Prefix:       prefix,
				HasDelimiter: true,
				Delimiter:    slashDelim,
			}
			listPage := s3.ListMultipartUploadsPage{
				KeyMarker:      prevKeyMarker,
				UploadIDMarker: prevUploadIDMarker,
				MaxUploads:     maxKeys,
			}
			res, err := store.ListMultipartUploads(bucket, prefixArg, listPage)
			if err != nil {
				b.Fatal(err)
			} else if res.IsTruncated {
				paged++
				prevUploadIDMarker = res.NextUploadIDMarker
				prevKeyMarker = res.NextKeyMarker
				continue
			} else if len(res.CommonPrefixes) > 0 {
				prefix = res.CommonPrefixes[len(res.CommonPrefixes)-1]
				continue
			}

			prefix = ""
			prevKeyMarker = ""
			prevUploadIDMarker = [16]byte{}
		}
		if paged == 0 {
			b.Fatal("no pagination occurred during benchmark")
		}
	})

	// benchmark random delimiter with random prefix
	b.Run("random-delim", func(b *testing.B) {
		var consec bool
		for b.Loop() {
			delim := string(alphabet[frand.Intn(len(alphabet))])
			prefix := randomPath(1, minLength, maxDepth, alphabet, delim)
			prefixArg := s3.Prefix{
				HasPrefix:    prefix != "",
				Prefix:       prefix,
				HasDelimiter: true,
				Delimiter:    delim,
			}
			listPage := s3.ListMultipartUploadsPage{
				MaxUploads: maxKeys,
			}
			res, err := store.ListMultipartUploads(bucket, prefixArg, listPage)
			if err != nil {
				b.Fatal(err)
			} else if len(res.Uploads)+len(res.CommonPrefixes) == 0 {
				if consec {
					b.Fatalf("no results for two consecutive iterations, last prefix: %q", prefix)
				}
				consec = true
			} else {
				consec = false
			}
		}
	})
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

package sqlite

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
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
	if _, err := store.CreateMultipartUpload(bucket, object, nil); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatal(err)
	} else if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create multipart upload (and assert no error on duplicate creation)
	uid1, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}
	uid2, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if uid1 == uid2 {
		t.Fatal("expected unique upload IDs")
	}
	store.assertCount(2, "multipart_uploads")
}

func TestAddMultipartPart(t *testing.T) {
	const (
		unknownUID  = "a0188aceb938ca67b1d8ac03dfd361e9"
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
	if err := store.AddMultipartPart(bucket, object, unknownUID, 1); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add a part (assert no error on duplicate part addition)
	if err := store.AddMultipartPart(bucket, object, uid, 1); err != nil {
		t.Fatal(err)
	} else if err := store.AddMultipartPart(bucket, object, uid, 1); err != nil {
		t.Fatal(err)
	}
	store.assertCount(1, "multipart_parts")
}

func TestAbortMultipartUpload(t *testing.T) {
	const (
		unknownUID  = "a0188aceb938ca67b1d8ac03dfd361e9"
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
	if err := store.AbortMultipartUpload(bucket, object, unknownUID); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}

	// create multipart upload
	uid, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	}

	// add a part
	if err := store.AddMultipartPart(bucket, object, uid, 1); err != nil {
		t.Fatal(err)
	}

	// abort the upload
	if err := store.AbortMultipartUpload(bucket, object, uid); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "multipart_uploads")
	store.assertCount(0, "multipart_parts")

	// assert [s3errs.ErrNoSuchUpload] for aborted upload
	if err := store.AbortMultipartUpload(bucket, object, uid); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatal(err)
	}
}

func TestListMultipartUploads(t *testing.T) {
	store := initTestDB(t, zap.NewNop())

	setupBucket := func(keys []string) (string, map[string][]string) {
		t.Helper()

		entropy := frand.Entropy128()
		bucket := hex.EncodeToString(entropy[:8])
		if err := store.CreateBucket("", bucket); err != nil {
			t.Fatal(err)
		}

		uids := make(map[string][]string)
		for _, key := range keys {
			uid, err := store.CreateMultipartUpload(bucket, key, nil)
			if err != nil {
				t.Fatal(err)
			}
			uids[key] = append(uids[key], uid)
			sort.Strings(uids[key])
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
			uploadIDMarker string
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
			result, err := store.ListMultipartUploads(bucket, tc.prefix, noDelim, tc.keyMarker, tc.uploadIDMarker, 10)
			if err != nil {
				t.Fatal(err)
			}
			assertKeys(t, i, result.Uploads, tc.expectedKeys)
			assertCommonPrefixes(t, i, result.CommonPrefixes, nil)
		}

		// paginate through results
		var prevKeyMarker, prevUploadIDMarker string
		for i := range orderedKeys {
			result, err := store.ListMultipartUploads(bucket, "", noDelim, prevKeyMarker, prevUploadIDMarker, 1)
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
			uploadIDMarker   string
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
			result, err := store.ListMultipartUploads(bucket, tc.prefix, slashDelim, tc.keyMarker, tc.uploadIDMarker, tc.maxUploads)
			if err != nil {
				t.Fatal(err)
			}
			assertKeys(t, i, result.Uploads, tc.expectedKeys)
			assertCommonPrefixes(t, i, result.CommonPrefixes, tc.expectedPrefixes)
		}

		// paginate through results of the same bucket to ensure we advance past common prefixes
		first, err := store.ListMultipartUploads(bucket, "", slashDelim, "", "", 1)
		if err != nil {
			t.Fatal(err)
		} else if !first.IsTruncated {
			t.Fatal("unexpected")
		} else if len(first.Uploads) != 1 || first.Uploads[0].Key != "caseSensitive" {
			t.Fatalf("expected caseSensitive, got %+v", first.Uploads)
		}

		second, err := store.ListMultipartUploads(bucket, "", slashDelim, first.NextKeyMarker, first.NextUploadIDMarker, 1)
		if err != nil {
			t.Fatal(err)
		} else if !second.IsTruncated {
			t.Fatal("unexpected")
		} else if len(second.CommonPrefixes) != 1 || second.CommonPrefixes[0] != "double/" {
			t.Fatalf("expected common prefix double/, got %+v", second.CommonPrefixes)
		}

		third, err := store.ListMultipartUploads(bucket, "", slashDelim, second.NextKeyMarker, second.NextUploadIDMarker, 1)
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
			uploadIDMarker   string
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
			result, err := store.ListMultipartUploads(bucket, tc.prefix, otherDelim, tc.keyMarker, tc.uploadIDMarker, 10)
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
			alphabet = []rune("abc")
		)

		var keys []string
		for range numKeys {
			keys = append(keys, randomPath(minLength, maxLength, maxDepth, alphabet))
		}
		bucket, _ := setupBucket(keys)

		type page struct {
			prefix         string
			keyMarker      string
			uploadIDMarker string
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
			res, err := store.ListMultipartUploads(bucket, pg.prefix, slashDelim, pg.keyMarker, pg.uploadIDMarker, maxKeys)
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

	// populate database with multipart uploads
	for range numKeys {
		key := randomPath(minLength, maxLength, maxDepth, alphabet)
		_, err := store.CreateMultipartUpload(bucket, key, nil)
		if err != nil {
			b.Fatal(err)
		}
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
		var none int
		for b.Loop() {
			prefix := randomPath(1, minLength, maxDepth, alphabet)
			res, err := store.ListMultipartUploads(bucket, prefix, "", "", "", maxKeys)
			if err != nil {
				b.Fatal(err)
			} else if len(res.Uploads)+len(res.CommonPrefixes) == 0 {
				none++
				if none == 3 {
					b.Fatalf("no results for 3 consecutive iterations, last prefix: %q", prefix)
				}
			} else {
				none = 0
			}
		}
	})

	// benchmark / delimiter with random prefix
	b.Run("slash-delim", func(b *testing.B) {
		var none int
		for b.Loop() {
			prefix := randomPath(1, minLength, maxDepth, alphabet)
			res, err := store.ListMultipartUploads(bucket, prefix, slashDelim, "", "", maxKeys)
			if err != nil {
				b.Fatal(err)
			} else if len(res.Uploads)+len(res.CommonPrefixes) == 0 {
				none++
				if none == 3 {
					b.Fatalf("no results for 3 consecutive iterations, last prefix: %q", prefix)
				}
			} else {
				none = 0
			}
		}
	})

	// benchmark random delimiter with random prefix
	b.Run("random-delim", func(b *testing.B) {
		var none int
		for b.Loop() {
			prefix := randomPath(1, minLength, maxDepth, alphabet)
			delimiter := string(alphabet[frand.Intn(len(alphabet))])
			res, err := store.ListMultipartUploads(bucket, prefix, delimiter, "", "", maxKeys)
			if err != nil {
				b.Fatal(err)
			} else if len(res.Uploads)+len(res.CommonPrefixes) == 0 {
				none++
				if none == 3 {
					b.Fatalf("no results for 3 consecutive iterations, last prefix: %q", prefix)
				}
			} else {
				none = 0
			}
		}
	})

	// benchmark / delimiter with random prefix and paging
	b.Run("paging", func(b *testing.B) {
		var paged int
		var prefix, prevKeyMarker, prevUploadIDMarker string
		for b.Loop() {
			if prefix == "" {
				prefix = randomPath(1, minLength, maxDepth, alphabet)
			}

			res, err := store.ListMultipartUploads(bucket, prefix, slashDelim, prevKeyMarker, prevUploadIDMarker, maxKeys)
			if err != nil {
				b.Fatal(err)
			} else if res.IsTruncated {
				paged++
				prevUploadIDMarker = res.NextUploadIDMarker
				prevKeyMarker = res.NextKeyMarker
				continue
			}

			prevKeyMarker = ""
			prevUploadIDMarker = ""
			prefix = ""
		}
		if paged == 0 {
			b.Fatal("no pagination occurred during benchmark")
		}
	})
}

func randomPath(minLength, maxLength, maxDepth int, alphabet []rune) string {
	length := frand.Intn(maxLength-minLength+1) + minLength
	runes := make([]rune, length)
	for i := range runes {
		runes[i] = alphabet[frand.Intn(len(alphabet))]
	}

	key := string(runes)
	depth := frand.Intn(maxDepth)
	for i := 1; i < length && depth > 0; i++ {
		if frand.Intn(2) == 0 {
			key = key[:i] + "/" + key[i:]
			i++
			depth--
		}
	}

	return key
}

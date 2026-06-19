package sia_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// paginateVersions drives ListObjectVersions to completion with the given
// input, invoking fn for each page. The caller sets a small MaxKeys to force
// multiple pages.
func paginateVersions(t *testing.T, s3Tester *testutil.S3Tester, bucket string, in service.ListObjectVersionsInput, fn func(*service.ListObjectVersionsOutput)) {
	t.Helper()
	for {
		resp, err := s3Tester.ListObjectVersionsPage(t.Context(), bucket, &in)
		if err != nil {
			t.Fatal(err)
		}
		fn(resp)
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		in.KeyMarker, in.VersionIdMarker = resp.NextKeyMarker, resp.NextVersionIdMarker
	}
}

func TestBucketVersioningConfiguration(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	t.Run("get and put", func(t *testing.T) {
		bucket := "versioning"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// an unconfigured bucket reports no status
		status, err := s3Tester.GetBucketVersioning(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if status != "" {
			t.Fatalf("expected empty status, got %q", status)
		}

		for _, want := range []types.BucketVersioningStatus{types.BucketVersioningStatusEnabled, types.BucketVersioningStatusSuspended} {
			if err := s3Tester.PutBucketVersioning(t.Context(), bucket, want); err != nil {
				t.Fatal(err)
			}
			got, err := s3Tester.GetBucketVersioning(t.Context(), bucket)
			if err != nil {
				t.Fatal(err)
			} else if got != want {
				t.Fatalf("expected status %q, got %q", want, got)
			}
		}
	})

	t.Run("rejects MFA delete", func(t *testing.T) {
		bucket := "mfa-delete"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
		_, err := s3Tester.Client().PutBucketVersioning(t.Context(), &service.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				MFADelete: types.MFADeleteEnabled,
				Status:    types.BucketVersioningStatusEnabled,
			},
		})
		testutil.AssertS3Error(t, s3errs.ErrNotImplemented, err)
	})
}

func TestVersionedObjects(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	t.Run("lifecycle", func(t *testing.T) {
		bucket := "versioned-object"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}

		// two writes to the same key produce two distinct versions
		v1, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("first"))
		if err != nil {
			t.Fatal(err)
		}
		v2, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("second"))
		if err != nil {
			t.Fatal(err)
		} else if v1 == "" || v2 == "" {
			t.Fatalf("expected non-empty version IDs, got %q and %q", v1, v2)
		} else if v1 == v2 {
			t.Fatalf("expected distinct version IDs, both were %q", v1)
		}

		// the current version is the latest write; each version is retrievable
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", nil); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("second")) {
			t.Fatalf("expected current version %q, got %q", "second", got)
		}
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", &v1); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("first")) {
			t.Fatalf("expected version v1 %q, got %q", "first", got)
		}

		// ListObjectVersions returns both versions, newest marked latest
		versions, err := s3Tester.ListObjectVersionsPage(t.Context(), bucket, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(versions.Versions) != 2 {
			t.Fatalf("expected 2 versions, got %d", len(versions.Versions))
		}
		for _, v := range versions.Versions {
			isLatest := aws.ToString(v.VersionId) == v2
			if aws.ToBool(v.IsLatest) != isLatest {
				t.Fatalf("version %q IsLatest=%v, expected %v", aws.ToString(v.VersionId), aws.ToBool(v.IsLatest), isLatest)
			}
		}

		// a simple delete inserts a delete marker
		delResp, err := s3Tester.DeleteObjectVersion(t.Context(), bucket, "key", nil)
		if err != nil {
			t.Fatal(err)
		} else if !aws.ToBool(delResp.DeleteMarker) {
			t.Fatal("expected DeleteMarker=true on simple delete")
		}
		markerVersion := aws.ToString(delResp.VersionId)
		if markerVersion == "" {
			t.Fatal("expected a delete marker version ID")
		}

		// the current version now appears deleted
		_, err = s3Tester.GetObjectVersion(t.Context(), bucket, "key", nil)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)

		// the listing now has 2 versions and 1 delete marker (latest)
		versions, err = s3Tester.ListObjectVersionsPage(t.Context(), bucket, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(versions.Versions) != 2 || len(versions.DeleteMarkers) != 1 {
			t.Fatalf("expected 2 versions and 1 delete marker, got %d and %d", len(versions.Versions), len(versions.DeleteMarkers))
		} else if !aws.ToBool(versions.DeleteMarkers[0].IsLatest) {
			t.Fatal("expected delete marker to be the latest version")
		}

		// deleting the delete marker restores the object
		if _, err := s3Tester.DeleteObjectVersion(t.Context(), bucket, "key", &markerVersion); err != nil {
			t.Fatal(err)
		}
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", nil); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("second")) {
			t.Fatalf("expected restored current version %q, got %q", "second", got)
		}

		// permanently delete the first version
		if _, err := s3Tester.DeleteObjectVersion(t.Context(), bucket, "key", &v1); err != nil {
			t.Fatal(err)
		}
		versions, err = s3Tester.ListObjectVersionsPage(t.Context(), bucket, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(versions.Versions) != 1 || len(versions.DeleteMarkers) != 0 {
			t.Fatalf("expected 1 version and 0 delete markers, got %d and %d", len(versions.Versions), len(versions.DeleteMarkers))
		} else if aws.ToString(versions.Versions[0].VersionId) != v2 {
			t.Fatalf("expected remaining version %q, got %q", v2, aws.ToString(versions.Versions[0].VersionId))
		}
	})

	t.Run("missing version", func(t *testing.T) {
		bucket := "missing-version"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("data")); err != nil {
			t.Fatal(err)
		}

		_, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", aws.String("does-not-exist"))
		testutil.AssertS3Error(t, s3errs.ErrNoSuchVersion, err)
	})

	t.Run("delete marker returns 405", func(t *testing.T) {
		bucket := "delete-marker-status"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("data")); err != nil {
			t.Fatal(err)
		}

		delResp, err := s3Tester.DeleteObjectVersion(t.Context(), bucket, "key", nil)
		if err != nil {
			t.Fatal(err)
		}

		// requesting the delete marker version directly is not allowed, and
		// request modifiers must not mask the delete-marker response
		for _, tc := range []struct {
			name       string
			rangeHdr   *string
			partNumber *int32
		}{
			{name: "plain"},
			{name: "range", rangeHdr: aws.String("bytes=0-0")},
			{name: "part number", partNumber: aws.Int32(2)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := s3Tester.Client().GetObject(t.Context(), &service.GetObjectInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String("key"),
					VersionId:  delResp.VersionId,
					Range:      tc.rangeHdr,
					PartNumber: tc.partNumber,
				})
				testutil.AssertS3StatusCode(t, s3errs.ErrMethodNotAllowed, err)
			})
		}

		// a current-version GET sees the delete marker as a missing key
		_, err = s3Tester.Client().GetObject(t.Context(), &service.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("key"),
			Range:  aws.String("bytes=0-0"),
		})
		testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
	})

	t.Run("multi-object delete", func(t *testing.T) {
		bucket := "multi-object-delete"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		v1, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("first"))
		if err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("second")); err != nil {
			t.Fatal(err)
		}

		// permanently delete a specific version
		delResp, err := s3Tester.DeleteObjects(t.Context(), bucket, []types.ObjectIdentifier{
			{Key: aws.String("key"), VersionId: aws.String(v1)},
		}, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(delResp.Deleted) != 1 || aws.ToString(delResp.Deleted[0].VersionId) != v1 {
			t.Fatalf("expected deleted version %q, got %+v", v1, delResp.Deleted)
		}

		// a simple multi-delete inserts a delete marker
		delResp, err = s3Tester.DeleteObjects(t.Context(), bucket, []types.ObjectIdentifier{
			{Key: aws.String("key")},
		}, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(delResp.Deleted) != 1 || !aws.ToBool(delResp.Deleted[0].DeleteMarker) {
			t.Fatalf("expected a delete marker, got %+v", delResp.Deleted)
		} else if aws.ToString(delResp.Deleted[0].DeleteMarkerVersionId) == "" {
			t.Fatal("expected a delete marker version ID")
		}
	})

	t.Run("multipart upload", func(t *testing.T) {
		bucket := "multipart-version"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}

		create, err := s3Tester.CreateMultipartUpload(t.Context(), bucket, "mp", nil)
		if err != nil {
			t.Fatal(err)
		}
		uploadID := aws.ToString(create.UploadId)

		data := bytes.Repeat([]byte("x"), 1024)
		part, err := s3Tester.UploadPart(t.Context(), bucket, "mp", uploadID, 1, data)
		if err != nil {
			t.Fatal(err)
		}

		complete, err := s3Tester.CompleteMultipartUpload(t.Context(), bucket, "mp", uploadID, []types.CompletedPart{
			{ETag: part.ETag, PartNumber: aws.Int32(1)},
		})
		if err != nil {
			t.Fatal(err)
		} else if aws.ToString(complete.VersionId) == "" {
			t.Fatal("expected a version ID for the completed multipart upload")
		}

		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "mp", complete.VersionId); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, data) {
			t.Fatalf("multipart version body mismatch: got %d bytes", len(got))
		}
	})
}

func TestSuspendedVersioning(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	t.Run("writes use null version", func(t *testing.T) {
		bucket := "suspended"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// create a real version while enabled
		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		v1, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("versioned"))
		if err != nil {
			t.Fatal(err)
		} else if v1 == "" {
			t.Fatal("expected a version ID while enabled")
		}

		// suspended writes use the null version and report no version ID
		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusSuspended); err != nil {
			t.Fatal(err)
		}
		if v, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("null-1")); err != nil {
			t.Fatal(err)
		} else if v != "" {
			t.Fatalf("expected no version ID while suspended, got %q", v)
		}
		if v, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("null-2")); err != nil {
			t.Fatal(err)
		} else if v != "" {
			t.Fatalf("expected no version ID while suspended, got %q", v)
		}

		// the current version is the null version; the original is retained
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", nil); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("null-2")) {
			t.Fatalf("expected current null version %q, got %q", "null-2", got)
		}
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", &v1); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("versioned")) {
			t.Fatalf("expected retained version %q, got %q", "versioned", got)
		}

		// only one null version exists alongside the original
		versions, err := s3Tester.ListObjectVersionsPage(t.Context(), bucket, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(versions.Versions) != 2 {
			t.Fatalf("expected 2 versions (null + original), got %d", len(versions.Versions))
		}
		var nullCount int
		for _, v := range versions.Versions {
			if aws.ToString(v.VersionId) == "null" {
				nullCount++
			}
		}
		if nullCount != 1 {
			t.Fatalf("expected exactly one null version, got %d", nullCount)
		}
	})

	t.Run("null version addressing", func(t *testing.T) {
		bucket := "null-version-addressing"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// a real version, then a null version (written while suspended), then
		// re-enable so both coexist with the null version as current
		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		v1, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("v1"))
		if err != nil {
			t.Fatal(err)
		} else if v1 == "" || v1 == "null" {
			t.Fatalf("expected a real version ID, got %q", v1)
		}
		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusSuspended); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("null-data")); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}

		// versionId=null returns the null version; the real version is still addressable
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", aws.String("null")); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("null-data")) {
			t.Fatalf("expected null version %q, got %q", "null-data", got)
		}
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", &v1); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("v1")) {
			t.Fatalf("expected version v1 %q, got %q", "v1", got)
		}

		// deleting versionId=null permanently removes it (no delete marker) and
		// leaves the real version intact
		delResp, err := s3Tester.DeleteObjectVersion(t.Context(), bucket, "key", aws.String("null"))
		if err != nil {
			t.Fatal(err)
		} else if aws.ToBool(delResp.DeleteMarker) {
			t.Fatal("permanent delete of the null version must not report a delete marker")
		} else if aws.ToString(delResp.VersionId) != "null" {
			t.Fatalf("expected deleted version %q, got %q", "null", aws.ToString(delResp.VersionId))
		}

		// only the real version remains, and it is now current
		versions, err := s3Tester.ListObjectVersionsPage(t.Context(), bucket, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(versions.Versions) != 1 || len(versions.DeleteMarkers) != 0 {
			t.Fatalf("expected 1 version and 0 delete markers, got %d and %d", len(versions.Versions), len(versions.DeleteMarkers))
		} else if aws.ToString(versions.Versions[0].VersionId) != v1 {
			t.Fatalf("expected remaining version %q, got %q", v1, aws.ToString(versions.Versions[0].VersionId))
		}
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", nil); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("v1")) {
			t.Fatalf("expected current version %q, got %q", "v1", got)
		}
	})
}

func TestListObjectVersions(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	t.Run("pagination", func(t *testing.T) {
		bucket := "version-pagination"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}

		// 3 keys, 3 versions each; want[key] lists version IDs newest-first
		keys := []string{"a", "b", "c"}
		want := map[string][]string{}
		for _, k := range keys {
			for i := 0; i < 3; i++ {
				v, err := s3Tester.PutObjectVersion(t.Context(), bucket, k, []byte(fmt.Sprintf("%s-%d", k, i)))
				if err != nil {
					t.Fatal(err)
				}
				want[k] = append([]string{v}, want[k]...)
			}
		}

		// expected flat order: key ascending, newest version first
		var expected []string
		for _, k := range keys {
			for _, v := range want[k] {
				expected = append(expected, k+"/"+v)
			}
		}

		// a small max-keys exercises truncation and mid-key resumption
		var got []string
		seen := map[string]bool{}
		paginateVersions(t, s3Tester, bucket, service.ListObjectVersionsInput{
			MaxKeys: aws.Int32(2),
		}, func(resp *service.ListObjectVersionsOutput) {
			for _, v := range resp.Versions {
				id := aws.ToString(v.Key) + "/" + aws.ToString(v.VersionId)
				if seen[id] {
					t.Fatalf("version returned twice: %s", id)
				}
				seen[id] = true
				got = append(got, id)
			}
		})

		if len(got) != len(expected) {
			t.Fatalf("expected %d versions, got %d", len(expected), len(got))
		}
		for i := range expected {
			if got[i] != expected[i] {
				t.Fatalf("order mismatch at %d: expected %s, got %s", i, expected[i], got[i])
			}
		}
	})

	t.Run("delimiter pagination", func(t *testing.T) {
		bucket := "delimiter-pagination"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}

		for _, k := range []string{"dir1/x", "dir1/y", "dir2/z", "top"} {
			if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, k, []byte("v")); err != nil {
				t.Fatal(err)
			}
		}

		var prefixes []string
		var topKeys []string
		paginateVersions(t, s3Tester, bucket, service.ListObjectVersionsInput{
			Delimiter: aws.String("/"),
			MaxKeys:   aws.Int32(1),
		}, func(resp *service.ListObjectVersionsOutput) {
			for _, cp := range resp.CommonPrefixes {
				prefixes = append(prefixes, aws.ToString(cp.Prefix))
			}
			for _, v := range resp.Versions {
				topKeys = append(topKeys, aws.ToString(v.Key))
			}
		})

		wantPrefixes := []string{"dir1/", "dir2/"}
		if fmt.Sprint(prefixes) != fmt.Sprint(wantPrefixes) {
			t.Fatalf("expected common prefixes %v, got %v", wantPrefixes, prefixes)
		} else if fmt.Sprint(topKeys) != fmt.Sprint([]string{"top"}) {
			t.Fatalf("expected top-level keys [top], got %v", topKeys)
		}
	})

	t.Run("version-id marker without key marker", func(t *testing.T) {
		bucket := "version-id-marker"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("data")); err != nil {
			t.Fatal(err)
		}

		_, err := s3Tester.ListObjectVersionsPage(t.Context(), bucket, &service.ListObjectVersionsInput{
			VersionIdMarker: aws.String("some-version"),
		})
		testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, err)
	})

	t.Run("max-keys zero", func(t *testing.T) {
		bucket := "max-keys-zero"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("data")); err != nil {
			t.Fatal(err)
		}

		resp, err := s3Tester.ListObjectVersionsPage(t.Context(), bucket, &service.ListObjectVersionsInput{
			MaxKeys: aws.Int32(0),
		})
		if err != nil {
			t.Fatal(err)
		} else if len(resp.Versions) != 0 {
			t.Fatalf("expected no versions, got %d", len(resp.Versions))
		} else if aws.ToBool(resp.IsTruncated) {
			t.Fatal("expected IsTruncated false for max-keys=0")
		}
	})
}

func TestRestoreViaCopy(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	t.Run("previous version", func(t *testing.T) {
		bucket := "restore-previous"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}

		v1, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("first"))
		if err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("second")); err != nil {
			t.Fatal(err)
		}

		// copying an old version onto the same key without REPLACE is a restore,
		// not a no-op self-copy, so it must succeed
		resp, err := s3Tester.CopyObjectVersion(t.Context(), bucket, "key", &v1, bucket, "key")
		if err != nil {
			t.Fatalf("restore copy failed: %v", err)
		} else if aws.ToString(resp.VersionId) == "" || aws.ToString(resp.VersionId) == v1 {
			t.Fatalf("expected a new version ID distinct from the source, got %q", aws.ToString(resp.VersionId))
		}

		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", nil); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("first")) {
			t.Fatalf("expected restored current version %q, got %q", "first", got)
		}
	})

	t.Run("null version in suspended bucket", func(t *testing.T) {
		bucket := "restore-null"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// the pre-versioning object is the null version; after enabling, a later
		// write creates a non-null current version
		nullVersion, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("null"))
		if err != nil {
			t.Fatal(err)
		} else if nullVersion != "" {
			t.Fatalf("expected pre-versioning put to have no version header, got %q", nullVersion)
		}
		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		currentVersion, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("current"))
		if err != nil {
			t.Fatal(err)
		} else if currentVersion == "" {
			t.Fatal("expected enabled bucket put to return a version ID")
		}

		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusSuspended); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.CopyObjectVersion(t.Context(), bucket, "key", aws.String("null"), bucket, "key"); err != nil {
			t.Fatalf("restore copy failed: %v", err)
		}

		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, "key", nil); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, []byte("null")) {
			t.Fatalf("expected restored null version to be current, got %q", got)
		}
	})

	t.Run("self copy without version rejected", func(t *testing.T) {
		bucket := "self-copy"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, "key", []byte("data")); err != nil {
			t.Fatal(err)
		}

		_, err := s3Tester.CopyObjectVersion(t.Context(), bucket, "key", nil, bucket, "key")
		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)
	})
}

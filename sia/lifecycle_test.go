package sia_test

import (
	"bytes"
	"encoding/xml"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestApplyLifecycleRules(t *testing.T) {
	ctx := t.Context()
	backend, _ := testutil.NewBackend(t)

	const bucket = "lifecycle-bucket"
	if err := backend.CreateBucket(ctx, testutil.AccessKeyID, bucket, s3.CreateBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	put := func(object string) {
		t.Helper()
		if _, err := backend.PutObject(ctx, testutil.AccessKeyID, bucket, object, bytes.NewReader([]byte("data")), s3.PutObjectOptions{ContentLength: 4}); err != nil {
			t.Fatal(err)
		}
	}
	// object expired by a past-date rule
	put("logs/old")
	// object under the same prefix as a disabled rule; must survive
	put("data/keep")
	// object expired by a Days rule once the rule's window has elapsed
	put("days/obj")

	// two objects under one prefix, separated only by size, so a rule bounding
	// size must take the large one and leave the small one
	putSized := func(object string, size int) {
		t.Helper()
		if _, err := backend.PutObject(ctx, testutil.AccessKeyID, bucket, object, bytes.NewReader(make([]byte, size)), s3.PutObjectOptions{ContentLength: int64(size)}); err != nil {
			t.Fatal(err)
		}
	}
	putSized("sized/small", 10)
	putSized("sized/big", 100)

	// incomplete multipart upload with an on-disk part, aborted once the
	// rule's window has elapsed
	upload, err := backend.CreateMultipartUpload(ctx, testutil.AccessKeyID, bucket, "uploads/u", s3.CreateMultipartUploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.UploadPart(ctx, testutil.AccessKeyID, bucket, "uploads/u", upload.UploadID, bytes.NewReader([]byte("part")), s3.UploadPartOptions{PartNumber: 1, ContentLength: 4}); err != nil {
		t.Fatal(err)
	}

	config := s3.LifecycleConfiguration{
		Rules: []s3.LifecycleRule{
			{
				Status:     s3.LifecycleStatusEnabled,
				Filter:     &s3.LifecycleFilter{Prefix: aws.String("logs/")},
				Expiration: &s3.LifecycleExpiration{Date: "2020-01-01T00:00:00.000Z"},
			},
			{
				Status:     s3.LifecycleStatusEnabled,
				Filter:     &s3.LifecycleFilter{Prefix: aws.String("days/")},
				Expiration: &s3.LifecycleExpiration{Days: 30},
			},
			{
				Status:                         s3.LifecycleStatusEnabled,
				Filter:                         &s3.LifecycleFilter{Prefix: aws.String("uploads/")},
				AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{DaysAfterInitiation: 1},
			},
			{
				// disabled rule must not expire anything
				Status:     s3.LifecycleStatusDisabled,
				Filter:     &s3.LifecycleFilter{Prefix: aws.String("data/")},
				Expiration: &s3.LifecycleExpiration{Date: "2020-01-01T00:00:00.000Z"},
			},
			{
				Status:     s3.LifecycleStatusEnabled,
				Filter:     &s3.LifecycleFilter{And: &s3.LifecycleAndOperator{Prefix: aws.String("sized/"), ObjectSizeGreaterThan: aws.Int64(50)}},
				Expiration: &s3.LifecycleExpiration{Date: "2020-01-01T00:00:00.000Z"},
			},
		},
	}
	if err := backend.PutBucketLifecycleConfiguration(ctx, testutil.AccessKeyID, bucket, config); err != nil {
		t.Fatal(err)
	}

	// applying the rules now must only expire logs/old; everything else is
	// younger than the Days and DaysAfterInitiation windows
	backend.ApplyLifecycleRules(ctx, time.Now())

	accessKeyID := testutil.AccessKeyID
	if _, err := backend.GetObject(ctx, &accessKeyID, bucket, "logs/old", s3.NoVersion(), nil, nil); !errors.Is(err, s3errs.ErrNoSuchKey) {
		t.Fatalf("expected logs/old to be expired, got %v", err)
	}
	obj, err := backend.GetObject(ctx, &accessKeyID, bucket, "days/obj", s3.NoVersion(), nil, nil)
	if err != nil {
		t.Fatalf("expected days/obj to survive, got %v", err)
	}
	obj.Body.Close()
	if _, err := backend.ListParts(ctx, testutil.AccessKeyID, bucket, "uploads/u", upload.UploadID, s3.ListPartsPage{MaxParts: 10}); err != nil {
		t.Fatalf("expected fresh upload to survive, got %v", err)
	}

	// the rule survived the trip through the stored XML and took only the
	// object above its lower bound
	if _, err := backend.GetObject(ctx, &accessKeyID, bucket, "sized/big", s3.NoVersion(), nil, nil); !errors.Is(err, s3errs.ErrNoSuchKey) {
		t.Fatalf("expected sized/big to be expired, got %v", err)
	}
	obj, err = backend.GetObject(ctx, &accessKeyID, bucket, "sized/small", s3.NoVersion(), nil, nil)
	if err != nil {
		t.Fatalf("expected sized/small to survive, got %v", err)
	}
	obj.Body.Close()

	// applying the rules well past both windows must expire days/obj and
	// abort the upload, but still skip the disabled rule
	backend.ApplyLifecycleRules(ctx, time.Now().AddDate(0, 0, 31))

	if _, err := backend.GetObject(ctx, &accessKeyID, bucket, "days/obj", s3.NoVersion(), nil, nil); !errors.Is(err, s3errs.ErrNoSuchKey) {
		t.Fatalf("expected days/obj to be expired, got %v", err)
	}

	// data/keep should survive (rule disabled)
	obj, err = backend.GetObject(ctx, &accessKeyID, bucket, "data/keep", s3.NoVersion(), nil, nil)
	if err != nil {
		t.Fatalf("expected data/keep to survive, got %v", err)
	}
	obj.Body.Close()

	// the upload should have been aborted and its directory removed
	if _, err := backend.ListParts(ctx, testutil.AccessKeyID, bucket, "uploads/u", upload.UploadID, s3.ListPartsPage{MaxParts: 10}); !errors.Is(err, s3errs.ErrNoSuchUpload) {
		t.Fatalf("expected upload to be aborted, got %v", err)
	}
	uploadDir := backend.UploadDir(upload.UploadID)
	if _, err := os.Stat(uploadDir); !os.IsNotExist(err) {
		t.Fatalf("expected upload directory %q to be removed, got %v", uploadDir, err)
	}
}

func TestApplyLifecycleRulesSkipsInvalidRule(t *testing.T) {
	ctx := t.Context()
	backend, store := testutil.NewBackend(t)

	const bucket = "invalid-rule-bucket"
	if err := backend.CreateBucket(ctx, testutil.AccessKeyID, bucket, s3.CreateBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	upload, err := backend.CreateMultipartUpload(ctx, testutil.AccessKeyID, bucket, "uploads/u", s3.CreateMultipartUploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.UploadPart(ctx, testutil.AccessKeyID, bucket, "uploads/u", upload.UploadID, bytes.NewReader([]byte("part")), s3.UploadPartOptions{PartNumber: 1, ContentLength: 4}); err != nil {
		t.Fatal(err)
	}

	// the abort path cannot honor a size bound, so a rule carrying one is
	// rejected on write. storing the document directly bypasses that check the
	// way an older binary's configuration would.
	config := s3.LifecycleConfiguration{
		Rules: []s3.LifecycleRule{
			{
				ID:                             "invalid",
				Status:                         s3.LifecycleStatusEnabled,
				Filter:                         &s3.LifecycleFilter{And: &s3.LifecycleAndOperator{Prefix: aws.String("uploads/"), ObjectSizeGreaterThan: aws.Int64(50)}},
				AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{DaysAfterInitiation: 1},
			},
		},
	}
	if err := config.Validate(); err == nil {
		t.Fatal("expected the rule to fail validation")
	}
	buf, err := xml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutBucketLifecycleConfiguration(testutil.AccessKeyID, bucket, string(buf)); err != nil {
		t.Fatal(err)
	}

	// well past the abort window, so only the skipped rule keeps the upload
	backend.ApplyLifecycleRules(ctx, time.Now().AddDate(0, 0, 31))

	if _, err := backend.ListParts(ctx, testutil.AccessKeyID, bucket, "uploads/u", upload.UploadID, s3.ListPartsPage{MaxParts: 10}); err != nil {
		t.Fatalf("expected upload to survive an invalid rule, got %v", err)
	}
}

func TestApplyLifecycleRulesVersioned(t *testing.T) {
	ctx := t.Context()
	backend, _ := testutil.NewBackend(t)

	const (
		bucket = "versioned-lifecycle"
		kept   = "keep/obj"
		reaped = "reap/obj"
	)
	if err := backend.CreateBucket(ctx, testutil.AccessKeyID, bucket, s3.CreateBucketOptions{}); err != nil {
		t.Fatal(err)
	} else if err := backend.PutBucketVersioning(ctx, testutil.AccessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	put := func(object string) {
		t.Helper()
		if _, err := backend.PutObject(ctx, testutil.AccessKeyID, bucket, object, bytes.NewReader([]byte("data")), s3.PutObjectOptions{ContentLength: 4}); err != nil {
			t.Fatal(err)
		}
	}
	countVersions := func(object string) int {
		t.Helper()
		res, err := backend.ListObjectVersions(ctx, aws.String(testutil.AccessKeyID), bucket, s3.Prefix{HasPrefix: true, Prefix: object}, s3.ListObjectVersionsPage{MaxKeys: 100})
		if err != nil {
			t.Fatal(err)
		}
		return len(res.Versions) // delete markers are reported as versions
	}

	// three versions under each prefix, so one is current and two noncurrent
	for range 3 {
		put(kept)
		put(reaped)
	}

	// the keep/ prefix is covered by no rule, the reap/ prefix retains a single
	// noncurrent version
	config := s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{{
		Status:                      s3.LifecycleStatusEnabled,
		Filter:                      &s3.LifecycleFilter{Prefix: aws.String("reap/")},
		NoncurrentVersionExpiration: &s3.LifecycleNoncurrentVersionExpiration{NewerNoncurrentVersions: aws.Int(1)},
	}}}
	if err := backend.PutBucketLifecycleConfiguration(ctx, testutil.AccessKeyID, bucket, config); err != nil {
		t.Fatal(err)
	}

	backend.ApplyLifecycleRules(ctx, time.Now())

	if n := countVersions(reaped); n != 2 {
		t.Fatalf("expected the current version plus one retained noncurrent version, got %d", n)
	}
	if n := countVersions(kept); n != 3 {
		t.Fatalf("expected the unmatched prefix to keep all three versions, got %d", n)
	}

	// an unversioned delete leaves a marker over the remaining versions, so the
	// marker only becomes expired once the noncurrent rule has cleared them
	if _, err := backend.DeleteObject(ctx, testutil.AccessKeyID, bucket, s3.ObjectID{Key: reaped}, s3.DeleteObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	config.Rules[0].Expiration = &s3.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}
	config.Rules[0].NoncurrentVersionExpiration = &s3.LifecycleNoncurrentVersionExpiration{NewerNoncurrentVersions: aws.Int(1)}
	if err := backend.PutBucketLifecycleConfiguration(ctx, testutil.AccessKeyID, bucket, config); err != nil {
		t.Fatal(err)
	}

	// the retain bound still holds one noncurrent version back, so the marker
	// is not yet the last version standing
	backend.ApplyLifecycleRules(ctx, time.Now())
	if n := countVersions(reaped); n == 0 {
		t.Fatal("expected a retained noncurrent version to hold the marker back")
	}

	// dropping the retain bound clears the last noncurrent version, and the
	// following pass removes the marker it was holding back
	config.Rules[0].NoncurrentVersionExpiration = &s3.LifecycleNoncurrentVersionExpiration{NoncurrentDays: 1}
	if err := backend.PutBucketLifecycleConfiguration(ctx, testutil.AccessKeyID, bucket, config); err != nil {
		t.Fatal(err)
	}
	backend.ApplyLifecycleRules(ctx, time.Now().AddDate(0, 0, 2))
	if n := countVersions(reaped); n != 0 {
		t.Fatalf("expected every version and the marker to be gone, got %d", n)
	}
	if n := countVersions(kept); n != 3 {
		t.Fatalf("expected the unmatched prefix to be untouched, got %d", n)
	}
}

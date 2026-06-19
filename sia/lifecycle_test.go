package sia_test

import (
	"bytes"
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
	if err := backend.CreateBucket(ctx, testutil.AccessKeyID, bucket); err != nil {
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

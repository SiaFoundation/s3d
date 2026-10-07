package sia_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestBucketObjectLockConfiguration(t *testing.T) {
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

	governanceDays := func(days int32) *types.ObjectLockConfiguration {
		return &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
			Rule: &types.ObjectLockRule{
				DefaultRetention: &types.DefaultRetention{
					Mode: types.ObjectLockRetentionModeGovernance,
					Days: aws.Int32(days),
				},
			},
		}
	}

	t.Run("EnabledAtCreation", func(t *testing.T) {
		bucket := "locked"
		if err := s3Tester.CreateBucketWithObjectLock(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// object lock implies versioning
		status, err := s3Tester.GetBucketVersioning(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if status != types.BucketVersioningStatusEnabled {
			t.Fatalf("expected versioning enabled, got %q", status)
		}

		// enabled with no default retention yet
		config, err := s3Tester.GetObjectLockConfiguration(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if config.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
			t.Fatalf("expected object lock enabled, got %q", config.ObjectLockEnabled)
		} else if config.Rule != nil {
			t.Fatalf("expected no default retention, got %v", config.Rule)
		}
	})

	t.Run("DefaultRetention", func(t *testing.T) {
		bucket := "default-retention"
		if err := s3Tester.CreateBucketWithObjectLock(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		if err := s3Tester.PutObjectLockConfiguration(t.Context(), bucket, governanceDays(1)); err != nil {
			t.Fatal(err)
		}
		config, err := s3Tester.GetObjectLockConfiguration(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}
		retention := config.Rule.DefaultRetention
		if retention.Mode != types.ObjectLockRetentionModeGovernance {
			t.Fatalf("expected governance, got %q", retention.Mode)
		} else if aws.ToInt32(retention.Days) != 1 {
			t.Fatalf("expected 1 day, got %v", retention.Days)
		} else if retention.Years != nil {
			t.Fatalf("expected no years, got %v", *retention.Years)
		}

		// replacing days with years drops the days
		years := &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
			Rule: &types.ObjectLockRule{
				DefaultRetention: &types.DefaultRetention{
					Mode:  types.ObjectLockRetentionModeCompliance,
					Years: aws.Int32(2),
				},
			},
		}
		if err := s3Tester.PutObjectLockConfiguration(t.Context(), bucket, years); err != nil {
			t.Fatal(err)
		}
		config, err = s3Tester.GetObjectLockConfiguration(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}
		retention = config.Rule.DefaultRetention
		if retention.Mode != types.ObjectLockRetentionModeCompliance {
			t.Fatalf("expected compliance, got %q", retention.Mode)
		} else if aws.ToInt32(retention.Years) != 2 {
			t.Fatalf("expected 2 years, got %v", retention.Years)
		} else if retention.Days != nil {
			t.Fatalf("expected no days, got %v", *retention.Days)
		}

		// a configuration without a rule clears the default retention
		cleared := &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled}
		if err := s3Tester.PutObjectLockConfiguration(t.Context(), bucket, cleared); err != nil {
			t.Fatal(err)
		}
		config, err = s3Tester.GetObjectLockConfiguration(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if config.Rule != nil {
			t.Fatalf("expected no default retention, got %v", config.Rule)
		}
	})

	t.Run("EnableAfterCreate", func(t *testing.T) {
		bucket := "enable-after-create"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// an unversioned bucket cannot be upgraded
		err := s3Tester.PutObjectLockConfiguration(t.Context(), bucket, governanceDays(1))
		testutil.AssertS3Error(t, s3errs.ErrInvalidBucketState, err)

		// nor a suspended one
		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusSuspended); err != nil {
			t.Fatal(err)
		}
		err = s3Tester.PutObjectLockConfiguration(t.Context(), bucket, governanceDays(1))
		testutil.AssertS3Error(t, s3errs.ErrInvalidBucketState, err)

		// a versioning enabled bucket can
		if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		if err := s3Tester.PutObjectLockConfiguration(t.Context(), bucket, governanceDays(1)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("VersioningCannotBeSuspended", func(t *testing.T) {
		bucket := "no-suspend"
		if err := s3Tester.CreateBucketWithObjectLock(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
		err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusSuspended)
		testutil.AssertS3Error(t, s3errs.ErrInvalidBucketState, err)
	})

	t.Run("NotEnabled", func(t *testing.T) {
		bucket := "unlocked"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
		_, err := s3Tester.GetObjectLockConfiguration(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrObjectLockConfigurationNotFoundError, err)
	})

	t.Run("OtherUserDenied", func(t *testing.T) {
		bucket := "owner-only"
		if err := s3Tester.CreateBucketWithObjectLock(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
		other := s3Tester.ChangeAccessKey(t, "foo", "bar")
		_, err := other.GetObjectLockConfiguration(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

		err = other.PutObjectLockConfiguration(t.Context(), bucket, governanceDays(1))
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	})
}

func TestObjectLockState(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	ctx := t.Context()
	retainUntil := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Millisecond)

	lockedBucket := func(t *testing.T, name string) {
		t.Helper()
		if err := s3Tester.CreateBucketWithObjectLock(ctx, name); err != nil {
			t.Fatal(err)
		}
	}

	putLocked := func(t *testing.T, bucket, key string, mode types.ObjectLockMode, hold types.ObjectLockLegalHoldStatus) string {
		t.Helper()
		out, err := s3Tester.Client().PutObject(ctx, &service.PutObjectInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String(key),
			Body:                      bytes.NewReader([]byte("data")),
			ObjectLockMode:            mode,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
			ObjectLockLegalHoldStatus: hold,
		})
		if err != nil {
			t.Fatal(err)
		}
		return aws.ToString(out.VersionId)
	}

	head := func(t *testing.T, bucket, key string) *service.HeadObjectOutput {
		t.Helper()
		out, err := s3Tester.Client().HeadObject(ctx, &service.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	headVersion := func(t *testing.T, bucket, key, versionID string) *service.HeadObjectOutput {
		t.Helper()
		out, err := s3Tester.Client().HeadObject(ctx, &service.HeadObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			VersionId: aws.String(versionID),
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	// completeUpload sends a single 5 MiB part and completes the upload
	completeUpload := func(t *testing.T, bucket, key, uploadID string) {
		t.Helper()
		part, err := s3Tester.UploadPart(ctx, bucket, key, uploadID, 1, bytes.Repeat([]byte("a"), 5*1024*1024))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s3Tester.CompleteMultipartUpload(ctx, bucket, key, uploadID, []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: part.ETag},
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("StoredAndReported", func(t *testing.T) {
		bucket := "stored"
		lockedBucket(t, bucket)
		putLocked(t, bucket, "locked", types.ObjectLockModeGovernance, types.ObjectLockLegalHoldStatusOn)

		if h := head(t, bucket, "locked"); h.ObjectLockMode != types.ObjectLockModeGovernance {
			t.Fatalf("expected GOVERNANCE, got %q", h.ObjectLockMode)
		} else if h.ObjectLockRetainUntilDate == nil || !h.ObjectLockRetainUntilDate.Equal(retainUntil) {
			t.Fatalf("expected %v, got %v", retainUntil, h.ObjectLockRetainUntilDate)
		} else if h.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
			t.Fatalf("expected ON, got %q", h.ObjectLockLegalHoldStatus)
		}

		// a version that never carried a lock reports no headers at all, which
		// AWS distinguishes from an explicit OFF
		if _, err := s3Tester.PutObject(ctx, bucket, "plain", bytes.NewReader([]byte("data")), nil); err != nil {
			t.Fatal(err)
		}
		if h := head(t, bucket, "plain"); h.ObjectLockMode != "" {
			t.Fatalf("expected no retention, got %q", h.ObjectLockMode)
		} else if h.ObjectLockLegalHoldStatus != "" {
			t.Fatalf("expected no legal hold, got %q", h.ObjectLockLegalHoldStatus)
		}
	})

	t.Run("PerVersion", func(t *testing.T) {
		bucket := "versions"
		lockedBucket(t, bucket)
		governance := putLocked(t, bucket, "key", types.ObjectLockModeGovernance, types.ObjectLockLegalHoldStatusOn)
		compliance := putLocked(t, bucket, "key", types.ObjectLockModeCompliance, types.ObjectLockLegalHoldStatusOff)

		if h := headVersion(t, bucket, "key", governance); h.ObjectLockMode != types.ObjectLockModeGovernance {
			t.Fatalf("expected GOVERNANCE, got %q", h.ObjectLockMode)
		} else if h.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
			t.Fatalf("expected ON, got %q", h.ObjectLockLegalHoldStatus)
		}
		if h := headVersion(t, bucket, "key", compliance); h.ObjectLockMode != types.ObjectLockModeCompliance {
			t.Fatalf("expected COMPLIANCE, got %q", h.ObjectLockMode)
		} else if h.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOff {
			t.Fatalf("expected OFF, got %q", h.ObjectLockLegalHoldStatus)
		}
		if h := head(t, bucket, "key"); h.ObjectLockMode != types.ObjectLockModeCompliance {
			t.Fatalf("expected the latest version, got %q", h.ObjectLockMode)
		}
	})

	t.Run("BucketDefault", func(t *testing.T) {
		bucket := "defaulted"
		lockedBucket(t, bucket)
		if err := s3Tester.PutObjectLockConfiguration(ctx, bucket, &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
			Rule: &types.ObjectLockRule{
				DefaultRetention: &types.DefaultRetention{
					Mode: types.ObjectLockRetentionModeCompliance,
					Days: aws.Int32(3),
				},
			},
		}); err != nil {
			t.Fatal(err)
		}

		// an unadorned write takes the default
		before := time.Now().Truncate(time.Millisecond)
		if _, err := s3Tester.PutObject(ctx, bucket, "auto", bytes.NewReader([]byte("data")), nil); err != nil {
			t.Fatal(err)
		}
		want := before.AddDate(0, 0, 3)
		if h := head(t, bucket, "auto"); h.ObjectLockMode != types.ObjectLockModeCompliance {
			t.Fatalf("expected COMPLIANCE, got %q", h.ObjectLockMode)
		} else if h.ObjectLockRetainUntilDate == nil {
			t.Fatal("expected a retain until date")
		} else if d := h.ObjectLockRetainUntilDate.Sub(want); d < 0 || d > time.Minute {
			t.Fatalf("expected roughly %v, got %v", want, h.ObjectLockRetainUntilDate)
		}

		// an explicit header beats the default
		putLocked(t, bucket, "explicit", types.ObjectLockModeGovernance, "")
		if h := head(t, bucket, "explicit"); h.ObjectLockMode != types.ObjectLockModeGovernance {
			t.Fatalf("expected GOVERNANCE, got %q", h.ObjectLockMode)
		} else if !h.ObjectLockRetainUntilDate.Equal(retainUntil) {
			t.Fatalf("expected %v, got %v", retainUntil, h.ObjectLockRetainUntilDate)
		}

		// a multipart default is resolved at completion, where the version is
		// created. The pause separates the two instants by more than the
		// millisecond the store keeps.
		created, err := s3Tester.CreateMultipartUpload(ctx, bucket, "autobig", nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		beforeComplete := time.Now().Truncate(time.Millisecond)
		completeUpload(t, bucket, "autobig", *created.UploadId)
		completed := time.Now()
		if h := head(t, bucket, "autobig"); h.ObjectLockMode != types.ObjectLockModeCompliance {
			t.Fatalf("expected COMPLIANCE, got %q", h.ObjectLockMode)
		} else if h.ObjectLockRetainUntilDate.Before(beforeComplete.AddDate(0, 0, 3)) {
			t.Fatalf("retention was anchored to initiation, got %v", h.ObjectLockRetainUntilDate)
		} else if h.ObjectLockRetainUntilDate.After(completed.AddDate(0, 0, 3)) {
			t.Fatalf("expected at most %v, got %v", completed.AddDate(0, 0, 3), h.ObjectLockRetainUntilDate)
		}

		// an explicit lock given at initiation survives to the completed version
		big, err := s3Tester.Client().CreateMultipartUpload(ctx, &service.CreateMultipartUploadInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String("big"),
			ObjectLockMode:            types.ObjectLockModeGovernance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
		})
		if err != nil {
			t.Fatal(err)
		}
		completeUpload(t, bucket, "big", *big.UploadId)
		if h := head(t, bucket, "big"); h.ObjectLockMode != types.ObjectLockModeGovernance {
			t.Fatalf("expected GOVERNANCE, got %q", h.ObjectLockMode)
		} else if !h.ObjectLockRetainUntilDate.Equal(retainUntil) {
			t.Fatalf("expected %v, got %v", retainUntil, h.ObjectLockRetainUntilDate)
		}
	})

	t.Run("RefusedWithoutObjectLock", func(t *testing.T) {
		bucket := "plainbucket"
		if err := s3Tester.CreateBucket(ctx, bucket); err != nil {
			t.Fatal(err)
		}
		_, err := s3Tester.Client().PutObject(ctx, &service.PutObjectInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String("nope"),
			Body:                      bytes.NewReader([]byte("data")),
			ObjectLockMode:            types.ObjectLockModeGovernance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
		})
		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

		_, err = s3Tester.Client().CreateMultipartUpload(ctx, &service.CreateMultipartUploadInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String("nope"),
			ObjectLockMode:            types.ObjectLockModeGovernance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
		})
		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)

		// a copy onto its own key rewrites the row in place rather than writing a
		// new version, so it has to refuse the headers on the same terms
		if _, err := s3Tester.PutObject(ctx, bucket, "self", bytes.NewReader([]byte("data")), nil); err != nil {
			t.Fatal(err)
		}
		_, err = s3Tester.Client().CopyObject(ctx, &service.CopyObjectInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String("self"),
			CopySource:                aws.String(bucket + "/self"),
			MetadataDirective:         types.MetadataDirectiveReplace,
			ObjectLockMode:            types.ObjectLockModeGovernance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
		})
		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)
	})

	// AWS gates the lock headers on s3:GetObjectRetention and
	// s3:GetObjectLegalHold, which s3d policies do not carry, so a reader who
	// does not own the bucket never sees them
	t.Run("HiddenFromNonOwner", func(t *testing.T) {
		bucket := "public"
		lockedBucket(t, bucket)
		putLocked(t, bucket, "readable", types.ObjectLockModeGovernance, types.ObjectLockLegalHoldStatusOn)
		if err := s3Tester.PutBucketPolicy(ctx, bucket, publicReadPolicy(bucket)); err != nil {
			t.Fatal(err)
		}

		anon, err := s3Tester.Anonymous().Client().GetObject(ctx, &service.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("readable"),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer anon.Body.Close()
		if anon.ObjectLockMode != "" {
			t.Fatalf("expected no retention, got %q", anon.ObjectLockMode)
		} else if anon.ObjectLockRetainUntilDate != nil {
			t.Fatalf("expected no retain until date, got %v", anon.ObjectLockRetainUntilDate)
		} else if anon.ObjectLockLegalHoldStatus != "" {
			t.Fatalf("expected no legal hold, got %q", anon.ObjectLockLegalHoldStatus)
		}

		// the owner still sees them
		if h := head(t, bucket, "readable"); h.ObjectLockMode != types.ObjectLockModeGovernance {
			t.Fatalf("expected the owner to see GOVERNANCE, got %q", h.ObjectLockMode)
		}
	})

	t.Run("Copy", func(t *testing.T) {
		bucket := "copies"
		lockedBucket(t, bucket)
		putLocked(t, bucket, "src", types.ObjectLockModeGovernance, "")

		// a copy carries the lock the request asked for
		if _, err := s3Tester.Client().CopyObject(ctx, &service.CopyObjectInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String("locked"),
			CopySource:                aws.String(bucket + "/src"),
			ObjectLockMode:            types.ObjectLockModeCompliance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
			ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
		}); err != nil {
			t.Fatal(err)
		}
		if h := head(t, bucket, "locked"); h.ObjectLockMode != types.ObjectLockModeCompliance {
			t.Fatalf("expected COMPLIANCE on the copy, got %q", h.ObjectLockMode)
		} else if h.ObjectLockRetainUntilDate == nil || !h.ObjectLockRetainUntilDate.Equal(retainUntil) {
			t.Fatalf("expected %v, got %v", retainUntil, h.ObjectLockRetainUntilDate)
		} else if h.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
			t.Fatalf("expected ON on the copy, got %q", h.ObjectLockLegalHoldStatus)
		}

		// a bare copy of a locked source inherits nothing
		if _, err := s3Tester.Client().CopyObject(ctx, &service.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("bare"),
			CopySource: aws.String(bucket + "/src"),
		}); err != nil {
			t.Fatal(err)
		}
		if h := head(t, bucket, "bare"); h.ObjectLockMode != "" {
			t.Fatalf("expected no retention, got %q", h.ObjectLockMode)
		}

		// and the source keeps its own
		if h := head(t, bucket, "src"); h.ObjectLockMode != types.ObjectLockModeGovernance {
			t.Fatalf("expected the source to keep GOVERNANCE, got %q", h.ObjectLockMode)
		}
	})
}

func TestObjectRetentionAndLegalHold(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	ctx := t.Context()

	day := func(n int) time.Time {
		return time.Now().AddDate(0, 0, n).UTC().Truncate(time.Millisecond)
	}
	governance := func(until time.Time) *types.ObjectLockRetention {
		return &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance, RetainUntilDate: aws.Time(until)}
	}
	compliance := func(until time.Time) *types.ObjectLockRetention {
		return &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeCompliance, RetainUntilDate: aws.Time(until)}
	}
	put := func(tb testing.TB, bucket, object string) {
		tb.Helper()
		if _, err := s3Tester.PutObject(ctx, bucket, object, bytes.NewReader([]byte("data")), nil); err != nil {
			tb.Fatal(err)
		}
	}

	bucket := "retained"
	if err := s3Tester.CreateBucketWithObjectLock(ctx, bucket); err != nil {
		t.Fatal(err)
	}

	assertRetained := func(tb testing.TB, object string, version *string, want time.Time) {
		tb.Helper()
		got, err := s3Tester.GetObjectRetention(ctx, bucket, object, version)
		if err != nil {
			tb.Fatal(err)
		} else if got.Mode != types.ObjectLockRetentionModeGovernance {
			tb.Fatalf("expected GOVERNANCE, got %q", got.Mode)
		} else if !got.RetainUntilDate.Equal(want) {
			tb.Fatalf("expected %v, got %v", want, got.RetainUntilDate)
		}
	}

	t.Run("Governance", func(t *testing.T) {
		put(t, bucket, "a")
		_, err := s3Tester.GetObjectRetention(ctx, bucket, "a", nil)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchObjectLockConfiguration, err)

		until := day(10)
		if err := s3Tester.PutObjectRetention(ctx, bucket, "a", nil, governance(until), false); err != nil {
			t.Fatal(err)
		}
		assertRetained(t, "a", nil, until)

		// resending the same date is not a weakening
		if err := s3Tester.PutObjectRetention(ctx, bucket, "a", nil, governance(until), false); err != nil {
			t.Fatal(err)
		}

		longer := day(20)
		if err := s3Tester.PutObjectRetention(ctx, bucket, "a", nil, governance(longer), false); err != nil {
			t.Fatal(err)
		}
		assertRetained(t, "a", nil, longer)

		shorter := day(2)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, s3Tester.PutObjectRetention(ctx, bucket, "a", nil, governance(shorter), false))
		if err := s3Tester.PutObjectRetention(ctx, bucket, "a", nil, governance(shorter), true); err != nil {
			t.Fatal(err)
		}
		assertRetained(t, "a", nil, shorter)

		// an empty Retention document is the clear request
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, s3Tester.PutObjectRetention(ctx, bucket, "a", nil, &types.ObjectLockRetention{}, false))
		if err := s3Tester.PutObjectRetention(ctx, bucket, "a", nil, &types.ObjectLockRetention{}, true); err != nil {
			t.Fatal(err)
		}
		_, err = s3Tester.GetObjectRetention(ctx, bucket, "a", nil)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchObjectLockConfiguration, err)
	})

	t.Run("ComplianceRefusesEvenWithBypass", func(t *testing.T) {
		put(t, bucket, "d")
		if err := s3Tester.PutObjectRetention(ctx, bucket, "d", nil, compliance(day(20)), false); err != nil {
			t.Fatal(err)
		}

		err := s3Tester.PutObjectRetention(ctx, bucket, "d", nil, compliance(day(2)), true)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

		err = s3Tester.PutObjectRetention(ctx, bucket, "d", nil, governance(day(30)), true)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	})

	t.Run("LegalHoldTogglesFreely", func(t *testing.T) {
		put(t, bucket, "g")

		// a version that never had one has no legal hold to report
		_, err := s3Tester.GetObjectLegalHold(ctx, bucket, "g", nil)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchObjectLockConfiguration, err)

		for _, want := range []types.ObjectLockLegalHoldStatus{types.ObjectLockLegalHoldStatusOn, types.ObjectLockLegalHoldStatusOff} {
			if err := s3Tester.PutObjectLegalHold(ctx, bucket, "g", nil, want); err != nil {
				t.Fatal(err)
			}
			status, err := s3Tester.GetObjectLegalHold(ctx, bucket, "g", nil)
			if err != nil {
				t.Fatal(err)
			} else if status != want {
				t.Fatalf("expected %q, got %q", want, status)
			}
		}
	})

	t.Run("RefusedWithoutObjectLock", func(t *testing.T) {
		plain := "unlocked"
		if err := s3Tester.CreateBucket(ctx, plain); err != nil {
			t.Fatal(err)
		}
		put(t, plain, "h")

		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, s3Tester.PutObjectRetention(ctx, plain, "h", nil, governance(day(5)), false))
		_, err := s3Tester.GetObjectRetention(ctx, plain, "h", nil)
		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)
		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, s3Tester.PutObjectLegalHold(ctx, plain, "h", nil, types.ObjectLockLegalHoldStatusOn))
		_, err = s3Tester.GetObjectLegalHold(ctx, plain, "h", nil)
		testutil.AssertS3Error(t, s3errs.ErrInvalidRequest, err)
	})

	t.Run("DeleteMarkerIsNotLockable", func(t *testing.T) {
		put(t, bucket, "marked")
		marked, err := s3Tester.DeleteObjectVersion(ctx, bucket, "marked", nil)
		if err != nil {
			t.Fatal(err)
		}

		testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, s3Tester.PutObjectRetention(ctx, bucket, "marked", nil, compliance(day(30)), false))
		testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, s3Tester.PutObjectLegalHold(ctx, bucket, "marked", nil, types.ObjectLockLegalHoldStatusOn))
		testutil.AssertS3Error(t, s3errs.ErrMethodNotAllowed, s3Tester.PutObjectRetention(ctx, bucket, "marked", marked.VersionId, compliance(day(30)), false))
	})

	t.Run("VersionAddressed", func(t *testing.T) {
		older, err := s3Tester.PutObjectVersion(ctx, bucket, "two", []byte("first"))
		if err != nil {
			t.Fatal(err)
		}
		current, err := s3Tester.PutObjectVersion(ctx, bucket, "two", []byte("second"))
		if err != nil {
			t.Fatal(err)
		}

		until := day(15)
		if err := s3Tester.PutObjectRetention(ctx, bucket, "two", &older, governance(until), false); err != nil {
			t.Fatal(err)
		}
		assertRetained(t, "two", &older, until)

		// the current version is untouched
		_, err = s3Tester.GetObjectRetention(ctx, bucket, "two", &current)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchObjectLockConfiguration, err)

		_, err = s3Tester.GetObjectRetention(ctx, bucket, "two", aws.String("0000000000000000000000000000000000000000000000000000000000000000"))
		testutil.AssertS3Error(t, s3errs.ErrNoSuchVersion, err)

		_, err = s3Tester.GetObjectRetention(ctx, bucket, "nope", nil)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
	})
}

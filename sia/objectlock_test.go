package sia_test

import (
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
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

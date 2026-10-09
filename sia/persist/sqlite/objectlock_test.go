package sqlite

import (
	"errors"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap/zaptest"
)

// TestPutBucketObjectLockConfigurationValidates checks the store rejects a
// malformed configuration.
func TestPutBucketObjectLockConfigurationValidates(t *testing.T) {
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, "bucket", true); err != nil {
		t.Fatal(err)
	}

	negative := -5
	ruleWithoutDefaultRetention := s3.ObjectLockConfiguration{
		ObjectLockEnabled: s3.ObjectLockEnabled,
		Rule:              &s3.ObjectLockRule{},
	}
	unknownMode := s3.ObjectLockConfiguration{
		ObjectLockEnabled: s3.ObjectLockEnabled,
		Rule:              &s3.ObjectLockRule{DefaultRetention: &s3.DefaultRetention{Mode: "bogus"}},
	}
	negativeDays := s3.ObjectLockConfiguration{
		ObjectLockEnabled: s3.ObjectLockEnabled,
		Rule: &s3.ObjectLockRule{
			DefaultRetention: &s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: &negative},
		},
	}

	if err := store.PutBucketObjectLockConfiguration(testAccessKeyID, "bucket", ruleWithoutDefaultRetention); !errors.Is(err, s3errs.ErrMalformedXML) {
		t.Fatalf("expected %v, got %v", s3errs.ErrMalformedXML, err)
	} else if err := store.PutBucketObjectLockConfiguration(testAccessKeyID, "bucket", unknownMode); !errors.Is(err, s3errs.ErrMalformedXML) {
		t.Fatalf("expected %v, got %v", s3errs.ErrMalformedXML, err)
	} else if err := store.PutBucketObjectLockConfiguration(testAccessKeyID, "bucket", negativeDays); !errors.Is(err, s3errs.ErrInvalidRetentionPeriod) {
		t.Fatalf("expected %v, got %v", s3errs.ErrInvalidRetentionPeriod, err)
	}
}

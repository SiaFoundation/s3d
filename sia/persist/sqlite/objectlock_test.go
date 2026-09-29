package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap/zaptest"
)

// TestPutBucketObjectLockConfigurationValidates checks the store rejects a
// malformed configuration.
func TestPutBucketObjectLockConfigurationValidates(t *testing.T) {
	store, err := OpenDatabase(filepath.Join(t.TempDir(), "s3d.sqlite3"), zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.CreateUser("user"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey("user", testAccessKeyID, "secret"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testAccessKeyID, "bucket", true); err != nil {
		t.Fatal(err)
	}

	negative := -5
	tests := []struct {
		name   string
		config s3.ObjectLockConfiguration
		err    s3errs.Error
	}{
		{
			name:   "RuleWithoutDefaultRetention",
			config: s3.ObjectLockConfiguration{ObjectLockEnabled: s3.ObjectLockEnabled, Rule: &s3.ObjectLockRule{}},
			err:    s3errs.ErrMalformedXML,
		},
		{
			name: "UnknownMode",
			config: s3.ObjectLockConfiguration{ObjectLockEnabled: s3.ObjectLockEnabled,
				Rule: &s3.ObjectLockRule{DefaultRetention: &s3.DefaultRetention{Mode: "bogus"}}},
			err: s3errs.ErrMalformedXML,
		},
		{
			name: "NegativeDays",
			config: s3.ObjectLockConfiguration{ObjectLockEnabled: s3.ObjectLockEnabled,
				Rule: &s3.ObjectLockRule{DefaultRetention: &s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: &negative}}},
			err: s3errs.ErrInvalidRetentionPeriod,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := store.PutBucketObjectLockConfiguration(testAccessKeyID, "bucket", tt.config); !errors.Is(err, tt.err) {
				t.Fatalf("expected %v, got %v", tt.err, err)
			}
		})
	}
}

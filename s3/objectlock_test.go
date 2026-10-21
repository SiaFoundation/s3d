package s3_test

import (
	"errors"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestObjectLockConfigurationValidate(t *testing.T) {
	retention := func(r s3.DefaultRetention) s3.ObjectLockConfiguration {
		return s3.ObjectLockConfiguration{
			ObjectLockEnabled: s3.ObjectLockEnabled,
			Rule:              &s3.ObjectLockRule{DefaultRetention: &r},
		}
	}

	tests := []struct {
		name    string
		config  s3.ObjectLockConfiguration
		wantErr error
	}{
		{"governance days", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: aws.Int(1)}), nil},
		{"compliance years", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeCompliance, Years: aws.Int(1)}), nil},
		{"no rule clears the default", s3.ObjectLockConfiguration{ObjectLockEnabled: s3.ObjectLockEnabled}, nil},
		{"disabled", s3.ObjectLockConfiguration{ObjectLockEnabled: "Disabled"}, s3errs.ErrMalformedXML},
		{"missing enabled", s3.ObjectLockConfiguration{}, s3errs.ErrMalformedXML},
		{"empty rule", s3.ObjectLockConfiguration{ObjectLockEnabled: s3.ObjectLockEnabled, Rule: &s3.ObjectLockRule{}}, s3errs.ErrMalformedXML},
		{"unknown mode", retention(s3.DefaultRetention{Mode: "abc", Years: aws.Int(1)}), s3errs.ErrMalformedXML},
		{"lowercase mode", retention(s3.DefaultRetention{Mode: "governance", Years: aws.Int(1)}), s3errs.ErrMalformedXML},
		{"days and years", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: aws.Int(1), Years: aws.Int(1)}), s3errs.ErrMalformedXML},
		{"no period", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance}), s3errs.ErrMalformedXML},
		{"zero days", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: aws.Int(0)}), s3errs.ErrInvalidRetentionPeriod},
		{"negative years", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Years: aws.Int(-1)}), s3errs.ErrInvalidRetentionPeriod},
		{"max days", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: aws.Int(36500)}), nil},
		{"too many days", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: aws.Int(36501)}), s3errs.ErrInvalidRetentionPeriod},
		{"max years", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Years: aws.Int(100)}), nil},
		{"too many years", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Years: aws.Int(101)}), s3errs.ErrInvalidRetentionPeriod},
		{"event hold unsupported", retention(s3.DefaultRetention{Mode: s3.ObjectLockModeGovernance, Days: aws.Int(1), DefaultEventHold: &s3.EventHoldDuration{Days: aws.Int(90)}}), s3errs.ErrNotImplemented},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Validate(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

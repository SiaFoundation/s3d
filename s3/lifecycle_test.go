package s3_test

import (
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestBucketLifecycleConfiguration(t *testing.T) {
	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", "foo", "bar"))

	const bucket = "lifecycle-bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// no configuration yet
	_, err := s3Tester.GetBucketLifecycleConfiguration(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchLifecycleConfiguration, err)

	rules := []types.LifecycleRule{
		{
			ID:     aws.String("abort-incomplete"),
			Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("uploads/")},
			AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
				DaysAfterInitiation: aws.Int32(7),
			},
		},
		{
			ID:         aws.String("expire-logs"),
			Status:     types.ExpirationStatusEnabled,
			Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
			Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)},
		},
	}

	// store the configuration
	if err := s3Tester.PutBucketLifecycleConfiguration(t.Context(), bucket, rules); err != nil {
		t.Fatal(err)
	}

	// read it back
	resp, err := s3Tester.GetBucketLifecycleConfiguration(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	} else if len(resp.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(resp.Rules))
	} else if abort := resp.Rules[0].AbortIncompleteMultipartUpload; abort == nil || aws.ToInt32(abort.DaysAfterInitiation) != 7 {
		t.Fatalf("unexpected abort rule: %+v", resp.Rules[0])
	} else if exp := resp.Rules[1].Expiration; exp == nil || aws.ToInt32(exp.Days) != 30 {
		t.Fatalf("unexpected expiration rule: %+v", resp.Rules[1])
	}

	// a bucket we don't own is forbidden
	otherTester := s3Tester.ChangeAccessKey(t, "foo", "bar")
	_, err = otherTester.GetBucketLifecycleConfiguration(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// delete the configuration
	if err := s3Tester.DeleteBucketLifecycle(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	_, err = s3Tester.GetBucketLifecycleConfiguration(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchLifecycleConfiguration, err)

	// delete is idempotent
	if err := s3Tester.DeleteBucketLifecycle(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// unknown bucket
	err = s3Tester.PutBucketLifecycleConfiguration(t.Context(), "nonexistent", rules)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// a malformed configuration maps onto ErrMalformedXML
	err = s3Tester.PutBucketLifecycleConfiguration(t.Context(), bucket, []types.LifecycleRule{{
		Status: types.ExpirationStatusEnabled,
		Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
	}})
	testutil.AssertS3Error(t, s3errs.ErrMalformedXML, err)

	// an unsupported feature maps onto ErrNotImplemented; a transition must be
	// rejected, not silently dropped from the stored configuration
	err = s3Tester.PutBucketLifecycleConfiguration(t.Context(), bucket, []types.LifecycleRule{{
		Status:     types.ExpirationStatusEnabled,
		Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
		Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
		Transitions: []types.Transition{{
			Days:         aws.Int32(1),
			StorageClass: types.TransitionStorageClassGlacier,
		}},
	}})
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, err)
}

func TestLifecycleCutoffs(t *testing.T) {
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		exp  *s3.LifecycleExpiration
		want time.Time
		ok   bool
	}{
		{"nil", nil, time.Time{}, false},
		// Days is anchored to the start of the current UTC day, not a rolling
		// window from now: 3 days before 2026-06-09T12:00Z is 2026-06-06T00:00Z.
		{"days", &s3.LifecycleExpiration{Days: 3}, time.Date(2026, 6, 6, 0, 0, 0, 0, time.UTC), true},
		{"past date", &s3.LifecycleExpiration{Date: "2020-01-01T00:00:00.000Z"}, now, true},
		{"future date", &s3.LifecycleExpiration{Date: "2030-01-01T00:00:00.000Z"}, time.Time{}, false},
		{"delete marker only", &s3.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}, time.Time{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got, ok := tc.exp.ExpiryCutoff(now); ok != tc.ok {
				t.Fatalf("expected ok=%v, got %v", tc.ok, ok)
			} else if ok && !got.Equal(tc.want) {
				t.Fatalf("expected cutoff %v, got %v", tc.want, got)
			}
		})
	}

	// AbortCutoff is anchored the same way as Days expiration: 5 days before
	// 2026-06-09T12:00Z is 2026-06-04T00:00Z.
	abort := &s3.AbortIncompleteMultipartUpload{DaysAfterInitiation: 5}
	want := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	if got := abort.AbortCutoff(now); !got.Equal(want) {
		t.Fatalf("expected abort cutoff %v, got %v", want, got)
	}
}

func TestLifecycleConfigurationValidate(t *testing.T) {
	enabled := func(rule s3.LifecycleRule) s3.LifecycleConfiguration {
		rule.Status = s3.LifecycleStatusEnabled
		if rule.Prefix == nil && rule.Filter == nil {
			// an explicitly empty filter matches the whole bucket
			rule.Filter = &s3.LifecycleFilter{}
		}
		return s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{rule}}
	}

	tests := []struct {
		name    string
		config  s3.LifecycleConfiguration
		wantErr bool
	}{
		{"valid abort", enabled(s3.LifecycleRule{AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{DaysAfterInitiation: 7}}), false},
		{"valid days", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 30}}), false},
		{"valid date", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Date: "2020-01-01T00:00:00.000Z"}}), false},
		{"no rules", s3.LifecycleConfiguration{}, true},
		{"bad status", s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{{Status: "Bogus", Expiration: &s3.LifecycleExpiration{Days: 1}}}}, true},
		{"prefix and filter", enabled(s3.LifecycleRule{Prefix: aws.String(""), Filter: &s3.LifecycleFilter{}, Expiration: &s3.LifecycleExpiration{Days: 1}}), true},
		{"no prefix or filter", s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{{Status: s3.LifecycleStatusEnabled, Expiration: &s3.LifecycleExpiration{Days: 1}}}}, true},
		{"no action", enabled(s3.LifecycleRule{}), true},
		{"days and date", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1, Date: "2020-01-01T00:00:00.000Z"}}), true},
		{"negative days", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: -1}}), true},
		{"bad date", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Date: "not-a-date"}}), true},
		{"empty expiration", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{}}), true},
		{"expired delete marker unsupported", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}}), true},
		{"days and expired delete marker", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1, ExpiredObjectDeleteMarker: aws.Bool(true)}}), true},
		{"abort zero days", enabled(s3.LifecycleRule{AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{DaysAfterInitiation: 0}}), true},
		{"tag filter", enabled(s3.LifecycleRule{Filter: &s3.LifecycleFilter{Tag: &s3.LifecycleTag{Key: "k", Value: "v"}}, Expiration: &s3.LifecycleExpiration{Days: 1}}), true},
		{"filter prefix and and", enabled(s3.LifecycleRule{Filter: &s3.LifecycleFilter{Prefix: aws.String("a/"), And: &s3.LifecycleAndOperator{Prefix: aws.String("b/")}}, Expiration: &s3.LifecycleExpiration{Days: 1}}), true},
		{"empty and filter", enabled(s3.LifecycleRule{Filter: &s3.LifecycleFilter{And: &s3.LifecycleAndOperator{}}, Expiration: &s3.LifecycleExpiration{Days: 1}}), true},
		{"transition action", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1}, Transitions: []s3.LifecycleUnsupportedAction{{}}}), true},
		{"noncurrent version expiration action", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1}, NoncurrentVersionExpiration: &s3.LifecycleUnsupportedAction{}}), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.wantErr != (err != nil) {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLifecycleEffectivePrefix(t *testing.T) {
	tests := []struct {
		name string
		rule s3.LifecycleRule
		want string
	}{
		{"none", s3.LifecycleRule{}, ""},
		{"rule prefix", s3.LifecycleRule{Prefix: aws.String("a/")}, "a/"},
		{"filter prefix", s3.LifecycleRule{Filter: &s3.LifecycleFilter{Prefix: aws.String("b/")}}, "b/"},
		{"and prefix", s3.LifecycleRule{Filter: &s3.LifecycleFilter{And: &s3.LifecycleAndOperator{Prefix: aws.String("c/")}}}, "c/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rule.EffectivePrefix(); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

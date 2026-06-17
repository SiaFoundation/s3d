package s3_test

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
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

func TestLifecycleExpirationResponseHeader(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	client := s3Tester.Client()

	const bucket = "lifecycle-bucket"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// a fixed future Date keeps the expected header deterministic regardless of
	// when the object is created
	expiry := time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := s3Tester.PutBucketLifecycleConfiguration(t.Context(), bucket, []types.LifecycleRule{{
		ID:         aws.String("expire-logs"),
		Status:     types.ExpirationStatusEnabled,
		Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
		Expiration: &types.LifecycleExpiration{Date: aws.Time(expiry)},
	}}); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("expiry-date=%q, rule-id=%q", expiry.Format(http.TimeFormat), "expire-logs")

	// PutObject, GetObject and HeadObject all advertise the applicable rule
	const object = "logs/app.log"
	put, err := client.PutObject(t.Context(), &service.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   strings.NewReader("hello"),
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(put.Expiration); got != want {
		t.Fatalf("expected PutObject expiration %q, got %q", want, got)
	}

	get, err := client.GetObject(t.Context(), &service.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		t.Fatal(err)
	}
	get.Body.Close()
	if got := aws.ToString(get.Expiration); got != want {
		t.Fatalf("expected GetObject expiration %q, got %q", want, got)
	}

	head, err := client.HeadObject(t.Context(), &service.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		t.Fatal(err)
	} else if got := aws.ToString(head.Expiration); got != want {
		t.Fatalf("expected HeadObject expiration %q, got %q", want, got)
	}

	// an object outside the rule's prefix carries no expiration header
	const unmatched = "data/app.log"
	if _, err := client.PutObject(t.Context(), &service.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(unmatched),
		Body:   strings.NewReader("hello"),
	}); err != nil {
		t.Fatal(err)
	}
	get, err = client.GetObject(t.Context(), &service.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(unmatched),
	})
	if err != nil {
		t.Fatal(err)
	}
	get.Body.Close()
	if get.Expiration != nil {
		t.Fatalf("expected no expiration header, got %q", aws.ToString(get.Expiration))
	}
}

func TestLifecycleCutoffs(t *testing.T) {
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		exp         *s3.LifecycleExpiration
		dayDuration time.Duration // zero defaults to 24h
		want        time.Time
		ok          bool
	}{
		{"nil", nil, 0, time.Time{}, false},
		// Days is anchored to the start of the current UTC day, not a rolling
		// window from now: 3 days before 2026-06-09T12:00Z is 2026-06-06T00:00Z.
		{"days", &s3.LifecycleExpiration{Days: 3}, 0, time.Date(2026, 6, 6, 0, 0, 0, 0, time.UTC), true},
		// a shortened day duration drops the calendar anchoring: 5 "days" of
		// 10s is a flat 50s before now.
		{"accelerated days", &s3.LifecycleExpiration{Days: 5}, 10 * time.Second, now.Add(-50 * time.Second), true},
		{"past date", &s3.LifecycleExpiration{Date: "2020-01-01T00:00:00.000Z"}, 0, now, true},
		{"future date", &s3.LifecycleExpiration{Date: "2030-01-01T00:00:00.000Z"}, 0, time.Time{}, false},
		{"delete marker only", &s3.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}, 0, time.Time{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dayDuration := tc.dayDuration
			if dayDuration == 0 {
				dayDuration = 24 * time.Hour
			}
			if got, ok := tc.exp.ExpiryCutoff(now, dayDuration); ok != tc.ok {
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
	if got := abort.AbortCutoff(now, 24*time.Hour); !got.Equal(want) {
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

	// wantErr is the expected wrapped sentinel error, or nil when the
	// configuration is valid.
	tests := []struct {
		name    string
		config  s3.LifecycleConfiguration
		wantErr error
	}{
		{"valid abort", enabled(s3.LifecycleRule{AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{DaysAfterInitiation: 7}}), nil},
		{"valid days", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 30}}), nil},
		{"valid date", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Date: "2020-01-01T00:00:00.000Z"}}), nil},
		{"id at limit", enabled(s3.LifecycleRule{ID: strings.Repeat("a", 255), Expiration: &s3.LifecycleExpiration{Days: 1}}), nil},
		{"id too long", enabled(s3.LifecycleRule{ID: strings.Repeat("a", 256), Expiration: &s3.LifecycleExpiration{Days: 1}}), s3errs.ErrInvalidArgument},
		{"duplicate ids", s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{
			{ID: "dup", Status: s3.LifecycleStatusEnabled, Filter: &s3.LifecycleFilter{}, Expiration: &s3.LifecycleExpiration{Days: 1}},
			{ID: "dup", Status: s3.LifecycleStatusEnabled, Filter: &s3.LifecycleFilter{}, Expiration: &s3.LifecycleExpiration{Days: 2}},
		}}, s3errs.ErrInvalidArgument},
		{"no rules", s3.LifecycleConfiguration{}, s3errs.ErrMalformedXML},
		{"bad status", s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{{Status: "Bogus", Expiration: &s3.LifecycleExpiration{Days: 1}}}}, s3errs.ErrMalformedXML},
		{"prefix and filter", enabled(s3.LifecycleRule{Prefix: aws.String(""), Filter: &s3.LifecycleFilter{}, Expiration: &s3.LifecycleExpiration{Days: 1}}), s3errs.ErrMalformedXML},
		{"no prefix or filter", s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{{Status: s3.LifecycleStatusEnabled, Expiration: &s3.LifecycleExpiration{Days: 1}}}}, s3errs.ErrMalformedXML},
		{"no action", enabled(s3.LifecycleRule{}), s3errs.ErrMalformedXML},
		{"days and date", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1, Date: "2020-01-01T00:00:00.000Z"}}), s3errs.ErrMalformedXML},
		{"negative days", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: -1}}), s3errs.ErrInvalidArgument},
		{"zero days", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{}}), s3errs.ErrInvalidArgument},
		{"bad date", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Date: "not-a-date"}}), s3errs.ErrMalformedXML},
		{"non-midnight date", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Date: "2020-01-01T12:34:56Z"}}), s3errs.ErrInvalidArgument},
		{"expired delete marker unsupported", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}}), s3errs.ErrNotImplemented},
		{"days and expired delete marker", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1, ExpiredObjectDeleteMarker: aws.Bool(true)}}), s3errs.ErrMalformedXML},
		{"abort zero days", enabled(s3.LifecycleRule{AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{DaysAfterInitiation: 0}}), s3errs.ErrMalformedXML},
		{"tag filter", enabled(s3.LifecycleRule{Filter: &s3.LifecycleFilter{Tag: &s3.LifecycleTag{Key: "k", Value: "v"}}, Expiration: &s3.LifecycleExpiration{Days: 1}}), s3errs.ErrNotImplemented},
		{"filter prefix and and", enabled(s3.LifecycleRule{Filter: &s3.LifecycleFilter{Prefix: aws.String("a/"), And: &s3.LifecycleAndOperator{Prefix: aws.String("b/")}}, Expiration: &s3.LifecycleExpiration{Days: 1}}), s3errs.ErrMalformedXML},
		{"empty and filter", enabled(s3.LifecycleRule{Filter: &s3.LifecycleFilter{And: &s3.LifecycleAndOperator{}}, Expiration: &s3.LifecycleExpiration{Days: 1}}), s3errs.ErrMalformedXML},
		{"transition action", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1}, Transitions: []s3.LifecycleUnsupportedAction{{}}}), s3errs.ErrNotImplemented},
		{"noncurrent version expiration action", enabled(s3.LifecycleRule{Expiration: &s3.LifecycleExpiration{Days: 1}, NoncurrentVersionExpiration: &s3.LifecycleUnsupportedAction{}}), s3errs.ErrNotImplemented},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Validate(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLifecycleExpirationHeader(t *testing.T) {
	// last-modified just after midnight so the +Days window rounds up to a
	// predictable midnight boundary.
	mod := time.Date(2026, 6, 11, 9, 30, 0, 0, time.UTC)

	config := s3.LifecycleConfiguration{Rules: []s3.LifecycleRule{
		{ID: "disabled", Status: s3.LifecycleStatusDisabled, Prefix: aws.String("logs/"), Expiration: &s3.LifecycleExpiration{Days: 1}},
		{ID: "soon", Status: s3.LifecycleStatusEnabled, Prefix: aws.String("logs/"), Expiration: &s3.LifecycleExpiration{Days: 3}},
		{ID: "later", Status: s3.LifecycleStatusEnabled, Prefix: aws.String("logs/"), Expiration: &s3.LifecycleExpiration{Days: 10}},
	}}

	// the soonest applicable enabled rule wins: 2026-06-11 09:30 + 3 days =
	// 2026-06-14 09:30, rounded up to the next midnight = 2026-06-15.
	want := `expiry-date="Mon, 15 Jun 2026 00:00:00 GMT", rule-id="soon"`
	if got := config.ExpirationHeader("logs/app.log", mod); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}

	// no rule matches a different prefix
	if got := config.ExpirationHeader("data/app.log", mod); got != "" {
		t.Fatalf("expected no header, got %q", got)
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

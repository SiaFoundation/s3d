package s3

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap/zaptest"
)

var (
	// ownerCaller signs with one of the owner's access keys
	ownerCaller = &auth.Caller{AccessKeyID: "second-owner-key", UserID: "owner"}
	otherCaller = &auth.Caller{AccessKeyID: "foo", UserID: "other"}
)

// authorizationBackend serves a single bucket, "bucket", whose policy document
// a test can replace between requests. This package's tests cannot use
// testutil, which imports it, so it stands in for a real backend.
type authorizationBackend struct {
	Backend
	document string
}

func (b *authorizationBackend) BucketAccessInfo(_ context.Context, bucket string) (BucketAccessInfo, error) {
	if bucket != "bucket" {
		return BucketAccessInfo{}, s3errs.ErrNoSuchBucket
	}
	return BucketAccessInfo{Owner: &UserInfo{ID: "owner"}, PolicyDocument: b.document}, nil
}

// TestAuthorize checks that a bucket without a policy is only accessible to
// its owner, and that an anonymous caller cannot probe for buckets.
func TestAuthorize(t *testing.T) {
	backend := &authorizationBackend{}
	handler := &s3{backend: backend, logger: zaptest.NewLogger(t)}
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	tests := []struct {
		name   string
		caller *auth.Caller
		bucket string
		err    error
	}{
		{"owner", ownerCaller, "bucket", nil},
		{"anonymous", nil, "bucket", s3errs.ErrAccessDenied},
		{"other user", otherCaller, "bucket", s3errs.ErrAccessDenied},
		{"anonymous missing bucket", nil, "missing", s3errs.ErrAccessDenied},
		{"signed missing bucket", otherCaller, "missing", s3errs.ErrNoSuchBucket},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := handler.authorize(req, test.caller, test.bucket, "key", operation{action: actionGetObject}); !errors.Is(err, test.err) {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
		})
	}

	// even the owner is denied an empty action
	if _, err := handler.authorize(req, ownerCaller, "bucket", "", operation{}); !errors.Is(err, s3errs.ErrAccessDenied) {
		t.Fatalf("empty action: expected %v, got %v", s3errs.ErrAccessDenied, err)
	}
}

// TestAuthorizeCurrentPolicy checks that each request is decided by the
// bucket's current policy.
func TestAuthorizeCurrentPolicy(t *testing.T) {
	backend := &authorizationBackend{}
	handler := &s3{backend: backend, logger: zaptest.NewLogger(t)}
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)

	backend.document = `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}}`
	if _, err := handler.authorize(req, nil, "bucket", "key", operation{action: actionGetObject}); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"", actionGetObjectVersion, actionListBucket, actionPutObject} {
		if _, err := handler.authorize(req, nil, "bucket", "key", operation{action: action}); !errors.Is(err, s3errs.ErrAccessDenied) {
			t.Fatalf("action %q: expected %v, got %v", action, s3errs.ErrAccessDenied, err)
		}
	}

	// replacing and deleting a policy must take effect even after a cache hit
	backend.document = `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}}`
	if _, err := handler.authorize(req, nil, "bucket", "key", operation{action: actionGetObject}); !errors.Is(err, s3errs.ErrAccessDenied) {
		t.Fatalf("replaced policy: expected %v, got %v", s3errs.ErrAccessDenied, err)
	}
	if _, err := handler.authorize(req, nil, "bucket", "", operation{action: actionListBucket}); err != nil {
		t.Fatal(err)
	}
	backend.document = ""
	if _, err := handler.authorize(req, nil, "bucket", "", operation{action: actionListBucket}); !errors.Is(err, s3errs.ErrAccessDenied) {
		t.Fatalf("deleted policy: expected %v, got %v", s3errs.ErrAccessDenied, err)
	}

	// a policy that no longer parses must not lock the owner out of fixing it
	backend.document = `{`
	if _, err := handler.authorize(req, ownerCaller, "bucket", "", operation{action: actionPutBucketPolicy}); err != nil {
		t.Fatal(err)
	}
	// and grants nothing to anyone else, without revealing that it is broken
	for _, caller := range []*auth.Caller{nil, otherCaller} {
		if _, err := handler.authorize(req, caller, "bucket", "", operation{action: actionListBucket}); !errors.Is(err, s3errs.ErrAccessDenied) {
			t.Fatalf("unparseable policy: expected %v, got %v", s3errs.ErrAccessDenied, err)
		}
	}
}

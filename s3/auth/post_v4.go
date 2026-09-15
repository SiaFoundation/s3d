package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// maxPostPolicySignatureAge is the maximum allowed age of a SigV4 POST policy
// signature, measured from the submitted x-amz-date.
//
// https://docs.aws.amazon.com/AmazonS3/latest/developerguide/bucket-policy-s3-sigv4-conditions.html
const maxPostPolicySignatureAge = 7 * 24 * time.Hour

// PostPolicyAuth carries the authentication form fields of a POST Object
// request. Policy is the base64 document exactly as it was submitted, since
// that is the string the signature covers.
//
// https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-authentication-HTTPPOST.html
type PostPolicyAuth struct {
	Algorithm  string
	Credential string
	Date       string
	Policy     string
	Signature  string
}

// Verify checks the SigV4 signature over the submitted base64 policy and
// returns the access key ID it was signed with.
//
// Verify does not parse or validate the policy document. Before authorizing
// the upload, the caller must validate its expiration and all conditions
// against the submitted form, target bucket, and uploaded content. This
// includes requiring and enforcing exact matches for x-amz-algorithm,
// x-amz-credential, and x-amz-date, and ensuring all required form fields are
// covered by policy conditions.
//
// - 'now' refers to the current time and is used to reject signatures dated
// in the future beyond the allowed clock skew or more than seven days in
// the past. The policy's expiration may impose an earlier deadline.
// - 'region' is the AWS region the request is targeted to. If the region is an
// empty string, every region is allowed.
func (p PostPolicyAuth) Verify(ctx context.Context, store KeyStore, region string, now time.Time) (string, error) {
	switch p.Algorithm {
	case AuthorizationAWS4HMACSHA256:
	case AuthorizationAWS4ECDSAP256SHA256:
		return "", s3errs.ErrNotImplemented // Signature Version 4A is not implemented
	default:
		return "", s3errs.ErrAccessDenied
	}

	credential, ok := parseCredential(p.Credential)
	if !ok {
		return "", s3errs.ErrAccessDenied
	} else if region != "" && credential.Scope.Region != region {
		return "", s3errs.ErrAccessDenied
	}

	date, err := time.Parse(layoutISO8601, p.Date)
	if err != nil || !sameDay(date, credential.Scope.Date) {
		return "", s3errs.ErrAccessDenied
	} else if date.After(now.Add(maxClockSkew)) {
		return "", s3errs.ErrRequestTimeTooSkewed
	} else if now.After(date.Add(maxPostPolicySignatureAge)) {
		return "", s3errs.ErrAccessDeniedExpired
	}

	if p.Policy == "" {
		return "", s3errs.ErrAccessDenied
	}

	// reject a malformed signature before loading the secret or hashing the
	// policy
	if decoded, err := hex.DecodeString(p.Signature); err != nil || len(decoded) != sha256.Size {
		return "", s3errs.ErrSignatureDoesNotMatch
	}

	secret, err := store.LoadSecret(ctx, credential.AccessKeyID)
	if err != nil {
		return "", err
	}
	defer clear(secret)

	signingKey := signingKey(secret, date, credential.Scope.Region)
	defer clear(signingKey)

	// unlike the header and presigned URL paths, there is no canonical
	// request, the submitted base64 policy is signed without modification
	expected := getSignature(signingKey, p.Policy)
	if subtle.ConstantTimeCompare([]byte(expected), []byte(p.Signature)) != 1 {
		return "", s3errs.ErrSignatureDoesNotMatch
	}
	return credential.AccessKeyID, nil
}

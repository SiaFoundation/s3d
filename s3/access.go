package s3

import (
	"errors"
	"net/http"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// maxCachedPolicies bounds the parsed policy cache, whose entries outlive
// deleted buckets and policies.
const maxCachedPolicies = 1000

// cachedPolicy is a bucket's policy document and its parsed form.
type cachedPolicy struct {
	document string
	policy   bucketPolicy
}

// BucketAccessInfo is what the S3 API handler needs to authorize a request on
// a bucket, as returned by [Backend.BucketAccessInfo].
type BucketAccessInfo struct {
	// Owner is the bucket's owner. Its ID must be the user ID the backend's
	// LoadSecret reports for the owner's access keys, see [auth.Caller].
	Owner *UserInfo
	// PolicyDocument is the bucket's policy document, or "" if it has none.
	PolicyDocument string
}

// bucketAccess is what one caller may do in one bucket, loaded once per
// request.
type bucketAccess struct {
	caller         *auth.Caller
	bucket         string
	owner          *UserInfo
	policyDocument string
	policy         bucketPolicy
	isOwner        bool // false for an anonymous caller
}

// operation is a routed request: the action it requires and the handler that
// serves it once the caller is allowed that action.
type operation struct {
	action string
	// multiObjectDelete marks a DeleteObjects request. Its handler authorizes
	// each object named in the body, so it is not given an action.
	multiObjectDelete bool
	serve             func(http.ResponseWriter, *http.Request, *bucketAccess) error
}

// authorize resolves the caller's access to bucket and requires op's action on
// it, or on object if the action applies to objects.
func (s *s3) authorize(r *http.Request, caller *auth.Caller, bucket, object string, op operation) (*bucketAccess, error) {
	switch op.action {
	case actionCreateBucket, actionListAllMyBuckets:
		// no bucket policy governs these and s3d has no identity policies, so
		// every signed caller is allowed them
		if caller == nil {
			return nil, s3errs.ErrAccessDenied
		}
		return &bucketAccess{caller: caller, bucket: bucket}, nil
	}

	access, err := s.bucketAccess(r, caller, bucket)
	if err != nil {
		return nil, err
	}
	switch {
	case op.multiObjectDelete:
		// the handler authorizes each object once it has read their names, but
		// a caller that may delete nothing is denied before the body is read
		err = access.assertAllowedInBucket(actionDeleteObject, actionDeleteObjectVersion)
	case isObjectAction(op.action):
		err = access.assertObjectAllowed(op.action, object)
	default:
		err = access.assertBucketAllowed(op.action)
	}
	if err != nil {
		return nil, err
	}
	return access, nil
}

// bucketAccess resolves the caller's access to bucket. An anonymous caller gets
// ErrAccessDenied for a missing bucket, so it cannot probe for bucket names.
func (s *s3) bucketAccess(r *http.Request, caller *auth.Caller, bucket string) (*bucketAccess, error) {
	info, err := s.backend.BucketAccessInfo(r.Context(), bucket)
	if errors.Is(err, s3errs.ErrNoSuchBucket) && caller == nil {
		return nil, s3errs.ErrAccessDenied
	} else if err != nil {
		return nil, err
	}
	access := &bucketAccess{
		caller:         caller,
		bucket:         bucket,
		owner:          info.Owner,
		policyDocument: info.PolicyDocument,
		isOwner:        caller != nil && caller.UserID == info.Owner.ID,
	}
	// the owner is allowed everything, so a policy that no longer parses never
	// locks them out of replacing it
	if !access.isOwner {
		access.policy = s.parsedPolicy(bucket, info.PolicyDocument)
	}
	return access, nil
}

// authorizeCopySource checks that the caller may read srcVersion of srcObject,
// reusing dst's access for a same-bucket copy.
func (s *s3) authorizeCopySource(r *http.Request, dst *bucketAccess, srcBucket, srcObject string, srcVersion VersionRequest) (*bucketAccess, error) {
	src := dst
	if srcBucket != dst.bucket {
		var err error
		if src, err = s.bucketAccess(r, dst.caller, srcBucket); err != nil {
			return nil, err
		}
	}
	if err := src.assertObjectAllowed(readAction(srcVersion), srcObject); err != nil {
		return nil, err
	}
	return src, nil
}

// assertAuth returns the access key ID of a signed caller, or ErrAccessDenied
// for an anonymous one.
func (a *bucketAccess) assertAuth() (string, error) {
	if a.caller == nil {
		return "", s3errs.ErrAccessDenied
	}
	return a.caller.AccessKeyID, nil
}

// assertAllowed checks that the caller may perform action on resource. An
// empty action is always denied, even for the owner.
func (a *bucketAccess) assertAllowed(action, resource string) error {
	if action != "" && (a.isOwner || a.policy.allows(action, resource)) {
		return nil
	}
	return s3errs.ErrAccessDenied
}

// assertBucketAllowed checks that the caller may perform action on the bucket
// itself.
func (a *bucketAccess) assertBucketAllowed(action string) error {
	return a.assertAllowed(action, bucketARN(a.bucket))
}

// assertObjectAllowed checks that the caller may perform action on an object
// in the bucket.
func (a *bucketAccess) assertObjectAllowed(action, object string) error {
	return a.assertAllowed(action, objectARN(a.bucket, object))
}

// assertAllowedInBucket checks that the caller may perform one of actions on
// at least one resource in the bucket.
func (a *bucketAccess) assertAllowedInBucket(actions ...string) error {
	for _, action := range actions {
		if a.isOwner || a.policy.allowsInBucket(action, a.bucket) {
			return nil
		}
	}
	return s3errs.ErrAccessDenied
}

// hideMissing hides missing keys and versions from callers that may not
// list the bucket, so they cannot enumerate it by probing keys.
func (a *bucketAccess) hideMissing(err error) error {
	if errors.Is(err, s3errs.ErrNoSuchKey) || errors.Is(err, s3errs.ErrNoSuchVersion) {
		if denied := a.assertBucketAllowed(actionListBucket); denied != nil {
			return denied
		}
	}
	return err
}

// hideMissingSource is [bucketAccess.hideMissing] for errors the backend
// attributes to the source of a copy.
func (a *bucketAccess) hideMissingSource(err error) error {
	if errors.As(err, new(CopySourceError)) {
		return a.hideMissing(err)
	}
	return err
}

// parsedPolicy returns the parsed form of a bucket's policy document, cached
// until the document changes. A document that no longer parses grants nothing;
// the error is only logged, so callers cannot tell the bucket has a policy.
func (s *s3) parsedPolicy(bucket, document string) bucketPolicy {
	if document == "" {
		return bucketPolicy{}
	}
	s.policyMu.Lock()
	cached, ok := s.policies[bucket]
	s.policyMu.Unlock()
	if ok && cached.document == document {
		return cached.policy
	}

	// parse outside the lock so a miss does not stall every other request
	policy, err := parseBucketPolicy(bucket, document)
	if err != nil {
		// cached too, so a broken document is only logged once
		s.logger.Warn("failed to parse stored bucket policy", zap.String("bucket", bucket), zap.Error(err))
		policy = bucketPolicy{}
	}

	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	if s.policies == nil {
		s.policies = make(map[string]cachedPolicy)
	}
	if _, ok := s.policies[bucket]; !ok && len(s.policies) >= maxCachedPolicies {
		// evict an arbitrary entry; a hot bucket is simply parsed again
		for evict := range s.policies {
			delete(s.policies, evict)
			break
		}
	}
	s.policies[bucket] = cachedPolicy{document: document, policy: policy}
	return policy
}

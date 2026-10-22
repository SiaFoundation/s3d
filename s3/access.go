package s3

import (
	"errors"
	"math"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// BucketID identifies a bucket for its lifetime.
type BucketID int64

// NewBucketID returns a random, positive bucket ID.
func NewBucketID() BucketID {
	return BucketID(frand.Uint64n(math.MaxInt64)) + 1
}

// BucketAccessInfo is what the S3 API handler needs to authorize a request on
// a bucket, as returned by [Backend.BucketAccessInfo].
type BucketAccessInfo struct {
	// ID is the bucket's ID, passed to backend calls instead of its name.
	ID BucketID
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
	bucketID       BucketID
	bucket         string
	owner          *UserInfo
	policyDocument string
	policy         bucketPolicy
	isOwner        bool          // false for an anonymous caller
	requestContext policyContext // nil when the policy has no statements
	request        *http.Request
}

// operation is a routed request: the action it requires and the handler that
// serves it once the caller is allowed that action.
type operation struct {
	action string
	// multiObjectDelete marks a DeleteObjects request. Its handler authorizes
	// each object named in the body, so it is not given an action.
	multiObjectDelete bool
	// bodyConditions marks an action whose condition keys are read from the
	// body, so its handler checks the action once it has read the body.
	bodyConditions bool
	// formAuth marks a POST Object request, which authenticates through its
	// form fields. Its handler authorizes the caller once it has verified
	// them, so it is not given an action.
	formAuth bool
	serve    func(http.ResponseWriter, *http.Request, *bucketAccess) error
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
	if op.formAuth {
		// the bucket is not resolved until the form is verified, so an
		// anonymous caller cannot probe for bucket names
		return &bucketAccess{caller: caller, bucket: bucket}, nil
	}

	access, err := s.bucketAccess(r, caller, bucket)
	if err != nil {
		return nil, err
	}
	switch {
	case op.multiObjectDelete:
		// the keys and version IDs are only known once the body is read, so
		// deleteObjects checks every key, including explicit denies
		err = access.assertAnyAllowedInBucket(actionDeleteObject, actionDeleteObjectVersion)
	case op.bodyConditions:
		// the handler checks the action once the body supplies its keys
		err = access.assertAnyAllowedInBucket(op.action)
	case isObjectAction(op.action):
		err = access.assertObjectAllowed(op.action, object)
	default:
		err = access.assertBucketAllowed(op.action)
	}
	if err != nil {
		return nil, err
	}
	switch op.action {
	case actionGetBucketPolicy, actionPutBucketPolicy, actionDeleteBucketPolicy:
		// S3 reserves the policy APIs for the owner even when a policy grants
		// them to another user
		if !access.isOwner {
			return nil, s3errs.ErrMethodNotAllowed
		}
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
		bucketID:       info.ID,
		bucket:         bucket,
		owner:          info.Owner,
		policyDocument: info.PolicyDocument,
		isOwner:        caller != nil && caller.UserID == info.Owner.ID,
		policy:         s.parsedPolicy(bucket, info.PolicyDocument),
		request:        r,
	}
	// without statements only ownership decides access
	if len(access.policy.statements) > 0 {
		access.requestContext = s.requestPolicyContext(r, caller)
		// the bucket's owner is the user the resource belongs to
		access.requestContext["aws:resourceaccount"] = []string{info.Owner.ID}
		access.requestContext["s3:resourceaccount"] = []string{info.Owner.ID}
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
	if err := src.assertVersionAllowed(readAction(srcVersion), srcObject, srcVersion, nil); err != nil {
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
	requestContext, err := a.actionPolicyContext(action)
	if err != nil {
		return err
	}
	return a.check(requestContext, action, resource)
}

// check checks that the policy allows the caller action on resource, given
// requestContext.
func (a *bucketAccess) check(requestContext policyContext, action, resource string) error {
	if a.policy.allows(a.caller, requestContext, a.isOwner, action, resource) {
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

// assertAnyAllowedInBucket denies a request up front when no statement could
// allow any of actions. Resource and condition checks wait for the body.
func (a *bucketAccess) assertAnyAllowedInBucket(actions ...string) error {
	if a.isOwner {
		return nil
	}
	for _, stmt := range a.policy.statements {
		if stmt.Effect == policyEffectAllow && stmt.principalMatches(a.caller) && slices.ContainsFunc(actions, stmt.namesAction) {
			return nil
		}
	}
	return s3errs.ErrAccessDenied
}

// assertVersionAllowed supplies a version and extra context keys independently
// of the URL, for copy sources and individual keys in a multi-delete request.
func (a *bucketAccess) assertVersionAllowed(action, object string, version VersionRequest, extra policyContext) error {
	if version.Specified {
		extra = extra.merge(policyContext{"s3:versionid": {FormatVersion(version.ID)}})
	}
	return a.check(a.requestContext.merge(extra), action, objectARN(a.bucket, object))
}

// assertObjectLockAllowed checks that the caller may stamp lock on a new
// version of object. Each action sees only its own part of the lock.
func (a *bucketAccess) assertObjectLockAllowed(object string, lock *ObjectLockState) error {
	if lock.Retained() {
		retention := &ObjectLockState{Mode: lock.Mode, RetainUntil: lock.RetainUntil}
		if err := a.assertLockActionAllowed(actionPutObjectRetention, object, retention); err != nil {
			return err
		}
	}
	if lock != nil && lock.LegalHold != "" {
		return a.assertLockActionAllowed(actionPutObjectLegalHold, object, &ObjectLockState{LegalHold: lock.LegalHold})
	}
	return nil
}

// assertLockActionAllowed checks that the caller may perform action on object
// to apply lock.
func (a *bucketAccess) assertLockActionAllowed(action, object string, lock *ObjectLockState) error {
	requestContext, err := a.actionPolicyContext(action)
	if err != nil {
		return err
	}
	return a.check(requestContext.merge(objectLockPolicyContext(lock)), action, objectARN(a.bucket, object))
}

// visibleObjectLock returns the part of a version's lock state the caller may
// read.
func (a *bucketAccess) visibleObjectLock(object string, lock *ObjectLockState) *ObjectLockState {
	if lock == nil {
		return nil
	}
	visible := *lock
	if a.assertObjectAllowed(actionGetObjectRetention, object) != nil {
		visible.Mode, visible.RetainUntil = "", time.Time{}
	}
	if a.assertObjectAllowed(actionGetObjectLegalHold, object) != nil {
		visible.LegalHold = ""
	}
	return &visible
}

// bypassGovernanceRetention reports whether the request asked to bypass a
// GOVERNANCE retention and the caller may bypass it on every one of objects.
func (a *bucketAccess) bypassGovernanceRetention(h http.Header, objects ...string) bool {
	if !bypassGovernanceRetention(h) {
		return false
	}
	for _, object := range objects {
		if a.assertObjectAllowed(actionBypassGovernanceRetention, object) != nil {
			return false
		}
	}
	return true
}

// hideMissing hides a missing object or version from callers that may not
// list it, so they cannot enumerate the bucket by probing keys.
func (a *bucketAccess) hideMissing(err error, object string) error {
	if errors.Is(err, s3errs.ErrNoSuchKey) || errors.Is(err, s3errs.ErrNoSuchVersion) {
		if denied := a.assertListAllowed(object); denied != nil {
			return denied
		}
	}
	return err
}

// assertListAllowed checks that the caller may list the bucket with object as
// the prefix, and so learn whether object exists. s3:max-keys is the default
// limit, since a listing always has one.
func (a *bucketAccess) assertListAllowed(object string) error {
	requestContext := a.requestContext.merge(policyContext{
		"s3:prefix":   {object},
		"s3:max-keys": {strconv.Itoa(DefaultMaxBucketKeys)},
	})
	return a.check(requestContext, actionListBucket, bucketARN(a.bucket))
}

// hideMissingSource is [bucketAccess.hideMissing] for errors the backend
// attributes to the source of a copy.
func (a *bucketAccess) hideMissingSource(err error, object string) error {
	if errors.As(err, new(CopySourceError)) {
		return a.hideMissing(err, object)
	}
	return err
}

// parsedPolicy returns the parsed form of a bucket's policy document. A
// document that no longer parses grants nothing; the error is only logged, so
// callers cannot tell the bucket has a policy.
func (s *s3) parsedPolicy(bucket, document string) bucketPolicy {
	if document == "" {
		return bucketPolicy{}
	}
	policy, err := parseBucketPolicy(bucket, document)
	if err != nil {
		s.logger.Warn("failed to parse stored bucket policy", zap.String("bucket", bucket), zap.Error(err))
		return bucketPolicy{}
	}
	return policy
}

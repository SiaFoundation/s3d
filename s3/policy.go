package s3

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

const (
	// the only policy language version accepted; "2008-10-17" defaults differ
	policyVersion = "2012-10-17"

	policyEffectAllow = "Allow"
	policyEffectDeny  = "Deny"

	// the only principal accepted, which makes a grant available to
	// unauthenticated callers
	policyWildcard = "*"

	policyARNPrefix = "arn:aws:s3:::"

	// matches the limit AWS enforces on bucket policies
	maxPolicySize = 20 << 10
)

// PolicyActions is a set of S3 actions that a bucket policy grants to
// everyone.
type PolicyActions uint8

// These values are stored verbatim in the buckets table, so a bit may never be
// reordered or reused; a new action takes the next free bit.
const (
	// ActionGetObject grants GetObject and HeadObject for an object's current version.
	ActionGetObject PolicyActions = 1
	// ActionGetObjectVersion grants GetObject and HeadObject for a named version.
	ActionGetObjectVersion PolicyActions = 2
	// ActionListBucket grants ListObjects.
	ActionListBucket PolicyActions = 4
	// ActionListBucketVersions grants ListObjectVersions.
	ActionListBucketVersions PolicyActions = 8
)

// S3 grants an object action on "<bucket>/*" and a bucket action on "<bucket>".
const (
	objectActions = ActionGetObject | ActionGetObjectVersion
	bucketActions = ActionListBucket | ActionListBucketVersions
)

// ReadAction returns the action a read of the given version requires. The two
// are granted separately, so every read path must ask for the one it performs.
func ReadAction(version VersionRequest) PolicyActions {
	if version.Specified {
		return ActionGetObjectVersion
	}
	return ActionGetObject
}

// Allows reports whether every action in want is granted. An empty want is not
// allowed: it is contained in every set, so treating it as satisfied would
// authorize a caller that named no action against any policy, including none.
func (a PolicyActions) Allows(want PolicyActions) bool {
	return want != 0 && a&want == want
}

// supportedPolicyActions is keyed by lowercase name: AWS treats the service
// prefix and the action name as case insensitive.
//
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_action.html
var supportedPolicyActions = map[string]PolicyActions{
	"s3:getobject":          ActionGetObject,
	"s3:getobjectversion":   ActionGetObjectVersion,
	"s3:listbucket":         ActionListBucket,
	"s3:listbucketversions": ActionListBucketVersions,
}

// BucketPolicy is a bucket policy accepted by [parseBucketPolicy].
type BucketPolicy struct {
	// Document is the policy JSON as the client supplied it. It is kept
	// verbatim so GetBucketPolicy round-trips byte-for-byte, which clients like
	// Terraform rely on to detect drift.
	Document string

	// Public is the set of actions the policy grants to everyone, which
	// includes signed requests from other users as well as unsigned ones. It is
	// derived once, when the policy is stored.
	Public PolicyActions
}

// policyDocument is the JSON form of a bucket policy, whose keys are PascalCase.
//
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-policies.html
//
// nolint:tagliatelle
type policyDocument struct {
	Version   string           `json:"Version"`
	ID        string           `json:"Id"`
	Statement policyStatements `json:"Statement"`
}

// policyStatements is the Statement element, which AWS allows as either a
// single statement or an array of them.
type policyStatements []policyStatement

// UnmarshalJSONFrom implements json.UnmarshalerFrom. Decoding through the
// caller's decoder keeps its options, so members unknown to a statement are
// still rejected.
func (p *policyStatements) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() == '{' {
		var single policyStatement
		if err := json.UnmarshalDecode(dec, &single); err != nil {
			return err
		}
		*p = policyStatements{single}
		return nil
	}
	return json.UnmarshalDecode(dec, (*[]policyStatement)(p))
}

// policyStatement is a single statement of a [policyDocument]. The elements s3d
// cannot honor are still parsed so their presence can be rejected.
//
// nolint:tagliatelle
type policyStatement struct {
	Sid       string           `json:"Sid"`
	Effect    string           `json:"Effect"`
	Principal *policyPrincipal `json:"Principal"`
	Action    policyStrings    `json:"Action"`
	Resource  policyStrings    `json:"Resource"`

	NotPrincipal jsontext.Value `json:"NotPrincipal"`
	NotAction    jsontext.Value `json:"NotAction"`
	NotResource  jsontext.Value `json:"NotResource"`
	Condition    jsontext.Value `json:"Condition"`
}

// policyStrings is a policy element that is either a string or an array of
// them, the form Action and Resource both take.
type policyStrings []string

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (p *policyStrings) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() == '"' {
		var single string
		if err := json.UnmarshalDecode(dec, &single); err != nil {
			return err
		}
		*p = policyStrings{single}
		return nil
	}
	return json.UnmarshalDecode(dec, (*[]string)(p))
}

// policyPrincipal is the Principal element: either "*" or an object keyed by
// principal type, e.g. {"AWS": "*"}. Only whether it covers every caller
// matters, so an unsupported principal decodes to false rather than erroring.
type policyPrincipal bool

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (p *policyPrincipal) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() == '"' {
		var single string
		if err := json.UnmarshalDecode(dec, &single); err != nil {
			return err
		}
		*p = policyPrincipal(single == policyWildcard)
		return nil
	}
	var byType map[string]policyStrings
	if err := json.UnmarshalDecode(dec, &byType); err != nil {
		return err
	}
	aws := byType["AWS"]
	*p = policyPrincipal(len(byType) == 1 && len(aws) == 1 && aws[0] == policyWildcard)
	return nil
}

// parseBucketPolicy reads and validates a bucket policy for the named bucket,
// returning the actions it grants anonymously.
//
// A document is accepted only when every statement allows actions from
// [supportedPolicyActions] to everyone. Anything that would narrow, invert or
// widen such a grant is rejected rather than ignored, which would grant more
// than the document describes: [s3errs.ErrMalformedPolicy] if it is
// structurally wrong or names another bucket, [s3errs.ErrNotImplemented] if it
// is valid but expresses access s3d cannot represent.
func parseBucketPolicy(bucket string, body io.Reader) (BucketPolicy, error) {
	// read one byte past the limit so an oversized document is reported as such
	// rather than truncated into a malformed one
	document, err := io.ReadAll(io.LimitReader(body, maxPolicySize+1))
	if err != nil {
		return BucketPolicy{}, err
	} else if len(document) > maxPolicySize {
		return BucketPolicy{}, s3errs.ErrPolicyTooLarge
	}

	// json/v2 rejects duplicate member names and trailing content, neither of
	// which v1 catches: keeping the last of two "Effect" members would read a
	// document containing a deny as a plain allow
	var doc policyDocument
	if err := json.Unmarshal(document, &doc, json.RejectUnknownMembers(true)); err != nil {
		return BucketPolicy{}, s3errs.ErrMalformedPolicy
	} else if doc.Version != policyVersion || len(doc.Statement) == 0 {
		return BucketPolicy{}, s3errs.ErrMalformedPolicy
	}

	bucketARN := policyARNPrefix + bucket
	objectsARN := bucketARN + "/*"

	var granted PolicyActions
	for _, stmt := range doc.Statement {
		switch {
		case stmt.NotPrincipal != nil, stmt.NotAction != nil,
			stmt.NotResource != nil, stmt.Condition != nil:
			// each of these restricts or inverts the grant
			return BucketPolicy{}, s3errs.ErrNotImplemented
		case stmt.Effect == policyEffectDeny:
			return BucketPolicy{}, s3errs.ErrNotImplemented
		case stmt.Effect != policyEffectAllow:
			return BucketPolicy{}, s3errs.ErrMalformedPolicy
		case stmt.Principal == nil:
			// a resource policy must say who it grants to
			return BucketPolicy{}, s3errs.ErrMalformedPolicy
		case !bool(*stmt.Principal):
			// a bucket has one owner and s3d has no other accounts or roles, so
			// "everyone" is the only principal that can mean anything
			return BucketPolicy{}, s3errs.ErrNotImplemented
		case len(stmt.Action) == 0, len(stmt.Resource) == 0:
			return BucketPolicy{}, s3errs.ErrMalformedPolicy
		}

		// the actions this statement's resources can carry
		var allowed PolicyActions
		for _, resource := range stmt.Resource {
			switch resource {
			case bucketARN:
				allowed |= bucketActions
			case objectsARN:
				allowed |= objectActions
			default:
				if !strings.HasPrefix(resource, bucketARN+"/") {
					// a bucket policy may not govern another bucket
					return BucketPolicy{}, s3errs.ErrMalformedPolicy
				}
				// valid, but scopes the grant to part of the bucket; s3d records
				// grants per bucket
				return BucketPolicy{}, s3errs.ErrNotImplemented
			}
		}

		for _, action := range stmt.Action {
			a, ok := supportedPolicyActions[strings.ToLower(action)]
			if !ok {
				return BucketPolicy{}, s3errs.ErrNotImplemented
			}
			// an action with no matching resource grants nothing, as in S3
			granted |= a & allowed
		}
	}

	return BucketPolicy{Document: string(document), Public: granted}, nil
}

// routeBucketPolicy operates on routes that contain '?policy' in the query
// string.
func (s *s3) routeBucketPolicy(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}
	switch r.Method {
	case http.MethodPut:
		return s.putBucketPolicy(w, r, validatedKey, bucket)
	case http.MethodGet:
		return s.getBucketPolicy(w, r, validatedKey, bucket)
	case http.MethodDelete:
		return s.deleteBucketPolicy(w, r, validatedKey, bucket)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// putBucketPolicy handles PUT Bucket policy requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
func (s *s3) putBucketPolicy(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("putting bucket policy", zap.String("bucket", bucket))

	policy, err := parseBucketPolicy(bucket, r.Body)
	if err != nil {
		return err
	}
	if err := s.backend.PutBucketPolicy(r.Context(), accessKeyID, bucket, policy); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// getBucketPolicy handles GET Bucket policy requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
func (s *s3) getBucketPolicy(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("getting bucket policy", zap.String("bucket", bucket))

	policy, err := s.backend.GetBucketPolicy(r.Context(), accessKeyID, bucket)
	if err != nil {
		return err
	}
	// unlike most S3 responses this one is JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = io.WriteString(w, policy.Document)
	return err
}

// deleteBucketPolicy handles DELETE Bucket policy requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
func (s *s3) deleteBucketPolicy(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("deleting bucket policy", zap.String("bucket", bucket))

	if err := s.backend.DeleteBucketPolicy(r.Context(), accessKeyID, bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// routeBucketPolicyStatus operates on routes that contain '?policyStatus' in
// the query string.
func (s *s3) routeBucketPolicyStatus(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	} else if r.Method != http.MethodGet {
		return s3errs.ErrMethodNotAllowed
	}
	s.logger.Debug("getting bucket policy status", zap.String("bucket", bucket))

	policy, err := s.backend.GetBucketPolicy(r.Context(), validatedKey, bucket)
	if errors.Is(err, s3errs.ErrNoSuchBucketPolicy) {
		// no policy is a status, not an error: the bucket is simply not public
		policy = BucketPolicy{}
	} else if err != nil {
		return err
	}
	return writeXMLResponse(w, http.StatusOK, PolicyStatus{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		IsPublic: policy.Public != 0,
	})
}

package s3

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
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

// S3 actions that s3d authorizes. A policy may only name those in
// [supportedPolicyActions].
const (
	actionGetObject                  = "s3:GetObject"
	actionGetObjectVersion           = "s3:GetObjectVersion"
	actionPutObject                  = "s3:PutObject"
	actionDeleteObject               = "s3:DeleteObject"
	actionDeleteObjectVersion        = "s3:DeleteObjectVersion"
	actionAbortMultipartUpload       = "s3:AbortMultipartUpload"
	actionListMultipartUploadParts   = "s3:ListMultipartUploadParts"
	actionListBucket                 = "s3:ListBucket"
	actionListBucketVersions         = "s3:ListBucketVersions"
	actionListBucketMultipartUploads = "s3:ListBucketMultipartUploads"
	actionDeleteBucket               = "s3:DeleteBucket"
	actionGetBucketLocation          = "s3:GetBucketLocation"
	actionGetBucketVersioning        = "s3:GetBucketVersioning"
	actionPutBucketVersioning        = "s3:PutBucketVersioning"
	actionGetLifecycleConfiguration  = "s3:GetLifecycleConfiguration"
	actionPutLifecycleConfiguration  = "s3:PutLifecycleConfiguration"
	actionGetBucketPolicy            = "s3:GetBucketPolicy"
	actionPutBucketPolicy            = "s3:PutBucketPolicy"
	actionDeleteBucketPolicy         = "s3:DeleteBucketPolicy"
	actionGetBucketPolicyStatus      = "s3:GetBucketPolicyStatus"
	actionCreateBucket               = "s3:CreateBucket"
	actionListAllMyBuckets           = "s3:ListAllMyBuckets"
)

// supportedPolicyActions are the actions a policy may grant, keyed by
// lowercase name since AWS matches actions case-insensitively.
//
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_action.html
var supportedPolicyActions = map[string]string{
	"s3:getobject":          actionGetObject,
	"s3:getobjectversion":   actionGetObjectVersion,
	"s3:listbucket":         actionListBucket,
	"s3:listbucketversions": actionListBucketVersions,
}

// bucketPolicy is a bucket policy accepted by [parseBucketPolicy]. Evaluation
// only matches actions and resources, so it relies on every statement being an
// unconditional allow to everyone.
type bucketPolicy struct {
	statements policyStatements
}

// readAction returns the action a read of the given version requires. The two
// are granted separately, so every read path must ask for the one it performs.
func readAction(version VersionRequest) string {
	if version.Specified {
		return actionGetObjectVersion
	}
	return actionGetObject
}

// deleteAction returns the action a delete of the given version requires, as
// [readAction] does for reads.
func deleteAction(version VersionRequest) string {
	if version.Specified {
		return actionDeleteObjectVersion
	}
	return actionDeleteObject
}

// bucketARN returns the resource a bucket action is granted on.
func bucketARN(bucket string) string {
	return policyARNPrefix + bucket
}

// objectARN returns the resource an object action is granted on.
func objectARN(bucket, object string) string {
	return bucketARN(bucket) + "/" + object
}

// isObjectAction reports whether action applies to objects rather than to the
// bucket itself. S3 grants an object action on "<bucket>/*" and a bucket action
// on "<bucket>".
func isObjectAction(action string) bool {
	switch action {
	case actionGetObject, actionGetObjectVersion, actionPutObject,
		actionDeleteObject, actionDeleteObjectVersion,
		actionAbortMultipartUpload, actionListMultipartUploadParts:
		return true
	}
	return false
}

// allows reports whether the policy grants action on resource.
func (p bucketPolicy) allows(action, resource string) bool {
	for _, stmt := range p.statements {
		if !stmt.namesAction(action) {
			continue
		}
		for _, r := range stmt.Resource {
			if r == resource || (strings.HasSuffix(r, "/*") && strings.HasPrefix(resource, strings.TrimSuffix(r, "*"))) {
				return true
			}
		}
	}
	return false
}

// allowsInBucket reports whether the policy grants action on any resource in
// bucket of the kind it applies to: an object, or the bucket itself.
func (p bucketPolicy) allowsInBucket(action, bucket string) bool {
	bucketResource, objectsPrefix := bucketARN(bucket), objectARN(bucket, "")
	objectAction := isObjectAction(action)
	for _, stmt := range p.statements {
		if !stmt.namesAction(action) {
			continue
		}
		for _, r := range stmt.Resource {
			if objectAction && strings.HasPrefix(r, objectsPrefix) {
				return true
			} else if !objectAction && r == bucketResource {
				return true
			}
		}
	}
	return false
}

// isPublic reports whether the policy grants anything on the bucket or its
// objects.
func (p bucketPolicy) isPublic(bucket string) bool {
	for _, action := range supportedPolicyActions {
		if p.allowsInBucket(action, bucket) {
			return true
		}
	}
	return false
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

// namesAction reports whether the statement's Action element names action. It
// does not consider the statement's Effect or Principal.
func (stmt policyStatement) namesAction(action string) bool {
	for _, a := range stmt.Action {
		if strings.EqualFold(a, action) {
			return true
		}
	}
	return false
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

// readBucketPolicy reads a policy document from a request body. The document
// is kept verbatim so GetBucketPolicy round-trips byte-for-byte, which clients
// like Terraform rely on to detect drift.
func readBucketPolicy(body io.Reader) (string, error) {
	// read one byte past the limit so an oversized document is reported as such
	// rather than truncated into a malformed one
	document, err := io.ReadAll(io.LimitReader(body, maxPolicySize+1))
	if err != nil {
		return "", err
	} else if len(document) > maxPolicySize {
		return "", s3errs.ErrPolicyTooLarge
	}
	return string(document), nil
}

// parseBucketPolicy validates a bucket policy for the named bucket.
//
// A document is accepted only when every statement allows actions from
// [supportedPolicyActions] to everyone. Anything that would narrow, invert or
// widen such a grant is rejected rather than ignored, which would grant more
// than the document describes: [s3errs.ErrMalformedPolicy] if it is
// structurally wrong or names another bucket, [s3errs.ErrNotImplemented] if it
// is valid but expresses access s3d cannot represent.
func parseBucketPolicy(bucket, document string) (bucketPolicy, error) {
	// json/v2 rejects duplicate member names and trailing content, neither of
	// which v1 catches: keeping the last of two "Effect" members would read a
	// document containing a deny as a plain allow
	var doc policyDocument
	if err := json.Unmarshal([]byte(document), &doc, json.RejectUnknownMembers(true)); err != nil {
		return bucketPolicy{}, s3errs.ErrMalformedPolicy
	} else if doc.Version != policyVersion || len(doc.Statement) == 0 {
		return bucketPolicy{}, s3errs.ErrMalformedPolicy
	}

	bucketResource, objectsPrefix := bucketARN(bucket), objectARN(bucket, "")
	objectsResource := objectsPrefix + "*"

	for _, stmt := range doc.Statement {
		switch {
		case stmt.NotPrincipal != nil, stmt.NotAction != nil,
			stmt.NotResource != nil, stmt.Condition != nil:
			// each of these restricts or inverts the grant
			return bucketPolicy{}, s3errs.ErrNotImplemented
		case stmt.Effect == policyEffectDeny:
			return bucketPolicy{}, s3errs.ErrNotImplemented
		case stmt.Effect != policyEffectAllow:
			return bucketPolicy{}, s3errs.ErrMalformedPolicy
		case stmt.Principal == nil:
			// a resource policy must say who it grants to
			return bucketPolicy{}, s3errs.ErrMalformedPolicy
		case !bool(*stmt.Principal):
			// the supported policy subset only grants to everyone
			return bucketPolicy{}, s3errs.ErrNotImplemented
		case len(stmt.Action) == 0, len(stmt.Resource) == 0:
			return bucketPolicy{}, s3errs.ErrMalformedPolicy
		}

		for _, resource := range stmt.Resource {
			if resource != bucketResource && resource != objectsResource {
				if !strings.HasPrefix(resource, objectsPrefix) {
					// a bucket policy may not govern another bucket
					return bucketPolicy{}, s3errs.ErrMalformedPolicy
				}
				// valid, but scopes the grant to part of the bucket
				return bucketPolicy{}, s3errs.ErrNotImplemented
			}
		}
		for _, action := range stmt.Action {
			if _, ok := supportedPolicyActions[strings.ToLower(action)]; !ok {
				return bucketPolicy{}, s3errs.ErrNotImplemented
			}
		}
	}

	return bucketPolicy{statements: doc.Statement}, nil
}

// routeBucketPolicy routes requests that contain '?policy' in the query
// string.
func (s *s3) routeBucketPolicy(r *http.Request) (operation, error) {
	switch r.Method {
	case http.MethodPut:
		return operation{action: actionPutBucketPolicy, serve: s.putBucketPolicy}, nil
	case http.MethodGet:
		return operation{action: actionGetBucketPolicy, serve: s.getBucketPolicy}, nil
	case http.MethodDelete:
		return operation{action: actionDeleteBucketPolicy, serve: s.deleteBucketPolicy}, nil
	default:
		return operation{}, s3errs.ErrMethodNotAllowed
	}
}

// routeBucketPolicyStatus routes requests that contain '?policyStatus' in the
// query string.
func (s *s3) routeBucketPolicyStatus(r *http.Request) (operation, error) {
	if r.Method != http.MethodGet {
		return operation{}, s3errs.ErrMethodNotAllowed
	}
	return operation{action: actionGetBucketPolicyStatus, serve: s.getBucketPolicyStatus}, nil
}

// putBucketPolicy handles PUT Bucket policy requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
func (s *s3) putBucketPolicy(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("putting bucket policy", zap.String("bucket", access.bucket))

	document, err := readBucketPolicy(r.Body)
	if err != nil {
		return err
	} else if _, err := parseBucketPolicy(access.bucket, document); err != nil {
		return err
	}
	if err := s.backend.PutBucketPolicy(r.Context(), access.bucket, document); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// getBucketPolicy handles GET Bucket policy requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
func (s *s3) getBucketPolicy(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("getting bucket policy", zap.String("bucket", access.bucket))

	// the document was already loaded by authorize
	if access.policyDocument == "" {
		return s3errs.ErrNoSuchBucketPolicy
	}
	// unlike most S3 responses this one is JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err := io.WriteString(w, access.policyDocument)
	return err
}

// deleteBucketPolicy handles DELETE Bucket policy requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
func (s *s3) deleteBucketPolicy(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("deleting bucket policy", zap.String("bucket", access.bucket))

	if err := s.backend.DeleteBucketPolicy(r.Context(), access.bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// getBucketPolicyStatus handles GET Bucket policy status requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html
func (s *s3) getBucketPolicyStatus(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("getting bucket policy status", zap.String("bucket", access.bucket))

	// no policy, or one that no longer parses, is a status rather than an
	// error: it grants nothing, so the bucket is not public
	policy := s.parsedPolicy(access.bucket, access.policyDocument)
	return writeXMLResponse(w, http.StatusOK, PolicyStatus{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		IsPublic: policy.isPublic(access.bucket),
	})
}

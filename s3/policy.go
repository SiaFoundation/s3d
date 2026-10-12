package s3

import (
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

const (
	// policy variables are only expanded under this language version
	policyVersion = "2012-10-17"
	// the legacy language version, which reads variables literally
	legacyPolicyVersion = "2008-10-17"

	policyEffectAllow = "Allow"
	policyEffectDeny  = "Deny"

	// covers every caller as a principal, and every resource as a resource
	policyWildcard = "*"

	// the principal types s3d accepts, both naming s3d users
	principalTypeAWS       = "AWS"
	principalTypeCanonical = "CanonicalUser"

	// the set qualifiers of condition operators
	conditionForAllValues = "ForAllValues"
	conditionForAnyValue  = "ForAnyValue"

	policyARNPrefix = "arn:aws:s3:::"

	// matches the limit AWS enforces on bucket policies
	maxPolicySize = 20 << 10
)

// S3 actions that s3d authorizes.
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

// bucketPolicy is a validated resource policy.
type bucketPolicy struct {
	variables  bool // the policy version expands variables
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
	return policyActionResources[strings.ToLower(action)] == resourceTypeObject
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

// oneOrMany is a policy element that is either a single value or an array of
// them, the form most elements take.
type oneOrMany[T any] []T

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (p *oneOrMany[T]) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() == '[' {
		return json.UnmarshalDecode(dec, (*[]T)(p))
	}
	var single T
	if err := json.UnmarshalDecode(dec, &single); err != nil {
		return err
	}
	*p = oneOrMany[T]{single}
	return nil
}

type (
	// policyStatements is the Statement element.
	policyStatements = oneOrMany[policyStatement]
	// policyStrings is an Action, Resource or principal element.
	policyStrings = oneOrMany[string]
	// policyValues is a condition's values.
	policyValues = oneOrMany[policyValue]
)

// policyStatement is a single statement of a [policyDocument].
//
// nolint:tagliatelle
type policyStatement struct {
	Sid       string           `json:"Sid"`
	Effect    string           `json:"Effect"`
	Principal *policyPrincipal `json:"Principal"`
	Action    policyStrings    `json:"Action"`
	Resource  policyStrings    `json:"Resource"`

	NotPrincipal *policyPrincipal `json:"NotPrincipal"`
	NotAction    policyStrings    `json:"NotAction"`
	NotResource  policyStrings    `json:"NotResource"`
	Condition    policyConditions `json:"Condition"`

	conditions conditionSet // Condition, parsed by parseBucketPolicy
}

// principal returns the Principal or NotPrincipal element, whichever is set.
func (stmt policyStatement) principal() *policyPrincipal {
	if stmt.Principal != nil {
		return stmt.Principal
	}
	return stmt.NotPrincipal
}

// UnmarshalJSONFrom rejects null elements rather than silently ignoring a
// restriction, and preserves unknown-member and duplicate-member validation.
func (stmt *policyStatement) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	raw, err := dec.ReadValue()
	if err != nil {
		return err
	}
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members); err != nil {
		return err
	}
	for key, value := range members {
		if string(value) != "null" {
			continue
		} else if key == "Principal" || key == "NotPrincipal" {
			return fmt.Errorf("null %s: %w", key, s3errs.ErrMalformedPolicyPrincipal)
		}
		return fmt.Errorf("null %s: %w", key, s3errs.ErrMalformedPolicy)
	}
	type plain policyStatement
	return json.Unmarshal(raw, (*plain)(stmt), json.RejectUnknownMembers(true))
}

// policyPrincipal is the Principal or NotPrincipal element, keyed by principal
// type. A bare string is read as an AWS principal.
type policyPrincipal map[string]policyStrings

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (p *policyPrincipal) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() == '"' {
		var single string
		if err := json.UnmarshalDecode(dec, &single); err != nil {
			return err
		}
		*p = policyPrincipal{principalTypeAWS: {single}}
		return nil
	}
	if err := json.UnmarshalDecode(dec, (*map[string]policyStrings)(p)); err != nil {
		return s3errs.ErrMalformedPolicyPrincipal
	}
	return nil
}

// principalUserID returns the user ID a principal value names: a bare user ID
// or "arn:aws:iam::<user ID>:root".
func principalUserID(value string) (string, bool) {
	if rest, ok := strings.CutPrefix(value, "arn:aws:iam::"); ok {
		if value, ok = strings.CutSuffix(rest, ":root"); !ok {
			return "", false
		}
	}
	return value, value != "" && !strings.ContainsAny(value, ":/*?{} \t\r\n")
}

// valid reports whether every principal is "*" or names an s3d user.
func (p policyPrincipal) valid() bool {
	if len(p) == 0 {
		return false
	}
	for kind, values := range p {
		if (kind != principalTypeAWS && kind != principalTypeCanonical) || len(values) == 0 {
			return false
		}
		for _, value := range values {
			if kind == principalTypeAWS && value == policyWildcard {
				continue
			}
			if _, ok := principalUserID(value); !ok {
				return false
			}
			if kind == principalTypeCanonical && strings.HasPrefix(value, "arn:") {
				return false
			}
		}
	}
	return true
}

// policyValue is a single condition value, which may also be a JSON number or
// boolean.
type policyValue string

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (p *policyValue) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	kind := dec.PeekKind()
	if kind == '"' {
		var value string
		if err := json.UnmarshalDecode(dec, &value); err != nil {
			return err
		}
		*p = policyValue(value)
		return nil
	}
	if kind != '0' && kind != 't' && kind != 'f' {
		return fmt.Errorf("condition value must be a string, number or boolean: %w", s3errs.ErrMalformedPolicy)
	}
	raw, err := dec.ReadValue()
	if err != nil {
		return err
	}
	*p = policyValue(raw)
	return nil
}

// policyConditions is the Condition element: operator to key to values.
type policyConditions map[string]map[string]policyValues

// conditionOperator is a parsed condition operator name.
type conditionOperator struct {
	base, set          string
	ifExists, negative bool
}

// parseConditionOperator splits an operator name into its set qualifier, base
// operator and IfExists suffix.
func parseConditionOperator(name string) (op conditionOperator, ok bool) {
	if set, base, found := strings.Cut(name, ":"); found {
		if set != conditionForAllValues && set != conditionForAnyValue {
			return op, false
		}
		op.set, name = set, base
	}
	op.ifExists = strings.HasSuffix(name, "IfExists")
	op.base = strings.TrimSuffix(name, "IfExists")
	switch op.base {
	case "StringNotEquals", "StringNotEqualsIgnoreCase", "StringNotLike", "NumericNotEquals", "DateNotEquals", "NotIpAddress", "ArnNotEquals", "ArnNotLike":
		op.negative = true
	case "StringEquals", "StringEqualsIgnoreCase", "StringLike", "NumericEquals", "NumericLessThan", "NumericLessThanEquals", "NumericGreaterThan", "NumericGreaterThanEquals", "DateEquals", "DateLessThan", "DateLessThanEquals", "DateGreaterThan", "DateGreaterThanEquals", "Bool", "IpAddress", "ArnEquals", "ArnLike", "BinaryEquals":
	case "Null":
		if op.ifExists || op.set != "" {
			return op, false
		}
	default:
		return op, false
	}
	return op, true
}

// policyIPPrefix parses an IP address or CIDR.
func policyIPPrefix(value string) (netip.Prefix, error) {
	if ip, err := netip.ParseAddr(value); err == nil {
		return netip.PrefixFrom(ip, ip.BitLen()), nil
	}
	return netip.ParsePrefix(value)
}

// policyNumberSyntax is a plain decimal number. Hex floats and digit
// separators are excluded, and the exponent is bounded by policyNumber.
var policyNumberSyntax = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d{1,4})?$`)

// policyNumber parses a numeric condition value. Bounding the exponent keeps
// an attacker-controlled value from forcing huge big.Rat allocations.
func policyNumber(value string) (*big.Rat, bool) {
	match := policyNumberSyntax.FindStringSubmatch(value)
	if match == nil {
		return nil, false
	}
	if exponent := match[2]; exponent != "" {
		if e, err := strconv.Atoi(exponent[1:]); err != nil || e < -1024 || e > 1024 {
			return nil, false
		}
	}
	return new(big.Rat).SetString(value)
}

// policyDate parses an RFC 3339 date or decimal epoch seconds.
func policyDate(value string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, true
	}
	if !policyNumberSyntax.MatchString(value) {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds < -62135596800 || seconds > 253402300799 {
		return time.Time{}, false
	}
	whole, fraction := math.Modf(seconds)
	return time.Unix(int64(whole), int64(fraction*1e9)), true
}

// condition is a single operator and key test of a Condition element.
type condition struct {
	op      conditionOperator
	key     string // lowercase, like policyContext
	values  []string
	pattern bool // values are wildcard patterns
}

// conditionSet is a parsed Condition element; every condition must match.
type conditionSet []condition

// parse validates the Condition element so evaluation need not re-parse it.
// variables reports whether the policy version expands them.
func (c policyConditions) parse(variables bool) (conditionSet, error) {
	if c != nil && len(c) == 0 {
		return nil, fmt.Errorf("empty Condition: %w", s3errs.ErrMalformedPolicy)
	}
	var set conditionSet
	for name, keys := range c {
		op, ok := parseConditionOperator(name)
		if !ok {
			return nil, fmt.Errorf("invalid condition operator %q: %w", name, s3errs.ErrMalformedPolicy)
		} else if len(keys) == 0 {
			return nil, fmt.Errorf("condition operator %q has no keys: %w", name, s3errs.ErrMalformedPolicy)
		}
		for key, values := range keys {
			if !validConditionKey(key) {
				return nil, fmt.Errorf("unknown condition key %q: %w", key, s3errs.ErrMalformedPolicy)
			} else if len(values) == 0 {
				return nil, fmt.Errorf("condition key %q has no values: %w", key, s3errs.ErrMalformedPolicy)
			}
			c := condition{
				op:      op,
				key:     strings.ToLower(key),
				pattern: op.base == "StringLike" || op.base == "StringNotLike" || strings.HasPrefix(op.base, "Arn"),
			}
			for _, value := range values {
				value := string(value)
				c.values = append(c.values, value)
				// variables are resolved during evaluation. As in IAM, a missing
				// one matches nothing, so negated operators match it.
				if variables && strings.Contains(value, "${") {
					if (!strings.HasPrefix(op.base, "String") && !strings.HasPrefix(op.base, "Arn")) || !validVariables(value) {
						return nil, fmt.Errorf("invalid policy variable in %q: %w", value, s3errs.ErrMalformedPolicy)
					}
					continue
				}
				valid := true
				switch {
				case op.base == "Null" || op.base == "Bool":
					valid = value == "true" || value == "false"
				case strings.Contains(op.base, "IpAddress"):
					_, err := policyIPPrefix(value)
					valid = err == nil
				case strings.HasPrefix(op.base, "Numeric"):
					_, valid = policyNumber(value)
				case strings.HasPrefix(op.base, "Date"):
					_, valid = policyDate(value)
				case op.base == "BinaryEquals":
					_, err := base64.StdEncoding.DecodeString(value)
					valid = err == nil
				}
				if !valid {
					return nil, fmt.Errorf("invalid %s value %q: %w", op.base, value, s3errs.ErrMalformedPolicy)
				}
			}
			set = append(set, c)
		}
	}
	return set, nil
}

// validVariables reports whether every policy variable in value is terminated
// and valid, so a misspelled one cannot silently disable a statement.
func validVariables(value string) bool {
	for {
		i := strings.Index(value, "${")
		if i < 0 {
			return true
		}
		value = value[i+2:]
		end := strings.IndexByte(value, '}')
		if end < 0 {
			return false
		}
		if _, _, ok := parseVariable(value[:end]); !ok {
			return false
		}
		value = value[end+1:]
	}
}

// parseVariable parses the contents of a ${...} policy variable: "*", "?" or
// "$", which stand for themselves, or a known condition key with an optional
// quoted default. key is lowercase, and "" for the literals.
func parseVariable(variable string) (key string, fallback []string, ok bool) {
	switch variable {
	case "*", "?", "$":
		return "", []string{variable}, true
	}
	key = variable
	if name, quoted, found := strings.Cut(variable, ","); found {
		quoted = strings.TrimSpace(quoted)
		if len(quoted) < 2 || quoted[0] != '\'' || quoted[len(quoted)-1] != '\'' {
			return "", nil, false
		}
		key, fallback = strings.TrimSpace(name), []string{quoted[1 : len(quoted)-1]}
	}
	return strings.ToLower(key), fallback, validConditionKey(key)
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

// parseBucketPolicy rejects malformed or unsupported policy elements rather
// than silently weakening restrictions. Known condition keys s3d cannot
// populate evaluate as missing.
func parseBucketPolicy(bucket, document string) (bucketPolicy, error) {
	var doc policyDocument
	if len(document) > maxPolicySize {
		return bucketPolicy{}, s3errs.ErrPolicyTooLarge
	}
	if err := json.Unmarshal([]byte(document), &doc, json.RejectUnknownMembers(true)); err != nil {
		if errors.Is(err, s3errs.ErrMalformedPolicyPrincipal) {
			return bucketPolicy{}, err
		}
		return bucketPolicy{}, fmt.Errorf("%w: %w", err, s3errs.ErrMalformedPolicy)
	} else if doc.Version != policyVersion && doc.Version != legacyPolicyVersion {
		return bucketPolicy{}, fmt.Errorf("unsupported policy version %q: %w", doc.Version, s3errs.ErrMalformedPolicy)
	} else if len(doc.Statement) == 0 {
		return bucketPolicy{}, fmt.Errorf("policy has no statements: %w", s3errs.ErrMalformedPolicy)
	}
	// only 2012-10-17 expands variables; older versions read them literally
	variables := doc.Version == policyVersion
	sids := make(map[string]bool)
	for i := range doc.Statement {
		stmt := &doc.Statement[i]
		if stmt.Sid != "" {
			if sids[stmt.Sid] {
				return bucketPolicy{}, fmt.Errorf("duplicate statement ID %q: %w", stmt.Sid, s3errs.ErrMalformedPolicy)
			}
			sids[stmt.Sid] = true
		}
		if (stmt.Effect != policyEffectAllow && stmt.Effect != policyEffectDeny) ||
			(stmt.Principal == nil) == (stmt.NotPrincipal == nil) ||
			(stmt.Action == nil) == (stmt.NotAction == nil) || len(stmt.Action)+len(stmt.NotAction) == 0 ||
			(stmt.Resource == nil) == (stmt.NotResource == nil) || len(stmt.Resource)+len(stmt.NotResource) == 0 {
			return bucketPolicy{}, fmt.Errorf("statement %d needs an Effect and one of each principal, action and resource element: %w", i, s3errs.ErrMalformedPolicy)
		} else if stmt.Effect == policyEffectAllow && stmt.NotPrincipal != nil {
			// S3 only supports NotPrincipal in a Deny
			return bucketPolicy{}, fmt.Errorf("statement %d allows with NotPrincipal: %w", i, s3errs.ErrMalformedPolicy)
		} else if !stmt.principal().valid() {
			return bucketPolicy{}, fmt.Errorf("statement %d: %w", i, s3errs.ErrMalformedPolicyPrincipal)
		}
		for _, resources := range []policyStrings{stmt.Resource, stmt.NotResource} {
			for _, resource := range resources {
				resourceBucket, _, _ := strings.Cut(strings.TrimPrefix(resource, policyARNPrefix), "/")
				variable := variables && strings.Contains(resourceBucket, "${")
				if resource != policyWildcard && (!strings.HasPrefix(resource, policyARNPrefix) || (!wildcardMatch(resourceBucket, bucket) && !variable)) {
					return bucketPolicy{}, fmt.Errorf("resource %q: %w", resource, s3errs.ErrMalformedPolicyResource)
				} else if variables && !validVariables(resource) {
					return bucketPolicy{}, fmt.Errorf("invalid policy variable in %q: %w", resource, s3errs.ErrMalformedPolicy)
				}
			}
		}
		for _, actions := range []policyStrings{stmt.Action, stmt.NotAction} {
			for j, action := range actions {
				if !validPolicyAction(action) {
					return bucketPolicy{}, fmt.Errorf("action %q: %w", action, s3errs.ErrMalformedPolicyAction)
				}
				// actions are case insensitive
				actions[j] = strings.ToLower(action)
			}
		}
		if stmt.Action != nil && stmt.Resource != nil && !actionsApply(stmt.Action, stmt.Resource) {
			return bucketPolicy{}, fmt.Errorf("statement %d: %w", i, s3errs.ErrMalformedPolicyInapplicable)
		}
		var err error
		if stmt.conditions, err = stmt.Condition.parse(variables); err != nil {
			return bucketPolicy{}, fmt.Errorf("statement %d: %w", i, err)
		}
	}
	return bucketPolicy{variables: variables, statements: doc.Statement}, nil
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
	return writeXMLResponse(w, http.StatusOK, PolicyStatus{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		IsPublic: access.policy.isPublic(),
	})
}

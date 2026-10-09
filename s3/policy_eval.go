package s3

import (
	"encoding/base64"
	"net/netip"
	"slices"
	"strings"

	"github.com/SiaFoundation/s3d/s3/auth"
)

// wildcardMatch implements IAM globbing, including '/' and Unicode keys.
func wildcardMatch(pattern, value string) bool { return matchGlob(pattern, value, false) }

// matchGlob accepts escapes only for patterns escaped by expandValue. Literal
// policy backslashes have no special meaning and are preserved by expandValue.
func matchGlob(pattern, value string, escapes bool) bool {
	type token struct {
		char     rune
		wildcard bool
	}
	var tokens []token
	quoted := false
	for _, r := range pattern {
		if escapes && !quoted && r == '\\' {
			quoted = true
			continue
		}
		tokens = append(tokens, token{r, !quoted && (r == '*' || r == '?')})
		quoted = false
	}
	if quoted {
		tokens = append(tokens, token{char: '\\'})
	}
	text := []rune(value)
	p, v, star, retry := 0, 0, -1, 0
	for v < len(text) {
		if p < len(tokens) && ((!tokens[p].wildcard && tokens[p].char == text[v]) || (tokens[p].wildcard && tokens[p].char == '?')) {
			p++
			v++
		} else if p < len(tokens) && tokens[p].wildcard && tokens[p].char == '*' {
			star = p
			p++
			retry = v
		} else if star >= 0 {
			p = star + 1
			retry++
			v = retry
		} else {
			return false
		}
	}
	for p < len(tokens) && tokens[p].wildcard && tokens[p].char == '*' {
		p++
	}
	return p == len(tokens)
}

// matches reports whether the principal names caller.
func (p policyPrincipal) matches(caller *auth.Caller) bool {
	for kind, values := range p {
		for _, value := range values {
			if kind == principalTypeAWS && value == policyWildcard {
				return true
			}
			id, _ := principalUserID(value)
			if caller != nil && caller.UserID == id {
				return true
			}
		}
	}
	return false
}

// patternEscaper escapes a variable's value so matchGlob reads its wildcard
// characters literally.
var patternEscaper = strings.NewReplacer(`\`, `\\`, "*", `\*`, "?", `\?`)

// expandValue resolves the variables in value, escaping it for matchGlob if
// it is a pattern. Missing variables do not match.
func expandValue(value string, requestContext policyContext, variables, pattern bool) (string, bool) {
	literal := func(value string) string {
		if pattern {
			return strings.ReplaceAll(value, `\`, `\\`)
		}
		return value
	}
	if !variables || !strings.Contains(value, "${") {
		return literal(value), true
	}
	var out strings.Builder
	for {
		i := strings.Index(value, "${")
		if i < 0 {
			out.WriteString(literal(value))
			return out.String(), true
		}
		out.WriteString(literal(value[:i]))
		value = value[i+2:]
		end := strings.IndexByte(value, '}')
		if end < 0 {
			return "", false
		}
		key, fallback, ok := parseVariable(value[:end])
		if !ok {
			return "", false
		}
		replacement := requestContext[key]
		if len(replacement) == 0 {
			replacement = fallback
		}
		if len(replacement) != 1 {
			return "", false
		}
		if pattern {
			out.WriteString(patternEscaper.Replace(replacement[0]))
		} else {
			out.WriteString(replacement[0])
		}
		value = value[end+1:]
	}
}

// elementMatches reports whether match holds for a value of element, or for
// none of notElement's values. Exactly one of the two is set.
func elementMatches(element, notElement policyStrings, match func(string) bool) bool {
	if element == nil {
		element = notElement
	}
	return slices.ContainsFunc(element, match) != (notElement != nil)
}

// namesAction reports whether the statement's Action or NotAction element,
// lowercased by parseBucketPolicy, covers action.
func (stmt policyStatement) namesAction(action string) bool {
	action = strings.ToLower(action)
	return elementMatches(stmt.Action, stmt.NotAction, func(pattern string) bool {
		return wildcardMatch(pattern, action)
	})
}

// principalMatches reports whether the statement applies to caller.
func (stmt policyStatement) principalMatches(caller *auth.Caller) bool {
	return stmt.principal().matches(caller) != (stmt.NotPrincipal != nil)
}

// matches reports whether stmt applies to the request. variables reports
// whether the policy version expands them.
func (stmt *policyStatement) matches(caller *auth.Caller, requestContext policyContext, action, resource string, variables bool) bool {
	if !stmt.principalMatches(caller) || !stmt.namesAction(action) {
		return false
	}
	return elementMatches(stmt.Resource, stmt.NotResource, func(pattern string) bool {
		expanded, ok := expandValue(pattern, requestContext, variables, true)
		return ok && matchGlob(expanded, resource, true)
	}) && stmt.conditions.matches(requestContext, variables)
}

// allows reports whether the caller may perform action on resource. An
// explicit deny overrides both policy grants and the owner's access, except to
// the bucket policy APIs, so the owner can always recover.
func (p bucketPolicy) allows(caller *auth.Caller, requestContext policyContext, isOwner bool, action, resource string) bool {
	if action == "" {
		return false
	}
	if isOwner && (action == actionGetBucketPolicy || action == actionPutBucketPolicy || action == actionDeleteBucketPolicy) {
		return true
	}
	allowed := isOwner
	for i := range p.statements {
		stmt := &p.statements[i]
		// once allowed, only a Deny can change the outcome
		if allowed && stmt.Effect == policyEffectAllow {
			continue
		}
		if !stmt.matches(caller, requestContext, action, resource, p.variables) {
			continue
		}
		if stmt.Effect == policyEffectDeny {
			return false
		}
		allowed = true
	}
	return allowed
}

// compare applies the operator to one request value and one policy value,
// ignoring negation.
func (op conditionOperator) compare(actual, expected string) bool {
	switch op.base {
	case "StringEquals", "StringNotEquals":
		return actual == expected
	case "StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase":
		return strings.EqualFold(actual, expected)
	case "StringLike", "StringNotLike":
		return matchGlob(expected, actual, true)
	case "ArnEquals", "ArnNotEquals", "ArnLike", "ArnNotLike":
		actualParts, expectedParts := strings.SplitN(actual, ":", 6), strings.SplitN(expected, ":", 6)
		if expected == policyWildcard {
			return len(actualParts) == 6
		}
		if len(actualParts) != 6 || len(expectedParts) != 6 {
			return false
		}
		for i := range actualParts {
			if !matchGlob(expectedParts[i], actualParts[i], true) {
				return false
			}
		}
		return true
	case "Bool":
		return (actual == "true" || actual == "false") && actual == expected
	case "IpAddress", "NotIpAddress":
		ip, err := netip.ParseAddr(actual)
		if err != nil {
			return false
		}
		prefix, err := policyIPPrefix(expected)
		return err == nil && prefix.Contains(ip.Unmap())
	case "BinaryEquals":
		a, err := base64.StdEncoding.DecodeString(actual)
		if err != nil {
			return false
		}
		b, err := base64.StdEncoding.DecodeString(expected)
		return err == nil && string(a) == string(b)
	}
	var cmp int
	switch {
	case strings.HasPrefix(op.base, "Numeric"):
		a, okA := policyNumber(actual)
		b, okB := policyNumber(expected)
		if !okA || !okB {
			return false
		}
		cmp = a.Cmp(b)
	case strings.HasPrefix(op.base, "Date"):
		a, okA := policyDate(actual)
		b, okB := policyDate(expected)
		if !okA || !okB {
			return false
		}
		cmp = a.Compare(b)
	default:
		return false
	}
	switch strings.TrimPrefix(strings.TrimPrefix(op.base, "Numeric"), "Date") {
	case "Equals", "NotEquals":
		return cmp == 0
	case "LessThan":
		return cmp < 0
	case "LessThanEquals":
		return cmp <= 0
	case "GreaterThan":
		return cmp > 0
	case "GreaterThanEquals":
		return cmp >= 0
	}
	return false
}

// matches applies the operator, including negation, IfExists and set
// qualifiers, to a key's request values.
func (op conditionOperator) matches(actual []string, expected []string) bool {
	if op.base == "Null" {
		for _, value := range expected {
			if (value == "true") == (len(actual) == 0) {
				return true
			}
		}
		return false
	}
	// ForAllValues also treats an empty string as a null dataset
	if len(actual) == 0 || (op.set == conditionForAllValues && len(actual) == 1 && actual[0] == "") {
		// IfExists makes any operator true for a missing key
		if op.ifExists {
			return true
		}
		if op.set != "" {
			return op.set == conditionForAllValues
		}
		return op.negative
	}
	// multiple policy values are ORed for positive operators and NORed for
	// negative operators. Unqualified negative operators require all request
	// values to match; explicit set qualifiers override that quantifier.
	all := op.set == conditionForAllValues || (op.set == "" && op.negative)
	for _, value := range actual {
		match := false
		for _, want := range expected {
			if op.compare(value, want) {
				match = true
				break
			}
		}
		if op.negative {
			match = !match
		}
		if all && !match {
			return false
		}
		if !all && match {
			return true
		}
	}
	return all
}

// matches reports whether every condition in the set holds for requestContext.
func (set conditionSet) matches(requestContext policyContext, variables bool) bool {
	for _, c := range set {
		expanded := make([]string, 0, len(c.values))
		for _, value := range c.values {
			if v, ok := expandValue(value, requestContext, variables, c.pattern); ok {
				expanded = append(expanded, v)
			}
		}
		if !c.op.matches(requestContext[c.key], expanded) {
			return false
		}
	}
	return true
}

// isPublic follows S3's fixed-principal/fixed-condition classification; Deny
// statements do not make a public Allow non-public.
//
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html
func (p bucketPolicy) isPublic() bool {
	for _, stmt := range p.statements {
		// an Allow cannot use NotPrincipal, and only "*" matches anonymous
		// callers
		if stmt.Effect == policyEffectAllow && stmt.Principal.matches(nil) && !stmt.conditions.nonPublic(p.variables) {
			return true
		}
	}
	return false
}

// nonPublic reports whether a condition in the set limits a statement to
// fixed callers. variables reports whether the policy version expands them.
func (set conditionSet) nonPublic(variables bool) bool {
	for _, c := range set {
		if c.op.negative || c.op.ifExists || (c.op.set == conditionForAllValues && !set.requiresKey(c.key)) {
			continue
		}
		if c.fixesCallers(variables) {
			return true
		}
	}
	return false
}

// fixesCallers reports whether c, if it must hold, limits a statement to fixed
// callers.
func (c condition) fixesCallers(variables bool) bool {
	if c.key == "aws:sourceip" && c.op.base == "IpAddress" {
		for _, value := range c.values {
			prefix, err := policyIPPrefix(value)
			if err != nil || (prefix.Addr().Is4() && prefix.Bits() < 8) || (prefix.Addr().Is6() && prefix.Bits() < 32) {
				return false
			}
		}
		return true
	}
	switch c.key {
	case "aws:principalorgid", "aws:principalaccount", "aws:principalarn", "aws:sourcearn", "aws:sourcevpc", "aws:sourcevpce", "aws:sourceowner", "aws:sourceaccount", "aws:userid", "s3:dataaccesspointarn", "s3:dataaccesspointaccount":
	default:
		return false
	}
	switch c.op.base {
	case "StringEquals", "StringLike", "ArnEquals", "ArnLike":
	default:
		return false
	}
	for _, value := range c.values {
		if !fixedPublicCondition(c.key, value, variables) {
			return false
		}
	}
	return true
}

// requiresKey reports whether the set requires key to be present.
func (set conditionSet) requiresKey(key string) bool {
	for _, c := range set {
		if c.op.base == "Null" && c.key == key && len(c.values) == 1 && c.values[0] == "false" {
			return true
		}
	}
	return false
}

// fixedPublicCondition reports whether value names fixed callers for key.
func fixedPublicCondition(key, value string, variables bool) bool {
	if variables && strings.Contains(value, "${") {
		return false
	}
	// every anonymous request has this user ID and principal account
	if (key == "aws:userid" || key == "aws:principalaccount") && value == AnonymousUserID {
		return false
	}
	if !strings.ContainsAny(value, "*?") {
		return true
	}
	// bucket policies may wildcard access-point names while fixing the account
	if key == "s3:dataaccesspointarn" {
		parts := strings.SplitN(value, ":", 6)
		return len(parts) == 6 && parts[0] == "arn" && parts[2] == "s3" && parts[4] != "" && !strings.ContainsAny(strings.Join(parts[:5], ":"), "*?") && strings.HasPrefix(parts[5], "accesspoint/")
	}
	return false
}

package s3

import (
	"bytes"
	"encoding/base64"
	"net/netip"
	"slices"
	"strings"

	"github.com/SiaFoundation/s3d/s3/auth"
)

// wildcardMatch implements IAM globbing, including '/' and Unicode keys.
func wildcardMatch(pattern, value string) bool { return matchGlob(pattern, value, false) }

// matchGlob accepts escapes only for patterns escaped by expander.expand.
// Literal policy backslashes have no special meaning and are preserved.
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

// maxVariableExpansion bounds the bytes variables may substitute in one
// authorization, since a small policy can repeat a large header many times.
const maxVariableExpansion = 64 << 10

// expander resolves policy variables against a request.
type expander struct {
	requestContext policyContext
	// variables reports whether the policy version expands them
	variables bool
	// budget is the bytes left to substitute, negative once exhausted
	budget int
}

// expand resolves the variables in value, escaping it for matchGlob if it is
// a pattern. Missing variables do not match.
func (e *expander) expand(value string, pattern bool) (string, bool) {
	literal := func(value string) string {
		if pattern {
			return strings.ReplaceAll(value, `\`, `\\`)
		}
		return value
	}
	if !e.variables || !strings.Contains(value, "${") {
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
		replacement := e.requestContext[key]
		if len(replacement) == 0 {
			replacement = fallback
		}
		if len(replacement) != 1 {
			return "", false
		}
		if e.budget -= len(replacement[0]); e.budget < 0 {
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

// matches reports whether stmt applies to the request.
func (stmt *policyStatement) matches(caller *auth.Caller, e *expander, action, resource string) bool {
	if !stmt.principalMatches(caller) || !stmt.namesAction(action) {
		return false
	}
	return elementMatches(stmt.Resource, stmt.NotResource, func(pattern string) bool {
		expanded, ok := e.expand(pattern, true)
		return ok && matchGlob(expanded, resource, true)
	}) && stmt.conditions.matches(e)
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
	e := &expander{requestContext: requestContext, variables: p.variables, budget: maxVariableExpansion}
	for i := range p.statements {
		stmt := &p.statements[i]
		// once allowed, only a Deny can change the outcome
		if allowed && stmt.Effect == policyEffectAllow {
			continue
		}
		matched := stmt.matches(caller, e, action, resource)
		// an exhausted budget may have skipped a Deny
		if e.budget < 0 {
			return false
		}
		if !matched {
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
func (op conditionOperator) compare(actual string, expected conditionValue) bool {
	switch op.kind {
	case compareString:
		return actual == expected.text
	case compareStringIgnoreCase:
		return strings.EqualFold(actual, expected.text)
	case compareStringLike:
		return matchGlob(expected.text, actual, true)
	case compareArn:
		actualParts, expectedParts := strings.SplitN(actual, ":", 6), strings.SplitN(expected.text, ":", 6)
		if expected.text == policyWildcard {
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
	case compareBool:
		return (actual == "true" || actual == "false") && actual == expected.text
	case compareIP:
		ip, err := netip.ParseAddr(actual)
		return err == nil && expected.prefix.Contains(ip.Unmap())
	case compareBinary:
		a, err := base64.StdEncoding.DecodeString(actual)
		return err == nil && bytes.Equal(a, expected.binary)
	case compareNumeric:
		a, ok := policyNumber(actual)
		return ok && op.order(a.Cmp(expected.number))
	case compareDate:
		a, ok := policyDate(actual)
		return ok && op.order(a.Compare(expected.date))
	}
	return false
}

// matches applies the operator, including negation, IfExists and set
// qualifiers, to a key's request values.
func (op conditionOperator) matches(actual []string, expected []conditionValue) bool {
	if op.kind == compareNull {
		for _, value := range expected {
			if (value.text == "true") == (len(actual) == 0) {
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

// matches reports whether every condition in the set holds for the request.
func (set conditionSet) matches(e *expander) bool {
	for _, c := range set {
		// only string values can hold variables; the rest were parsed up front
		expected := c.values
		if c.op.textual() {
			expected = make([]conditionValue, 0, len(c.values))
			for _, value := range c.values {
				if v, ok := e.expand(value.text, c.op.pattern()); ok {
					expected = append(expected, conditionValue{text: v})
				}
			}
		}
		if !c.op.matches(e.requestContext[c.key], expected) {
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
	if c.op.negative {
		return false
	}
	if c.key == "aws:sourceip" && c.op.kind == compareIP {
		for _, value := range c.values {
			if prefix := value.prefix; (prefix.Addr().Is4() && prefix.Bits() < 8) || (prefix.Addr().Is6() && prefix.Bits() < 32) {
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
	switch c.op.kind {
	case compareString, compareStringLike, compareArn:
	default:
		return false
	}
	for _, value := range c.values {
		if !fixedPublicCondition(c.key, value.text, variables) {
			return false
		}
	}
	return true
}

// requiresKey reports whether the set requires key to be present.
func (set conditionSet) requiresKey(key string) bool {
	for _, c := range set {
		if c.op.kind == compareNull && c.key == key && len(c.values) == 1 && c.values[0].text == "false" {
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

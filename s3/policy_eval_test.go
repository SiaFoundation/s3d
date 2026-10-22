package s3

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3/auth"
)

// TestPolicyEvaluation checks how statements combine to allow or deny a
// request.
func TestPolicyEvaluation(t *testing.T) {
	tests := []struct {
		name           string
		statements     string
		version        string
		action         string
		object         string
		caller         *auth.Caller
		requestContext policyContext
		isOwner        bool
		want           bool
	}{
		{name: "implicit deny", statements: `{"Effect":"Allow","Principal":"other","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}`},
		{name: "account root", caller: otherCaller, want: true, statements: `{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::other:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}`},
		{name: "canonical ID", caller: otherCaller, want: true, statements: `{"Effect":"Allow","Principal":{"CanonicalUser":"other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}`},
		{name: "principal array", caller: otherCaller, want: true, statements: `{"Effect":"Allow","Principal":{"AWS":["owner","other"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}`},
		{name: "named principal excludes anonymous", statements: `{"Effect":"Allow","Principal":{"AWS":"other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}`},
		{name: "deny beats allow", object: "secret/file", statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"},{"Effect":"Deny","Principal":"*","Action":"s3:Get*","Resource":"arn:aws:s3:::bucket/secret/*"}`},
		{name: "deny owner", caller: ownerCaller, isOwner: true, statements: `{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"*"}`},
		{name: "owner implicit allow", caller: ownerCaller, isOwner: true, want: true, statements: `{"Effect":"Deny","Principal":"other","Action":"s3:GetObject","Resource":"*"}`},
		{name: "owner policy recovery", caller: ownerCaller, isOwner: true, want: true, action: actionPutBucketPolicy, statements: `{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":"*"}`},
		{name: "non-owner policy deny", caller: otherCaller, action: actionGetBucketPolicy, statements: `{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":"*"}`},
		{name: "NotPrincipal excepted", caller: otherCaller, want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"},{"Effect":"Deny","NotPrincipal":{"AWS":"other"},"Action":"s3:GetObject","Resource":"*"}`},
		{name: "NotPrincipal denies anonymous", statements: `{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"},{"Effect":"Deny","NotPrincipal":{"AWS":"other"},"Action":"s3:GetObject","Resource":"*"}`},
		{name: "NotAction", want: true, statements: `{"Effect":"Allow","Principal":"*","NotAction":"s3:Delete*","Resource":"*"}`},
		{name: "NotAction excluded", action: actionDeleteObject, statements: `{"Effect":"Allow","Principal":"*","NotAction":"s3:Delete*","Resource":"*"}`},
		{name: "NotResource", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","NotResource":"arn:aws:s3:::bucket/secret/*"}`},
		{name: "NotResource excluded", object: "secret/key", statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","NotResource":"arn:aws:s3:::bucket/secret/*"}`},
		{name: "question mark wildcard", object: "public/a/x", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"S3:g?t*","Resource":"arn:aws:s3:::bucket/public/?/*"}`},
		{name: "question mark wildcard miss", object: "public/ab/x", statements: `{"Effect":"Allow","Principal":"*","Action":"s3:Get*","Resource":"arn:aws:s3:::bucket/public/?/*"}`},
		{name: "resource case sensitive", object: "Public/a", statements: `{"Effect":"Allow","Principal":"*","Action":"s3:Get*","Resource":"arn:aws:s3:::bucket/public/*"}`},
		{name: "variable", caller: otherCaller, requestContext: policyContext{"aws:username": {"other"}}, object: "other/key", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${aws:username}/*"}`},
		{name: "literal special wildcard", object: "*/key", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${*}/*"}`},
		{name: "literal wildcard excludes others", object: "other/key", statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${*}/*"}`},
		{name: "missing variable with default", object: "public/key", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${aws:username, 'public'}/*"}`},
		{name: "missing negated condition variable", requestContext: policyContext{"aws:userid": {"other"}}, want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*","Condition":{"StringNotEquals":{"aws:userid":"${aws:username}"}}}`},
		{name: "missing resource variable in array", object: "public/key", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::bucket/${aws:username}/*","arn:aws:s3:::bucket/public/*"]}`},
		{name: "missing NotResource variable", object: "public/key", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","NotResource":"arn:aws:s3:::bucket/${aws:username}/*"}`},
		{name: "variable does not introduce wildcard", object: "private/key", requestContext: policyContext{"aws:username": {"*"}}, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${aws:username}/*"}`},
		{name: "missing variable", object: "/key", statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${aws:username}/*"}`},
		{name: "legacy literal variable", version: "2008-10-17", object: "${aws:username}/key", want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${aws:username}/*"}`},
		{name: "legacy no substitution", version: "2008-10-17", object: "other/key", requestContext: policyContext{"aws:username": {"other"}}, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::bucket/${aws:username}/*"}`},
		{name: "condition variable", want: true, requestContext: policyContext{"aws:username": {"other"}, "s3:prefix": {"other/"}}, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*","Condition":{"StringEquals":{"s3:prefix":"${aws:username}/"}}}`},
		{name: "AND conditions", requestContext: policyContext{"aws:securetransport": {"true"}, "aws:sourceip": {"192.0.2.10"}}, want: true, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*","Condition":{"Bool":{"aws:SecureTransport":true},"IpAddress":{"AWS:SourceIp":"192.0.2.0/24"}}}`},
		{name: "AND conditions failure", requestContext: policyContext{"aws:securetransport": {"false"}, "aws:sourceip": {"192.0.2.10"}}, statements: `{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*","Condition":{"Bool":{"aws:SecureTransport":true},"IpAddress":{"aws:SourceIp":"192.0.2.0/24"}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			version := test.version
			if version == "" {
				version = policyVersion
			}
			p, err := parseBucketPolicy("bucket", fmt.Sprintf(`{"Version":%q,"Statement":[%s]}`, version, test.statements))
			if err != nil {
				t.Fatal(err)
			}
			action := test.action
			if action == "" {
				action = actionGetObject
			}
			resource := bucketARN("bucket")
			if isObjectAction(action) {
				resource = objectARN("bucket", test.object)
			}
			if got := p.allows(test.caller, test.requestContext, test.isOwner, action, resource); got != test.want {
				t.Fatalf("expected %v, got %v", test.want, got)
			}
		})
	}
}

// TestPolicyConditions checks each condition operator against request values.
func TestPolicyConditions(t *testing.T) {
	tests := []struct {
		op               string
		actual, expected []string
		want             bool
	}{
		{"StringEquals", []string{"a"}, []string{"b", "a"}, true},
		{"StringEquals", []string{"A"}, []string{"a"}, false},
		{"StringEqualsIgnoreCase", []string{"A"}, []string{"a"}, true},
		{"StringNotEqualsIgnoreCase", []string{"A"}, []string{"a"}, false},
		{"StringLike", []string{"a/b/c"}, []string{"a/*"}, true},
		{"StringNotLike", []string{"a"}, []string{"b*", "a*"}, false},
		{"StringNotEquals", []string{"a"}, []string{"b", "c"}, true},
		{"StringNotEquals", nil, []string{"a"}, true},
		{"StringEquals", nil, []string{"a"}, false},
		{"StringEqualsIfExists", nil, []string{"a"}, true},
		{"StringNotEqualsIfExists", nil, []string{"a"}, true},
		{"Null", nil, []string{"true"}, true},
		{"Null", []string{""}, []string{"true"}, false},
		{"Null", []string{"a"}, []string{"false"}, true},
		{"ForAllValues:StringEquals", nil, []string{"a"}, true},
		{"ForAnyValue:StringEquals", nil, []string{"a"}, false},
		{"ForAllValues:StringEquals", []string{"a", "b"}, []string{"a", "b"}, true},
		{"ForAllValues:StringEquals", []string{"a", "c"}, []string{"a", "b"}, false},
		{"ForAnyValue:StringEquals", []string{"a", "c"}, []string{"a", "b"}, true},
		{"ForAllValues:StringNotEquals", []string{"a", "c"}, []string{"a", "b"}, false},
		{"ForAnyValue:StringNotEquals", []string{"a", "c"}, []string{"a", "b"}, true},
		{"ForAnyValue:StringEqualsIfExists", nil, []string{"a"}, true},
		{"ForAllValues:StringLike", []string{""}, []string{"a*"}, true},
		{"NumericEquals", []string{"1.0"}, []string{"1"}, true},
		{"NumericNotEquals", []string{"1"}, []string{"1", "2"}, false},
		{"NumericLessThan", []string{"9"}, []string{"10"}, true},
		{"NumericLessThanEquals", []string{"10"}, []string{"10"}, true},
		{"NumericGreaterThan", []string{"11"}, []string{"10"}, true},
		{"NumericGreaterThanEquals", []string{"10"}, []string{"10"}, true},
		{"NumericEquals", []string{"9007199254740993"}, []string{"9007199254740992"}, false},
		{"NumericEquals", []string{"junk"}, []string{"0"}, false},
		{"DateEquals", []string{"2026-10-09T00:00:00Z"}, []string{"2026-10-09T01:00:00+01:00"}, true},
		{"DateEquals", []string{"2025-10-09T08:53:20.1Z"}, []string{"1760000000.1"}, true},
		{"DateNotEquals", []string{"2026-10-09T00:00:00Z"}, []string{"2026-10-08T00:00:00Z"}, true},
		{"DateLessThan", []string{"2026-10-08T00:00:00Z"}, []string{"2026-10-09T00:00:00Z"}, true},
		{"DateLessThanEquals", []string{"2026-10-09T00:00:00Z"}, []string{"2026-10-09T00:00:00Z"}, true},
		{"DateGreaterThan", []string{"2026-10-10T00:00:00Z"}, []string{"2026-10-09T00:00:00Z"}, true},
		{"DateGreaterThanEquals", []string{"2026-10-09T00:00:00Z"}, []string{"2026-10-09T00:00:00Z"}, true},
		{"Bool", []string{"true"}, []string{"true"}, true},
		{"Bool", []string{"false"}, []string{"true"}, false},
		{"IpAddress", []string{"192.0.2.1"}, []string{"192.0.2.0/24"}, true},
		{"IpAddress", []string{"2001:db8::1"}, []string{"2001:db8::/32"}, true},
		{"IpAddress", []string{"192.0.2.1"}, []string{"192.0.2.1"}, true},
		{"IpAddress", []string{"192.0.2.1"}, []string{"::ffff:192.0.2.0/120"}, true},
		{"NotIpAddress", []string{"192.0.2.1"}, []string{"192.0.2.0/24", "198.51.100.0/24"}, false},
		{"ArnEquals", []string{"arn:aws:s3:::bucket/key"}, []string{"arn:aws:s3:::bucket/*"}, true},
		{"ArnLike", []string{"arn:aws:s3:us-east-1:other:bucket/key"}, []string{"arn:aws:s3:*:bucket/*"}, false},
		{"ArnLike", []string{"arn:aws:s3:::bucket/key"}, []string{"arn:aws:s3:::bucket/k?y"}, true},
		{"ArnNotEquals", []string{"arn:aws:s3:::bucket/key"}, []string{"arn:aws:s3:::other/*"}, true},
		{"ArnNotLike", []string{"arn:aws:s3:::bucket/key"}, []string{"arn:aws:s3:::bucket/*"}, false},
		{"BinaryEquals", []string{"YQ=="}, []string{"YQ=="}, true},
	}
	for _, test := range tests {
		t.Run(test.op, func(t *testing.T) {
			op, ok := parseConditionOperator(test.op)
			if !ok {
				t.Fatal("invalid operator")
			}
			var expected []conditionValue
			for _, value := range test.expected {
				v, ok := op.parseValue(value)
				if !ok {
					t.Fatalf("invalid value %q", value)
				}
				expected = append(expected, v)
			}
			if got := op.matches(test.actual, expected); got != test.want {
				t.Fatalf("expected %v, got %v", test.want, got)
			}
		})
	}
	// unsupported service context keys are absent, retaining Null/IfExists semantics
	for _, op := range []string{"StringEqualsIfExists", "Null"} {
		want := policyValue("vpc-1")
		if op == "Null" {
			want = "true"
		}
		c, err := policyConditions{op: {"aws:SourceVpc": {want}}}.parse(true)
		if err != nil {
			t.Fatal(err)
		} else if !c.matches(nil, true) {
			t.Fatalf("expected %s to match a missing key", op)
		}
	}
}

// TestWildcardMatch checks IAM globbing.
func TestWildcardMatch(t *testing.T) {
	for _, test := range []struct {
		pattern string
		value   string
		want    bool
	}{
		{"*", "", true},
		{"?", "", false},
		{"a*b?", "axybz", true},
		{"a*b?", "axyb", false},
		{"*a*b", "aacb", true},
		{"a/*", "a/b/c", true},
		{"?", "é", true},
		{"*x", "abc", false},
	} {
		if got := wildcardMatch(test.pattern, test.value); got != test.want {
			t.Fatalf("pattern %q, value %q: expected %v, got %v", test.pattern, test.value, test.want, got)
		}
	}
	// backtracking must not be exponential
	if wildcardMatch(strings.Repeat("*a", 100)+"b", strings.Repeat("a", 1000)) {
		t.Fatal("expected no match")
	}
}

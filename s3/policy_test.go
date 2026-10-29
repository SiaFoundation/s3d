package s3

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestParseBucketPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   []string
		err    error
	}{
		{
			name: "canonical public read",
			policy: `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicRead",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::bucket/*"
    }
  ]
}`,
			want: []string{actionGetObject},
		},
		{
			// AWS allows Statement as a single object, not only an array
			name: "single statement object",
			policy: `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}}`,
			want: []string{actionGetObject},
		},
		{
			// AWS treats the service prefix and action name as case insensitive
			name: "action in a different case",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"S3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: []string{actionGetObject},
		},
		{
			name: "action in lower case",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:listbucket","Resource":"arn:aws:s3:::bucket"}]}`,
			want: []string{actionListBucket},
		},
		{
			name: "principal as AWS object",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: []string{actionGetObject},
		},
		{
			name: "principal as AWS array",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":{"AWS":["*"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: []string{actionGetObject},
		},
		{
			name: "action and resource as arrays",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"]}]}`,
			want: []string{actionGetObject},
		},
		{
			name: "two equivalent statements",
			policy: `{"Version":"2012-10-17","Statement":[
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: []string{actionGetObject},
		},
		{
			name: "versioned read",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: []string{actionGetObject, actionGetObjectVersion},
		},
		{
			// s3:ListBucket is granted on the bucket ARN, without the "/*"
			name: "list bucket",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}`,
			want: []string{actionListBucket},
		},
		{
			name: "list object versions",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:ListBucket","s3:ListBucketVersions"],"Resource":"arn:aws:s3:::bucket"}]}`,
			want: []string{actionListBucket, actionListBucketVersions},
		},
		{
			name: "read and list as separate statements",
			policy: `{"Version":"2012-10-17","Statement":[
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},
				{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}`,
			want: []string{actionGetObject, actionListBucket},
		},
		{
			// one statement naming both resources pairs each action with its own
			name: "read and list in one statement",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:GetObject","s3:GetObjectVersion","s3:ListBucket","s3:ListBucketVersions"],
				"Resource":["arn:aws:s3:::bucket","arn:aws:s3:::bucket/*"]}]}`,
			want: []string{actionGetObject, actionGetObjectVersion, actionListBucket, actionListBucketVersions},
		},
		{
			// as in S3, an action must apply to one of the statement's resources
			name: "object action with only the bucket resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket"}]}`,
			err: s3errs.ErrMalformedPolicyInapplicable,
		},
		{
			name: "bucket action with only the object resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicyInapplicable,
		},

		// malformed documents
		{
			name:   "not json",
			policy: `not a policy`,
			err:    s3errs.ErrMalformedPolicy,
		},
		{
			name:   "empty",
			policy: ``,
			err:    s3errs.ErrMalformedPolicy,
		},
		{
			name:   "no statements",
			policy: `{"Version":"2012-10-17","Statement":[]}`,
			err:    s3errs.ErrMalformedPolicy,
		},
		{
			// a misspelled restriction must not be dropped, which would turn
			// this into the unconditional grant it only looks like
			name: "misspelled condition",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*",
				"Conditions":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "unknown statement member",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Ttl":60}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "unknown statement member in the single-object form",
			policy: `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*",
				"Conditions":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "unknown document member",
			policy: `{"Version":"2012-10-17","Statements":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			// json keeps the last value, so this must not read as a plain allow
			name: "duplicate effect hiding a deny",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Effect":"Allow",
				"Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "duplicate principal",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":{"AWS":"arn:aws:iam::111122223333:root"},"Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "duplicate action",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:PutObject","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "duplicate resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::other/*","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "duplicate document member",
			policy: `{"Version":"2008-10-17","Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			// nested objects are checked too, though a Condition is rejected anyway
			name: "duplicate member inside a condition",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*",
				"Condition":{"IpAddress":{"a":"1"},"IpAddress":{"b":"2"}}}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "trailing closing brace",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "trailing closing bracket",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}]`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "trailing content",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]} {"extra":1}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name:   "statement is not an object or array",
			policy: `{"Version":"2012-10-17","Statement":"nonsense"}`,
			err:    s3errs.ErrMalformedPolicy,
		},
		{
			name: "missing version",
			policy: `{"Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "legacy version",
			policy: `{"Version":"2008-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: []string{actionGetObject},
		},
		{
			name: "unknown effect",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Maybe","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "missing principal",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "missing action",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "missing resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "resource names another bucket",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::other/*"}]}`,
			err: s3errs.ErrMalformedPolicyResource,
		},
		{
			name: "resource names a bucket with a matching prefix",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-two/*"}]}`,
			err: s3errs.ErrMalformedPolicyResource,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document, err := readBucketPolicy(strings.NewReader(test.policy))
			if err != nil {
				t.Fatal(err)
			} else if document != test.policy {
				t.Fatalf("expected document to be read verbatim, got %q", document)
			}
			policy, err := parseBucketPolicy("bucket", document)
			if test.err != nil {
				if !errors.Is(err, test.err) {
					t.Fatalf("expected %v, got %v", test.err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// what the policy allows an anonymous caller, who owns nothing
			for _, action := range []string{actionGetObject, actionGetObjectVersion, actionListBucket, actionListBucketVersions} {
				resource := policyARNPrefix + "bucket"
				if isObjectAction(action) {
					resource += "/key"
				}
				if got, want := policy.allows(nil, nil, false, action, resource), slices.Contains(test.want, action); got != want {
					t.Fatalf("action %s: expected %v, got %v", action, want, got)
				}
			}
			if got, want := policy.isPublic(), len(test.want) > 0; got != want {
				t.Fatalf("expected public %v, got %v", want, got)
			}
		})
	}
}

// TestParseBucketPolicyTooLarge checks that an oversized document is reported
// as such rather than truncated into a malformed one.
func TestParseBucketPolicyTooLarge(t *testing.T) {
	// pad the Sid past the limit, keeping the document valid JSON
	padding := strings.Repeat("a", maxPolicySize)
	policy := `{"Version":"2012-10-17","Statement":[{"Sid":"` + padding + `","Effect":"Allow",
		"Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`

	if _, err := readBucketPolicy(strings.NewReader(policy)); !errors.Is(err, s3errs.ErrPolicyTooLarge) {
		t.Fatalf("expected %v, got %v", s3errs.ErrPolicyTooLarge, err)
	}
}

// TestPolicyValidation checks that malformed statements are rejected.
func TestPolicyValidation(t *testing.T) {
	tests := []struct {
		name      string
		statement string // the members after "Effect":"Allow"
		err       error
	}{
		{"Allow with NotPrincipal", `"NotPrincipal":{"AWS":"other"},"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicy},
		{"Principal and NotPrincipal", `"Principal":"*","NotPrincipal":"other","Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicy},
		{"Action and NotAction", `"Principal":"*","Action":"s3:*","NotAction":"s3:GetObject","Resource":"*"`, s3errs.ErrMalformedPolicy},
		{"empty Action", `"Principal":"*","Action":[],"NotAction":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicy},
		{"Resource and NotResource", `"Principal":"*","Action":"s3:*","Resource":"*","NotResource":"arn:aws:s3:::bucket/key"`, s3errs.ErrMalformedPolicy},
		{"unknown action", `"Principal":"*","Action":"s3:NotAnAction","Resource":"*"`, s3errs.ErrMalformedPolicyAction},
		{"unknown operator", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":{"NoOperator":{"aws:SecureTransport":"true"}}`, s3errs.ErrMalformedPolicy},
		{"invalid Bool", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":{"Bool":{"aws:SecureTransport":"yes"}}`, s3errs.ErrMalformedPolicy},
		{"invalid IP", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":{"IpAddress":{"aws:SourceIp":"bad"}}`, s3errs.ErrMalformedPolicy},
		{"huge exponent", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":{"NumericEquals":{"s3:max-keys":"1e999999999"}}`, s3errs.ErrMalformedPolicy},
		{"tiny exponent", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":{"NumericEquals":{"s3:max-keys":"1e-999999999"}}`, s3errs.ErrMalformedPolicy},
		{"null NotResource", `"Principal":"*","Action":"s3:*","Resource":"*","NotResource":null`, s3errs.ErrMalformedPolicy},
		{"null NotPrincipal", `"Principal":"*","Action":"s3:*","Resource":"*","NotPrincipal":null`, s3errs.ErrMalformedPolicyPrincipal},
		{"null NotAction", `"Principal":"*","Action":"s3:*","Resource":"*","NotAction":null`, s3errs.ErrMalformedPolicy},
		{"null Condition", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":null`, s3errs.ErrMalformedPolicy},
		{"empty Condition", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":{}`, s3errs.ErrMalformedPolicy},
		{"NullIfExists", `"Principal":"*","Action":"s3:*","Resource":"*","Condition":{"NullIfExists":{"aws:SourceVpc":"true"}}`, s3errs.ErrMalformedPolicy},
		{"service principal", `"Principal":{"Service":"s3.amazonaws.com"},"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"null principal", `"Principal":null,"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"number principal", `"Principal":123,"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"number AWS principal", `"Principal":{"AWS":123},"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"IAM user principal", `"Principal":"arn:aws:iam::other:user/name","Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"wildcard user principal", `"Principal":"oth*","Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"empty AWS principal", `"Principal":{"AWS":[]},"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"wildcard canonical principal", `"Principal":{"CanonicalUser":"*"},"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
		{"empty principal", `"Principal":{},"Action":"s3:*","Resource":"*"`, s3errs.ErrMalformedPolicyPrincipal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseBucketPolicy("bucket", `{"Version":"2012-10-17","Statement":{"Effect":"Allow",`+test.statement+`}}`)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
		})
	}

	stmt := `{"Sid":"duplicate","Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"}`
	if _, err := parseBucketPolicy("bucket", `{"Version":"2012-10-17","Statement":[`+stmt+`,`+stmt+`]}`); !errors.Is(err, s3errs.ErrMalformedPolicy) {
		t.Fatalf("duplicate Sid: expected %v, got %v", s3errs.ErrMalformedPolicy, err)
	}
}

// TestPolicyPublicStatus checks which grants GetBucketPolicyStatus reports as
// public.
func TestPolicyPublicStatus(t *testing.T) {
	tests := []struct {
		name      string
		principal string
		condition string
		want      bool
	}{
		{"public", "*", "", true},
		{"fixed root", `{"AWS":"arn:aws:iam::other:root"}`, "", false},
		{"canonical", `{"CanonicalUser":"other"}`, "", false},
		{"narrow IPv4", "*", `"IpAddress":{"aws:SourceIp":"192.0.2.0/24"}`, false},
		{"broad IPv4", "*", `"IpAddress":{"aws:SourceIp":"0.0.0.0/0"}`, true},
		{"narrow IPv6", "*", `"IpAddress":{"aws:SourceIp":"2001:db8::/32"}`, false},
		{"broad IPv6", "*", `"IpAddress":{"aws:SourceIp":"2000::/16"}`, true},
		{"mixed ranges", "*", `"IpAddress":{"aws:SourceIp":["192.0.2.0/24","0.0.0.0/0"]}`, true},
		{"negated IP", "*", `"NotIpAddress":{"aws:SourceIp":"192.0.2.0/24"}`, true},
		{"IfExists IP", "*", `"IpAddressIfExists":{"aws:SourceIp":"192.0.2.0/24"}`, true},
		{"referer remains public", "*", `"StringEquals":{"aws:Referer":"https://example.com"}`, true},
		{"fixed VPC", "*", `"StringEquals":{"aws:SourceVpc":"vpc-1"}`, false},
		{"wildcard VPC", "*", `"StringLike":{"aws:SourceVpc":"vpc-*"}`, true},
		{"wildcard access point account fixed", "*", `"ArnLike":{"s3:DataAccessPointArn":"arn:aws:s3:us-east-1:123456789012:accesspoint/*"}`, false},
		{"wildcard access point account", "*", `"ArnLike":{"s3:DataAccessPointArn":"arn:aws:s3:us-east-1:*:accesspoint/*"}`, true},
		{"ForAny IfExists", "*", `"ForAnyValue:StringEqualsIfExists":{"aws:SourceVpc":"vpc-1"}`, true},
		{"ForAll missing", "*", `"ForAllValues:StringEquals":{"aws:SourceVpc":"vpc-1"}`, true},
		{"ForAll guarded", "*", `"ForAllValues:StringEquals":{"aws:SourceVpc":"vpc-1"},"Null":{"aws:SourceVpc":"false"}`, false},
		{"variable user", "*", `"StringEquals":{"aws:userid":"${aws:username}"}`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			principal := test.principal
			if principal == "*" {
				principal = `"*"`
			}
			condition := ""
			if test.condition != "" {
				condition = `,"Condition":{` + test.condition + `}`
			}
			p, err := parseBucketPolicy("bucket", `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":`+principal+`,"Action":"s3:*","Resource":"*"`+condition+`}}`)
			if err != nil {
				t.Fatal(err)
			}
			if got := p.isPublic(); got != test.want {
				t.Fatalf("expected %v, got %v", test.want, got)
			}
		})
	}
}

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
			// an object action with no object resource grants nothing, as in S3
			name: "object action with only the bucket resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket"}]}`,
			want: nil,
		},
		{
			name: "bucket action with only the object resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: nil,
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
			err: s3errs.ErrMalformedPolicy,
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
			err: s3errs.ErrMalformedPolicy,
		},
		{
			name: "resource names a bucket with a matching prefix",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-two/*"}]}`,
			err: s3errs.ErrMalformedPolicy,
		},

		// valid policies expressing access s3d cannot represent; accepting one as
		// a plain read grant would allow more than it says
		{
			name: "deny statement",
			policy: `{"Version":"2012-10-17","Statement":[
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},
				{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/secret/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "condition",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*",
				"Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "not action",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"NotAction":"s3:DeleteObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "not principal",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"NotPrincipal":{"AWS":"arn:aws:iam::111122223333:root"},
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "not resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","NotResource":"arn:aws:s3:::bucket/secret/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "named principal",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":{"AWS":"arn:aws:iam::111122223333:root"},
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "service principal",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":{"Service":"cloudfront.amazonaws.com"},
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "wildcard action",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:*","Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "write action",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*"}]}`,
			err: s3errs.ErrNotImplemented,
		},
		{
			name: "prefix scoped resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/public/*"}]}`,
			err: s3errs.ErrNotImplemented,
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
			for _, action := range supportedPolicyActions {
				resource := policyARNPrefix + "bucket"
				if isObjectAction(action) {
					resource += "/key"
				}
				if got, want := policy.allows(action, resource), slices.Contains(test.want, action); got != want {
					t.Fatalf("action %s: expected %v, got %v", action, want, got)
				}
			}
			if got, want := policy.isPublic("bucket"), len(test.want) > 0; got != want {
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

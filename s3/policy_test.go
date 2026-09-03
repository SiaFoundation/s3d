package s3

import (
	"errors"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestParseBucketPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   PolicyActions
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
			want: ActionGetObject,
		},
		{
			// AWS allows Statement as a single object, not only an array
			name: "single statement object",
			policy: `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}}`,
			want: ActionGetObject,
		},
		{
			name: "principal as AWS object",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: ActionGetObject,
		},
		{
			name: "principal as AWS array",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
				"Principal":{"AWS":["*"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: ActionGetObject,
		},
		{
			name: "action and resource as arrays",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"]}]}`,
			want: ActionGetObject,
		},
		{
			name: "two equivalent statements",
			policy: `{"Version":"2012-10-17","Statement":[
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: ActionGetObject,
		},
		{
			name: "versioned read",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: ActionGetObject | ActionGetObjectVersion,
		},
		{
			// s3:ListBucket is granted on the bucket ARN, without the "/*"
			name: "list bucket",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}`,
			want: ActionListBucket,
		},
		{
			name: "list object versions",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:ListBucket","s3:ListBucketVersions"],"Resource":"arn:aws:s3:::bucket"}]}`,
			want: ActionListBucket | ActionListBucketVersions,
		},
		{
			name: "read and list as separate statements",
			policy: `{"Version":"2012-10-17","Statement":[
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},
				{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}`,
			want: ActionGetObject | ActionListBucket,
		},
		{
			// one statement naming both resources pairs each action with its own
			name: "read and list in one statement",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":["s3:GetObject","s3:GetObjectVersion","s3:ListBucket","s3:ListBucketVersions"],
				"Resource":["arn:aws:s3:::bucket","arn:aws:s3:::bucket/*"]}]}`,
			want: ActionGetObject | ActionGetObjectVersion | ActionListBucket | ActionListBucketVersions,
		},
		{
			// an object action with no object resource grants nothing, as in S3
			name: "object action with only the bucket resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket"}]}`,
			want: 0,
		},
		{
			name: "bucket action with only the object resource",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket/*"}]}`,
			want: 0,
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
			policy, err := parseBucketPolicy("bucket", strings.NewReader(test.policy))
			if test.err != nil {
				if !errors.Is(err, test.err) {
					t.Fatalf("expected %v, got %v", test.err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			} else if policy.Public != test.want {
				t.Fatalf("expected actions %b, got %b", test.want, policy.Public)
			} else if policy.Document != test.policy {
				t.Fatalf("expected document to be stored verbatim, got %q", policy.Document)
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

	if _, err := parseBucketPolicy("bucket", strings.NewReader(policy)); !errors.Is(err, s3errs.ErrPolicyTooLarge) {
		t.Fatalf("expected %v, got %v", s3errs.ErrPolicyTooLarge, err)
	}
}

func TestPolicyActionsAllows(t *testing.T) {
	granted := ActionGetObject | ActionListBucket

	tests := []struct {
		name string
		want PolicyActions
		ok   bool
	}{
		{"granted", ActionGetObject, true},
		{"other granted", ActionListBucket, true},
		{"both granted", ActionGetObject | ActionListBucket, true},
		{"not granted", ActionGetObjectVersion, false},
		{"one of two not granted", ActionGetObject | ActionGetObjectVersion, false},
		// an empty set is contained in every set, so allowing it would
		// authorize a caller that named no action against any policy
		{"no action", 0, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := granted.Allows(test.want); got != test.ok {
				t.Fatalf("expected %v, got %v", test.ok, got)
			}
		})
	}

	// including against a policy that grants nothing
	var none PolicyActions
	if none.Allows(0) {
		t.Fatal("expected an empty want to be denied by an empty grant")
	}
}

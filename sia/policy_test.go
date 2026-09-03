package sia_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// A "*" principal covers unsigned requests and signed requests from non-owners.
// The two take different paths through the backend, so both are asserted.
const (
	otherAccessKeyID = "foo"
	otherSecretKey   = "bar"
)

// publicCaller is one kind of caller a "*" principal covers.
type publicCaller struct {
	client *testutil.S3Tester
	// missingBucket is what this caller sees for a bucket that does not exist;
	// an anonymous one must not learn that.
	missingBucket *s3errs.Error
}

// forEachPublicCaller runs fn for both kinds of caller. The tester must have
// been created with the "other" key pair registered.
func forEachPublicCaller(t *testing.T, s3Tester *testutil.S3Tester, fn func(*testing.T, publicCaller)) {
	t.Helper()
	for _, c := range []struct {
		name   string
		caller publicCaller
	}{
		{"anonymous", publicCaller{s3Tester.Anonymous(), &s3errs.ErrAccessDenied}},
		{"other user", publicCaller{s3Tester.ChangeAccessKey(t, otherAccessKeyID, otherSecretKey), &s3errs.ErrNoSuchBucket}},
	} {
		t.Run(c.name, func(t *testing.T) { fn(t, c.caller) })
	}
}

// publicReadPolicy makes every object in the bucket readable by anyone.
func publicReadPolicy(bucket string) string {
	return fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicRead",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::%s/*"
    }
  ]
}`, bucket)
}

// readAndListPolicy grants every action s3d can hand to anonymous callers.
func readAndListPolicy(bucket string) string {
	return fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::%s/*"
    },
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:ListBucket", "s3:ListBucketVersions"],
      "Resource": "arn:aws:s3:::%s"
    }
  ]
}`, bucket, bucket)
}

func TestBucketPolicyPublicRead(t *testing.T) {
	const (
		bucket = "bucket"
		object = "key"
	)
	contents := []byte("value")

	s3Tester := testutil.NewTester(t)
	anon := s3Tester.Anonymous()

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, object, bytes.NewReader(contents), nil); err != nil {
		t.Fatal(err)
	}

	// a bucket with no policy has none to get
	_, err := s3Tester.GetBucketPolicy(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucketPolicy, err)

	// and is not public
	if public, err := s3Tester.GetBucketPolicyStatus(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if public {
		t.Fatal("expected bucket without a policy to not be public")
	}

	// so an anonymous read is denied
	_, err = anon.GetObjectVersion(t.Context(), bucket, object, nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// make the bucket public
	policy := publicReadPolicy(bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	// the document must round-trip byte-for-byte
	if got, err := s3Tester.GetBucketPolicy(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if got != policy {
		t.Fatalf("expected policy %q, got %q", policy, got)
	}

	if public, err := s3Tester.GetBucketPolicyStatus(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if !public {
		t.Fatal("expected bucket to be public")
	}

	// the anonymous read now succeeds
	if body, err := anon.GetObjectVersion(t.Context(), bucket, object, nil); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(body, contents) {
		t.Fatalf("expected %q, got %q", contents, body)
	}

	// removing the policy revokes the access again
	if err := s3Tester.DeleteBucketPolicy(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	_, err = s3Tester.GetBucketPolicy(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucketPolicy, err)

	_, err = anon.GetObjectVersion(t.Context(), bucket, object, nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
}

// TestBucketPolicyPublicReadScope checks that a public-read policy opens up
// exactly what it describes and nothing more.
func TestBucketPolicyPublicReadScope(t *testing.T) {
	const (
		bucket  = "bucket"
		private = "private-bucket"
		object  = "key"
	)

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", otherAccessKeyID, otherSecretKey))

	for _, b := range []string{bucket, private} {
		if err := s3Tester.CreateBucket(t.Context(), b); err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObject(t.Context(), b, object, bytes.NewReader([]byte("value")), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, publicReadPolicy(bucket)); err != nil {
		t.Fatal(err)
	}

	forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
		tests := []struct {
			name string
			do   func() error
			err  *s3errs.Error
		}{
			{
				name: "read the public object",
				do: func() error {
					_, err := c.client.GetObjectVersion(t.Context(), bucket, object, nil)
					return err
				},
			},
			{
				name: "head the public object",
				do: func() error {
					_, err := c.client.HeadObject(t.Context(), bucket, object, nil)
					return err
				},
			},
			{
				name: "a missing key is still reported as missing",
				do: func() error {
					_, err := c.client.GetObjectVersion(t.Context(), bucket, "missing", nil)
					return err
				},
				err: &s3errs.ErrNoSuchKey,
			},
			{
				// s3:GetObject does not cover addressing a specific version
				name: "read a specific version",
				do: func() error {
					_, err := c.client.GetObjectVersion(t.Context(), bucket, object, aws.String("null"))
					return err
				},
				err: &s3errs.ErrAccessDenied,
			},
			{
				// the policy grants no s3:ListBucket
				name: "list the bucket",
				do: func() error {
					_, err := c.client.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
					return err
				},
				err: &s3errs.ErrAccessDenied,
			},
			{
				// nor s3:ListBucketVersions
				name: "list the bucket's versions",
				do: func() error {
					_, err := c.client.ListObjectVersionsPage(t.Context(), bucket, nil)
					return err
				},
				err: &s3errs.ErrAccessDenied,
			},
			{
				name: "write an object",
				do: func() error {
					_, err := c.client.PutObject(t.Context(), bucket, "other", bytes.NewReader([]byte("nope")), nil)
					return err
				},
				err: &s3errs.ErrAccessDenied,
			},
			{
				name: "delete the object",
				do:   func() error { return c.client.DeleteObject(t.Context(), bucket, object) },
				err:  &s3errs.ErrAccessDenied,
			},
			{
				name: "read the policy",
				do: func() error {
					_, err := c.client.GetBucketPolicy(t.Context(), bucket)
					return err
				},
				err: &s3errs.ErrAccessDenied,
			},
			{
				name: "the policy covers only its own bucket",
				do: func() error {
					_, err := c.client.GetObjectVersion(t.Context(), private, object, nil)
					return err
				},
				err: &s3errs.ErrAccessDenied,
			},
			{
				// an anonymous caller must not learn whether a bucket it may not
				// touch exists; a signed one already gets NoSuchBucket from the
				// ordinary ownership check
				name: "a bucket that does not exist",
				do: func() error {
					_, err := c.client.GetObjectVersion(t.Context(), "nonexistent", object, nil)
					return err
				},
				err: c.missingBucket,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				err := test.do()
				if test.err == nil {
					if err != nil {
						t.Fatalf("expected success, got %v", err)
					}
					return
				}
				testutil.AssertS3Error(t, *test.err, err)
			})
		}
	})
}

// TestBucketPolicyOwnership checks that only the owner can read or change the
// policy.
func TestBucketPolicyOwnership(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", otherAccessKeyID, otherSecretKey))
	other := s3Tester.ChangeAccessKey(t, otherAccessKeyID, otherSecretKey)

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	policy := publicReadPolicy(bucket)
	err := other.PutBucketPolicy(t.Context(), bucket, policy)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	// a publicly readable bucket does not have a publicly readable configuration
	_, err = other.GetBucketPolicy(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	err = other.DeleteBucketPolicy(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// the owner's policy survived
	if got, err := s3Tester.GetBucketPolicy(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if got != policy {
		t.Fatalf("expected policy %q, got %q", policy, got)
	}

	// nor does a public bucket show up in anyone else's bucket list
	if buckets, err := other.ListBuckets(t.Context()); err != nil {
		t.Fatal(err)
	} else if len(buckets) != 0 {
		t.Fatalf("expected no buckets for the other user, got %v", buckets)
	}
}

// TestBucketPolicyRejected checks that a policy s3d cannot honor is refused
// rather than stored as a weaker one.
func TestBucketPolicyRejected(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		policy string
		err    s3errs.Error
	}{
		{
			name:   "malformed",
			policy: `not a policy`,
			err:    s3errs.ErrMalformedPolicy,
		},
		{
			name: "conditional grant",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
				"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*",
				"Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`,
			err: s3errs.ErrNotImplemented,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s3Tester.PutBucketPolicy(t.Context(), bucket, test.policy)
			testutil.AssertS3Error(t, test.err, err)

			// a rejected policy must leave no trace
			_, err = s3Tester.GetBucketPolicy(t.Context(), bucket)
			testutil.AssertS3Error(t, s3errs.ErrNoSuchBucketPolicy, err)
		})
	}
}

// TestBucketPolicyPublicListAndVersions checks the list and version grants.
func TestBucketPolicyPublicListAndVersions(t *testing.T) {
	const (
		bucket = "bucket"
		object = "key"
	)

	s3Tester := testutil.NewTester(t)
	anon := s3Tester.Anonymous()

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	// two versions, so a versioned read has an older one to address
	oldVersion, err := s3Tester.PutObjectVersion(t.Context(), bucket, object, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, object, []byte("new")); err != nil {
		t.Fatal(err)
	}

	// none of it is reachable before the policy exists
	_, err = anon.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, readAndListPolicy(bucket)); err != nil {
		t.Fatal(err)
	}

	// ListObjects v1 and v2
	v1, err := anon.ListObjects(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	if err != nil {
		t.Fatal(err)
	} else if len(v1.Contents) != 1 || aws.ToString(v1.Contents[0].Key) != object {
		t.Fatalf("unexpected v1 listing: %v", v1.Contents)
	}

	v2, err := anon.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	if err != nil {
		t.Fatal(err)
	} else if len(v2.Contents) != 1 || aws.ToString(v2.Contents[0].Key) != object {
		t.Fatalf("unexpected v2 listing: %v", v2.Contents)
	}

	// ListObjectVersions
	versions, err := anon.ListObjectVersionsPage(t.Context(), bucket, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(versions.Versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(versions.Versions))
	}

	// the current version, and an older one by ID
	if body, err := anon.GetObjectVersion(t.Context(), bucket, object, nil); err != nil {
		t.Fatal(err)
	} else if string(body) != "new" {
		t.Fatalf("expected %q, got %q", "new", body)
	}
	if body, err := anon.GetObjectVersion(t.Context(), bucket, object, aws.String(oldVersion)); err != nil {
		t.Fatal(err)
	} else if string(body) != "old" {
		t.Fatalf("expected %q, got %q", "old", body)
	}

	// writes are still denied
	_, err = anon.PutObject(t.Context(), bucket, "other", bytes.NewReader([]byte("nope")), nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
}

// TestBucketPolicyGrantsAreIndependent checks that allowing listing does not
// also allow reading, or vice versa.
func TestBucketPolicyGrantsAreIndependent(t *testing.T) {
	const (
		bucket = "bucket"
		object = "key"
	)

	// each policy grants exactly one action; every other action must stay denied
	grants := []struct{ action, resource string }{
		{"s3:GetObject", bucket + "/*"},
		{"s3:GetObjectVersion", bucket + "/*"},
		{"s3:ListBucket", bucket},
		{"s3:ListBucketVersions", bucket},
	}

	for _, grant := range grants {
		t.Run(grant.action, func(t *testing.T) {
			s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", otherAccessKeyID, otherSecretKey))
			if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
				t.Fatal(err)
			} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
				t.Fatal(err)
			}
			version, err := s3Tester.PutObjectVersion(t.Context(), bucket, object, []byte("value"))
			if err != nil {
				t.Fatal(err)
			}
			if err := s3Tester.PutBucketPolicy(t.Context(), bucket, statementPolicy(grant.action, grant.resource)); err != nil {
				t.Fatal(err)
			}

			forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
				// what each action does when the policy allows it
				attempts := []struct {
					action string
					do     func() error
				}{
					{"s3:GetObject", func() error {
						_, err := c.client.GetObjectVersion(t.Context(), bucket, object, nil)
						return err
					}},
					{"s3:GetObjectVersion", func() error {
						_, err := c.client.GetObjectVersion(t.Context(), bucket, object, aws.String(version))
						return err
					}},
					{"s3:ListBucket", func() error {
						_, err := c.client.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
						return err
					}},
					{"s3:ListBucketVersions", func() error {
						_, err := c.client.ListObjectVersionsPage(t.Context(), bucket, nil)
						return err
					}},
				}

				for _, attempt := range attempts {
					t.Run(attempt.action, func(t *testing.T) {
						err := attempt.do()
						if attempt.action == grant.action {
							if err != nil {
								t.Fatalf("expected the granted action to succeed, got %v", err)
							}
							return
						}
						testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
					})
				}
			})
		})
	}
}

// statementPolicy grants action on resource to everyone.
func statementPolicy(action, resource string) string {
	return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
		"Action":"%s","Resource":"arn:aws:s3:::%s"}]}`, action, resource)
}

// TestBucketPolicyListAuthorizesEmptyPage checks that a listing requesting no
// keys is still authorized. It drives the backend directly because the AWS
// client omits MaxKeys when it is zero.
func TestBucketPolicyListAuthorizesEmptyPage(t *testing.T) {
	const (
		bucket = "bucket"
		object = "key"
	)

	backend, _ := testutil.NewBackend(t, testutil.WithKeyPair("other", otherAccessKeyID, otherSecretKey))
	if err := backend.CreateBucket(t.Context(), testutil.AccessKeyID, bucket); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.PutObject(t.Context(), testutil.AccessKeyID, bucket, object,
		bytes.NewReader([]byte("value")), s3.PutObjectOptions{ContentLength: 5}); err != nil {
		t.Fatal(err)
	}

	// the bucket has no policy at all, so neither caller may list it
	otherKey := otherAccessKeyID
	for _, caller := range []struct {
		name        string
		accessKeyID *string
	}{
		{"anonymous", nil},
		{"other user", &otherKey},
	} {
		t.Run(caller.name, func(t *testing.T) {
			_, err := backend.ListObjects(t.Context(), caller.accessKeyID, bucket, s3.Prefix{}, s3.ListObjectsPage{MaxKeys: 0})
			if !errors.Is(err, s3errs.ErrAccessDenied) {
				t.Fatalf("ListObjects: expected %v, got %v", s3errs.ErrAccessDenied, err)
			}

			_, err = backend.ListObjectVersions(t.Context(), caller.accessKeyID, bucket, s3.Prefix{}, s3.ListObjectVersionsPage{MaxKeys: 0})
			if !errors.Is(err, s3errs.ErrAccessDenied) {
				t.Fatalf("ListObjectVersions: expected %v, got %v", s3errs.ErrAccessDenied, err)
			}
		})
	}
}

// TestBucketPolicyListingReportsBucketOwner checks that a listing attributes
// objects to the bucket's owner rather than to whoever asked for the listing.
func TestBucketPolicyListingReportsBucketOwner(t *testing.T) {
	const (
		bucket = "bucket"
		object = "key"
	)

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", otherAccessKeyID, otherSecretKey))
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, object, bytes.NewReader([]byte("value")), nil); err != nil {
		t.Fatal(err)
	}
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, readAndListPolicy(bucket)); err != nil {
		t.Fatal(err)
	}

	assertOwner := func(t *testing.T, owner *types.Owner) {
		t.Helper()
		if owner == nil {
			t.Fatal("expected an owner")
		} else if aws.ToString(owner.ID) != testutil.Owner {
			t.Fatalf("expected owner %q, got %q", testutil.Owner, aws.ToString(owner.ID))
		}
	}

	forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
		listed, err := c.client.ListObjectsV2(t.Context(), bucket, nil, nil, s3.ListObjectsPage{FetchOwner: aws.Bool(true)})
		if err != nil {
			t.Fatal(err)
		} else if len(listed.Contents) != 1 {
			t.Fatalf("unexpected listing: %v", listed.Contents)
		}
		assertOwner(t, listed.Contents[0].Owner)

		// the versions endpoint always asks for owner data
		versions, err := c.client.ListObjectVersionsPage(t.Context(), bucket, nil)
		if err != nil {
			t.Fatal(err)
		} else if len(versions.Versions) != 1 {
			t.Fatalf("expected 1 version, got %d", len(versions.Versions))
		}
		assertOwner(t, versions.Versions[0].Owner)
	})
}

// TestBucketPolicyCopySourceGrants checks that the copy paths authorize their
// source read per version, as a direct read does.
func TestBucketPolicyCopySourceGrants(t *testing.T) {
	const (
		public = "public-bucket"
		own    = "own-bucket"
		object = "key"
	)

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair("other", otherAccessKeyID, otherSecretKey))
	other := s3Tester.ChangeAccessKey(t, otherAccessKeyID, otherSecretKey)

	if err := s3Tester.CreateBucket(t.Context(), public); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), public, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}
	oldVersion, err := s3Tester.PutObjectVersion(t.Context(), public, object, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3Tester.PutObjectVersion(t.Context(), public, object, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := other.CreateBucket(t.Context(), own); err != nil {
		t.Fatal(err)
	}

	// s3:GetObject only: the current version may be copied, an older one may not
	if err := s3Tester.PutBucketPolicy(t.Context(), public, publicReadPolicy(public)); err != nil {
		t.Fatal(err)
	}
	if _, err := other.CopyObjectVersion(t.Context(), public, object, nil, own, "copy"); err != nil {
		t.Fatal(err)
	}
	_, err = other.CopyObjectVersion(t.Context(), public, object, aws.String(oldVersion), own, "copy-old")
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// UploadPartCopy authorizes its source the same way
	upload, err := other.CreateMultipartUpload(t.Context(), own, "multipart", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = other.UploadPartCopy(t.Context(), public, object, own, "multipart", aws.ToString(upload.UploadId),
		testutil.UploadPartCopyOptions{PartNumber: 1, SourceVersionID: aws.String(oldVersion)})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// granting the version action unlocks both
	if err := s3Tester.PutBucketPolicy(t.Context(), public, readAndListPolicy(public)); err != nil {
		t.Fatal(err)
	}
	if _, err := other.CopyObjectVersion(t.Context(), public, object, aws.String(oldVersion), own, "copy-old"); err != nil {
		t.Fatal(err)
	}
}

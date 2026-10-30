package sia_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap/zaptest"
)

// publicCaller is one kind of caller a "*" principal covers, which includes
// unsigned requests and signed requests from non-owners. Both are asserted.
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
		{"other user", publicCaller{s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey), &s3errs.ErrNoSuchBucket}},
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

// rawRequest sends an anonymous request without the AWS SDK, so it carries no
// X-Amz-Content-Sha256 header, and returns the response body and headers.
func rawRequest(t *testing.T, server *httptest.Server, method, path, body string) ([]byte, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: expected status %d, got %d: %s", method, path, http.StatusOK, resp.StatusCode, data)
	}
	return data, resp.Header
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

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))

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
		// a HEAD response carries no body, so only its status is checkable.
		// s3:ListBucket also authorizes HeadBucket, and is not granted here.
		testutil.AssertS3StatusCode(t, s3errs.ErrAccessDenied, c.client.HeadBucket(t.Context(), bucket))

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
				name: "a missing key is hidden, not reported as missing",
				do: func() error {
					_, err := c.client.GetObjectVersion(t.Context(), bucket, "missing", nil)
					return err
				},
				err: &s3errs.ErrAccessDenied,
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

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)

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

// TestBucketPolicyRejected checks that a malformed policy is refused and leaves
// no trace.
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
	// s3:ListBucket also authorizes HeadBucket
	if err := anon.HeadBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

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
			s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
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

// TestBucketPolicyListingReportsBucketOwner checks that a listing attributes
// objects to the bucket's owner rather than to whoever asked for the listing.
func TestBucketPolicyListingReportsBucketOwner(t *testing.T) {
	const (
		bucket = "bucket"
		object = "key"
	)

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
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
		v1, err := c.client.ListObjects(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
		if err != nil {
			t.Fatal(err)
		} else if len(v1.Contents) != 1 {
			t.Fatalf("unexpected v1 listing: %v", v1.Contents)
		}
		assertOwner(t, v1.Contents[0].Owner)

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

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)

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

// TestBucketPolicyMissingKeyVisibility checks that a missing key is reported as
// missing only to a caller allowed to list the bucket. Without s3:ListBucket,
// S3 returns AccessDenied so the caller cannot probe for keys.
func TestBucketPolicyMissingKeyVisibility(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// the owner may list, so it still sees a plain 404
	_, err := s3Tester.GetObjectVersion(t.Context(), bucket, "missing", nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)

	// read but no list: existence is hidden
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, publicReadPolicy(bucket)); err != nil {
		t.Fatal(err)
	}
	forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
		_, err := c.client.GetObjectVersion(t.Context(), bucket, "missing", nil)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	})

	// read and list: the key is reported missing
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, readAndListPolicy(bucket)); err != nil {
		t.Fatal(err)
	}
	forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
		_, err := c.client.GetObjectVersion(t.Context(), bucket, "missing", nil)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
	})
}

// TestBucketPolicyDeleteMarkerHidden checks that a delete-marked key is
// indistinguishable from one that never existed for a caller that may not list
// the bucket. Otherwise the 404 and its delete-marker headers reveal that the
// key was once there.
func TestBucketPolicyDeleteMarkerHidden(t *testing.T) {
	const (
		bucket = "bucket"
		object = "key"
	)

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObjectVersion(t.Context(), bucket, object, []byte("value")); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.DeleteObject(t.Context(), bucket, object); err != nil {
		t.Fatal(err)
	}

	// read but no list: the delete marker looks exactly like an absent key
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, publicReadPolicy(bucket)); err != nil {
		t.Fatal(err)
	}
	forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
		_, err := c.client.GetObjectVersion(t.Context(), bucket, object, nil)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

		_, err = c.client.GetObjectVersion(t.Context(), bucket, "never-existed", nil)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

		// nor can a copy be used to tell them apart
		_, err = c.client.CopyObjectVersion(t.Context(), bucket, object, nil, bucket, "copy")
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	})

	// with s3:ListBucket the delete marker is reported as such
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, readAndListPolicy(bucket)); err != nil {
		t.Fatal(err)
	}
	forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
		_, err := c.client.GetObjectVersion(t.Context(), bucket, object, nil)
		testutil.AssertS3StatusCode(t, s3errs.ErrNoSuchKey, err)
	})
}

// TestBucketPolicyKeepsConfigurationPrivate checks that a public bucket's own
// configuration stays owner-only.
func TestBucketPolicyKeepsConfigurationPrivate(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketPolicy(t.Context(), bucket, readAndListPolicy(bucket)); err != nil {
		t.Fatal(err)
	}

	// the owner can still read it
	if _, err := s3Tester.BucketLocation(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	forEachPublicCaller(t, s3Tester, func(t *testing.T, c publicCaller) {
		// s3:ListBucket lets the caller head the bucket, but not read its
		// configuration
		if err := c.client.HeadBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
		_, err := c.client.BucketLocation(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

		_, err = c.client.GetBucketVersioning(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	})
}

// TestBucketPolicyCopyHidesOnlySource checks that a copy from a bucket the
// caller may read but not list hides a missing source, but not a missing
// destination, which is the caller's own.
func TestBucketPolicyCopyHidesOnlySource(t *testing.T) {
	const (
		public = "public-bucket"
		own    = "own-bucket"
		object = "key"
	)

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)

	if err := s3Tester.CreateBucket(t.Context(), public); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.AddObject(public, object, []byte("value"), nil); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketPolicy(t.Context(), public, publicReadPolicy(public)); err != nil {
		t.Fatal(err)
	} else if err := other.CreateBucket(t.Context(), own); err != nil {
		t.Fatal(err)
	}

	// the source is missing, which the caller may not learn
	err := other.ConditionalCopyObject(t.Context(), public, "missing", own, object, testutil.Preconditions{}, testutil.Preconditions{})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// the destination is missing, which the caller may learn
	err = other.ConditionalCopyObject(t.Context(), public, object, own, object, testutil.Preconditions{}, testutil.Preconditions{IfMatch: aws.String("*")})
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
}

// TestBucketPolicyCopyAuthorizesSource checks that a copy into a bucket the
// caller owns still requires read access to the source.
func TestBucketPolicyCopyAuthorizesSource(t *testing.T) {
	const (
		private = "private-bucket"
		own     = "own-bucket"
		object  = "key"
	)

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)

	if err := s3Tester.CreateBucket(t.Context(), private); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.AddObject(private, object, []byte("value"), nil); err != nil {
		t.Fatal(err)
	} else if err := other.CreateBucket(t.Context(), own); err != nil {
		t.Fatal(err)
	}

	_, err := other.CopyObjectVersion(t.Context(), private, object, nil, own, "copy")
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	upload, err := other.CreateMultipartUpload(t.Context(), own, "multipart", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = other.UploadPartCopy(t.Context(), private, object, own, "multipart", aws.ToString(upload.UploadId),
		testutil.UploadPartCopyOptions{PartNumber: 1})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
}

// TestBucketPolicyAnonymousWrites checks that a policy can let anonymous
// callers write under a prefix, and that the owner owns what they write.
func TestBucketPolicyAnonymousWrites(t *testing.T) {
	const bucket = "bucket"

	backend, _ := testutil.NewBackend(t)
	s3Tester := testutil.NewTester(t, testutil.WithBackend(backend))
	anon := s3Tester.Anonymous()

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"],
      "Resource": ["arn:aws:s3:::%s", "arn:aws:s3:::%s/public/*"]
    }
  ]
}`, bucket, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	// an ordinary upload
	if _, err := anon.PutObject(t.Context(), bucket, "public/ordinary", strings.NewReader("ordinary"), nil); err != nil {
		t.Fatal(err)
	}

	// a multipart upload
	upload, err := anon.CreateMultipartUpload(t.Context(), bucket, "public/multipart", nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadID := aws.ToString(upload.UploadId)
	part, err := anon.UploadPart(t.Context(), bucket, "public/multipart", uploadID, 1, []byte("multipart"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := anon.ListParts(t.Context(), bucket, "public/multipart", uploadID, nil, nil); err != nil {
		t.Fatal(err)
	} else if _, err := anon.CompleteMultipartUpload(t.Context(), bucket, "public/multipart", uploadID, []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}); err != nil {
		t.Fatal(err)
	}

	// an aborted multipart upload
	upload, err = anon.CreateMultipartUpload(t.Context(), bucket, "public/aborted", nil)
	if err != nil {
		t.Fatal(err)
	} else if err := anon.AbortMultipartUpload(t.Context(), bucket, "public/aborted", aws.ToString(upload.UploadId)); err != nil {
		t.Fatal(err)
	}

	// nothing outside the prefix
	_, err = anon.PutObject(t.Context(), bucket, "private", strings.NewReader("private"), nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// uploads from a client that sends no payload hash
	server := httptest.NewServer(s3.New(backend, s3.WithLogger(zaptest.NewLogger(t))))
	t.Cleanup(server.Close)
	rawRequest(t, server, http.MethodPut, "/"+bucket+"/public/raw", "raw")

	path := "/" + bucket + "/public/raw-multipart"
	body, _ := rawRequest(t, server, http.MethodPost, path+"?uploads", "")
	var rawUpload s3.InitiateMultipartUploadResponse
	if err := xml.Unmarshal(body, &rawUpload); err != nil {
		t.Fatal(err)
	}
	_, headers := rawRequest(t, server, http.MethodPut, path+"?partNumber=1&uploadId="+rawUpload.UploadID, "raw multipart")
	complete, err := xml.Marshal(s3.CompleteMultipartUploadRequest{
		Parts: []s3.CompleteMultipartPart{{PartNumber: 1, ETag: headers.Get("ETag")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rawRequest(t, server, http.MethodPost, path+"?uploadId="+rawUpload.UploadID, string(complete))

	// the owner owns every object, including those uploaded anonymously
	listing, err := s3Tester.ListObjects(t.Context(), bucket, nil, nil, s3.ListObjectsPage{})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range listing.Contents {
		if object.Owner == nil || aws.ToString(object.Owner.ID) != testutil.Owner {
			t.Fatalf("expected owner %q, got %+v", testutil.Owner, object.Owner)
		}
	}
	for object, want := range map[string]string{"public/ordinary": "ordinary", "public/multipart": "multipart", "public/raw": "raw", "public/raw-multipart": "raw multipart"} {
		if got, err := s3Tester.GetObjectVersion(t.Context(), bucket, object, nil); err != nil {
			t.Fatal(err)
		} else if string(got) != want {
			t.Fatalf("%s: expected %q, got %q", object, want, got)
		}
	}
}

// TestBucketPolicyCrossUser checks a grant to another user scoped by a policy
// variable.
func TestBucketPolicyCrossUser(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, "private/key", strings.NewReader("private"), nil); err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::%s:root"},
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::%s/${aws:userid}/*"
    }
  ]
}`, testutil.OtherOwner, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	// the other user may use their own prefix
	if _, err := other.PutObject(t.Context(), bucket, "other/key", strings.NewReader("value"), nil); err != nil {
		t.Fatal(err)
	} else if _, err := other.GetObjectVersion(t.Context(), bucket, "other/key", nil); err != nil {
		t.Fatal(err)
	}

	// but an anonymous caller may not
	_, err := s3Tester.Anonymous().GetObjectVersion(t.Context(), bucket, "other/key", nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// a multi-delete request authorizes each key
	res, err := other.DeleteObjects(t.Context(), bucket, []types.ObjectIdentifier{{Key: aws.String("other/key")}, {Key: aws.String("private/key")}}, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Deleted) != 1 || len(res.Errors) != 1 || aws.ToString(res.Errors[0].Code) != s3errs.ErrAccessDenied.Code {
		t.Fatalf("expected one deletion and one AccessDenied, got %+v", res)
	}
}

// TestBucketPolicyDeny checks that a Deny overrides even the owner, who can
// still replace, read and delete the policy.
func TestBucketPolicyDeny(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, "secret/key", strings.NewReader("secret"), nil); err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::%s/secret/*"
    }
  ]
}`, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	_, err := s3Tester.GetObjectVersion(t.Context(), bucket, "secret/key", nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// even a policy denying everything
	deny := `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": "*"
    }
  ]
}`
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, deny); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.GetBucketPolicy(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.DeleteBucketPolicy(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
}

// TestBucketPolicyConditions checks that conditions see the request's listing
// parameters, and that a key s3d cannot populate is missing.
func TestBucketPolicyConditions(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t)
	anon := s3Tester.Anonymous()

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, "public/key", strings.NewReader("value"), nil); err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::%s",
      "Condition": {
        "StringLike": {"s3:prefix": "public/*"},
        "NumericLessThanEquals": {"s3:max-keys": 10}
      }
    },
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::%s/public/*",
      "Condition": {"StringEqualsIfExists": {"aws:SourceVpc": "vpc-1"}}
    }
  ]
}`, bucket, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	if _, err := anon.ListObjectsV2(t.Context(), bucket, aws.String("public/"), nil, s3.ListObjectsPage{MaxKeys: 10}); err != nil {
		t.Fatal(err)
	}

	// the prefix and the limit must both satisfy the conditions
	_, err := anon.ListObjectsV2(t.Context(), bucket, aws.String("private/"), nil, s3.ListObjectsPage{MaxKeys: 10})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	_, err = anon.ListObjectsV2(t.Context(), bucket, aws.String("public/"), nil, s3.ListObjectsPage{MaxKeys: 11})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// a request has no VPC, so the IfExists condition holds
	if _, err := anon.GetObjectVersion(t.Context(), bucket, "public/key", nil); err != nil {
		t.Fatal(err)
	}
}

// TestBucketPolicyVersionConditions checks that s3:VersionId conditions apply
// to reads, copies and multi-delete requests of a version.
func TestBucketPolicyVersionConditions(t *testing.T) {
	const (
		bucket = "bucket"
		dest   = "dest"
		object = "key"
	)

	s3Tester := testutil.NewTester(t)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.CreateBucket(t.Context(), dest); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}
	v1, err := s3Tester.PutObjectVersion(t.Context(), bucket, object, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := s3Tester.PutObjectVersion(t.Context(), bucket, object, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}

	// deny access to the first version only
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:GetObjectVersion", "s3:DeleteObjectVersion"],
      "Resource": "arn:aws:s3:::%s/*",
      "Condition": {"StringEquals": {"s3:VersionId": %q}}
    }
  ]
}`, bucket, v1)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	_, err = s3Tester.GetObjectVersion(t.Context(), bucket, object, aws.String(v1))
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	if _, err := s3Tester.GetObjectVersion(t.Context(), bucket, object, aws.String(v2)); err != nil {
		t.Fatal(err)
	}

	_, err = s3Tester.CopyObjectVersion(t.Context(), bucket, object, aws.String(v1), dest, "copy")
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	if _, err := s3Tester.CopyObjectVersion(t.Context(), bucket, object, aws.String(v2), dest, "copy"); err != nil {
		t.Fatal(err)
	}

	res, err := s3Tester.DeleteObjects(t.Context(), bucket, []types.ObjectIdentifier{{Key: aws.String(object), VersionId: aws.String(v1)}, {Key: aws.String(object), VersionId: aws.String(v2)}}, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Errors) != 1 || len(res.Deleted) != 1 || aws.ToString(res.Errors[0].VersionId) != v1 {
		t.Fatalf("expected only version %s to be denied, got %+v", v1, res)
	}
}

// TestBucketPolicyOwnerOnlyAPIs checks that, as in S3, a policy cannot grant
// the bucket policy APIs to another user.
func TestBucketPolicyOwnerOnlyAPIs(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)

	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	grant := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": %q,
      "Action": ["s3:GetBucketPolicy", "s3:PutBucketPolicy"],
      "Resource": "arn:aws:s3:::%s"
    }
  ]
}`, testutil.OtherOwner, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, grant); err != nil {
		t.Fatal(err)
	}

	// S3 answers a granted user other than the owner with 405
	_, err := other.GetBucketPolicy(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrMethodNotAllowed, err)
	err = other.PutBucketPolicy(t.Context(), bucket, publicReadPolicy(bucket))
	testutil.AssertS3Error(t, s3errs.ErrMethodNotAllowed, err)

	// and one without a grant with 403
	err = other.DeleteBucketPolicy(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	if got, err := s3Tester.GetBucketPolicy(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if got != grant {
		t.Fatalf("expected policy %q, got %q", grant, got)
	}
}

// TestBucketPolicyObjectLockConditions checks that retention conditions see
// the retention a PutObjectRetention body applies.
func TestBucketPolicyObjectLockConditions(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)

	if err := s3Tester.CreateBucketWithObjectLock(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObject(t.Context(), bucket, "key", strings.NewReader("value"), nil); err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": %q,
      "Action": "s3:PutObjectRetention",
      "Resource": "arn:aws:s3:::%s/*",
      "Condition": {"NumericLessThanEquals": {"s3:object-lock-remaining-retention-days": 10}}
    },
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObjectRetention",
      "Resource": "arn:aws:s3:::%s/*",
      "Condition": {"NumericGreaterThan": {"s3:object-lock-remaining-retention-days": 30}}
    }
  ]
}`, testutil.OtherOwner, bucket, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}

	retention := func(days int) *types.ObjectLockRetention {
		return &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeCompliance, RetainUntilDate: aws.Time(time.Now().AddDate(0, 0, days))}
	}
	if err := other.PutObjectRetention(t.Context(), bucket, "key", nil, retention(5), false); err != nil {
		t.Fatal(err)
	}
	err := other.PutObjectRetention(t.Context(), bucket, "key", nil, retention(20), false)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	err = s3Tester.PutObjectRetention(t.Context(), bucket, "key", nil, retention(40), false)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
}

// putTaggedObject stores data under object with the X-Amz-Tagging value tags.
func putTaggedObject(t *testing.T, s3Tester *testutil.S3Tester, bucket, object, tags string) *service.PutObjectOutput {
	t.Helper()
	resp, err := s3Tester.Client().PutObject(t.Context(), &service.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(object), Body: strings.NewReader("data"), Tagging: aws.String(tags)})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestBucketPolicyTaggingActions checks that each tagging API is granted by
// its own action.
func TestBucketPolicyTaggingActions(t *testing.T) {
	const bucket = "bucket"

	s3Tester := testutil.NewTester(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}
	version := putTaggedObject(t, s3Tester, bucket, "key", "project=one")
	setPolicy := func(action, resource string) {
		t.Helper()
		if err := s3Tester.PutBucketPolicy(t.Context(), bucket, statementPolicy(action, resource)); err != nil {
			t.Fatal(err)
		}
	}

	// GetObject alone does not disclose the tag count
	setPolicy("s3:GetObject", bucket+"/*")
	resp, err := other.Client().GetObject(t.Context(), &service.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("key")})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.TagCount != nil {
		t.Fatalf("expected no tag count, got %d", *resp.TagCount)
	}

	// the version action grants reading an explicit version only
	setPolicy("s3:GetObjectVersionTagging", bucket+"/*")
	tagging, err := other.GetObjectTagging(t.Context(), bucket, "key", version.VersionId)
	if err != nil {
		t.Fatal(err)
	} else if len(tagging.TagSet) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(tagging.TagSet))
	}
	_, err = other.GetObjectTagging(t.Context(), bucket, "key", nil)
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

	// a copy that keeps the source's tags needs s3:PutObjectTagging, but one
	// that replaces them with an empty set does not
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::%s/*"}
  ]
}`, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	}
	_, err = other.Client().CopyObject(t.Context(), &service.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: aws.String(bucket + "/key")})
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
	if _, err := other.Client().CopyObject(t.Context(), &service.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: aws.String(bucket + "/key"), TaggingDirective: types.TaggingDirectiveReplace}); err != nil {
		t.Fatal(err)
	}

	// s3:PutBucketTagging grants deleting the tags as well
	policy = fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Principal": "*", "Action": ["s3:GetBucketTagging", "s3:PutBucketTagging"], "Resource": "arn:aws:s3:::%s"}
  ]
}`, bucket)
	if err := s3Tester.PutBucketPolicy(t.Context(), bucket, policy); err != nil {
		t.Fatal(err)
	} else if err := other.PutBucketTagging(t.Context(), bucket, []types.Tag{{Key: aws.String("owner"), Value: aws.String("other")}}); err != nil {
		t.Fatal(err)
	} else if _, err := other.GetBucketTagging(t.Context(), bucket); err != nil {
		t.Fatal(err)
	} else if err := other.DeleteBucketTagging(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
}

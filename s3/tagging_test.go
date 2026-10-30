package s3_test

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap/zaptest"
)

// tagMap returns tags as a map, so tests can compare tag sets regardless of
// order.
func tagMap(tags []types.Tag) map[string]string {
	m := make(map[string]string)
	for _, tag := range tags {
		m[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return m
}

func TestBucketTagging(t *testing.T) {
	run := func(t *testing.T, pathStyle bool) {
		s3Tester := testutil.NewTester(t, testutil.WithServiceOptions(func(o *service.Options) {
			o.UsePathStyle = pathStyle
		}), testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))

		const bucket = "tagging-bucket"
		if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}

		// no tags yet
		_, err := s3Tester.GetBucketTagging(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchTagSet, err)

		// store the maximum number of tags and read them back
		tags := make([]types.Tag, 50)
		for i := range tags {
			tags[i] = types.Tag{Key: aws.String(fmt.Sprint(i)), Value: aws.String("value")}
		}
		if err := s3Tester.PutBucketTagging(t.Context(), bucket, tags); err != nil {
			t.Fatal(err)
		}
		got, err := s3Tester.GetBucketTagging(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(tagMap(got), tagMap(tags)) {
			t.Fatalf("expected %v, got %v", tagMap(tags), tagMap(got))
		}

		// a bucket we don't own is forbidden
		other := s3Tester.ChangeAccessKey(t, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)
		_, err = other.GetBucketTagging(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)

		// one tag too many is rejected
		tooMany := append(append([]types.Tag{}, tags...), types.Tag{Key: aws.String("extra"), Value: aws.String("")})
		err = s3Tester.PutBucketTagging(t.Context(), bucket, tooMany)
		testutil.AssertS3Error(t, s3errs.ErrInvalidTag, err)

		// a PUT replaces the whole set
		tags = []types.Tag{{Key: aws.String("replacement"), Value: aws.String("")}}
		if err := s3Tester.PutBucketTagging(t.Context(), bucket, tags); err != nil {
			t.Fatal(err)
		}
		got, err = s3Tester.GetBucketTagging(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(tagMap(got), tagMap(tags)) {
			t.Fatalf("expected %v, got %v", tagMap(tags), tagMap(got))
		}

		// delete the tags; delete is idempotent
		for range 2 {
			if err := s3Tester.DeleteBucketTagging(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
		}
		_, err = s3Tester.GetBucketTagging(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchTagSet, err)

		// an empty set removes the tags as well
		if err := s3Tester.PutBucketTagging(t.Context(), bucket, []types.Tag{}); err != nil {
			t.Fatal(err)
		}
		_, err = s3Tester.GetBucketTagging(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchTagSet, err)

		// a recreated bucket starts without tags
		if err := s3Tester.PutBucketTagging(t.Context(), bucket, tags); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.DeleteBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		} else if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
		_, err = s3Tester.GetBucketTagging(t.Context(), bucket)
		testutil.AssertS3Error(t, s3errs.ErrNoSuchTagSet, err)
	}

	t.Run("VirtualHostedStyle", func(t *testing.T) {
		run(t, false)
	})

	t.Run("PathStyle", func(t *testing.T) {
		run(t, true)
	})
}

func TestObjectTagging(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	client := s3Tester.Client()

	const (
		bucket = "object-tags"
		object = "key"
	)
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	tag := func(key, value string) types.Tag {
		return types.Tag{Key: aws.String(key), Value: aws.String(value)}
	}
	putObject := func(tags ...types.Tag) *service.PutObjectOutput {
		t.Helper()
		resp, err := client.PutObject(t.Context(), &service.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(object), Body: strings.NewReader("data")})
		if err != nil {
			t.Fatal(err)
		} else if _, err := s3Tester.PutObjectTagging(t.Context(), bucket, object, resp.VersionId, tags); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	assertTags := func(object string, versionID *string, want map[string]string) {
		t.Helper()
		resp, err := s3Tester.GetObjectTagging(t.Context(), bucket, object, versionID)
		if err != nil {
			t.Fatal(err)
		} else if got := tagMap(resp.TagSet); !reflect.DeepEqual(got, want) {
			t.Fatalf("expected tags %v, got %v", want, got)
		} else if versionID != nil && aws.ToString(resp.VersionId) != *versionID {
			t.Fatalf("expected version %q, got %q", *versionID, aws.ToString(resp.VersionId))
		}
	}

	// tags are stored and read back
	putObject(tag("project", "one"), tag("empty", ""), tag("unicode", "界"))
	original := map[string]string{"project": "one", "empty": "", "unicode": "界"}
	assertTags(object, nil, original)

	// each version keeps its own tags
	before, err := client.HeadObject(t.Context(), &service.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(object)})
	if err != nil {
		t.Fatal(err)
	} else if err := s3Tester.PutBucketVersioning(t.Context(), bucket, types.BucketVersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}
	second := putObject(tag("project", "two"))
	assertTags(object, aws.String("null"), original)
	assertTags(object, second.VersionId, map[string]string{"project": "two"})

	// replacing the tags of a version changes nothing else about it
	versionID, err := s3Tester.PutObjectTagging(t.Context(), bucket, object, aws.String("null"), []types.Tag{{Key: aws.String("replacement"), Value: aws.String("value")}})
	if err != nil {
		t.Fatal(err)
	} else if aws.ToString(versionID) != "null" {
		t.Fatalf("expected version %q, got %q", "null", aws.ToString(versionID))
	}
	assertTags(object, aws.String("null"), map[string]string{"replacement": "value"})
	after, err := client.HeadObject(t.Context(), &service.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(object), VersionId: aws.String("null")})
	if err != nil {
		t.Fatal(err)
	} else if !before.LastModified.Equal(*after.LastModified) || aws.ToString(before.ETag) != aws.ToString(after.ETag) {
		t.Fatal("expected a tag update to leave the version unchanged")
	}

	// delete the tags of a version
	if err := s3Tester.DeleteObjectTagging(t.Context(), bucket, object, second.VersionId); err != nil {
		t.Fatal(err)
	}
	assertTags(object, second.VersionId, map[string]string{})

	// a missing version and a delete marker have no tags
	_, err = s3Tester.GetObjectTagging(t.Context(), bucket, object, aws.String("absent"))
	testutil.AssertS3Error(t, s3errs.ErrNoSuchVersion, err)
	marker, err := client.DeleteObject(t.Context(), &service.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(object)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s3Tester.GetObjectTagging(t.Context(), bucket, object, nil)
	testutil.AssertS3Error(t, s3errs.ErrNoSuchKey, err)
	_, err = s3Tester.GetObjectTagging(t.Context(), bucket, object, marker.VersionId)
	testutil.AssertS3Error(t, s3errs.ErrMethodNotAllowed, err)
	assertTags(object, aws.String("null"), map[string]string{"replacement": "value"})
}

func TestObjectTaggingOrder(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	const bucket = "tag-order"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// tags keep the order the client sent them in
	want := []string{"zeta", "alpha", "mid"}
	var tags []types.Tag
	for i, key := range want {
		tags = append(tags, types.Tag{Key: aws.String(key), Value: aws.String(fmt.Sprint(i))})
	}
	if _, err := s3Tester.PutObject(t.Context(), bucket, "key", strings.NewReader("data"), nil); err != nil {
		t.Fatal(err)
	} else if _, err := s3Tester.PutObjectTagging(t.Context(), bucket, "key", nil, tags); err != nil {
		t.Fatal(err)
	}
	resp, err := s3Tester.GetObjectTagging(t.Context(), bucket, "key", nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, tag := range resp.TagSet {
		got = append(got, aws.ToString(tag.Key))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestTaggingChecksums(t *testing.T) {
	s3Tester := testutil.NewTester(t)

	const bucket = "checksum-tags"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	for _, algorithm := range []types.ChecksumAlgorithm{
		types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmCrc64nvme,
		types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256, types.ChecksumAlgorithmSha512,
	} {
		t.Run(string(algorithm), func(t *testing.T) {
			_, err := s3Tester.Client().PutBucketTagging(t.Context(), &service.PutBucketTaggingInput{
				Bucket:            aws.String(bucket),
				ChecksumAlgorithm: algorithm,
				Tagging:           &types.Tagging{TagSet: []types.Tag{{Key: aws.String("key"), Value: aws.String("value")}}},
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestTaggingRequests sends raw requests to the ?tagging subresource, to
// exercise digests and bodies the SDK cannot produce, such as MD5 checksums
// and algorithms the server cannot compute.
func TestTaggingRequests(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	if err := backend.CreateBucket(t.Context(), testutil.AccessKeyID, "bucket", s3.CreateBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s3.New(backend, s3.WithLogger(zaptest.NewLogger(t))))
	defer server.Close()

	const valid = `<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>`
	md5Digest := func(body string) string {
		sum := md5.Sum([]byte(body))
		return base64.StdEncoding.EncodeToString(sum[:])
	}

	tests := []struct {
		name    string
		method  string
		body    string
		headers map[string]string
		status  int
		code    string
	}{
		{name: "missing digest", method: http.MethodPut, body: valid, status: http.StatusBadRequest, code: "InvalidRequest"},
		{name: "bad digest encoding", method: http.MethodPut, body: valid, headers: map[string]string{"Content-MD5": "bad"}, status: http.StatusBadRequest, code: "InvalidDigest"},
		{name: "wrong digest", method: http.MethodPut, body: valid, headers: map[string]string{"Content-MD5": md5Digest("different body")}, status: http.StatusBadRequest, code: "BadDigest"},
		{name: "wrong checksum", method: http.MethodPut, body: valid, headers: map[string]string{"X-Amz-Checksum-Crc32": "AAAAAA=="}, status: http.StatusBadRequest, code: "BadDigest"},
		{name: "md5 checksum", method: http.MethodPut, body: valid, headers: map[string]string{"X-Amz-Sdk-Checksum-Algorithm": "MD5", "X-Amz-Checksum-Md5": md5Digest(valid)}, status: http.StatusNoContent},
		{name: "md5 checksum mismatch", method: http.MethodPut, body: valid, headers: map[string]string{"X-Amz-Sdk-Checksum-Algorithm": "MD5", "X-Amz-Checksum-Md5": md5Digest("different body")}, status: http.StatusBadRequest, code: "BadDigest"},
		// a checksum the server cannot compute is left unverified, as for
		// PutObject, rather than refused
		{name: "uncomputable checksum", method: http.MethodPut, body: valid, headers: map[string]string{"X-Amz-Sdk-Checksum-Algorithm": "XXHASH64", "X-Amz-Checksum-Xxhash64": "AAAAAAAAAAA="}, status: http.StatusNoContent},
		{name: "algorithm without checksum", method: http.MethodPut, body: valid, headers: map[string]string{"X-Amz-Sdk-Checksum-Algorithm": "SHA256"}, status: http.StatusBadRequest, code: "InvalidRequest"},
		{name: "missing tag set", method: http.MethodPut, body: `<Tagging/>`, headers: map[string]string{"Content-MD5": md5Digest(`<Tagging/>`)}, status: http.StatusBadRequest, code: "MalformedXML"},
		// S3 answers PutBucketTagging with 204
		{name: "put", method: http.MethodPut, body: valid, headers: map[string]string{"Content-MD5": md5Digest(valid)}, status: http.StatusNoContent},
		{name: "unsupported method", method: http.MethodPost, status: http.StatusMethodNotAllowed, code: "MethodNotAllowed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tc.method, server.URL+"/bucket?tagging", strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			for name, value := range tc.headers {
				req.Header.Set(name, value)
			}
			signRequest(t, req, tc.body, testutil.AccessKeyID, testutil.SecretAccessKey)

			resp, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			} else if resp.StatusCode != tc.status {
				t.Fatalf("expected status %d, got %d: %s", tc.status, resp.StatusCode, body)
			} else if tc.code != "" && !strings.Contains(string(body), "<Code>"+tc.code+"</Code>") {
				t.Fatalf("expected code %s, got %s", tc.code, body)
			}
		})
	}
}

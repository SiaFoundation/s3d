package s3_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"lukechampine.com/frand"
)

// withHeaders sets raw request headers the SDK does not model.
func withHeaders(headers map[string]string) func(*service.Options) {
	return func(o *service.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Build.Add(middleware.BuildMiddlewareFunc("testSetHeaders",
				func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
					if req, ok := in.Request.(*smithyhttp.Request); ok {
						for k, v := range headers {
							req.Header.Set(k, v)
						}
					}
					return next.HandleBuild(ctx, in)
				}), middleware.After)
		})
	}
}

// captureResponseHeaders records the raw response headers of a call.
func captureResponseHeaders(into *http.Header) func(*service.Options) {
	return func(o *service.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("testCaptureHeaders",
				func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
					out, md, err := next.HandleDeserialize(ctx, in)
					if resp, ok := out.RawResponse.(*smithyhttp.Response); ok {
						*into = resp.Header.Clone()
					}
					return out, md, err
				}), middleware.After)
		})
	}
}

func aes256Rules() []types.ServerSideEncryptionRule {
	return []types.ServerSideEncryptionRule{{
		ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
			SSEAlgorithm: types.ServerSideEncryptionAes256,
		},
	}}
}

func TestBucketEncryptionConfiguration(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// no configuration yet
	_, err := s3Tester.GetBucketEncryption(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrServerSideEncryptionConfigurationNotFoundError, err)

	// deleting a configuration that does not exist succeeds
	if err := s3Tester.DeleteBucketEncryption(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// store and read back
	if err := s3Tester.PutBucketEncryption(t.Context(), bucket, aes256Rules()); err != nil {
		t.Fatal(err)
	}
	res, err := s3Tester.GetBucketEncryption(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	} else if rules := res.ServerSideEncryptionConfiguration.Rules; len(rules) != 1 {
		t.Fatal("unexpected", len(rules))
	} else if got := rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm; got != types.ServerSideEncryptionAes256 {
		t.Fatal("unexpected", got)
	}

	// delete
	if err := s3Tester.DeleteBucketEncryption(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	_, err = s3Tester.GetBucketEncryption(t.Context(), bucket)
	testutil.AssertS3Error(t, s3errs.ErrServerSideEncryptionConfigurationNotFoundError, err)

	// unknown bucket
	err = s3Tester.PutBucketEncryption(t.Context(), "nonexistent", aes256Rules())
	testutil.AssertS3Error(t, s3errs.ErrNoSuchBucket, err)

	// kms is rejected rather than stored as something it is not
	err = s3Tester.PutBucketEncryption(t.Context(), bucket, []types.ServerSideEncryptionRule{{
		ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
			SSEAlgorithm: types.ServerSideEncryptionAwsKms,
		},
	}})
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, err)

	// a rule with no default algorithm carries no information
	err = s3Tester.PutBucketEncryption(t.Context(), bucket, []types.ServerSideEncryptionRule{{}})
	testutil.AssertS3Error(t, s3errs.ErrMalformedXML, err)
}

func TestObjectServerSideEncryption(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	c := s3Tester.Client()
	data := frand.Bytes(100)

	// an explicit request is satisfied and reported back
	put, err := c.PutObject(t.Context(), &service.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String("requested"),
		Body:                 bytes.NewReader(data),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	})
	if err != nil {
		t.Fatal(err)
	} else if put.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatal("unexpected", put.ServerSideEncryption)
	}

	get, err := c.GetObject(t.Context(), &service.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("requested"),
	})
	if err != nil {
		t.Fatal(err)
	}
	get.Body.Close()
	if get.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatal("unexpected", get.ServerSideEncryption)
	}

	// an object written without encryption headers is reported the same way
	if _, err := c.PutObject(t.Context(), &service.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("unrequested"),
		Body:   bytes.NewReader(data),
	}); err != nil {
		t.Fatal(err)
	}
	head, err := c.HeadObject(t.Context(), &service.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("unrequested"),
	})
	if err != nil {
		t.Fatal(err)
	} else if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatal("unexpected", head.ServerSideEncryption)
	}

	// user metadata is undisturbed
	if _, err := c.PutObject(t.Context(), &service.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String("meta"),
		Body:                 bytes.NewReader(data),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
		Metadata:             map[string]string{"keep": "kept"},
	}); err != nil {
		t.Fatal(err)
	}
	obj, err := s3Tester.HeadObject(t.Context(), bucket, "meta", nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.Metadata["keep"] != "kept" {
		t.Fatal("mismatch", obj.Metadata)
	}
}

func TestObjectServerSideEncryptionWriteRejections(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	c := s3Tester.Client()

	put := func(headers map[string]string) error {
		t.Helper()
		_, err := c.PutObject(t.Context(), &service.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("rejected"),
			Body:   bytes.NewReader([]byte("data")),
		}, withHeaders(headers))
		return err
	}

	// two kinds of encryption at once is malformed
	testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, put(map[string]string{
		"x-amz-server-side-encryption":                    "AES256",
		"x-amz-server-side-encryption-customer-algorithm": "AES256",
	}))

	// customer provided keys
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, put(testutil.SSECustomerHeaders))
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, put(map[string]string{
		"x-amz-copy-source-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
	}))

	// kms
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, put(map[string]string{
		"x-amz-server-side-encryption": "aws:kms",
	}))
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, put(map[string]string{
		"x-amz-server-side-encryption-aws-kms-key-id": "abc",
	}))

	// unknown algorithm
	testutil.AssertS3Error(t, s3errs.ErrInvalidEncryptionAlgorithmError, put(map[string]string{
		"x-amz-server-side-encryption": "AES128",
	}))

	// bucket keys
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, put(map[string]string{
		"x-amz-server-side-encryption":                    "AES256",
		"x-amz-server-side-encryption-bucket-key-enabled": "true",
	}))
	testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, put(map[string]string{
		"x-amz-server-side-encryption-bucket-key-enabled": "yes-please",
	}))

	// s3d never uses a bucket key, so disabling one is satisfied
	if err := put(map[string]string{
		"x-amz-server-side-encryption":                    "AES256",
		"x-amz-server-side-encryption-bucket-key-enabled": "false",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestObjectServerSideEncryptionReadRejections(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	c := s3Tester.Client()

	if _, err := c.PutObject(t.Context(), &service.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("readable"),
		Body:   bytes.NewReader([]byte("data")),
	}); err != nil {
		t.Fatal(err)
	}

	// the algorithm header is not valid on a read. a HEAD response has no body
	// to carry the error code
	_, err := c.HeadObject(t.Context(), &service.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("readable"),
	}, withHeaders(map[string]string{"x-amz-server-side-encryption": "AES256"}))
	testutil.AssertS3StatusCode(t, s3errs.ErrInvalidArgument, err)

	_, err = c.GetObject(t.Context(), &service.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("readable"),
	}, withHeaders(map[string]string{"x-amz-server-side-encryption": "AES256"}))
	testutil.AssertS3Error(t, s3errs.ErrInvalidArgument, err)

	// customer provided keys
	_, err = c.GetObject(t.Context(), &service.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("readable"),
	}, withHeaders(testutil.SSECustomerHeaders))
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, err)
}

func TestMultipartServerSideEncryptionRejections(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	c := s3Tester.Client()

	// create
	_, err := c.CreateMultipartUpload(t.Context(), &service.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("mpu"),
	}, withHeaders(testutil.SSECustomerHeaders))
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, err)

	// upload part
	mpu, err := c.CreateMultipartUpload(t.Context(), &service.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("parts"),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.UploadPart(t.Context(), &service.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("parts"),
		UploadId:   mpu.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(frand.Bytes(16)),
	}, withHeaders(testutil.SSECustomerHeaders))
	testutil.AssertS3Error(t, s3errs.ErrNotImplemented, err)
}

func TestLegacyEncryptionMetadataNotServed(t *testing.T) {
	s3Tester := testutil.NewTester(t)
	bucket := "foo"
	if err := s3Tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	c := s3Tester.Client()

	// seed the metadata an older s3d stored, which was every x-amz- request
	// header rather than only the object's own
	legacy := map[string]string{
		"X-Amz-Server-Side-Encryption":                    "aws:kms",
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   "DWygnHRtgiJ77HCm+1rvHw==",
		"X-Amz-Content-Sha256":                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		"X-Amz-Date":                                      "20260908T000000Z",
		"X-Amz-Copy-Source":                               "otherbucket/otherkey",
		"X-Amz-Meta-Keep":                                 "kept",
	}
	if err := s3Tester.AddObject(bucket, "legacy", frand.Bytes(32), legacy); err != nil {
		t.Fatal(err)
	}

	assertServed := func(headers http.Header) {
		t.Helper()
		for name := range legacy {
			if name == "X-Amz-Meta-Keep" || name == "X-Amz-Server-Side-Encryption" {
				continue
			}
			if got := headers.Get(name); got != "" {
				t.Fatal("served", name, got)
			}
		}
		// the algorithm is reported as what s3d does, not the stored aws:kms
		if got := headers.Get("x-amz-server-side-encryption"); got != "AES256" {
			t.Fatal("unexpected", got)
		} else if got := headers.Get("x-amz-meta-keep"); got != "kept" {
			t.Fatal("mismatch", got)
		}
	}

	// head
	var headHeaders http.Header
	if _, err := c.HeadObject(t.Context(), &service.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("legacy"),
	}, captureResponseHeaders(&headHeaders)); err != nil {
		t.Fatal(err)
	}
	assertServed(headHeaders)

	// get
	var getHeaders http.Header
	res, err := c.GetObject(t.Context(), &service.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("legacy"),
	}, captureResponseHeaders(&getHeaders))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	assertServed(getHeaders)
}

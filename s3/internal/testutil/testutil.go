package testutil

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/testutils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap/zaptest"
)

type S3Tester struct {
	client *service.Client
}

func (t *S3Tester) CreateBucket(ctx context.Context, bucket string) error {
	_, err := t.client.CreateBucket(ctx, &service.CreateBucketInput{
		Bucket:                    aws.String(bucket),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{},
	})
	return err
}

func (t *S3Tester) ListBuckets(ctx context.Context) ([]types.Bucket, error) {
	resp, err := t.client.ListBuckets(ctx, &service.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	return resp.Buckets, err
}

func NewTester(t testing.TB, optFns ...func(*service.Options)) *S3Tester {
	t.Helper()

	const (
		accessKeyID     = "AKIA7GQ3XN52WQLYDHZP"
		secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)

	backend := testutils.NewMemoryBackend()
	if err := backend.AddAccessKey(t.Context(), accessKeyID, secretAccessKey); err != nil {
		t.Fatal(err)
	}

	handler := s3.New(backend,
		s3.WithHostBucketBases([]string{"localhost"}),
		s3.WithLogger(zaptest.NewLogger(t)))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		http.Serve(listener, handler)
	}()

	listenerAddr := listener.Addr().String()
	_, port, _ := net.SplitHostPort(listenerAddr)

	cfg, err := config.LoadDefaultConfig(t.Context())
	if err != nil {
		t.Fatalf("unable to load SDK config, %v", err)
	}

	opts := []func(*service.Options){
		func(o *service.Options) {
			o.Region = "us-east-1"
			o.BaseEndpoint = aws.String(fmt.Sprintf("http://localhost:%s", port))
			o.UsePathStyle = true
			o.Credentials = aws.NewCredentialsCache(&credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     accessKeyID,
					SecretAccessKey: secretAccessKey,
				},
			})
		},
	}
	opts = append(opts, optFns...)

	return &S3Tester{
		client: service.NewFromConfig(cfg, opts...),
	}
}

// AssertS3Error is a helper to check an error returned from the AWS SDK against
// an expected s3.S3Error. Unfortunately the SDK doesn't expose its internal
// error type so reflection is not an option and we need to extract the status
// code from the string.
func AssertS3Error(t testing.TB, expected s3errs.Error, got error) {
	t.Helper()
	if got == nil {
		t.Fatal("expected error, got nil")
	}

	// check status code
	re := regexp.MustCompile(`StatusCode: (\d{3})`)
	matches := re.FindStringSubmatch(got.Error())
	if len(matches) != 2 {
		t.Fatalf("expected error to contain status code, got: %v", got)
	}
	var code int
	fmt.Sscanf(matches[1], "%d", &code)
	if code != expected.HTTPStatus {
		t.Fatalf("expected status code %d, got %d", expected.HTTPStatus, code)
	}

	// check error
	if !strings.Contains(got.Error(), expected.Code) {
		t.Fatalf("expected error code %q, got %q", expected.Code, got.Error())
	}
}

package auth_test

import (
	"testing"

	"github.com/SiaFoundation/s3d/s3/internal/testutil"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestAnonymousCredentials(t *testing.T) {
	s3Tester := testutil.NewTester(t, func(o *service.Options) {
		o.Credentials = aws.AnonymousCredentials{}
	})

	// attempt to create a bucket, should fail with [ErrAccessDenied]
	err := s3Tester.CreateBucket(t.Context(), "bucket")
	testutil.AssertS3Error(t, s3errs.ErrAccessDenied, err)
}

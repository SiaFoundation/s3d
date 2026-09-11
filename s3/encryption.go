package s3

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// Server side encryption algorithms.
const (
	// SSEAlgorithmAES256 is the only algorithm s3d reports.
	SSEAlgorithmAES256 = "AES256"
	// SSEAlgorithmKMS names the KMS backed algorithm, which s3d rejects.
	SSEAlgorithmKMS = "aws:kms"
)

// Server side encryption headers.
const (
	headerSSE = "X-Amz-Server-Side-Encryption"

	headerSSEKMSKeyID         = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	headerSSEKMSContext       = "X-Amz-Server-Side-Encryption-Context"
	headerSSEBucketKeyEnabled = "X-Amz-Server-Side-Encryption-Bucket-Key-Enabled"

	headerSSECAlgorithm = "X-Amz-Server-Side-Encryption-Customer-Algorithm"
	headerSSECKey       = "X-Amz-Server-Side-Encryption-Customer-Key"
	headerSSECKeyMD5    = "X-Amz-Server-Side-Encryption-Customer-Key-Md5"

	headerCopySSECAlgorithm = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"
	headerCopySSECKey       = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"
	headerCopySSECKeyMD5    = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"
)

// sseCustomerHeaders are the customer provided key headers, for both the
// request target and a copy source.
var sseCustomerHeaders = []string{
	headerSSECAlgorithm,
	headerSSECKey,
	headerSSECKeyMD5,
	headerCopySSECAlgorithm,
	headerCopySSECKey,
	headerCopySSECKeyMD5,
}

// hasAnySSECustomerHeader reports whether h carries any customer provided
// key header.
func hasAnySSECustomerHeader(h http.Header) bool {
	for _, name := range sseCustomerHeaders {
		if _, ok := h[name]; ok {
			return true
		}
	}
	return false
}

// validateSSEWriteHeaders checks the encryption headers on a request that
// writes an object. AES256 is accepted. Customer provided keys, KMS and bucket
// keys are refused.
func validateSSEWriteHeaders(h http.Header) error {
	algorithm := h.Get(headerSSE)

	if hasAnySSECustomerHeader(h) {
		// a malformed request takes precedence over an unsupported one
		if algorithm != "" {
			return fmt.Errorf("cannot combine %q with customer provided keys: %w", algorithm, s3errs.ErrInvalidArgument)
		}
		return fmt.Errorf("customer provided encryption keys are not supported: %w", s3errs.ErrNotImplemented)
	}

	switch algorithm {
	case "", SSEAlgorithmAES256:
	case SSEAlgorithmKMS:
		return fmt.Errorf("KMS managed encryption is not supported: %w", s3errs.ErrNotImplemented)
	default:
		return fmt.Errorf("unsupported encryption algorithm %q: %w", algorithm, s3errs.ErrInvalidEncryptionAlgorithmError)
	}

	// KMS only, and meaningless without a key management service
	_, hasKMSKeyID := h[headerSSEKMSKeyID]
	_, hasKMSContext := h[headerSSEKMSContext]
	if hasKMSKeyID || hasKMSContext {
		return fmt.Errorf("KMS managed encryption is not supported: %w", s3errs.ErrNotImplemented)
	}

	// only a request to enable a bucket key has to be refused
	if _, ok := h[headerSSEBucketKeyEnabled]; ok {
		v := h.Get(headerSSEBucketKeyEnabled)
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid %s value %q: %w", headerSSEBucketKeyEnabled, v, s3errs.ErrInvalidArgument)
		} else if enabled {
			return fmt.Errorf("bucket keys are not supported: %w", s3errs.ErrNotImplemented)
		}
	}
	return nil
}

// validateSSEReadHeaders checks the encryption headers on a request that reads
// an object. The algorithm header is not valid on a read, and customer
// provided keys are refused.
func validateSSEReadHeaders(h http.Header) error {
	if _, ok := h[headerSSE]; ok {
		return fmt.Errorf("%q is not valid on a read: %w", headerSSE, s3errs.ErrInvalidArgument)
	} else if hasAnySSECustomerHeader(h) {
		return fmt.Errorf("customer provided encryption keys are not supported: %w", s3errs.ErrNotImplemented)
	}
	return nil
}

// setSSEResponseHeader reports the encryption applied to an object. The answer
// does not depend on the request or on the bucket configuration.
func setSSEResponseHeader(w http.ResponseWriter) {
	w.Header().Set(headerSSE, SSEAlgorithmAES256)
}

// Validate checks that the configuration only asks for encryption s3d can
// report.
func (c ServerSideEncryptionConfiguration) Validate() error {
	if len(c.Rules) == 0 {
		return fmt.Errorf("configuration has no rules: %w", s3errs.ErrMalformedXML)
	}
	for _, rule := range c.Rules {
		if rule.BucketKeyEnabled != nil && *rule.BucketKeyEnabled {
			return fmt.Errorf("bucket keys are not supported: %w", s3errs.ErrNotImplemented)
		}
		apply := rule.ApplyServerSideEncryptionByDefault
		if apply == nil {
			return fmt.Errorf("rule has no default encryption: %w", s3errs.ErrMalformedXML)
		}
		switch apply.SSEAlgorithm {
		case SSEAlgorithmAES256:
			if apply.KMSMasterKeyID != nil {
				return fmt.Errorf("a KMS key may not be set for %s: %w", SSEAlgorithmAES256, s3errs.ErrInvalidArgument)
			}
		case SSEAlgorithmKMS:
			return fmt.Errorf("KMS managed encryption is not supported: %w", s3errs.ErrNotImplemented)
		default:
			return fmt.Errorf("unsupported encryption algorithm %q: %w", apply.SSEAlgorithm, s3errs.ErrInvalidEncryptionAlgorithmError)
		}
	}
	return nil
}

// routeBucketEncryption dispatches the ?encryption bucket subresource.
func (s *s3) routeBucketEncryption(w http.ResponseWriter, r *http.Request, accessKeyID *string, bucket string) error {
	validatedKey, err := assertAuth(accessKeyID)
	if err != nil {
		return err
	}
	switch r.Method {
	case http.MethodPut:
		return s.putBucketEncryption(w, r, validatedKey, bucket)
	case http.MethodGet:
		return s.getBucketEncryption(w, r, validatedKey, bucket)
	case http.MethodDelete:
		return s.deleteBucketEncryption(w, r, validatedKey, bucket)
	default:
		return s3errs.ErrMethodNotAllowed
	}
}

// putBucketEncryption handles PUT Bucket encryption requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
func (s *s3) putBucketEncryption(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("putting bucket encryption configuration", zap.String("bucket", bucket))

	var config ServerSideEncryptionConfiguration
	if err := decodeXMLBody(r.Body, &config); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	return s.backend.PutBucketEncryptionConfiguration(r.Context(), accessKeyID, bucket, config)
}

// getBucketEncryption handles GET Bucket encryption requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
func (s *s3) getBucketEncryption(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("getting bucket encryption configuration", zap.String("bucket", bucket))

	config, err := s.backend.GetBucketEncryptionConfiguration(r.Context(), accessKeyID, bucket)
	if err != nil {
		return err
	}
	return writeXMLResponse(w, http.StatusOK, ServerSideEncryptionConfiguration{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Rules: config.Rules,
	})
}

// deleteBucketEncryption handles DELETE Bucket encryption requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html
func (s *s3) deleteBucketEncryption(w http.ResponseWriter, r *http.Request, accessKeyID, bucket string) error {
	s.logger.Debug("deleting bucket encryption configuration", zap.String("bucket", bucket))

	if err := s.backend.DeleteBucketEncryptionConfiguration(r.Context(), accessKeyID, bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

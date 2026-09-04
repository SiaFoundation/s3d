package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestHandleAuthV4Presigned(t *testing.T) {
	// the presigned URL of the AWS SigV4 query string authentication example
	const presignedURL = "https://" + exampleHost + "/test.txt?" +
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&" +
		"X-Amz-Credential=" + exampleAccessKey + "%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&" +
		"X-Amz-Date=20130524T000000Z&" +
		"X-Amz-Expires=86400&" +
		"X-Amz-SignedHeaders=host&" +
		"X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	store := mockKeyStore{exampleAccessKey: SecretAccessKey(exampleSecret)}

	req := httptest.NewRequest(http.MethodGet, presignedURL, nil)
	accessKeyID, err := HandleAuth(req, store, exampleRegion, exampleTime.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	} else if accessKeyID == nil {
		t.Fatal("expected access key ID, got anonymous")
	} else if *accessKeyID != exampleAccessKey {
		t.Fatalf("expected access key ID %q, got %q", exampleAccessKey, *accessKeyID)
	}

	// setQuery returns a mutation that sets query parameter key to value, or
	// removes it if value is nil
	setQuery := func(key string, value *string) func(*http.Request) {
		return func(req *http.Request) {
			q := req.URL.Query()
			if value == nil {
				q.Del(key)
			} else {
				q.Set(key, *value)
			}
			req.URL.RawQuery = q.Encode()
		}
	}
	str := func(s string) *string { return &s }

	tests := []struct {
		name   string
		now    time.Time
		mutate func(*http.Request)
		want   error
	}{
		{
			name: "expired",
			now:  exampleTime.Add(24*time.Hour + time.Second),
			want: s3errs.ErrAccessDeniedExpired,
		},
		{
			name: "not valid yet",
			now:  exampleTime.Add(-6 * time.Minute),
			want: s3errs.ErrRequestTimeTooSkewed,
		},
		{
			name:   "tampered signature",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZSignature, str(strings.Repeat("0", 64))),
			want:   s3errs.ErrSignatureDoesNotMatch,
		},
		{
			name:   "expiry exceeds seven days",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZExpires, str("604801")),
			want:   s3errs.ErrAuthorizationQueryParametersError,
		},
		{
			name:   "missing parameter",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZCredential, nil),
			want:   s3errs.ErrAuthorizationQueryParametersError,
		},
		{
			name: "unparseable query string",
			now:  exampleTime,
			mutate: func(req *http.Request) {
				req.URL.RawQuery += "&%zz"
			},
			want: s3errs.ErrInvalidURI,
		},
		{
			name:   "sigv4a algorithm",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZAlgorithm, str(AuthorizationAWS4ECDSAP256SHA256)),
			want:   s3errs.ErrNotImplemented,
		},
		{
			name:   "unsupported algorithm",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZAlgorithm, str("AWS4-HMAC-SHA1")),
			want:   s3errs.ErrAuthorizationQueryParametersError,
		},
		{
			name:   "signed headers without host",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZSignedHeaders, str("x-amz-date")),
			want:   s3errs.ErrAuthorizationQueryParametersError,
		},
		{
			name:   "unknown signed header",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZSignedHeaders, str("host;x-not-sent")),
			want:   s3errs.ErrAuthorizationQueryParametersError,
		},
		{
			name:   "wrong region",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZCredential, str(exampleAccessKey+"/20130524/eu-west-1/s3/aws4_request")),
			want:   s3errs.ErrAuthorizationQueryParametersError,
		},
		{
			name:   "unknown access key",
			now:    exampleTime,
			mutate: setQuery(QueryXAMZCredential, str("AKIANOSUCHKEYEXAMPLE/"+exampleScope)),
			want:   s3errs.ErrInvalidAccessKeyId,
		},
		{
			name: "duplicate parameter",
			now:  exampleTime,
			mutate: func(req *http.Request) {
				req.URL.RawQuery += "&" + QueryXAMZExpires + "=604800"
			},
			want: s3errs.ErrAuthorizationQueryParametersError,
		},
		{
			name: "added parameter",
			now:  exampleTime,
			mutate: func(req *http.Request) {
				req.URL.RawQuery += "&versionId=1"
			},
			want: s3errs.ErrSignatureDoesNotMatch,
		},
		{
			name: "substituted method",
			now:  exampleTime,
			mutate: func(req *http.Request) {
				req.Method = http.MethodDelete
			},
			want: s3errs.ErrSignatureDoesNotMatch,
		},
		{
			name: "unsigned x-amz header",
			now:  exampleTime,
			mutate: func(req *http.Request) {
				req.Header.Set("X-Amz-Copy-Source", "bucket/secret.txt")
			},
			want: s3errs.ErrAccessDeniedUnsignedHeaders,
		},
		{
			// the Authorization header is well-formed, so only the combination
			// of the two mechanisms can be what is rejected
			name: "both auth mechanisms",
			now:  exampleTime,
			mutate: func(req *http.Request) {
				req.Header.Set(HeaderAuthorization, AuthorizationAWS4HMACSHA256+
					" Credential="+exampleAccessKey+"/"+exampleScope+
					",SignedHeaders=host,Signature="+strings.Repeat("0", 64))
			},
			want: s3errs.ErrInvalidArgumentMultipleAuth,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, presignedURL, nil)
			if tc.mutate != nil {
				tc.mutate(req)
			}
			if _, err := HandleAuth(req, store, exampleRegion, tc.now); !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

// TestHandleAuthV4PresignedPayload checks the ways a presigned request can
// declare its payload hash.
func TestHandleAuthV4PresignedPayload(t *testing.T) {
	store := mockKeyStore{exampleAccessKey: SecretAccessKey(exampleSecret)}
	now := exampleTime.Add(time.Minute)

	payload := []byte("hello world")
	payloadSum := sha256.Sum256(payload)
	payloadHex := hex.EncodeToString(payloadSum[:])

	// presign builds a presigned PUT with the given extra query parameters and
	// signed headers
	presign := func(extraQuery url.Values, headers http.Header, payloadHash string, body []byte) *http.Request {
		// the canonical headers are lowercased when signing
		signed := http.Header{"host": {exampleHost}}
		maps.Copy(signed, headers)

		query := url.Values{}
		maps.Copy(query, extraQuery)
		query.Set(QueryXAMZAlgorithm, AuthorizationAWS4HMACSHA256)
		query.Set(QueryXAMZCredential, exampleAccessKey+"/"+exampleScope)
		query.Set(QueryXAMZDate, exampleTime.Format(layoutISO8601))
		query.Set(QueryXAMZExpires, "3600")
		query.Set(QueryXAMZSignedHeaders, canonicalSignedHeaders(signed))

		rawQuery := canonicalQueryString(query)
		canonical := canonicalRequest(signed, payloadHash, rawQuery, "/test.txt", http.MethodPut)
		toSign := canonicalStringToSign(canonical, exampleTime, exampleScope)
		signature := getSignature(signingKey(SecretAccessKey(exampleSecret), exampleTime, exampleRegion), toSign)

		rawURL := "https://" + exampleHost + "/test.txt?" + rawQuery + "&" + QueryXAMZSignature + "=" + signature
		req := httptest.NewRequest(http.MethodPut, rawURL, bytes.NewReader(body))
		for name, values := range headers {
			req.Header[http.CanonicalHeaderKey(name)] = values
		}
		return req
	}

	// authenticate returns the payload hash the handlers would verify the body
	// against
	authenticate := func(t *testing.T, req *http.Request) *[32]byte {
		t.Helper()
		accessKeyID, err := HandleAuth(req, store, exampleRegion, now)
		if err != nil {
			t.Fatal(err)
		} else if accessKeyID == nil || *accessKeyID != exampleAccessKey {
			t.Fatal("access key ID mismatch")
		}
		hash, err := Sha256HashFromRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}

	t.Run("hash in signed header", func(t *testing.T) {
		headers := http.Header{HeaderXAMZContentSHA256: {payloadHex}}
		req := presign(nil, headers, payloadHex, payload)
		if hash := authenticate(t, req); hash == nil || *hash != payloadSum {
			t.Fatalf("expected payload hash %x, got %v", payloadSum, hash)
		}
	})

	t.Run("hash in query string", func(t *testing.T) {
		query := url.Values{QueryXAMZContentSHA256: {payloadHex}}
		req := presign(query, nil, payloadHex, payload)
		if hash := authenticate(t, req); hash == nil || *hash != payloadSum {
			t.Fatalf("expected payload hash %x, got %v", payloadSum, hash)
		}
	})

	t.Run("unsigned header ignored", func(t *testing.T) {
		req := presign(nil, nil, ContentUnsignedPayload, payload)
		req.Header.Set(HeaderXAMZContentSHA256, strings.Repeat("ab", 32))
		if hash := authenticate(t, req); hash != nil {
			t.Fatalf("expected no payload hash, got %v", hash)
		}
	})

	t.Run("streaming unsigned trailer", func(t *testing.T) {
		var chunked bytes.Buffer
		fmt.Fprintf(&chunked, "%x\r\n", len(payload))
		chunked.Write(payload)
		chunked.WriteString("\r\n0\r\n")
		sum := base64.StdEncoding.EncodeToString(payloadSum[:])
		chunked.WriteString("x-amz-checksum-sha256:" + sum + "\r\n\r\n")

		query := url.Values{QueryXAMZContentSHA256: {ContentStreamingUnsignedPayloadTrailer}}
		headers := http.Header{
			HeaderXAMZDecodedContentLength: {strconv.Itoa(len(payload))},
			HeaderXAMZTrailer:              {"x-amz-checksum-sha256"},
		}
		req := presign(query, headers, ContentStreamingUnsignedPayloadTrailer, chunked.Bytes())
		if hash := authenticate(t, req); hash != nil {
			t.Fatalf("expected no payload hash, got %v", hash)
		}
		// the aws-chunked framing must have been stripped from the body
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(body, payload) {
			t.Fatalf("expected body %q, got %q", payload, body)
		} else if req.ContentLength != int64(len(payload)) {
			t.Fatalf("expected content length %d, got %d", len(payload), req.ContentLength)
		}
	})
}

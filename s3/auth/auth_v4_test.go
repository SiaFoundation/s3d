package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"lukechampine.com/frand"
)

type mockKeyStore map[string]SecretAccessKey

func (s mockKeyStore) LoadSecret(_ context.Context, id string) (SecretAccessKey, error) {
	v, ok := s[id]
	if !ok {
		return nil, s3errs.ErrInvalidAccessKeyId
	}
	return slices.Clone(v), nil
}

// The following constants are taken from the AWS SigV4 documentation examples.
const (
	exampleAccessKey = "AKIAIOSFODNN7EXAMPLE"
	exampleSecret    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	exampleHost      = "examplebucket.s3.amazonaws.com"
	exampleRegion    = "us-east-1"
	exampleScope     = "20130524/us-east-1/s3/aws4_request"
)

var exampleTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

// exampleHeaders returns the headers a signed example request carries.
func exampleHeaders() http.Header {
	return http.Header{
		"host":                 {exampleHost},
		"x-amz-date":           {exampleTime.Format(layoutISO8601)},
		"x-amz-content-sha256": {ContentUnsignedPayload},
	}
}

// signedRequest builds an example request signed with the Authorization header.
// scope and signedHeaderList are used verbatim so tests can send malformed
// values.
func signedRequest(method, region, scope, signedHeaderList string, signed http.Header) *http.Request {
	req := httptest.NewRequest(method, "https://"+exampleHost+"/test.txt", nil)
	for name, values := range signed {
		if !strings.EqualFold(name, "host") { // Go serves "host" out of Request.Host
			req.Header[http.CanonicalHeaderKey(name)] = values
		}
	}
	canonical := canonicalRequest(signed, ContentUnsignedPayload, "", "/test.txt", method)
	toSign := canonicalStringToSign(canonical, exampleTime, scope)
	signature := getSignature(signingKey(SecretAccessKey(exampleSecret), exampleTime, region), toSign)
	req.Header.Set(HeaderAuthorization, AuthorizationAWS4HMACSHA256+
		" Credential="+exampleAccessKey+"/"+scope+
		",SignedHeaders="+signedHeaderList+
		",Signature="+signature)
	return req
}

func TestParseAuthHeader(t *testing.T) {
	// Example AWSv4 Authorization header
	// "Authorization: AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=d609f580a2aba01cc8cc2a0e62fb695748c2733b1cf3df64a623d74dfc4e3a39"
	header := make(http.Header)
	header.Set(HeaderAuthorization, "AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/20251017/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=f66373650f043e2074da14a5439516bdb2fb4cd209d9376ae4c8df139f944100")

	parsed, err := parseAuthHeader(header)
	if err != nil {
		t.Fatal(err)
	}

	date, _ := time.Parse("20060102", "20251017")
	expected := parsedAuthHeader{
		Credential: credentialHeader{
			AccessKeyID: "AKIA7GQ3XN52WQLYDHZP",
			Scope: signScope{
				Date:    date,
				Region:  "us-east-1",
				Service: "s3",
				Request: "aws4_request",
			},
		},
		SignedHeaders: []string{
			"accept-encoding",
			"amz-sdk-invocation-id",
			"amz-sdk-request",
			"content-length",
			"content-type",
			"host",
			"x-amz-content-sha256",
			"x-amz-date",
		},
		Signature: "f66373650f043e2074da14a5439516bdb2fb4cd209d9376ae4c8df139f944100",
	}

	if !reflect.DeepEqual(*parsed, expected) {
		t.Fatalf("parsed auth header does not match expected\nexpected: %+v\nparsed: %+v", expected, *parsed)
	}
}

func TestDateValidation(t *testing.T) {
	header := make(http.Header)
	now := time.Now().UTC()
	header.Set(HeaderAuthorization, fmt.Sprintf("AWS4-HMAC-SHA256 Credential=AKIA7GQ3XN52WQLYDHZP/%s/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=f66373650f043e2074da14a5439516bdb2fb4cd209d9376ae4c8df139f944100", now.Format(yyyymmdd)))
	req := &http.Request{Header: header}
	store := mockKeyStore{}

	// Case 1: date not set
	_, err := verifyV4SignedRequest(req, nil, store, "", now)
	if !errors.Is(err, s3errs.ErrMissingAuthenticationToken) {
		t.Fatalf("expected ErrMissingAuthenticationToken, got %v", err)
	}

	// Case 2: credential date is in the past
	header.Set(HeaderXAMZDate, now.Add(-24*time.Hour).Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, nil, store, "", now)
	if !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 3: credential date is in the future
	header.Set(HeaderXAMZDate, now.Add(24*time.Hour).Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, nil, store, "", now)
	if !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 4: date is skewed too far in the past
	header.Set(HeaderXAMZDate, now.Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, nil, store, "", now.Add(6*time.Minute))
	if !errors.Is(err, s3errs.ErrRequestTimeTooSkewed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 5: date is skewed too far in the future
	_, err = verifyV4SignedRequest(req, nil, store, "", now.Add(-6*time.Minute))
	if !errors.Is(err, s3errs.ErrRequestTimeTooSkewed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 6: date is valid but we don't have the access key
	_, err = verifyV4SignedRequest(req, nil, store, "", now)
	if !errors.Is(err, s3errs.ErrInvalidAccessKeyId) {
		t.Fatal(err)
	}
}

// TestCanonicalQueryString checks what canonicalQueryString adds on top of
// url.Values.Encode.
func TestCanonicalQueryString(t *testing.T) {
	tests := []struct {
		name  string
		query url.Values
		want  string
	}{
		{
			name:  "sorted by name then value",
			query: url.Values{"b": {"2"}, "a": {"z", "x"}},
			want:  "a=x&a=z&b=2",
		},
		{
			name:  "space is percent encoded",
			query: url.Values{"k": {"a b"}},
			want:  "k=a%20b",
		},
		{
			name:  "plus is escaped",
			query: url.Values{"k": {"a+b"}},
			want:  "k=a%2Bb",
		},
		{
			// AWS sorts after encoding, so '{' sorts before 'a' as "%7B"
			name:  "names sorted after encoding",
			query: url.Values{"{": {"1"}, "a": {"2"}},
			want:  "%7B=1&a=2",
		},
		{
			name:  "values sorted after encoding",
			query: url.Values{"k": {"{", "a"}},
			want:  "k=%7B&k=a",
		},
		{
			// the name is compared before its value, so encoded pairs must not
			// be sorted as whole strings
			name:  "name compared before value",
			query: url.Values{"a b": {"1"}, "a": {"2"}},
			want:  "a=2&a%20b=1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalQueryString(tc.query); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

// TestHandleAuthV4 checks the validation of a request signed with the
// Authorization header and the handling of an anonymous request.
func TestHandleAuthV4(t *testing.T) {
	const headerList = "host;x-amz-content-sha256;x-amz-date"
	store := mockKeyStore{exampleAccessKey: SecretAccessKey(exampleSecret)}

	t.Run("valid", func(t *testing.T) {
		req := signedRequest(http.MethodPut, exampleRegion, exampleScope, headerList, exampleHeaders())
		accessKeyID, err := HandleAuth(req, store, exampleRegion, exampleTime)
		if err != nil {
			t.Fatal(err)
		} else if accessKeyID == nil {
			t.Fatal("expected access key ID, got anonymous")
		} else if *accessKeyID != exampleAccessKey {
			t.Fatalf("expected access key ID %q, got %q", exampleAccessKey, *accessKeyID)
		}
	})

	t.Run("mixed-case signed headers", func(t *testing.T) {
		req := signedRequest(http.MethodPut, exampleRegion, exampleScope, "host;X-Amz-Content-Sha256;X-Amz-Date", exampleHeaders())
		if _, err := HandleAuth(req, store, exampleRegion, exampleTime); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("malformed credential scope", func(t *testing.T) {
		for _, scope := range []string{
			"20130524/us-east-1/not-s3/aws4_request",
			"20130524/us-east-1/s3/not-aws4_request",
			"20130524//s3/aws4_request",
		} {
			req := signedRequest(http.MethodPut, exampleRegion, scope, headerList, exampleHeaders())
			if _, err := HandleAuth(req, store, exampleRegion, exampleTime); !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
				t.Fatalf("%s: expected ErrAuthorizationHeaderMalformed, got %v", scope, err)
			}
		}
	})

	t.Run("region", func(t *testing.T) {
		const scope = "20130524/eu-west-1/s3/aws4_request"
		// an empty region allows any region
		req := signedRequest(http.MethodPut, "eu-west-1", scope, headerList, exampleHeaders())
		if _, err := HandleAuth(req, store, "", exampleTime); err != nil {
			t.Fatal("any region:", err)
		}
		req = signedRequest(http.MethodPut, "eu-west-1", scope, headerList, exampleHeaders())
		if _, err := HandleAuth(req, store, exampleRegion, exampleTime); !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
			t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
		}
	})

	t.Run("unsigned x-amz header", func(t *testing.T) {
		req := signedRequest(http.MethodPut, exampleRegion, exampleScope, headerList, exampleHeaders())
		req.Header.Set("X-Amz-Copy-Source", "bucket/secret.txt")
		if _, err := HandleAuth(req, store, exampleRegion, exampleTime); !errors.Is(err, s3errs.ErrAccessDeniedUnsignedHeaders) {
			t.Fatalf("expected ErrAccessDeniedUnsignedHeaders, got %v", err)
		}
	})

	t.Run("unsigned x-amz-content-sha256 exempt", func(t *testing.T) {
		signed := exampleHeaders()
		delete(signed, "x-amz-content-sha256")
		req := signedRequest(http.MethodPut, exampleRegion, exampleScope, canonicalSignedHeaders(signed), signed)
		req.Header.Set(HeaderXAMZContentSHA256, ContentUnsignedPayload)
		if _, err := HandleAuth(req, store, exampleRegion, exampleTime); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("anonymous", func(t *testing.T) {
		// the query string is normalized and validated for the handlers
		req := httptest.NewRequest(http.MethodGet, "https://host/bucket/key?versionId=v;1", nil)
		accessKeyID, err := HandleAuth(req, store, exampleRegion, exampleTime)
		if err != nil {
			t.Fatal(err)
		} else if accessKeyID != nil {
			t.Fatalf("expected anonymous request, got access key ID %q", *accessKeyID)
		} else if got := req.URL.Query().Get("versionId"); got != "v;1" {
			t.Fatalf("expected versionId %q, got %q", "v;1", got)
		}

		req = httptest.NewRequest(http.MethodGet, "https://host/bucket/key?prefix=%zz", nil)
		if _, err := HandleAuth(req, store, exampleRegion, exampleTime); !errors.Is(err, s3errs.ErrInvalidURI) {
			t.Fatalf("expected ErrInvalidURI, got %v", err)
		}
	})
}

// TestHandleAuthV4PayloadClearsSigningKey checks that handleAuthV4Payload
// clears the signing key even after handing it to the chunk verifier.
func TestHandleAuthV4PayloadClearsSigningKey(t *testing.T) {
	const payloadHash = ContentStreamingAWS4HMACSHA256PayloadTrailer
	req := httptest.NewRequest(http.MethodPut, "https://host/bucket/key", strings.NewReader("0\r\n\r\n"))
	req.Header.Set(HeaderXAMZContentSHA256, payloadHash)
	req.Header.Set(HeaderXAMZDecodedContentLength, "0")
	req.Header.Set(HeaderXAMZTrailer, "x-amz-checksum-sha256")

	key := bytes.Repeat([]byte{0xAB}, sha256.Size)
	result := &v4SignResult{
		AccessKeyID: exampleAccessKey,
		SigningKey:  key,
		Scope:       exampleScope,
		Timestamp:   exampleTime.Format(layoutISO8601),
		SeedSig:     strings.Repeat("0", 64),
	}
	if err := handleAuthV4Payload(req, payloadHash, result); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(key, make([]byte, len(key))) {
		t.Fatalf("expected zeroed signing key, got %x", key)
	}
}

func TestHandleAuthV4Streaming(t *testing.T) {
	skey := bytes.Repeat([]byte{0x42}, 32)
	const (
		scope     = "20260101/us-east-1/s3/aws4_request"
		timestamp = "20260101T000000Z"
		seedSig   = "1111111111111111111111111111111111111111111111111111111111111111"
		chunkSize = 64 * 1024
	)
	payload := bytes.Repeat([]byte("a"), 66560)

	result := &v4SignResult{
		SigningKey: skey,
		Scope:      scope,
		Timestamp:  timestamp,
		SeedSig:    seedSig,
	}

	hmacHex := func(data string) string {
		mac := hmac.New(sha256.New, skey)
		mac.Write([]byte(data))
		return hex.EncodeToString(mac.Sum(nil))
	}
	emptyHash := sha256.Sum256(nil)
	emptyHex := hex.EncodeToString(emptyHash[:])

	chunkSig := func(prevSig string, data []byte) string {
		dataHash := sha256.Sum256(data)
		return hmacHex(strings.Join([]string{
			"AWS4-HMAC-SHA256-PAYLOAD",
			timestamp, scope, prevSig,
			emptyHex,
			hex.EncodeToString(dataHash[:]),
		}, "\n"))
	}

	trailerSig := func(prevSig, canonicalTrailer string) string {
		trailerHash := sha256.Sum256([]byte(canonicalTrailer))
		return hmacHex(strings.Join([]string{
			"AWS4-HMAC-SHA256-TRAILER",
			timestamp, scope, prevSig,
			hex.EncodeToString(trailerHash[:]),
		}, "\n"))
	}

	signedChunks := func() ([]byte, string) {
		var buf bytes.Buffer
		prev := seedSig
		for i := 0; i < len(payload); i += chunkSize {
			end := min(i+chunkSize, len(payload))
			sig := chunkSig(prev, payload[i:end])
			fmt.Fprintf(&buf, "%x;chunk-signature=%s\r\n", end-i, sig)
			buf.Write(payload[i:end])
			buf.WriteString("\r\n")
			prev = sig
		}
		finalSig := chunkSig(prev, nil)
		fmt.Fprintf(&buf, "0;chunk-signature=%s\r\n", finalSig)
		return buf.Bytes(), finalSig
	}

	unsignedChunks := func() []byte {
		var buf bytes.Buffer
		for i := 0; i < len(payload); i += chunkSize {
			end := min(i+chunkSize, len(payload))
			fmt.Fprintf(&buf, "%x\r\n", end-i)
			buf.Write(payload[i:end])
			buf.WriteString("\r\n")
		}
		buf.WriteString("0\r\n")
		return buf.Bytes()
	}

	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	crc.Write(payload)
	crcB64 := base64.StdEncoding.EncodeToString(crc.Sum(nil))
	canonicalCrc := "x-amz-checksum-crc32c:" + crcB64 + "\n"

	sha := sha256.Sum256(payload)
	shaB64 := base64.StdEncoding.EncodeToString(sha[:])
	canonicalCrcSha := canonicalCrc + "x-amz-checksum-sha256:" + shaB64 + "\n"

	chunks, finalSig := signedChunks()
	crcTrailerBlock := fmt.Sprintf("x-amz-checksum-crc32c:%s\r\nx-amz-trailer-signature:%s\r\n\r\n",
		crcB64, trailerSig(finalSig, canonicalCrc))
	multiTrailerBlock := fmt.Sprintf("x-amz-checksum-sha256:%s\r\nx-amz-checksum-crc32c:%s\r\nx-amz-trailer-signature:%s\r\n\r\n",
		shaB64, crcB64, trailerSig(finalSig, canonicalCrcSha))

	tamperedChunkSig := bytes.Replace(chunks,
		[]byte(chunkSig(seedSig, payload[:chunkSize])),
		[]byte(strings.Repeat("0", 64)), 1)
	tamperedTrailer := strings.Replace(crcTrailerBlock,
		trailerSig(finalSig, canonicalCrc),
		strings.Repeat("0", 64), 1)
	tamperedPayload := bytes.Replace(chunks, payload[:8], []byte("AAAAAAAA"), 1)
	truncated := chunks[:len(chunks)-len("0;chunk-signature=")-64-len("\r\n")]

	unsigned := unsignedChunks()
	unsignedTrailer := []byte("x-amz-checksum-sha256:" + shaB64 + "\r\n\r\n")
	unsignedSpurious := []byte("x-amz-checksum-sha256:" + shaB64 + "\r\nx-amz-trailer-signature:" + strings.Repeat("0", 64) + "\r\n\r\n")

	cases := []struct {
		name          string
		contentSha    string
		xAmzTrailer   string
		decodedLength string
		result        *v4SignResult
		body          []byte
		wantSetupErr  error
		wantReadErr   error
		wantBody      []byte
	}{
		{
			name:       "payload",
			contentSha: ContentStreamingAWS4HMACSHA256Payload,
			result:     result,
			body:       slices.Concat(chunks, []byte("\r\n")),
			wantBody:   payload,
		},
		{
			name:        "payload-trailer",
			contentSha:  ContentStreamingAWS4HMACSHA256PayloadTrailer,
			xAmzTrailer: xAmzChecksumCrc32C,
			result:      result,
			body:        slices.Concat(chunks, []byte(crcTrailerBlock)),
			wantBody:    payload,
		},
		{
			name:        "tampered chunk signature",
			contentSha:  ContentStreamingAWS4HMACSHA256Payload,
			result:      result,
			body:        slices.Concat(tamperedChunkSig, []byte("\r\n")),
			wantReadErr: s3errs.ErrInvalidSignature,
		},
		{
			name:        "tampered trailer signature",
			contentSha:  ContentStreamingAWS4HMACSHA256PayloadTrailer,
			xAmzTrailer: xAmzChecksumCrc32C,
			result:      result,
			body:        slices.Concat(chunks, []byte(tamperedTrailer)),
			wantReadErr: s3errs.ErrInvalidSignature,
		},
		{
			name:        "tampered payload byte",
			contentSha:  ContentStreamingAWS4HMACSHA256Payload,
			result:      result,
			body:        slices.Concat(tamperedPayload, []byte("\r\n")),
			wantReadErr: s3errs.ErrInvalidSignature,
		},
		{
			name:        "truncated body",
			contentSha:  ContentStreamingAWS4HMACSHA256Payload,
			result:      result,
			body:        truncated,
			wantReadErr: io.ErrUnexpectedEOF,
		},
		{
			name:        "missing trailer signature",
			contentSha:  ContentStreamingAWS4HMACSHA256PayloadTrailer,
			xAmzTrailer: xAmzChecksumCrc32C,
			result:      result,
			body:        slices.Concat(chunks, []byte("x-amz-checksum-crc32c:"+crcB64+"\r\n\r\n")),
			wantReadErr: s3errs.ErrInvalidSignature,
		},
		{
			name:        "two declared trailers",
			contentSha:  ContentStreamingAWS4HMACSHA256PayloadTrailer,
			xAmzTrailer: xAmzChecksumCrc32C + "," + xAmzChecksumSha256,
			result:      result,
			body:        slices.Concat(chunks, []byte(multiTrailerBlock)),
			wantBody:    payload,
		},
		{
			name:        "unsigned trailer variant",
			contentSha:  ContentStreamingUnsignedPayloadTrailer,
			xAmzTrailer: xAmzChecksumSha256,
			body:        slices.Concat(unsigned, unsignedTrailer),
			wantBody:    payload,
		},
		{
			name:        "spurious trailer signature on unsigned variant",
			contentSha:  ContentStreamingUnsignedPayloadTrailer,
			xAmzTrailer: xAmzChecksumSha256,
			body:        slices.Concat(unsigned, unsignedSpurious),
			wantReadErr: s3errs.ErrInvalidArgument,
		},
		{
			name:          "negative decoded content length",
			contentSha:    ContentStreamingAWS4HMACSHA256Payload,
			decodedLength: "-1",
			result:        result,
			wantSetupErr:  s3errs.ErrInvalidArgument,
		},
	}

	for _, c := range cases {
		header := make(http.Header)
		header.Set(HeaderXAMZContentSHA256, c.contentSha)
		if c.decodedLength == "" {
			header.Set(HeaderXAMZDecodedContentLength, strconv.Itoa(len(payload)))
		} else {
			header.Set(HeaderXAMZDecodedContentLength, c.decodedLength)
		}
		if c.xAmzTrailer != "" {
			header.Set(HeaderXAMZTrailer, c.xAmzTrailer)
		}

		req := &http.Request{Header: header, Body: io.NopCloser(bytes.NewReader(c.body))}
		err := handleAuthV4Streaming(req, c.contentSha, c.result)
		if c.wantSetupErr != nil {
			if !errors.Is(err, c.wantSetupErr) {
				t.Fatal(c.name, "setup:", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(c.name, "setup:", err)
		}

		got, err := io.ReadAll(req.Body)
		if c.wantReadErr != nil {
			if !errors.Is(err, c.wantReadErr) {
				t.Fatal(c.name, "read:", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(c.name, "read:", err)
		}
		if !bytes.Equal(got, c.wantBody) {
			t.Fatal(c.name, "payload mismatch")
		}
	}
}

func TestStreamingSignEndToEnd(t *testing.T) {
	const (
		accessKey = "AKIA7GQ3XN52WQLYDHZP"
		secret    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		region    = "us-east-1"
		host      = "localhost"
		path      = "/foo/bar"
		chunkSize = 64 * 1024
	)
	now := time.Now().UTC().Truncate(time.Second)
	payload := frand.Bytes(66560)

	store := mockKeyStore{accessKey: SecretAccessKey(secret)}

	// derive what the server will derive
	sk := signingKey(SecretAccessKey(secret), now, region)
	timestamp := now.Format(layoutISO8601)
	scope := now.Format(yyyymmdd) + "/" + region + "/s3/aws4_request"

	// build the seed-signed header set the server will reconstruct
	signed := http.Header{}
	signed.Set("Host", host)
	signed.Set(HeaderXAMZContentSHA256, ContentStreamingAWS4HMACSHA256Payload)
	signed.Set(HeaderXAMZDate, timestamp)
	signed.Set(HeaderXAMZDecodedContentLength, strconv.Itoa(len(payload)))
	signedNames := []string{"host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"}

	cr := canonicalRequest(signed, ContentStreamingAWS4HMACSHA256Payload, "", path, http.MethodPut)
	seedSig := getSignature(sk, canonicalStringToSign(cr, now, scope))

	// helper for per-chunk signature
	emptyHash := sha256.Sum256(nil)
	emptyHex := hex.EncodeToString(emptyHash[:])
	chunkSig := func(prevSig string, data []byte) string {
		dataHash := sha256.Sum256(data)
		mac := hmac.New(sha256.New, sk)
		mac.Write([]byte(strings.Join([]string{
			"AWS4-HMAC-SHA256-PAYLOAD",
			timestamp, scope, prevSig, emptyHex,
			hex.EncodeToString(dataHash[:]),
		}, "\n")))
		return hex.EncodeToString(mac.Sum(nil))
	}

	// build the aws-chunked body
	var body bytes.Buffer
	prev := seedSig
	for i := 0; i < len(payload); i += chunkSize {
		end := min(i+chunkSize, len(payload))
		sig := chunkSig(prev, payload[i:end])
		fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", end-i, sig)
		body.Write(payload[i:end])
		body.WriteString("\r\n")
		prev = sig
	}
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n\r\n", chunkSig(prev, nil))

	// assemble the real request
	req := httptest.NewRequest(http.MethodPut, "http://"+host+path, &body)
	req.Header.Set(HeaderXAMZContentSHA256, ContentStreamingAWS4HMACSHA256Payload)
	req.Header.Set(HeaderXAMZDate, timestamp)
	req.Header.Set(HeaderXAMZDecodedContentLength, strconv.Itoa(len(payload)))
	req.Header.Set(HeaderAuthorization, fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s,SignedHeaders=%s,Signature=%s",
		accessKey, scope, strings.Join(signedNames, ";"), seedSig))

	// verify the whole pipeline: seed sig, key derivation, chunk sigs
	gotKey, err := HandleAuth(req, store, region, now)
	if err != nil {
		t.Fatal(err)
	} else if gotKey == nil {
		t.Fatal("expected access key ID, got anonymous")
	} else if *gotKey != accessKey {
		t.Fatal("access key ID mismatch:", *gotKey)
	}

	got, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}

	// tamper with the seed sig: HandleAuth must reject
	req2 := req.Clone(t.Context())
	req2.Header.Set(HeaderAuthorization, strings.Replace(
		req.Header.Get(HeaderAuthorization),
		"Signature="+seedSig,
		"Signature="+strings.Repeat("0", 64), 1))
	if _, err := HandleAuth(req2, store, region, now); !errors.Is(err, s3errs.ErrSignatureDoesNotMatch) {
		t.Fatal("expected ErrSignatureDoesNotMatch, got", err)
	}
}

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
	_, err := verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrMissingAuthenticationToken) {
		t.Fatalf("expected ErrMissingAuthenticationToken, got %v", err)
	}

	// Case 2: credential date is in the past
	header.Set(HeaderXAMZDate, now.Add(-24*time.Hour).Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 3: credential date is in the future
	header.Set(HeaderXAMZDate, now.Add(24*time.Hour).Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrAuthorizationHeaderMalformed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 4: date is skewed too far in the past
	header.Set(HeaderXAMZDate, now.Format(layoutISO8601))
	_, err = verifyV4SignedRequest(req, store, "", now.Add(6*time.Minute))
	if !errors.Is(err, s3errs.ErrRequestTimeTooSkewed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 5: date is skewed too far in the future
	_, err = verifyV4SignedRequest(req, store, "", now.Add(-6*time.Minute))
	if !errors.Is(err, s3errs.ErrRequestTimeTooSkewed) {
		t.Fatalf("expected ErrAuthorizationHeaderMalformed, got %v", err)
	}

	// Case 6: date is valid but we don't have the access key
	_, err = verifyV4SignedRequest(req, store, "", now)
	if !errors.Is(err, s3errs.ErrInvalidAccessKeyId) {
		t.Fatal(err)
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
		err := handleAuthV4Streaming(req, c.result)
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
	} else if gotKey != accessKey {
		t.Fatal("access key mismatch:", gotKey)
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

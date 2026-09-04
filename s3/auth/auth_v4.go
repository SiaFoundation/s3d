package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// if object matches reserved string, no need to url encode them
var reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// KeyStore provides an interface for a secure key store.
type KeyStore interface {
	// LoadSecret loads the secret key for the given access key ID. If the
	// access key wasn't found, the error s3errs.ErrInvalidAccessKeyID must be
	// returned.
	LoadSecret(ctx context.Context, accessKeyID string) (SecretAccessKey, error)
}

// SecretAccessKey represents a secret access key. It is obtained from a
// KeyStore by calling LoadSecret and should be cleared after usage.
type SecretAccessKey []byte

// urlEncode encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func urlEncode(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname strings.Builder
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		default:
			runeLen := utf8.RuneLen(s)
			if runeLen < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, runeLen)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname.WriteString("%" + strings.ToUpper(hex))
			}
		}
	}
	return encodedPathname.String()
}

// canonicalHeaders generate a list of request headers with their values
func canonicalHeaders(signedHeaders http.Header) string {
	var headers []string
	vals := make(http.Header)
	for k, vv := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range vals[k] {
			if idx > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(signV4TrimAll(v))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// canonicalRequest generate a canonical request of style
//
// canonicalRequest =
//
//	<HTTPMethod>\n
//	<CanonicalURI>\n
//	<CanonicalQueryString>\n
//	<CanonicalHeaders>\n
//	<SignedHeaders>\n
//	<HashedPayload>
func canonicalRequest(extractedSignedHeaders http.Header, payload, canonicalQuery, urlPath, method string) string {
	encodedPath := urlEncode(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		canonicalQuery,
		canonicalHeaders(extractedSignedHeaders),
		canonicalSignedHeaders(extractedSignedHeaders),
		payload,
	}, "\n")
	return canonicalRequest
}

// queryEncode percent-encodes a query string name or value. url.QueryEscape
// follows the same rules apart from encoding a space as '+' rather than "%20".
func queryEncode(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// canonicalQueryString generates the canonical query string i.e an
// '&'-separated list of percent-encoded "name=value" pairs sorted by name, then
// value. The sort happens after encoding, so url.Values.Encode can't be used:
// it sorts the names before encoding them and never sorts the values of a
// repeated name.
func canonicalQueryString(query url.Values) string {
	var names []string
	vals := make(url.Values, len(query))
	for name, values := range query {
		encoded := make([]string, len(values))
		for i, value := range values {
			encoded[i] = queryEncode(value)
		}
		sort.Strings(encoded)
		name = queryEncode(name)
		names = append(names, name)
		vals[name] = encoded
	}
	sort.Strings(names)

	var pairs []string
	for _, name := range names {
		for _, value := range vals[name] {
			pairs = append(pairs, name+"="+value)
		}
	}
	return strings.Join(pairs, "&")
}

// canonicalSignedHeaders generate a string i.e alphabetically sorted,
// semicolon-separated list of lowercase request header names
func canonicalSignedHeaders(signedHeaders http.Header) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// signingKey hmac seed to calculate final signature.
//
// NOTE: service and request are hardcoded to "s3" and "aws4_request"
// respectively since this is s3 auth only.
func signingKey(secretKey SecretAccessKey, t time.Time, region string) []byte {
	secret := bytes.Join([][]byte{[]byte("AWS4"), secretKey}, []byte{})
	defer clear(secret)
	date := sumHMAC(secret, []byte(t.Format(yyyymmdd)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// canonicalStringToSign a string based on selected query values.
func canonicalStringToSign(canonicalRequest string, t time.Time, scope string) string {
	stringToSign := AuthorizationAWS4HMACSHA256 + "\n" + t.Format(layoutISO8601) + "\n"
	stringToSign += scope + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign += hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

// sameDay returns true if t1 and t2 are in the same UTC day.
func sameDay(t1, t2 time.Time) bool {
	t1 = t1.UTC()
	t2 = t2.UTC()
	return t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day()
}

// Trim leading and trailing spaces and replace sequential spaces with one space, following Trimall()
// in http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func signV4TrimAll(input string) string {
	// Compress adjacent spaces (a space is determined by
	// unicode.IsSpace() internally here) to one space and return
	return strings.Join(strings.Fields(input), " ")
}

func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

type v4SignResult struct {
	AccessKeyID string
	SigningKey  []byte
	Scope       string
	Timestamp   string
	SeedSig     string
}

// v4Signature is the signature of a request and the signed inputs, taken from
// either the Authorization header or the presigned query parameters.
type v4Signature struct {
	Credential    credentialHeader
	Date          time.Time
	SignedHeaders []string // lowercase header names
	PayloadHash   string
	Query         url.Values // excludes X-Amz-Signature for presigned requests
	Signature     string
}

// verify checks that req is signed for region with all of its x-amz-* headers
// and that the signature matches. malformed is the error for a wrong region or
// signed headers list since it depends on where the signature was taken from.
// The caller must clear the SigningKey of the result.
func (s v4Signature) verify(req *http.Request, store KeyStore, region string, malformed error) (*v4SignResult, error) {
	// an empty region allows any region
	if region != "" && s.Credential.Scope.Region != region {
		return nil, malformed
	}

	secretKey, err := store.LoadSecret(req.Context(), s.Credential.AccessKeyID)
	if err != nil {
		return nil, err
	}
	defer clear(secretKey)

	signedHeaders, err := extractSignedHeaders(req, s.SignedHeaders)
	if err != nil {
		return nil, malformed
	} else if err := assertSignedAMZHeaders(req.Header, s.SignedHeaders); err != nil {
		return nil, err
	}

	canonical := canonicalRequest(signedHeaders, s.PayloadHash, canonicalQueryString(s.Query), req.URL.Path, req.Method)
	scope := s.Credential.Scope.Canonical()
	toSign := canonicalStringToSign(canonical, s.Date, scope)
	signingKey := signingKey(secretKey, s.Date, s.Credential.Scope.Region)

	// compare signature in constant time to avoid timing attacks
	expectedSignature := getSignature(signingKey, toSign)
	if subtle.ConstantTimeCompare([]byte(expectedSignature), []byte(s.Signature)) != 1 {
		clear(signingKey)
		return nil, s3errs.ErrSignatureDoesNotMatch
	}
	return &v4SignResult{
		AccessKeyID: s.Credential.AccessKeyID,
		SigningKey:  signingKey,
		Scope:       scope,
		Timestamp:   s.Date.Format(layoutISO8601),
		SeedSig:     s.Signature,
	}, nil
}

// verifyV4SignedRequest verifies a request signed with the Authorization header.
func verifyV4SignedRequest(req *http.Request, query url.Values, store KeyStore, region string, now time.Time) (*v4SignResult, error) {
	// for the simple signature, we expect the full payload hash to be provided
	// in the header
	payloadHash := req.Header.Get(HeaderXAMZContentSHA256)
	if payloadHash == "" {
		payloadHash = emptySha256Hex
	}

	// parse authorization header
	header, err := parseAuthHeader(req.Header)
	if err != nil {
		return nil, err
	}

	// parse and validate date header
	date, err := parseDateHeader(req.Header)
	if err != nil {
		return nil, err
	} else if !sameDay(date, header.Credential.Scope.Date) {
		return nil, s3errs.ErrAuthorizationHeaderMalformed
	} else if date.Before(now.Add(-maxClockSkew)) || date.After(now.Add(maxClockSkew)) {
		return nil, s3errs.ErrRequestTimeTooSkewed
	}

	sig := v4Signature{
		Credential:    header.Credential,
		Date:          date,
		SignedHeaders: header.SignedHeaders,
		PayloadHash:   payloadHash,
		Query:         query,
		Signature:     header.Signature,
	}
	return sig.verify(req, store, region, s3errs.ErrAuthorizationHeaderMalformed)
}

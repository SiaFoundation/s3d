package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
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
func canonicalRequest(extractedSignedHeaders http.Header, payload, queryStr, urlPath, method string) string {
	rawQuery := strings.ReplaceAll(queryStr, "+", "%20")
	encodedPath := urlEncode(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		canonicalHeaders(extractedSignedHeaders),
		canonicalSignedHeaders(extractedSignedHeaders),
		payload,
	}, "\n")
	return canonicalRequest
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

func verifyV4SignedRequest(req *http.Request, store KeyStore, region string, now time.Time) (string, error) {
	// for the simple signature, we expect the full payload hash to be provided
	// in the header
	payloadHash := req.Header.Get(HeaderXAMZContentSHA256)
	if payloadHash == "" {
		payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 of empty string
	}

	// parse authorization header
	header, err := parseAuthHeader(req.Header)
	if err != nil {
		return "", err
	}

	// parse and validate date header
	const maxClockSkew = 5 * time.Minute
	date, err := parseDateHeader(req.Header)
	if err != nil {
		return "", err
	} else if !sameDay(date, header.Credential.Scope.Date) {
		return "", s3errs.ErrAuthorizationHeaderMalformed
	} else if date.Before(now.Add(-maxClockSkew)) || date.After(now.Add(maxClockSkew)) {
		return "", s3errs.ErrRequestTimeTooSkewed
	}

	secretKey, err := store.LoadSecret(req.Context(), header.Credential.AccessKeyID)
	if err != nil {
		return "", err
	}
	defer clear(secretKey)

	signedHeaders, err := extractSignedHeaders(req, header.SignedHeaders)
	if err != nil {
		return "", err
	}

	// create the canonical request to sign
	if err := req.ParseForm(); err != nil {
		return "", err
	}
	canonicalRequest := canonicalRequest(signedHeaders, payloadHash, req.Form.Encode(), req.URL.Path, req.Method)

	// combine it with the canonical scope to create the string to sign
	toSign := canonicalStringToSign(canonicalRequest, date, header.Credential.Scope.Canonical())

	// derive the signing key from the secret key using the date and region
	if region == "" {
		// use region from request if not provided which is equivalent to
		// allowing any region
		region = header.Credential.Scope.Region
	}
	signingKey := signingKey(secretKey, date, region)

	// compare signature in constant time to avoid timing attacks
	expectedSignature := getSignature(signingKey, toSign)
	if subtle.ConstantTimeCompare([]byte(expectedSignature), []byte(header.Signature)) != 1 {
		return "", s3errs.ErrInvalidSignature
	}
	return header.Credential.AccessKeyID, nil
}

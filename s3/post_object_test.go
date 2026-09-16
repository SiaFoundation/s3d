package s3_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
)

// postUpload builds a POST Object form the way the AWS SDKs do, with every
// field it sends named by the policy and the file sent last.
type postUpload struct {
	bucket     string
	key        string
	expiration time.Time
	region     string
	accessKey  string
	secretKey  string
	algorithm  string

	// capitalize spells the authentication field names the way the JavaScript
	// SDK does, boto3 writes them in lowercase
	capitalize bool

	// fields are sent and named by an exact condition
	fields [][2]string

	// conditionOverrides replace the exact condition value for a field
	conditionOverrides map[string]string

	// unsigned fields are sent without being named by any condition
	unsigned [][2]string

	// conditions are added to the policy without a matching field
	conditions []any

	// signature replaces the computed one, and policy replaces the encoded
	// document that is sent, in both cases only when set
	signature string
	policy    string

	// omitFile leaves out the file part entirely
	omitFile bool

	// signHeader also signs the request with an Authorization header, and
	// chunked sends it without a Content-Length
	signHeader bool
	chunked    bool

	file     []byte
	filename string
}

func newPostUpload(bucket, key string) *postUpload {
	return &postUpload{
		bucket:     bucket,
		key:        key,
		expiration: time.Now().Add(time.Hour),
		region:     "us-east-1",
		accessKey:  testutil.AccessKeyID,
		secretKey:  testutil.SecretAccessKey,
		algorithm:  "AWS4-HMAC-SHA256",
		file:       []byte("uploaded through a browser form"),
		filename:   "photo.txt",
	}
}

// name spells a protocol field name the way the configured SDK does.
func (u *postUpload) name(field string) string {
	if !u.capitalize {
		return field
	}
	parts := strings.Split(field, "-")
	for i, part := range parts {
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, "-")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// build returns the request body and its content type.
func (u *postUpload) build(t testing.TB) (*bytes.Buffer, string) {
	t.Helper()

	date := time.Now().UTC()
	stamp, day := date.Format("20060102T150405Z"), date.Format("20060102")
	credential := u.accessKey + "/" + day + "/" + u.region + "/s3/aws4_request"

	fields := append([][2]string{{"key", u.key}}, u.fields...)
	fields = append(fields,
		[2]string{u.name("x-amz-algorithm"), u.algorithm},
		[2]string{u.name("x-amz-credential"), credential},
		[2]string{u.name("x-amz-date"), stamp},
	)

	conditions := append([]any{map[string]string{"bucket": u.bucket}}, u.conditions...)
	for _, field := range fields {
		value := field[1]
		if override, ok := u.conditionOverrides[strings.ToLower(field[0])]; ok {
			value = override
		}
		conditions = append(conditions, map[string]string{field[0]: value})
	}

	document, err := json.Marshal(map[string]any{
		"expiration": u.expiration.UTC().Format(time.RFC3339),
		"conditions": conditions,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := base64.StdEncoding.EncodeToString(document)
	if u.policy != "" {
		policy = u.policy
	}

	signing := hmacSHA256(hmacSHA256(hmacSHA256(hmacSHA256(
		[]byte("AWS4"+u.secretKey), day), u.region), "s3"), "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signing, policy))
	if u.signature != "" {
		signature = u.signature
	}

	fields = append(fields,
		[2]string{u.name("policy"), policy},
		[2]string{u.name("x-amz-signature"), signature},
	)
	fields = append(fields, u.unsigned...)

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for _, field := range fields {
		if err := form.WriteField(field[0], field[1]); err != nil {
			t.Fatal(err)
		}
	}
	if !u.omitFile {
		file, err := form.CreateFormFile("file", u.filename)
		if err != nil {
			t.Fatal(err)
		} else if _, err := file.Write(u.file); err != nil {
			t.Fatal(err)
		}
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	return &body, form.FormDataContentType()
}

// post sends the form to the tester's endpoint and returns the response with
// its body already read. Redirects are not followed since a 303 is a result
// under test.
func (u *postUpload) post(t testing.TB, tester *testutil.S3Tester) (*http.Response, []byte) {
	t.Helper()

	body, contentType := u.build(t)
	endpoint := *tester.Client().Options().BaseEndpoint
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+"/"+u.bucket, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	if u.chunked {
		req.ContentLength = -1
	}
	if u.signHeader {
		creds := aws.Credentials{AccessKeyID: u.accessKey, SecretAccessKey: u.secretKey}
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		if err := v4.NewSigner().SignHTTP(t.Context(), creds, req, "UNSIGNED-PAYLOAD", "s3", u.region, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	client := http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	read, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, read
}

// TestPostObjectSDKForms covers the two form shapes the AWS SDKs produce. They
// differ in how they spell the protocol fields and in whether they send the
// bucket, and both were refused before POST Object was implemented.
func TestPostObjectSDKForms(t *testing.T) {
	const bucket = "bucket"

	tester := testutil.NewTester(t)
	if err := tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	assertUpload := func(upload *postUpload) {
		t.Helper()

		resp, body := upload.post(t, tester)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
		} else if resp.Header.Get("ETag") == "" {
			t.Fatal("missing ETag")
		}

		object, err := tester.Client().GetObject(t.Context(), &service.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(upload.key),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer object.Body.Close()
		stored, err := io.ReadAll(object.Body)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(stored, upload.file) {
			t.Fatal("content mismatch")
		} else if got := aws.ToString(object.ContentType); got != "text/plain" {
			t.Fatalf("unexpected content type %q", got)
		} else if got := object.Metadata["origin"]; got != "form" {
			t.Fatalf("unexpected metadata %q", got)
		}
	}

	// boto3 spells the fields in lowercase and sends no bucket field
	upload := newPostUpload(bucket, "boto3/photo.txt")
	upload.fields = [][2]string{
		{"Content-Type", "text/plain"},
		{"x-amz-meta-origin", "form"},
	}
	assertUpload(upload)

	// the JavaScript SDK capitalizes the fields and sends the bucket
	upload = newPostUpload(bucket, "javascript/photo.txt")
	upload.capitalize = true
	upload.fields = [][2]string{
		{"bucket", bucket},
		{"Content-Type", "text/plain"},
		{"x-amz-meta-origin", "form"},
	}
	assertUpload(upload)
}

// TestPostObjectResponses walks through the response shapes a form can ask for
// and the key handling of a successful upload.
func TestPostObjectResponses(t *testing.T) {
	const bucket = "bucket"

	tester := testutil.NewTester(t)
	if err := tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	// success_action_status 200 answers with an empty body
	upload := newPostUpload(bucket, "status-200")
	upload.fields = [][2]string{{"success_action_status", "200"}}
	resp, body := upload.post(t, tester)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	} else if len(body) != 0 {
		t.Fatalf("unexpected body %q", body)
	}

	// success_action_status 201 answers with a PostResponse document
	upload = newPostUpload(bucket, "status-201")
	upload.fields = [][2]string{{"success_action_status", "201"}}
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	}
	var parsed s3.PostObjectResponse
	if err := xml.Unmarshal(body, &parsed); err != nil {
		t.Fatal(err)
	} else if parsed.Bucket != bucket || parsed.Key != upload.key {
		t.Fatalf("unexpected response %+v", parsed)
	} else if parsed.ETag != resp.Header.Get("ETag") {
		t.Fatalf("ETag mismatch %q %q", resp.Header.Get("ETag"), parsed.ETag)
	} else if !strings.HasSuffix(parsed.Location, "/"+bucket+"/"+upload.key) {
		t.Fatalf("unexpected location %q", parsed.Location)
	}

	// success_action_redirect answers with a 303 carrying bucket, key and etag
	upload = newPostUpload(bucket, "redirect")
	upload.fields = [][2]string{{"success_action_redirect", "https://example.com/done?page=1"}}
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	}
	location := resp.Header.Get("Location")
	for _, want := range []string{"page=1", "bucket=" + bucket, "key=redirect", "etag="} {
		if !strings.Contains(location, want) {
			t.Fatalf("missing %q in %q", want, location)
		}
	}

	// the deprecated redirect field is accepted when named by the policy, but
	// no longer controls the response
	upload = newPostUpload(bucket, "legacy-redirect")
	upload.fields = [][2]string{{"redirect", "https://example.com/legacy"}}
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	} else if resp.Header.Get("Location") != "" {
		t.Fatalf("unexpected redirect %q", resp.Header.Get("Location"))
	}

	// the filename the client gave the file replaces the key variable before
	// the policy is checked
	upload = newPostUpload(bucket, "uploads/${filename}")
	upload.filename = "holiday.txt"
	upload.conditionOverrides = map[string]string{"key": "uploads/holiday.txt"}
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	} else if _, err := tester.HeadObject(t.Context(), bucket, "uploads/holiday.txt", nil); err != nil {
		t.Fatal(err)
	}

	// a Content-Type starts-with condition applies to each comma-separated
	// value
	upload = newPostUpload(bucket, "content-type-list")
	upload.conditions = []any{[]any{"starts-with", "$Content-Type", "image/"}}
	upload.unsigned = [][2]string{{"Content-Type", "image/png, image/jpeg"}}
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	}

	// a file inside the content-length-range bounds is accepted
	upload = newPostUpload(bucket, "sized")
	upload.conditions = []any{[]any{"content-length-range", 1, 1024}}
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	}

	// a condition on a field the form does not send holds when it demands
	// nothing, which is how the SDKs mark optional fields
	upload = newPostUpload(bucket, "optional")
	upload.conditions = []any{[]any{"starts-with", "$success_action_redirect", ""}}
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	}

	// an empty file stores an empty object
	upload = newPostUpload(bucket, "empty")
	upload.file = nil
	resp, body = upload.post(t, tester)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	}
	object, err := tester.GetObject(t.Context(), bucket, "empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer object.Body.Close()
	stored, err := io.ReadAll(object.Body)
	if err != nil {
		t.Fatal(err)
	} else if len(stored) != 0 {
		t.Fatalf("unexpected content %q", stored)
	}
}

// TestPostObjectRejections walks through the ways a form upload is refused and
// pins the error each one earns.
func TestPostObjectRejections(t *testing.T) {
	const bucket = "bucket"

	tester := testutil.NewTester(t)
	if err := tester.CreateBucket(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}

	assertRejected := func(upload *postUpload, expected s3errs.Error) {
		t.Helper()

		resp, body := upload.post(t, tester)
		var parsed s3.ErrorResponse
		if err := xml.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("unparsable error response %q: %v", body, err)
		} else if resp.StatusCode != expected.HTTPStatus || parsed.Code != expected.Code {
			t.Fatalf("expected %d %s, got %d %s", expected.HTTPStatus, expected.Code,
				resp.StatusCode, parsed.Code)
		}
	}

	// a wrong signature
	upload := newPostUpload(bucket, "rejected")
	upload.signature = strings.Repeat("a", 64)
	assertRejected(upload, s3errs.ErrSignatureDoesNotMatch)

	// an access key the store does not know
	upload = newPostUpload(bucket, "rejected")
	upload.accessKey = "AKIAIOSFODNN7EXAMPLE"
	assertRejected(upload, s3errs.ErrInvalidAccessKeyId)

	// an algorithm other than SigV4
	upload = newPostUpload(bucket, "rejected")
	upload.algorithm = "AWS2-HMAC-SHA1"
	assertRejected(upload, s3errs.ErrAccessDenied)

	// a policy past its expiration
	upload = newPostUpload(bucket, "rejected")
	upload.expiration = time.Now().Add(-time.Minute)
	assertRejected(upload, s3errs.ErrAccessDeniedExpired)

	// a field no condition names
	upload = newPostUpload(bucket, "rejected")
	upload.unsigned = [][2]string{{"x-amz-meta-injected", "value"}}
	assertRejected(upload, s3errs.ErrInvalidPolicyDocument)

	// a condition the form does not satisfy
	upload = newPostUpload(bucket, "rejected")
	upload.conditions = []any{[]any{"starts-with", "$key", "approved/"}}
	assertRejected(upload, s3errs.ErrInvalidPolicyDocument)

	// every entry in a comma-separated Content-Type has to satisfy its
	// starts-with condition
	upload = newPostUpload(bucket, "rejected")
	upload.conditions = []any{[]any{"starts-with", "$Content-Type", "image/"}}
	upload.unsigned = [][2]string{{"Content-Type", "image/png, text/plain"}}
	assertRejected(upload, s3errs.ErrInvalidPolicyDocument)

	// a bucket field that disagrees with the URL
	upload = newPostUpload(bucket, "rejected")
	upload.fields = [][2]string{{"bucket", "other"}}
	assertRejected(upload, s3errs.ErrInvalidPolicyDocument)

	// a signed policy that is not a JSON document
	upload = newPostUpload(bucket, "rejected")
	upload.policy = base64.StdEncoding.EncodeToString([]byte("{"))
	assertRejected(upload, s3errs.ErrMalformedPolicy)

	// a signed policy that is not base64
	upload = newPostUpload(bucket, "rejected")
	upload.policy = "not base64!"
	assertRejected(upload, s3errs.ErrMalformedPolicy)

	// an empty key
	upload = newPostUpload(bucket, "")
	assertRejected(upload, s3errs.ErrUserKeyMustBeSpecified)

	// a form without a file
	upload = newPostUpload(bucket, "rejected")
	upload.omitFile = true
	assertRejected(upload, s3errs.ErrIncorrectNumberOfFilesInPostRequest)

	// a file above the content-length-range maximum
	upload = newPostUpload(bucket, "rejected")
	upload.conditions = []any{[]any{"content-length-range", 0, 4}}
	assertRejected(upload, s3errs.ErrEntityTooLarge)

	// a file below the content-length-range minimum
	upload = newPostUpload(bucket, "rejected")
	upload.conditions = []any{[]any{"content-length-range", 1024, 4096}}
	assertRejected(upload, s3errs.ErrEntityTooSmall)

	// a content-md5 that does not match the file
	upload = newPostUpload(bucket, "rejected")
	upload.fields = [][2]string{{"content-md5", base64.StdEncoding.EncodeToString(make([]byte, 16))}}
	assertRejected(upload, s3errs.ErrBadDigest)

	// a form that is also signed with an Authorization header
	upload = newPostUpload(bucket, "rejected")
	upload.signHeader = true
	assertRejected(upload, s3errs.ErrInvalidArgumentMultipleAuth)

	// a form sent without a Content-Length
	upload = newPostUpload(bucket, "rejected")
	upload.chunked = true
	assertRejected(upload, s3errs.ErrMissingContentLength)

	// a POST that is not multipart form data at all
	endpoint := *tester.Client().Options().BaseEndpoint
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		endpoint+"/"+bucket, strings.NewReader("key=value"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var parsed s3.ErrorResponse
	expected := s3errs.ErrRequestIsNotMultiPartContent
	if err := xml.Unmarshal(body, &parsed); err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != expected.HTTPStatus || parsed.Code != expected.Code {
		t.Fatalf("expected %d %s, got %d %s", expected.HTTPStatus, expected.Code,
			resp.StatusCode, parsed.Code)
	}
}

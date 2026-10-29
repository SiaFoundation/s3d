package s3_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	service "github.com/aws/aws-sdk-go-v2/service/s3"
	"go.sia.tech/core/types"
	"go.uber.org/zap/zaptest"
)

// signRequest signs req, whose body is body, with the given key pair.
func signRequest(t *testing.T, req *http.Request, body, accessKeyID, secretKey string) {
	t.Helper()
	hash := sha256.Sum256([]byte(body))
	payload := hex.EncodeToString(hash[:])
	req.Header.Set("X-Amz-Content-Sha256", payload)
	creds := aws.Credentials{AccessKeyID: accessKeyID, SecretAccessKey: secretKey}
	if err := v4.NewSigner().SignHTTP(t.Context(), creds, req, payload, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
}

func newAdminServer(t *testing.T) (string, *http.Client) {
	t.Helper()
	backend, _ := testutil.NewBackend(t)
	server := httptest.NewServer(s3.NewAdmin(backend))
	t.Cleanup(server.Close)
	return server.URL, server.Client()
}

func TestPrometheus(t *testing.T) {
	baseURL, httpClient := newAdminServer(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/prometheus", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	} else if ct := resp.Header.Get("Content-Type"); ct != "text/plain; version=0.0.4" {
		t.Fatalf("expected Prometheus Content-Type, got %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("s3d_upload_pending_objects 0")) {
		t.Fatalf("expected prometheus metrics, got %q", body)
	} else if !bytes.Contains(body, []byte("s3d_transfer_ingress_bytes_total 0")) {
		t.Fatalf("expected transfer metrics, got %q", body)
	}
}

func TestUploadStats(t *testing.T) {
	baseURL, httpClient := newAdminServer(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/stats/uploads", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	} else if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected JSON Content-Type, got %q", ct)
	}

	var stats s3.UploadStats
	if err := json.UnmarshalRead(resp.Body, &stats); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(stats, s3.UploadStats{}) {
		t.Fatal("expected zero-value stats on an empty backend", stats)
	}
}

func TestCreateSnapshot(t *testing.T) {
	backend, store := testutil.NewBackend(t)
	server := httptest.NewServer(s3.NewAdmin(backend))
	t.Cleanup(server.Close)
	baseURL, httpClient := server.URL, server.Client()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, baseURL+"/snapshots", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// the response carries the recorded snapshot
	var snapshot s3.Snapshot
	if err := json.UnmarshalRead(resp.Body, &snapshot); err != nil {
		t.Fatal(err)
	} else if snapshot.SiaObjectID == (types.Hash256{}) {
		t.Fatal("expected snapshot to have a sia object id")
	}

	listSnapshots := func() []s3.Snapshot {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/snapshots", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		var list s3.SnapshotList
		if err := json.UnmarshalRead(resp.Body, &list); err != nil {
			t.Fatal(err)
		}
		return list.Snapshots
	}

	// the object is recorded before the pin and the record is completed once
	// the indexer confirms the object
	if known, err := store.HasSnapshotObject(snapshot.SiaObjectID); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected known object")
	}
	// the snapshot is listed
	snapshots := listSnapshots()
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	} else if snapshots[0].SiaObjectID != snapshot.SiaObjectID {
		t.Fatal("mismatch", snapshots[0].SiaObjectID)
	}

	// snapshots are addressed by their Sia object ID, the only identifier that
	// survives losing the database
	deleteSnapshot := func(id string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, baseURL+"/snapshots/"+id, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if status := deleteSnapshot(snapshots[0].SiaObjectID.String()); status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	if listed := listSnapshots(); len(listed) != 0 {
		t.Fatalf("expected 0 snapshots after delete, got %d", len(listed))
	}

	// deleting it again reports not found, and a malformed id is rejected
	// before it reaches the backend
	if status := deleteSnapshot(snapshots[0].SiaObjectID.String()); status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}
	if status := deleteSnapshot("not-a-sia-object-id"); status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
}

// TestInvalidCredentials tests that API calls with invalid credentials fail.
// adminRequest sends a request to the admin API test server, encoding body as
// JSON when it is not nil.
func adminRequest(t *testing.T, httpClient *http.Client, method, url string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestUsers(t *testing.T) {
	backend, store := testutil.NewBackend(t)
	server := httptest.NewServer(s3.NewAdmin(backend))
	t.Cleanup(server.Close)
	baseURL, httpClient := server.URL, server.Client()

	status := func(method, path string, body any) int {
		t.Helper()
		resp := adminRequest(t, httpClient, method, baseURL+path, body)
		resp.Body.Close()
		return resp.StatusCode
	}
	listUsers := func() []string {
		t.Helper()
		resp := adminRequest(t, httpClient, http.MethodGet, baseURL+"/users", nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		var users []string
		if err := json.UnmarshalRead(resp.Body, &users); err != nil {
			t.Fatal(err)
		}
		return users
	}

	// the test backend starts with its default user
	if users := listUsers(); len(users) != 1 || users[0] != testutil.Owner {
		t.Fatal("unexpected users", users)
	}

	if s := status(http.MethodPost, "/users", s3.CreateUserRequest{Name: "alice"}); s != http.StatusOK {
		t.Fatalf("expected 200, got %d", s)
	}
	if users := listUsers(); len(users) != 2 || !slices.Contains(users, "alice") {
		t.Fatal("unexpected users", users)
	}

	// creating the user again conflicts, and a missing name is rejected
	if s := status(http.MethodPost, "/users", s3.CreateUserRequest{Name: "alice"}); s != http.StatusConflict {
		t.Fatalf("expected 409, got %d", s)
	}
	if s := status(http.MethodPost, "/users", s3.CreateUserRequest{}); s != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", s)
	}

	// a user that owns a bucket cannot be deleted
	resp := adminRequest(t, httpClient, http.MethodPost, baseURL+"/keys", s3.CreateAccessKeyRequest{UserName: "alice"})
	var key s3.AccessKey
	if err := json.UnmarshalRead(resp.Body, &key); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if err := store.CreateBucket(key.AccessKeyID, "bucket", false); err != nil {
		t.Fatal(err)
	}
	if s := status(http.MethodDelete, "/users/alice", nil); s != http.StatusConflict {
		t.Fatalf("expected 409, got %d", s)
	}
	info, err := store.BucketAccessInfo("bucket")
	if err != nil {
		t.Fatal(err)
	} else if err := store.DeleteBucket(info.ID); err != nil {
		t.Fatal(err)
	}

	// deleting the user removes their keys, and deleting again reports not
	// found
	if s := status(http.MethodDelete, "/users/alice", nil); s != http.StatusOK {
		t.Fatalf("expected 200, got %d", s)
	}
	if users := listUsers(); len(users) != 1 {
		t.Fatal("unexpected users", users)
	}
	if s := status(http.MethodDelete, "/keys/"+key.AccessKeyID, nil); s != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", s)
	}
	if s := status(http.MethodDelete, "/users/alice", nil); s != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", s)
	}
}

func TestAccessKeys(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	server := httptest.NewServer(s3.NewAdmin(backend))
	t.Cleanup(server.Close)
	baseURL, httpClient := server.URL, server.Client()

	createKey := func(req s3.CreateAccessKeyRequest) (s3.AccessKey, int) {
		t.Helper()
		resp := adminRequest(t, httpClient, http.MethodPost, baseURL+"/keys", req)
		defer resp.Body.Close()
		var key s3.AccessKey
		if resp.StatusCode == http.StatusOK {
			if err := json.UnmarshalRead(resp.Body, &key); err != nil {
				t.Fatal(err)
			}
		}
		return key, resp.StatusCode
	}
	listKeys := func(userName string) ([]s3.AccessKey, string) {
		t.Helper()
		url := baseURL + "/keys"
		if userName != "" {
			url += "?userName=" + userName
		}
		resp := adminRequest(t, httpClient, http.MethodGet, url, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var keys []s3.AccessKey
		if err := json.Unmarshal(body, &keys); err != nil {
			t.Fatal(err)
		}
		return keys, strings.TrimSpace(string(body))
	}

	// the pair is generated when the request omits it
	generated, status := createKey(s3.CreateAccessKeyRequest{UserName: testutil.Owner})
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	} else if len(generated.AccessKeyID) != 20 || len(generated.SecretKey) != 40 || generated.UserName != testutil.Owner {
		t.Fatal("unexpected key", generated)
	}

	// an explicit pair is stored as given
	explicit := s3.CreateAccessKeyRequest{UserName: testutil.Owner, AccessKeyID: strings.Repeat("A", 20), SecretKey: strings.Repeat("s", 40)}
	if key, status := createKey(explicit); status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	} else if key.AccessKeyID != explicit.AccessKeyID || key.SecretKey != explicit.SecretKey {
		t.Fatal("unexpected key", key)
	}

	// reusing the ID conflicts, a short secret, a missing user name and an
	// unknown user are rejected
	if _, status := createKey(explicit); status != http.StatusConflict {
		t.Fatalf("expected 409, got %d", status)
	}
	if _, status := createKey(s3.CreateAccessKeyRequest{UserName: testutil.Owner, AccessKeyID: strings.Repeat("B", 20), SecretKey: "short"}); status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
	if _, status := createKey(s3.CreateAccessKeyRequest{}); status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
	if _, status := createKey(s3.CreateAccessKeyRequest{UserName: "nobody"}); status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}

	// listing never includes secrets, filters by user, and an unknown user
	// yields an empty array rather than null
	keys, _ := listKeys("")
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	for _, key := range keys {
		if key.SecretKey != "" {
			t.Fatal("expected secret to be omitted", key)
		} else if key.UserName != testutil.Owner {
			t.Fatal("unexpected user", key)
		}
	}
	if keys, body := listKeys("nobody"); len(keys) != 0 || body != "[]" {
		t.Fatalf("expected an empty array, got %q", body)
	}

	// deleting the key makes it unknown
	resp := adminRequest(t, httpClient, http.MethodDelete, baseURL+"/keys/"+generated.AccessKeyID, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	resp = adminRequest(t, httpClient, http.MethodDelete, baseURL+"/keys/"+generated.AccessKeyID, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	if keys, _ := listKeys(testutil.Owner); len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestInvalidCredentials(t *testing.T) {
	s3Tester := testutil.NewTester(t, testutil.WithServiceOptions(func(o *service.Options) {
		o.Credentials = aws.NewCredentialsCache(&credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "wrongID",
				SecretAccessKey: "wrongSecret",
			},
		})
	}))
	err := s3Tester.CreateBucket(t.Context(), "bucket")
	testutil.AssertS3Error(t, s3errs.ErrInvalidAccessKeyId, err)
}

// TestAccessDenied tests that API calls that are not supported for anonymous
// users fail with AccessDenied.
func TestAccessDenied(t *testing.T) {
	assertAccessDenied := func(t *testing.T, name string, run func(t *testing.T, s3 *testutil.S3Tester) error) {
		t.Run(name, func(t *testing.T) {
			s3Tester := testutil.NewTester(t, testutil.WithServiceOptions(func(o *service.Options) {
				o.Credentials = aws.NewCredentialsCache(aws.AnonymousCredentials{})
			}))
			testutil.AssertS3Error(t, s3errs.ErrAccessDenied, run(t, s3Tester))
		})
	}

	// bucket routes
	assertAccessDenied(t, "CreateBucket", func(t *testing.T, s3 *testutil.S3Tester) error {
		return s3.CreateBucket(t.Context(), "bucket")
	})
	assertAccessDenied(t, "ListBuckets", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.ListBuckets(t.Context())
		return err
	})

	// object routes
	assertAccessDenied(t, "PutObject", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.PutObject(t.Context(), "foo", "bar", bytes.NewReader(nil), nil)
		return err
	})
	assertAccessDenied(t, "DeleteObject", func(t *testing.T, s3 *testutil.S3Tester) error {
		err := s3.DeleteObject(t.Context(), "bucket", "object")
		return err
	})
	assertAccessDenied(t, "DeleteObjects", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.DeleteObjects(t.Context(), "bucket", testutil.ObjectIdentifiers("object1", "object2"), nil)
		return err
	})

	// multipart upload routes
	assertAccessDenied(t, "CreateMultipartUpload", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.CreateMultipartUpload(t.Context(), "bucket", "object", nil)
		return err
	})
	assertAccessDenied(t, "AbortMultipartUpload", func(t *testing.T, s3 *testutil.S3Tester) error {
		err := s3.AbortMultipartUpload(t.Context(), "bucket", "object", "uploadID")
		return err
	})
	assertAccessDenied(t, "CompleteMultipartUpload", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.CompleteMultipartUpload(t.Context(), "bucket", "object", "uploadID", nil)
		return err
	})
	assertAccessDenied(t, "UploadPart", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.UploadPart(t.Context(), "bucket", "object", "uploadID", 1, nil)
		return err
	})
	assertAccessDenied(t, "ListParts", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.ListParts(t.Context(), "bucket", "object", "uploadID", nil, nil)
		return err
	})
	assertAccessDenied(t, "ListMultipartUploads", func(t *testing.T, s3 *testutil.S3Tester) error {
		_, err := s3.ListMultipartUploads(t.Context(), "bucket", nil)
		return err
	})
}

// TestAuthorizationRoutes guards every implemented bucket and object route
// against anonymous and authenticated non-owner access to a private bucket.
func TestAuthorizationRoutes(t *testing.T) {
	const bucket = "private-bucket"
	backend, _ := testutil.NewBackend(t, testutil.WithKeyPair(testutil.OtherOwner, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey))
	if err := backend.CreateBucket(t.Context(), testutil.AccessKeyID, bucket, s3.CreateBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	upload := s3.NewUploadID().String()
	tests := []struct {
		name, method, path, body, source string
	}{
		{name: "HeadBucket", method: http.MethodHead, path: "/private-bucket"},
		{name: "ListObjects", method: http.MethodGet, path: "/private-bucket"},
		{name: "ListObjectsV2", method: http.MethodGet, path: "/private-bucket?list-type=2"},
		{name: "EmptyListObjects", method: http.MethodGet, path: "/private-bucket?max-keys=0"},
		{name: "ListObjectVersions", method: http.MethodGet, path: "/private-bucket?versions"},
		{name: "EmptyListObjectVersions", method: http.MethodGet, path: "/private-bucket?versions&max-keys=0"},
		{name: "DeleteBucket", method: http.MethodDelete, path: "/private-bucket"},
		{name: "GetBucketLocation", method: http.MethodGet, path: "/private-bucket?location"},
		{name: "GetBucketVersioning", method: http.MethodGet, path: "/private-bucket?versioning"},
		{name: "PutBucketVersioning", method: http.MethodPut, path: "/private-bucket?versioning"},
		{name: "GetBucketLifecycleConfiguration", method: http.MethodGet, path: "/private-bucket?lifecycle"},
		{name: "PutBucketLifecycleConfiguration", method: http.MethodPut, path: "/private-bucket?lifecycle"},
		{name: "DeleteBucketLifecycle", method: http.MethodDelete, path: "/private-bucket?lifecycle"},
		{name: "GetBucketPolicy", method: http.MethodGet, path: "/private-bucket?policy"},
		{name: "PutBucketPolicy", method: http.MethodPut, path: "/private-bucket?policy"},
		{name: "DeleteBucketPolicy", method: http.MethodDelete, path: "/private-bucket?policy"},
		{name: "GetBucketPolicyStatus", method: http.MethodGet, path: "/private-bucket?policyStatus"},
		{name: "GetObject", method: http.MethodGet, path: "/private-bucket/key"},
		{name: "HeadObject", method: http.MethodHead, path: "/private-bucket/key"},
		{name: "GetObjectVersion", method: http.MethodGet, path: "/private-bucket/key?versionId=null"},
		{name: "HeadObjectVersion", method: http.MethodHead, path: "/private-bucket/key?versionId=null"},
		{name: "DeleteObject", method: http.MethodDelete, path: "/private-bucket/key"},
		{name: "DeleteObjectVersion", method: http.MethodDelete, path: "/private-bucket/key?versionId=null"},
		{name: "PutObject", method: http.MethodPut, path: "/private-bucket/key"},
		{name: "CopyObject", method: http.MethodPut, path: "/private-bucket/key", source: "/private-bucket/source"},
		{name: "CreateMultipartUpload", method: http.MethodPost, path: "/private-bucket/key?uploads"},
		{name: "ListMultipartUploads", method: http.MethodGet, path: "/private-bucket?uploads"},
		{name: "UploadPart", method: http.MethodPut, path: "/private-bucket/key?uploadId=" + upload + "&partNumber=1"},
		{name: "UploadPartCopy", method: http.MethodPut, path: "/private-bucket/key?uploadId=" + upload + "&partNumber=1", source: "/private-bucket/source"},
		{name: "ListParts", method: http.MethodGet, path: "/private-bucket/key?uploadId=" + upload},
		{name: "CompleteMultipartUpload", method: http.MethodPost, path: "/private-bucket/key?uploadId=" + upload},
		{name: "AbortMultipartUpload", method: http.MethodDelete, path: "/private-bucket/key?uploadId=" + upload},
		{name: "DeleteObjects", method: http.MethodPost, path: "/private-bucket?delete", body: "<Delete><Object><Key>first</Key></Object><Object><Key>second</Key><VersionId>null</VersionId></Object></Delete>"},
	}
	for _, signed := range []bool{false, true} {
		caller := "anonymous"
		if signed {
			caller = "other user"
		}
		t.Run(caller, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					req := httptest.NewRequest(test.method, "http://localhost"+test.path, strings.NewReader(test.body))
					if test.source != "" {
						req.Header.Set("X-Amz-Copy-Source", test.source)
					}
					if signed {
						signRequest(t, req, test.body, testutil.OtherAccessKeyID, testutil.OtherSecretAccessKey)
					}
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, req)
					if rec.Code != http.StatusForbidden {
						t.Fatalf("expected status %d, got %d: %s", http.StatusForbidden, rec.Code, rec.Body)
					}
				})
			}
		})
	}
}

// TestRouteValidationOrder checks requests that no handler serves: a signed
// caller is told why, but an anonymous one is denied before its request is
// validated.
func TestRouteValidationOrder(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	if err := backend.CreateBucket(t.Context(), testutil.AccessKeyID, "bucket", s3.CreateBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	anonymous := func(method, path string) *http.Request {
		return httptest.NewRequest(method, "http://localhost"+path, nil)
	}
	signed := func(method, path string) *http.Request {
		req := anonymous(method, path)
		signRequest(t, req, "", testutil.AccessKeyID, testutil.SecretAccessKey)
		return req
	}
	tests := []struct {
		name string
		req  *http.Request
		code int
	}{
		{"anonymous object with wrong method", anonymous(http.MethodPost, "/bucket/key"), http.StatusForbidden},
		{"signed object with wrong method", signed(http.MethodPost, "/bucket/key"), http.StatusMethodNotAllowed},
		// a browser upload authenticates through its form, so it is validated
		// before it can be authorized
		{"anonymous browser upload", anonymous(http.MethodPost, "/bucket"), http.StatusPreconditionFailed},
		{"signed browser upload", signed(http.MethodPost, "/bucket"), http.StatusNotImplemented},
		{"anonymous uploads without a bucket", anonymous(http.MethodGet, "/?uploads"), http.StatusForbidden},
		{"signed uploads without a bucket", signed(http.MethodGet, "/?uploads"), http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, test.req)
			if rec.Code != test.code {
				t.Fatalf("expected status %d, got %d: %s", test.code, rec.Code, rec.Body)
			}
		})
	}
}

// TestSignedUploadWithoutPayloadHash checks that a signed request without the
// X-Amz-Content-Sha256 header is verified against the empty payload it was
// signed for, so its body can't be swapped.
func TestSignedUploadWithoutPayloadHash(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	if err := backend.CreateBucket(t.Context(), testutil.AccessKeyID, "bucket", s3.CreateBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", strings.NewReader("value"))
	emptyHash := sha256.Sum256(nil)
	creds := aws.Credentials{AccessKeyID: testutil.AccessKeyID, SecretAccessKey: testutil.SecretAccessKey}
	if err := v4.NewSigner().SignHTTP(t.Context(), creds, req, hex.EncodeToString(emptyHash[:]), "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body)
	}
}

// TestHostBucketStyles verifies that path-style and virtual-hosted-style
// requests both work, with and without a port in the Host header.
// "localhost" is implicitly available as a host bucket base.
func TestHostBucketStyles(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	do := func(t *testing.T, method, host, path, body string) *httptest.ResponseRecorder {
		t.Helper()

		req := httptest.NewRequest(method, "http://"+host+path, strings.NewReader(body))
		req.Host = host
		if body != "" {
			req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		}
		signRequest(t, req, body, testutil.AccessKeyID, testutil.SecretAccessKey)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// create a bucket containing an object using path-style requests
	if rec := do(t, http.MethodPut, "localhost:8000", "/bucket", ""); rec.Code != http.StatusOK {
		t.Fatalf("failed to create bucket: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, http.MethodPut, "localhost:8000", "/bucket/object", "hello"); rec.Code != http.StatusOK {
		t.Fatalf("failed to create object: %d %s", rec.Code, rec.Body)
	}

	tests := []struct {
		name string
		host string
		path string
	}{
		{"path style with port", "localhost:8000", "/bucket/object"},
		{"path style without port", "localhost", "/bucket/object"},
		{"virtual-hosted style with port", "bucket.localhost:8000", "/object"},
		{"virtual-hosted style without port", "bucket.localhost", "/object"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := do(t, http.MethodGet, test.host, test.path, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body)
			} else if body, err := io.ReadAll(rec.Result().Body); err != nil {
				t.Fatal(err)
			} else if string(body) != "hello" {
				t.Fatalf("expected body %q, got %q", "hello", body)
			}
		})
	}
}

// TestErrorResponseDrain verifies that a rejected request with an unread body
// still receives its error response over a connection that stays reusable and
// that a body over the drain cap closes the connection instead.
func TestErrorResponseDrain(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	// anonymous puts are rejected with AccessDenied before the body is read
	var reused bool
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
	}
	ctx := httptrace.WithClientTrace(t.Context(), trace)

	doPut := func(size int64) *http.Response {
		t.Helper()
		body := bytes.Repeat([]byte("a"), int(size))
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/bucket/object", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatal(err)
		} else if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// a body of minimum part size is drained so the response arrives on a
	// connection that stays alive
	resp := doPut(s3.MinUploadPartSize)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatal("unexpected", resp.StatusCode)
	} else if resp.Close {
		t.Fatal("expected connection to stay alive")
	}

	// the second request reuses the drained connection
	resp = doPut(s3.MinUploadPartSize)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatal("unexpected", resp.StatusCode)
	} else if !reused {
		t.Fatal("expected connection reuse")
	}

	// a body over the drain cap is not consumed and the connection is marked
	// to close after the response
	big := bytes.Repeat([]byte("a"), int(3*s3.MinUploadPartSize))
	req := httptest.NewRequest(http.MethodPut, "/bucket/object", bytes.NewReader(big))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatal("unexpected", rec.Code)
	} else if rec.Header().Get("Connection") != "close" {
		t.Fatal("expected Connection close header")
	}
}

// TestPutObjectContentMD5 covers the Content-MD5 header, which is decoded
// before the body is read. A value that did not decode to a 16 byte digest
// once panicked the handler and dropped the connection.
func TestPutObjectContentMD5(t *testing.T) {
	backend, _ := testutil.NewBackend(t)
	handler := s3.New(backend, s3.WithLogger(zaptest.NewLogger(t)))

	signer := v4.NewSigner()
	creds := aws.Credentials{
		AccessKeyID:     testutil.AccessKeyID,
		SecretAccessKey: testutil.SecretAccessKey,
	}

	put := func(t *testing.T, path, digest, body string) *httptest.ResponseRecorder {
		t.Helper()

		req := httptest.NewRequest(http.MethodPut, "http://localhost:8000"+path, strings.NewReader(body))
		req.Host = "localhost:8000"
		if body != "" {
			req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		}
		if digest != "" {
			req.Header.Set("Content-Md5", digest)
		}
		payloadHash := sha256.Sum256([]byte(body))
		hashHex := hex.EncodeToString(payloadHash[:])
		req.Header.Set("X-Amz-Content-Sha256", hashHex)
		if err := signer.SignHTTP(t.Context(), creds, req, hashHex, "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	if rec := put(t, "/bucket", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("failed to create bucket: %d %s", rec.Code, rec.Body)
	}

	expected := s3errs.ErrInvalidDigest
	for _, size := range []int{8, 32} {
		digest := base64.StdEncoding.EncodeToString(make([]byte, size))
		rec := put(t, "/bucket/object", digest, "payload")
		if rec.Code != expected.HTTPStatus || !strings.Contains(rec.Body.String(), expected.Code) {
			t.Fatalf("a %d byte digest: expected %d %s, got %d %s",
				size, expected.HTTPStatus, expected.Code, rec.Code, rec.Body)
		}
	}
}

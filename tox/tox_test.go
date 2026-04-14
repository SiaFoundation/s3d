package tox_test

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/testutils"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

type keyPair struct {
	accessKey string
	secretKey string
}

var (
	mainKeyPair = keyPair{
		accessKey: "0555b35654ad1656d804",
		secretKey: "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==",
	}
	altKeyPair = keyPair{
		accessKey: "NOPQRSTUVWXYZABCDEFG",
		secretKey: "nopqrstuvwxyzabcdefghijklmnabcdefghijklm",
	}
	tenantKeyPair = keyPair{
		accessKey: "HIJKLMNOPQRSTUVWXYZA",
		secretKey: "opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab",
	}
	allKeyPairs = []keyPair{mainKeyPair, altKeyPair, tenantKeyPair}
)

func TestS3(t *testing.T) {
	log := zap.NewNop()

	t.Run("memory backend", func(t *testing.T) {
		var opts []testutil.MemoryBackendOption
		for _, kp := range allKeyPairs {
			opts = append(opts, testutil.WithKeyPair(kp.accessKey, kp.accessKey, kp.secretKey))
		}
		backend := testutil.NewMemoryBackend(opts...)

		port := startS3Server(t, backend, log)
		confPath := writeS3TestsConf(t, t.TempDir(), port)
		runTox(t, confPath)
	})

	t.Run("sia backend", func(t *testing.T) {
		// spin up a test cluster with consensus, indexer, and hosts
		cluster := testutils.NewCluster(t, testutils.WithLogger(log), testutils.WithHosts(30))

		// create an account and wait for contracts
		sk := cluster.AddAccount(t)
		cluster.WaitForContracts(t)

		// create the SDK client pointing at the test indexer
		builder := sdk.NewBuilder(cluster.Indexer.AppURL, sdk.AppMetadata{
			ID:          types.HashBytes([]byte("s3d-tox")),
			Name:        "s3d tox tests",
			Description: "S3 compatibility tests for s3d",
		})
		sdkClient, err := builder.SDK(sk, sdk.WithLogger(log.Named("sdk")))
		if err != nil {
			t.Fatalf("failed to create SDK: %v", err)
		}
		defer sdkClient.Close()

		// open a SQLite store for s3d metadata
		dir := t.TempDir()
		store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		t.Cleanup(func() { store.Close() })

		// create the Sia backend with all test key pairs
		var siaOpts []sia.Option
		for _, kp := range allKeyPairs {
			siaOpts = append(siaOpts, sia.WithKeyPair(kp.accessKey, kp.secretKey))
		}
		siaOpts = append(siaOpts, sia.WithLogger(log.Named("backend")))

		backend, err := sia.New(t.Context(), sia.NewSDK(sdkClient), store, dir, siaOpts...)
		if err != nil {
			t.Fatalf("failed to create Sia backend: %v", err)
		}

		// start the s3 server and run the tox tests
		port := startS3Server(t, backend, log)
		confPath := writeS3TestsConf(t, t.TempDir(), port)
		runTox(t, confPath)
	})
}

// s3testsDir returns the path to the cloned s3-tests repo. It checks
// the S3_TESTS_DIR env var first, then falls back to ../s3-tests relative
// to the repo root.
func s3testsDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("S3_TESTS_DIR"); dir != "" {
		return dir
	}
	dir := filepath.Join("..", "s3-tests")
	if _, err := os.Stat(dir); err != nil {
		t.Skip("s3-tests directory not found; clone it or set S3_TESTS_DIR")
	}
	return dir
}

// startS3Server starts an HTTP server with the given s3 backend on a random
// port and returns the port. The server is shut down when the test finishes.
func startS3Server(t *testing.T, backend s3.Backend, log *zap.Logger) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	handler := s3.New(backend, s3.WithHostBucketBases([]string{"localhost"}), s3.WithLogger(log.Named("s3-server")))
	server := &http.Server{Handler: handler}
	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		close(errCh)
	}()
	t.Cleanup(func() {
		server.Close()
		if err := <-errCh; err != nil {
			t.Errorf("server error: %v", err)
		}
	})

	return listener.Addr().(*net.TCPAddr).Port
}

// writeS3TestsConf writes an s3tests.conf file to dir with the given port and
// returns the path to the file.
func writeS3TestsConf(t *testing.T, dir string, port int) string {
	t.Helper()

	conf := fmt.Sprintf(`[DEFAULT]
host = localhost
port = %d
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = s3d-{random}-
iam name prefix = s3-tests-
iam path prefix = /s3-tests/

[s3 main]
display_name = M. Tester
user_id = testid
email = tester@ceph.com
api_name = default
access_key = %s
secret_key = %s

[s3 alt]
display_name = john.doe
email = john.doe@example.com
user_id = 56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234
access_key = %s
secret_key = %s

[s3 tenant]
display_name = testx$tenanteduser
user_id = testx$9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef
access_key = %s
secret_key = %s
email = tenanteduser@example.com
tenant = testx

[iam]
email = s3@example.com
user_id = 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
access_key = ABCDEFGHIJKLMNOPQRST
secret_key = abcdefghijklmnopqrstuvwxyzabcdefghijklmn
display_name = youruseridhere

[iam root]
access_key = %s
secret_key = %s
user_id = rootuserid
email = root@example.com

[iam alt root]
access_key = %s
secret_key = %s
user_id = altrootuserid
email = altroot@example.com
`,
		port,
		mainKeyPair.accessKey, mainKeyPair.secretKey,
		altKeyPair.accessKey, altKeyPair.secretKey,
		tenantKeyPair.accessKey, tenantKeyPair.secretKey,
		mainKeyPair.accessKey, mainKeyPair.secretKey,
		altKeyPair.accessKey, altKeyPair.secretKey,
	)

	fp := filepath.Join(dir, "s3tests.conf")
	if err := os.WriteFile(fp, []byte(conf), 0644); err != nil {
		t.Fatalf("failed to write s3tests.conf: %v", err)
	}
	return fp
}

// runTox runs the tox s3-tests against the server. It expects tox and the
// s3-tests repo to be available.
func runTox(t *testing.T, confPath string) {
	t.Helper()

	testsDir := s3testsDir(t)

	// we ignore the following tests
	//  - the ones that require AWSv2 signatures since s3d only supports
	//    v4
	//  - tests around the "Expect" header since Go's http server doesn't
	//    give us enough control over it. S3 ignores bad Expect headers,
	//    but Go's http server responds with "417 Expectation Failed"
	//    instead
	//  - tests that are marked as "fails_on_rgw" since they are known to
	//    fail due to the tests not being able to correctly manipulate
	//    request headers
	//  - tests around ACLs since s3d currently does not implement ACL
	//    handling
	t.Run("test_headers", func(t *testing.T) {
		runToxCommand(t, confPath, testsDir,
			"s3tests/functional/test_headers.py",
			"-m", "not auth_aws2 and not fails_on_rgw",
			"-k", "not _create_bad_expect_ and not _acl",
		)
	})

	// we ignore the following tests
	//  - tests marked s3d_not_implemented or s3d_not_supported since they
	//    cover features s3d does not handle by design
	//  - tests marked s3d_not_delimiter_alt since s3d only supports "/"
	//    as a delimiter
	//  - bucket_logging tests since s3d does not implement bucket logging
	t.Run("test_s3", func(t *testing.T) {
		runToxCommand(t, confPath, testsDir,
			"s3tests/functional/test_s3.py",
			"-m", "(copy or delete or list_objects or multipart) and not s3d_not_implemented and not s3d_not_supported and not s3d_not_delimiter_alt and not bucket_logging",
		)
	})

	// the following suites are currently disabled; uncomment to enable

	// t.Run("test_iam", func(t *testing.T) {
	// 	runToxCommand(t, confPath, testsDir, "s3tests/functional/test_iam.py")
	// })

	// t.Run("test_s3select", func(t *testing.T) {
	// 	runToxCommand(t, confPath, testsDir, "s3tests/functional/test_s3select.py")
	// })

	// t.Run("test_sns", func(t *testing.T) {
	// 	runToxCommand(t, confPath, testsDir, "s3tests/functional/test_sns.py")
	// })

	// t.Run("test_sts", func(t *testing.T) {
	// 	runToxCommand(t, confPath, testsDir, "s3tests/functional/test_sts.py")
	// })

	// t.Run("test_utils", func(t *testing.T) {
	// 	runToxCommand(t, confPath, testsDir, "s3tests/functional/test_utils.py")
	// })
}

func runToxCommand(t *testing.T, confPath, testsDir string, args ...string) {
	t.Helper()

	toxArgs := []string{"run", "--skip-pkg-install", "-c", testsDir, "--"}
	toxArgs = append(toxArgs, args...)

	cmd := exec.Command("tox", toxArgs...)
	cmd.Env = append(os.Environ(),
		"S3TEST_CONF="+confPath,
		"S3_USE_SIGV4=True",
	)

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	err := cmd.Run()
	output := buf.String()
	if err != nil {
		t.Fatalf("tox failed: %v\n%s", err, output)
	} else if bytes.Contains(buf.Bytes(), []byte("FAILED")) {
		t.Fatalf("tox reported failures:\n%s", output)
	}
	t.Log(output)
}

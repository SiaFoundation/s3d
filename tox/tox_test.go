//go:build tox

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

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.sia.tech/indexd/testutils"
	"go.uber.org/zap"
	"gopkg.in/ini.v1"
)

func TestS3(t *testing.T) {
	log := zap.NewNop()
	dir := t.TempDir()

	// create a Sia cluster with 30 hosts
	cluster := testutils.NewCluster(t, testutils.WithLogger(log), testutils.WithHosts(30))

	// create an account and wait for contracts
	sk := cluster.AddAccount(t)
	cluster.WaitForContracts(t)

	// create the SDK client
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

	// create store
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	// parse tox conf
	toxConf, err := ini.Load("tox.conf")
	if err != nil {
		t.Fatalf("failed to load tox.conf: %v", err)
	}

	siaOpts := []sia.Option{sia.WithLogger(log.Named("backend"))}
	seen := make(map[string]bool)
	for _, section := range toxConf.Sections() {
		ak := section.Key("access_key").String()
		sk := section.Key("secret_key").String()
		if ak == "" || sk == "" || seen[ak] {
			continue
		}
		seen[ak] = true
		siaOpts = append(siaOpts, sia.WithKeyPair(ak, sk))
	}
	backend, err := sia.New(t.Context(), sia.NewSDK(sdkClient), store, dir, siaOpts...)
	if err != nil {
		t.Fatalf("failed to create Sia backend: %v", err)
	}

	// start the s3 server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	server := &http.Server{Handler: s3.New(backend, s3.WithHostBucketBases([]string{"localhost"}), s3.WithLogger(log.Named("s3")))}
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

	// write the s3tests.conf with the allocated port
	port := listener.Addr().(*net.TCPAddr).Port
	toxConf.Section("DEFAULT").Key("port").SetValue(fmt.Sprintf("%d", port))
	confPath := filepath.Join(t.TempDir(), "s3tests.conf")
	if err := toxConf.SaveTo(confPath); err != nil {
		t.Fatalf("failed to write s3tests.conf: %v", err)
	}

	// resolve the s3-tests directory
	testsDir := os.Getenv("S3_TESTS_DIR")
	if testsDir == "" {
		t.Skip("S3_TESTS_DIR not set")
	}

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
		runTox(t, confPath, testsDir,
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
		runTox(t, confPath, testsDir,
			"s3tests/functional/test_s3.py",
			"-m", "(copy or delete or list_objects or multipart) and not s3d_not_implemented and not s3d_not_supported and not s3d_not_delimiter_alt and not bucket_logging",
		)
	})

	// the following suites are currently disabled; uncomment to enable

	// t.Run("test_iam", func(t *testing.T) {
	// 	runTox(t, confPath, testsDir, "s3tests/functional/test_iam.py")
	// })

	// t.Run("test_s3select", func(t *testing.T) {
	// 	runTox(t, confPath, testsDir, "s3tests/functional/test_s3select.py")
	// })

	// t.Run("test_sns", func(t *testing.T) {
	// 	runTox(t, confPath, testsDir, "s3tests/functional/test_sns.py")
	// })

	// t.Run("test_sts", func(t *testing.T) {
	// 	runTox(t, confPath, testsDir, "s3tests/functional/test_sts.py")
	// })

	// t.Run("test_utils", func(t *testing.T) {
	// 	runTox(t, confPath, testsDir, "s3tests/functional/test_utils.py")
	// })
}

func runTox(t *testing.T, confPath, testsDir string, args ...string) {
	t.Helper()

	toxArgs := []string{"run", "--skip-pkg-install", "-c", testsDir, "--"}
	toxArgs = append(toxArgs, args...)

	cmd := exec.CommandContext(t.Context(), "tox", toxArgs...)
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
	}
	t.Log(output)
}

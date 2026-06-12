//go:build tox

package tox_test

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/testutils"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"gopkg.in/ini.v1"
)

func TestS3(t *testing.T) {
	log := zap.NewNop()
	dir := t.TempDir()

	// resolve the s3-tests directory
	testsDir := os.Getenv("S3_TESTS_DIR")
	if testsDir == "" {
		t.Skip("S3_TESTS_DIR not set")
	}

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

	// create store
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// parse tox conf
	ini.DefaultHeader = true
	toxConf, err := ini.Load("tox.conf")
	if err != nil {
		t.Fatalf("failed to load tox.conf: %v", err)
	}

	// create users and access keys from tox.conf sections
	seen := make(map[string]bool)
	for _, section := range toxConf.Sections() {
		ak := section.Key("access_key").String()
		ssk := section.Key("secret_key").String()
		uid := section.Key("user_id").String()
		if ak == "" || ssk == "" || seen[ak] {
			continue
		}
		seen[ak] = true
		if uid == "" {
			uid = ak
		}
		if err := store.CreateUser(uid); err != nil && !errors.Is(err, sia.ErrUserAlreadyExists) {
			t.Fatalf("failed to create user: %v", err)
		}
		if err := store.CreateAccessKey(uid, ak, ssk); err != nil {
			t.Fatalf("failed to create access key: %v", err)
		}
	}
	// run the lifecycle loop every second and treat a "day" as 11s so the
	// expiration tests observe objects expiring without waiting real days. The
	// 11s window keeps a Days=5 rule live at the tests' 40s check but expired
	// by their 70s check.
	backend, err := sia.New(t.Context(), sia.NewSDK(sdkClient), store, dir,
		sia.WithLogger(log.Named("backend")),
		sia.WithLifecycleLoopInterval(time.Second),
		sia.WithLifecycleDayDuration(11*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create Sia backend: %v", err)
	}

	// start the s3 server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	server := &http.Server{Handler: s3.New(backend, s3.WithLogger(log.Named("s3")))}
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
		backend.Close()
		sdkClient.Close()
		store.Close()
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

	// run all test_s3.py tests except those for features s3d does not
	// support. The marker exclusions cover broad categories while the -k
	// exclusions catch unmarked tests that use unsupported features by name.
	t.Run("test_s3", func(t *testing.T) {
		runTox(t, confPath, testsDir,
			"s3tests/functional/test_s3.py",
			"-m", "not s3d_not_implemented and not s3d_not_supported and not bucket_logging and not encryption and not sse_s3 and not bucket_encryption and not lifecycle_transition and not tagging and not bucket_policy and not conditional_write and not object_ownership and not checksum and not cloud_transition and not cloud_restore and not iam_user and not iam_account and not delete_marker",
			// the lifecycle exclusions drop tests for lifecycle features s3d
			// does not implement: tag and object-size filters, versioning and
			// noncurrent-version actions, delete-marker expiration, and
			// transitions.
			"-k", "not _acl and not versioning and not post_object and not _torrent and not cors and not object_lock and not retention and not legal_hold and not notification and not replication and not website and not _select and not bucket_recreate_not_overriding and not lifecycle_expiration_tags and not lifecycle_expiration_versioned and not lifecycle_expiration_size and not tags_head and not noncur and not deletemarker and not lifecycle_set_filter and not lifecycle_set_empty_filter and not lifecycle_transition_set_invalid_date",
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

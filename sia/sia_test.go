package sia_test

import (
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func NewTester(t testing.TB, opts ...testutil.TesterOption) *testutil.S3Tester {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	// use SQLite store
	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	return NewCustomTester(t, dir, store, testutil.NewMemorySDK(), log, opts...)
}

func NewCustomTester(t testing.TB, dir string, store sia.Store, sdk sia.SDK, log *zap.Logger, opts ...testutil.TesterOption) *testutil.S3Tester {
	backend, err := sia.New(t.Context(), sdk, store, dir, sia.WithKeyPair(testutil.AccessKeyID, testutil.SecretAccessKey),
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}

	mergedOpts := append([]testutil.TesterOption{testutil.WithBackend(backend)}, opts...)
	return testutil.NewTester(t, mergedOpts...)
}

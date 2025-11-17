package sia_test

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap/zaptest"
)

type MemorySDK struct {
}

func (s *MemorySDK) Download(ctx context.Context, w io.Writer, obj sdk.Object, opts ...sdk.DownloadOption) error {
	panic("not implemented")
}

func (s *MemorySDK) PinObject(ctx context.Context, obj sdk.Object) error {
	panic("not implemented")
}

func (s *MemorySDK) Upload(ctx context.Context, r io.Reader, opts ...sdk.UploadOption) (sdk.Object, error) {
	panic("not implemented")
}

func NewMemorySDK() *MemorySDK {
	return &MemorySDK{}
}

func NewTester(t testing.TB, opts ...testutil.TesterOption) *testutil.S3Tester {
	log := zaptest.NewLogger(t)

	// use in-memory SDK
	sdk := NewMemorySDK()

	// use SQLite store
	store, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	backend, err := sia.New(context.Background(), sdk, store, testutil.AccessKeyID, testutil.SecretAccessKey,
		sia.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}

	var mergedOpts []testutil.TesterOption
	mergedOpts = append(mergedOpts, testutil.WithBackend(backend))
	mergedOpts = append(mergedOpts, opts...)
	return testutil.NewTester(t, mergedOpts...)
}

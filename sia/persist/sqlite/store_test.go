package sqlite

import (
	"crypto/md5"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap"
)

const testAccessKeyID = "test-accesskey"

type testStore struct {
	*Store
	db *sql.DB
	t  testing.TB
}

func (store *testStore) assertCount(expected int, table string) {
	store.t.Helper()

	var got int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
		store.t.Fatalf("failed to scan count from %s: %v", table, err)
	} else if got != expected {
		store.t.Fatalf("expected %d rows in %s, got %d", expected, table, got)
	}
}

// bucketID returns the ID of the named bucket.
func (store *testStore) bucketID(name string) s3.BucketID {
	store.t.Helper()

	info, err := store.BucketAccessInfo(name)
	if err != nil {
		store.t.Fatal(err)
	}
	return info.ID
}

func newTestStore(t testing.TB, log *zap.Logger) *testStore {
	store, err := OpenDatabase(filepath.Join(t.TempDir(), "s3d.sqlite"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			panic(err)
		}
	})

	return &testStore{
		Store: store,
		db:    store.db,
		t:     t,
	}
}

func initTestDB(t testing.TB, log *zap.Logger) *testStore {
	store := newTestStore(t, log)

	if err := store.CreateUser("testuser"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey("testuser", testAccessKeyID, "testsecret"); err != nil {
		t.Fatal(err)
	}
	return store
}

// mustMultipartParts returns the uploaded parts of an upload in part order, as
// the backend hands them to CompleteMultipartUpload.
func mustMultipartParts(t testing.TB, store *testStore, bucket s3.BucketID, name string, uploadID s3.UploadID) []objects.Part {
	t.Helper()
	parts, err := store.MultipartParts(bucket, name, uploadID)
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

// multipartMD5 returns the MD5 a completed multipart object carries, the MD5 of
// its part MD5s in part order.
func multipartMD5(parts ...[16]byte) (sum [16]byte) {
	h := md5.New()
	for _, part := range parts {
		h.Write(part[:])
	}
	copy(sum[:], h.Sum(nil))
	return sum
}

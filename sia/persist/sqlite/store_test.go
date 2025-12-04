package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

type testStore struct {
	*Store
	db *sql.DB
}

func initTestDB(t testing.TB, log *zap.Logger) *testStore {
	db, err := initDB(filepath.Join(t.TempDir(), "s3d.sqlite"))
	if err != nil {
		t.Fatal(err)
	}

	store, err := initStore(db, log.Named("store"))
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
		db:    db,
	}
}

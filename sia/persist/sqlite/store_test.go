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
		t:     t,
	}
}

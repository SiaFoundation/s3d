package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

// ErrNoAppKey is returned when no application key is set.
var ErrNoAppKey = errors.New("no app key set")

// AppKey retrieves the application private key and the indexer URL it was
// registered with.
func (s *Store) AppKey() (types.PrivateKey, string, error) {
	var key sql.Null[[]byte]
	var indexerURL sql.NullString
	err := s.transaction(func(tx *txn) error {
		return tx.QueryRow(`SELECT app_key, indexer_url FROM global_settings LIMIT 1`).
			Scan(&key, &indexerURL)
	})
	if err != nil {
		return nil, "", err
	} else if !key.Valid {
		return nil, "", ErrNoAppKey
	}
	return types.PrivateKey(key.V), indexerURL.String, nil
}

// ReadAppKey returns the application private key and the indexer URL recorded
// in the database at fp. The database is opened readonly, so it is neither
// initialized nor migrated.
func ReadAppKey(fp string) (types.PrivateKey, string, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", fp, time.Minute.Milliseconds()))
	if err != nil {
		return nil, "", fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	var key sql.Null[[]byte]
	var indexerURL sql.NullString
	if err := db.QueryRow(`SELECT app_key, indexer_url FROM global_settings LIMIT 1`).Scan(&key, &indexerURL); err != nil {
		return nil, "", err
	} else if !key.Valid {
		return nil, "", ErrNoAppKey
	}
	return types.PrivateKey(key.V), indexerURL.String, nil
}

// SetAppKey sets the application private key and the indexer URL it was
// registered with.
func (s *Store) SetAppKey(key types.PrivateKey, indexerURL string) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec("UPDATE global_settings SET app_key = $1, indexer_url = $2;", key[:], indexerURL)
		return err
	})
}

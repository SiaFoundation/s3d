package sqlite

import (
	"database/sql"
	"errors"

	"go.sia.tech/core/types"
)

// ErrNoAppKey is returned when no application key is set.
var ErrNoAppKey = errors.New("no app key set")

// AppKey retrieves the application private key.
func (s *Store) AppKey() (types.PrivateKey, error) {
	var key sql.Null[[]byte]
	err := s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT app_key FROM global_settings LIMIT 1`).
			Scan(&key)
		return err
	})
	if err != nil {
		return nil, err
	} else if !key.Valid {
		return nil, ErrNoAppKey
	}
	return types.PrivateKey(key.V), nil
}

// SetAppKey sets the application private key.
func (s *Store) SetAppKey(key types.PrivateKey) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec("UPDATE global_settings SET app_key = $1;", key[:])
		return err
	})
}

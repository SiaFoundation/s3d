package sqlite

import (
	"errors"

	"go.sia.tech/core/types"
)

var ErrNoAppKey = errors.New("no app key set")

// AppKey retrieves the application private key.
func (s *Store) AppKey() (key types.PrivateKey, err error) {
	err = s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT app_key FROM global_settings LIMIT 1`).
			Scan(&key)
		return err
	})
	return
}

// SetAppKey sets the application private key.
func (s *Store) SetAppKey(key types.PrivateKey) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec("UPDATE global_settings SET app_key = $1;", key[:])
		return err
	})
}

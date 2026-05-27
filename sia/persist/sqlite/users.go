package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
)

// userIDForAccessKey returns the database ID of the user associated with the
// given access key, or ErrInvalidAccessKeyId if the key does not exist.
func userIDForAccessKey(tx *txn, accessKeyID string) (int64, error) {
	var uid int64
	err := tx.QueryRow("SELECT user_id FROM access_keys WHERE access_key_id = $1", accessKeyID).Scan(&uid)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, s3errs.ErrInvalidAccessKeyId
	}
	return uid, err
}

// CreateUser creates a new user with the given name.
func (s *Store) CreateUser(name string) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("INSERT INTO users (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", name)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return sia.ErrUserAlreadyExists
		}
		return nil
	})
}

// DeleteUser deletes the user with the given name. Access keys belonging to
// the user are deleted via cascade. Returns an error if the user owns any
// buckets.
func (s *Store) DeleteUser(name string) error {
	return s.transaction(func(tx *txn) error {
		var userID int64
		err := tx.QueryRow("SELECT id FROM users WHERE name = $1", name).Scan(&userID)
		if errors.Is(err, sql.ErrNoRows) {
			return sia.ErrUserNotFound
		} else if err != nil {
			return err
		}

		var hasBuckets bool
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM buckets WHERE user_id = $1)", userID).Scan(&hasBuckets); err != nil {
			return err
		} else if hasBuckets {
			return fmt.Errorf("user %q still owns buckets", name)
		}

		_, err = tx.Exec("DELETE FROM users WHERE id = $1", userID)
		return err
	})
}

// ListUsers returns the names of all users.
func (s *Store) ListUsers() ([]string, error) {
	var users []string
	err := s.transaction(func(tx *txn) error {
		users = users[:0]
		rows, err := tx.Query("SELECT name FROM users ORDER BY name ASC")
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			users = append(users, name)
		}
		return rows.Err()
	})
	return users, err
}

// CreateAccessKey creates a new access key for the given user.
func (s *Store) CreateAccessKey(userName, accessKeyID, secretKey string) error {
	return s.transaction(func(tx *txn) error {
		var userID int64
		err := tx.QueryRow("SELECT id FROM users WHERE name = $1", userName).Scan(&userID)
		if errors.Is(err, sql.ErrNoRows) {
			return sia.ErrUserNotFound
		} else if err != nil {
			return err
		}

		res, err := tx.Exec("INSERT INTO access_keys (access_key_id, secret_key, user_id) VALUES ($1, $2, $3) ON CONFLICT (access_key_id) DO NOTHING", accessKeyID, secretKey, userID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return sia.ErrAccessKeyAlreadyExists
		}
		return nil
	})
}

// DeleteAccessKey deletes the access key with the given ID.
func (s *Store) DeleteAccessKey(accessKeyID string) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM access_keys WHERE access_key_id = $1", accessKeyID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return fmt.Errorf("access key %q not found", accessKeyID)
		}
		return nil
	})
}

// ListAccessKeys returns all access keys for the given user. If userName is
// nil, all access keys are returned.
func (s *Store) ListAccessKeys(userName *string) ([]sia.AccessKeyInfo, error) {
	var keys []sia.AccessKeyInfo
	err := s.transaction(func(tx *txn) error {
		keys = keys[:0]

		var r *rows
		var err error
		if userName == nil {
			r, err = tx.Query(`
				SELECT ak.access_key_id, u.name
				FROM access_keys ak
				INNER JOIN users u ON ak.user_id = u.id
				ORDER BY u.name ASC, ak.access_key_id ASC`)
		} else {
			r, err = tx.Query(`
				SELECT ak.access_key_id, u.name
				FROM access_keys ak
				INNER JOIN users u ON ak.user_id = u.id
				WHERE u.name = $1
				ORDER BY ak.access_key_id ASC`, *userName)
		}
		if err != nil {
			return err
		}
		defer r.Close()

		for r.Next() {
			var k sia.AccessKeyInfo
			if err := r.Scan(&k.AccessKeyID, &k.UserName); err != nil {
				return err
			}
			keys = append(keys, k)
		}
		return r.Err()
	})
	return keys, err
}

// UserNameForAccessKey returns the user name associated with the given access
// key ID.
func (s *Store) UserNameForAccessKey(accessKeyID string) (name string, err error) {
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow(`
			SELECT u.name FROM users u
			INNER JOIN access_keys ak ON ak.user_id = u.id
			WHERE ak.access_key_id = $1`, accessKeyID).Scan(&name)
	})
	if errors.Is(err, sql.ErrNoRows) {
		err = s3errs.ErrInvalidAccessKeyId
	}
	return
}

// LoadSecret returns the secret key for the given access key ID.
func (s *Store) LoadSecret(accessKeyID string) (string, error) {
	var secret string
	err := s.transaction(func(tx *txn) error {
		return tx.QueryRow("SELECT secret_key FROM access_keys WHERE access_key_id = $1", accessKeyID).Scan(&secret)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", s3errs.ErrInvalidAccessKeyId
	}
	return secret, err
}

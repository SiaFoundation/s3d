package sqlite

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
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

// CreateUser creates a new user with the given name. Names are unique
// regardless of case, like AWS IAM user names, and the case it was created
// with is kept.
func (s *Store) CreateUser(name string) error {
	defer s.cache.reset()
	// a user's name is its S3 user ID
	if strings.EqualFold(name, s3.AnonymousUserID) {
		return s3.ErrReservedUserName
	}
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("INSERT INTO users (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return s3.ErrUserAlreadyExists
		}
		return nil
	})
}

// DeleteUser deletes the user with the given name, matched regardless of
// case. Access keys belonging to the user are deleted via cascade. Returns an
// error if the user owns any buckets.
func (s *Store) DeleteUser(name string) error {
	defer s.cache.reset()
	return s.transaction(func(tx *txn) error {
		var userID int64
		err := tx.QueryRow("SELECT id FROM users WHERE name = $1 COLLATE NOCASE", name).Scan(&userID)
		if errors.Is(err, sql.ErrNoRows) {
			return s3.ErrUserNotFound
		} else if err != nil {
			return err
		}

		var hasBuckets bool
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM buckets WHERE user_id = $1)", userID).Scan(&hasBuckets); err != nil {
			return err
		} else if hasBuckets {
			return s3.ErrUserHasBuckets
		}

		_, err = tx.Exec("DELETE FROM users WHERE id = $1", userID)
		return err
	})
}

// ListUsers returns the names of all users.
func (s *Store) ListUsers() ([]string, error) {
	users := []string{}
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
	defer s.cache.reset()
	return s.transaction(func(tx *txn) error {
		var userID int64
		err := tx.QueryRow("SELECT id FROM users WHERE name = $1 COLLATE NOCASE", userName).Scan(&userID)
		if errors.Is(err, sql.ErrNoRows) {
			return s3.ErrUserNotFound
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
			return s3.ErrAccessKeyAlreadyExists
		}
		return nil
	})
}

// DeleteAccessKey deletes the access key with the given ID.
func (s *Store) DeleteAccessKey(accessKeyID string) error {
	defer s.cache.reset()
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM access_keys WHERE access_key_id = $1", accessKeyID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return s3.ErrAccessKeyNotFound
		}
		return nil
	})
}

// ListAccessKeys returns all access keys for the given user. If userName is
// nil, all access keys are returned.
func (s *Store) ListAccessKeys(userName *string) ([]s3.AccessKeyMetadata, error) {
	keys := []s3.AccessKeyMetadata{}
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
				WHERE u.name = $1 COLLATE NOCASE
				ORDER BY ak.access_key_id ASC`, *userName)
		}
		if err != nil {
			return err
		}
		defer r.Close()

		for r.Next() {
			var k s3.AccessKeyMetadata
			if err := r.Scan(&k.AccessKeyID, &k.UserName); err != nil {
				return err
			}
			keys = append(keys, k)
		}
		return r.Err()
	})
	return keys, err
}

// LoadSecret returns the secret key for the given access key ID and the name
// of the user it belongs to.
func (s *Store) LoadSecret(accessKeyID string) (secret, userName string, err error) {
	v, gen, ok := s.cache.secret(accessKeyID)
	if ok {
		return v.secret, v.userName, nil
	}
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow(`
			SELECT ak.secret_key, u.name FROM access_keys ak
			INNER JOIN users u ON u.id = ak.user_id
			WHERE ak.access_key_id = $1`, accessKeyID).Scan(&secret, &userName)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", s3errs.ErrInvalidAccessKeyId
	} else if err == nil {
		s.cache.putSecret(gen, accessKeyID, cachedSecret{secret: secret, userName: userName})
	}
	return
}

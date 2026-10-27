package sia

import (
	"context"

	"github.com/SiaFoundation/s3d/s3"
)

// CreateAccessKey creates an access key pair for the given user.
func (s *Sia) CreateAccessKey(ctx context.Context, userName, accessKeyID, secretKey string) error {
	return s.store.CreateAccessKey(userName, accessKeyID, secretKey)
}

// CreateUser creates a user with the given name.
func (s *Sia) CreateUser(ctx context.Context, name string) error {
	return s.store.CreateUser(name)
}

// DeleteAccessKey deletes the access key with the given ID.
func (s *Sia) DeleteAccessKey(ctx context.Context, accessKeyID string) error {
	return s.store.DeleteAccessKey(accessKeyID)
}

// DeleteUser deletes the user with the given name and their access keys.
func (s *Sia) DeleteUser(ctx context.Context, name string) error {
	return s.store.DeleteUser(name)
}

// ListAccessKeys returns the access keys of the given user, or of every user
// when userName is nil.
func (s *Sia) ListAccessKeys(ctx context.Context, userName *string) ([]s3.AccessKey, error) {
	return s.store.ListAccessKeys(userName)
}

// ListUsers returns the names of all users.
func (s *Sia) ListUsers(ctx context.Context) ([]string, error) {
	return s.store.ListUsers()
}

package sqlite

import (
	"errors"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia"
	"go.uber.org/zap/zaptest"
)

func TestUsersCRUD(t *testing.T) {
	store := newTestStore(t, zaptest.NewLogger(t))

	// no users initially
	users, err := store.ListUsers()
	if err != nil {
		t.Fatal(err)
	} else if len(users) != 0 {
		t.Fatal("expected 0 users", len(users))
	}

	// create two users
	if err := store.CreateUser("alice"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateUser("bob"); err != nil {
		t.Fatal(err)
	}

	// duplicate name fails
	if err := store.CreateUser("alice"); !errors.Is(err, sia.ErrUserAlreadyExists) {
		t.Fatal("expected ErrUserAlreadyExists", err)
	}

	// list returns both, sorted by name
	users, err = store.ListUsers()
	if err != nil {
		t.Fatal(err)
	} else if len(users) != 2 {
		t.Fatal("expected 2 users", len(users))
	} else if users[0] != "alice" || users[1] != "bob" {
		t.Fatal("unexpected order", users[0], users[1])
	}

	// delete bob
	if err := store.DeleteUser("bob"); err != nil {
		t.Fatal(err)
	}

	// delete unknown user fails
	if err := store.DeleteUser("charlie"); !errors.Is(err, sia.ErrUserNotFound) {
		t.Fatal("expected ErrUserNotFound", err)
	}

	// only alice remains
	users, err = store.ListUsers()
	if err != nil {
		t.Fatal(err)
	} else if len(users) != 1 || users[0] != "alice" {
		t.Fatal("expected only alice")
	}
}

func TestAccessKeysCRUD(t *testing.T) {
	store := newTestStore(t, zaptest.NewLogger(t))

	if err := store.CreateUser("alice"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateUser("bob"); err != nil {
		t.Fatal(err)
	}

	// create keys for alice
	if err := store.CreateAccessKey("alice", "AKID1", "secret1"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey("alice", "AKID2", "secret2"); err != nil {
		t.Fatal(err)
	}

	// create key for bob
	if err := store.CreateAccessKey("bob", "AKID3", "secret3"); err != nil {
		t.Fatal(err)
	}

	// creating key for unknown user fails
	if err := store.CreateAccessKey("charlie", "AKID4", "secret4"); !errors.Is(err, sia.ErrUserNotFound) {
		t.Fatal("expected ErrUserNotFound", err)
	}

	// list all keys
	keys, err := store.ListAccessKeys(nil)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 3 {
		t.Fatal("expected 3 keys", len(keys))
	}

	// list alice's keys only
	alice := "alice"
	keys, err = store.ListAccessKeys(&alice)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 2 {
		t.Fatal("expected 2 keys for alice", len(keys))
	} else if keys[0].AccessKeyID != "AKID1" || keys[1].AccessKeyID != "AKID2" {
		t.Fatal("unexpected keys", keys[0].AccessKeyID, keys[1].AccessKeyID)
	} else if keys[0].SecretKey != "secret1" || keys[1].SecretKey != "secret2" {
		t.Fatal("unexpected secrets", keys[0].SecretKey, keys[1].SecretKey)
	} else if keys[0].UserName != "alice" {
		t.Fatal("unexpected user", keys[0].UserName)
	}

	// load secret
	secret, err := store.LoadSecret("AKID1")
	if err != nil {
		t.Fatal(err)
	} else if secret != "secret1" {
		t.Fatal("unexpected secret", secret)
	}

	// load unknown secret fails
	if _, err := store.LoadSecret("AKID_UNKNOWN"); err == nil {
		t.Fatal("expected error for unknown key")
	}

	// delete a key
	if err := store.DeleteAccessKey("AKID2"); err != nil {
		t.Fatal(err)
	}

	// deleting unknown key fails
	if err := store.DeleteAccessKey("AKID_UNKNOWN"); err == nil {
		t.Fatal("expected error for unknown key")
	}

	// alice now has one key
	keys, err = store.ListAccessKeys(&alice)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 || keys[0].AccessKeyID != "AKID1" {
		t.Fatal("expected only AKID1")
	}

	// deleting a user cascades keys
	if err := store.DeleteUser("bob"); err != nil {
		t.Fatal(err)
	}

	keys, err = store.ListAccessKeys(nil)
	if err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Fatal("expected 1 key after cascade", len(keys))
	} else if keys[0].AccessKeyID != "AKID1" {
		t.Fatal("expected AKID1 to survive", keys[0].AccessKeyID)
	}
}

func TestBucketOwnership(t *testing.T) {
	store := newTestStore(t, zaptest.NewLogger(t))

	// create two users with keys
	if err := store.CreateUser("alice"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey("alice", "ALICE_KEY", "secret"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateUser("bob"); err != nil {
		t.Fatal(err)
	} else if err := store.CreateAccessKey("bob", "BOB_KEY", "secret"); err != nil {
		t.Fatal(err)
	}

	// alice creates a bucket
	if err := store.CreateBucket("ALICE_KEY", "shared-name"); err != nil {
		t.Fatal(err)
	}

	// alice creating the same bucket again returns BucketAlreadyOwnedByYou
	if err := store.CreateBucket("ALICE_KEY", "shared-name"); !errors.Is(err, s3errs.ErrBucketAlreadyOwnedByYou) {
		t.Fatal("expected ErrBucketAlreadyOwnedByYou", err)
	}

	// bob creating the same bucket returns BucketAlreadyExists
	if err := store.CreateBucket("BOB_KEY", "shared-name"); !errors.Is(err, s3errs.ErrBucketAlreadyExists) {
		t.Fatal("expected ErrBucketAlreadyExists", err)
	}

	// alice can head her bucket
	if err := store.HeadBucket("ALICE_KEY", "shared-name"); err != nil {
		t.Fatal(err)
	}

	// bob gets access denied on head
	if err := store.HeadBucket("BOB_KEY", "shared-name"); !errors.Is(err, s3errs.ErrAccessDenied) {
		t.Fatal("expected ErrAccessDenied", err)
	}

	// bob gets access denied on delete
	if err := store.DeleteBucket("BOB_KEY", "shared-name"); !errors.Is(err, s3errs.ErrAccessDenied) {
		t.Fatal("expected ErrAccessDenied", err)
	}

	// list: alice sees the bucket, bob does not
	aliceBuckets, err := store.ListBuckets("ALICE_KEY")
	if err != nil {
		t.Fatal(err)
	} else if len(aliceBuckets) != 1 || aliceBuckets[0].Name != "shared-name" {
		t.Fatal("expected alice to see the bucket")
	}

	bobBuckets, err := store.ListBuckets("BOB_KEY")
	if err != nil {
		t.Fatal(err)
	} else if len(bobBuckets) != 0 {
		t.Fatal("expected bob to see no buckets")
	}

	// deleting alice fails while she owns buckets
	if err := store.DeleteUser("alice"); err == nil {
		t.Fatal("expected error deleting user with buckets")
	}

	// alice can delete her bucket
	if err := store.DeleteBucket("ALICE_KEY", "shared-name"); err != nil {
		t.Fatal(err)
	}

	// verify it is gone
	if err := store.HeadBucket("ALICE_KEY", "shared-name"); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatal("expected ErrNoSuchBucket", err)
	}
}

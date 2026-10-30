package sqlite

import (
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap/zaptest"
)

func TestTagging(t *testing.T) {
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, "bucket", false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID("bucket")

	// a version keeps the tags it is given, in order
	tags := s3.TagSet{{Key: "project", Value: "one"}, {Key: "empty", Value: ""}}
	if _, _, err := store.PutObject(bid, "key", objects.PutOptions{}); err != nil {
		t.Fatal(err)
	} else if err := store.PutObjectTagging(bid, "key", s3.NoVersion(), tags); err != nil {
		t.Fatal(err)
	}
	tagging, err := store.GetObjectTagging(bid, "key", s3.NoVersion())
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tagging.Tags, tags) {
		t.Fatalf("expected tags %v, got %v", tags, tagging.Tags)
	}

	// overwriting an unversioned object clears its previous tags
	if _, _, err := store.PutObject(bid, "key", objects.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	tagging, err = store.GetObjectTagging(bid, "key", s3.NoVersion())
	if err != nil {
		t.Fatal(err)
	} else if len(tagging.Tags) != 0 {
		t.Fatalf("expected no tags, got %v", tagging.Tags)
	}
}

func TestTaggingUnderObjectLock(t *testing.T) {
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, "locked", true); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID("locked")

	lock := &s3.ObjectLockState{Mode: s3.ObjectLockModeCompliance, RetainUntil: time.Now().Add(time.Hour), LegalHold: s3.LegalHoldOn}
	version, _, err := store.PutObject(bid, "key", objects.PutOptions{ObjectLock: lock})
	if err != nil {
		t.Fatal(err)
	}

	// a locked version's tags can still be replaced
	tags := s3.TagSet{{Key: "key", Value: "value"}}
	if err := store.PutObjectTagging(bid, "key", s3.NoVersion(), tags); err != nil {
		t.Fatal(err)
	}
	tagging, err := store.GetObjectTagging(bid, "key", s3.NoVersion())
	if err != nil {
		t.Fatal(err)
	} else if tagging.VersionID != version {
		t.Fatalf("expected version %q, got %q", version, tagging.VersionID)
	} else if !reflect.DeepEqual(tagging.Tags, tags) {
		t.Fatalf("expected tags %v, got %v", tags, tagging.Tags)
	}

	// and removed, without touching the lock
	if err := store.DeleteObjectTagging(bid, "key", s3.NoVersion()); err != nil {
		t.Fatal(err)
	}
	obj, err := store.GetObject(bid, "key", s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.ObjectLock.LegalHold != s3.LegalHoldOn || obj.ObjectLock.Mode != s3.ObjectLockModeCompliance {
		t.Fatalf("expected the lock to be unchanged, got %+v", obj.ObjectLock)
	}
}

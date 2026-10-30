package sqlite

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap/zaptest"
)

func TestTagging(t *testing.T) {
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, "bucket", false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID("bucket")

	// objects keep the tags they are stored with
	tags := s3.TagSet{{Key: "project", Value: "one"}, {Key: "empty", Value: ""}}
	if _, _, err := store.PutObject(bid, "match", objects.PutOptions{Tags: tags}); err != nil {
		t.Fatal(err)
	} else if _, _, err := store.PutObject(bid, "other", objects.PutOptions{Tags: s3.TagSet{{Key: "project", Value: "two"}}}); err != nil {
		t.Fatal(err)
	}
	obj, err := store.GetObject(bid, "match", s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(obj.Tags, tags) {
		t.Fatalf("expected object tags %v, got %v", tags, obj.Tags)
	}

	// overwriting an unversioned object clears its previous tags
	if _, _, err := store.PutObject(bid, "other", objects.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	tagging, err := store.GetObjectTagging(bid, "other", s3.NoVersion())
	if err != nil {
		t.Fatal(err)
	} else if len(tagging.Tags) != 0 {
		t.Fatalf("expected no tags, got %v", tagging.Tags)
	}
}

func TestTaggingCopySourceChanged(t *testing.T) {
	store := initTestDB(t, zaptest.NewLogger(t))
	if err := store.CreateBucket(testAccessKeyID, "bucket", false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID("bucket")

	tags := s3.TagSet{{Key: "project", Value: "one"}}
	if _, _, err := store.PutObject(bid, "a", objects.PutOptions{Tags: tags}); err != nil {
		t.Fatal(err)
	}
	tagging, err := store.GetObjectTagging(bid, "a", s3.NoVersion())
	if err != nil {
		t.Fatal(err)
	}

	// a write between looking up the source's version and the copy overwrites
	// the null version in place, so the copy must not pair the new content with
	// the old tags
	if _, _, err := store.PutObject(bid, "a", objects.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	opts := s3.CopyObjectOptions{Tags: tagging.Tags, CopyTags: true}
	if _, _, err := store.CopyObject(bid, "a", tagging.Version, bid, "a-copy", opts); !errors.Is(err, s3errs.ErrConditionalRequestConflict) {
		t.Fatalf("expected %v, got %v", s3errs.ErrConditionalRequestConflict, err)
	}

	// once the tags match again the copy takes them
	if err := store.PutObjectTagging(bid, "a", s3.NoVersion(), tags); err != nil {
		t.Fatal(err)
	} else if _, _, err := store.CopyObject(bid, "a", tagging.Version, bid, "a-copy", opts); err != nil {
		t.Fatal(err)
	}
	obj, err := store.GetObject(bid, "a-copy", s3.NoVersion(), nil)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(obj.Tags, tags) {
		t.Fatalf("expected tags %v, got %v", tags, obj.Tags)
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

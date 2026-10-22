package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestBackup(t *testing.T) {
	log := zaptest.NewLogger(t)
	store := initTestDB(t, log)

	const (
		plain     = "bucket-one"
		versioned = "bucket-two"
		locked    = "bucket-three"
	)
	if err := store.CreateBucket(testAccessKeyID, plain, false); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testAccessKeyID, versioned, false); err != nil {
		t.Fatal(err)
	} else if err := store.PutBucketVersioning(testAccessKeyID, versioned, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	} else if err := store.CreateBucket(testAccessKeyID, locked, true); err != nil {
		t.Fatal(err)
	}

	// stage puts an object and optionally marks it uploaded and pinned
	stage := func(bucket, name string, uploaded, pinned bool) {
		t.Helper()
		fn := name + ".upload"
		md5 := frand.Entropy128()
		versionID, _, err := store.PutObject(testAccessKeyID, bucket, name, objects.PutOptions{ContentMD5: md5, Length: 16, FileName: &fn})
		if err != nil {
			t.Fatal(err)
		} else if !uploaded {
			return
		}
		sealed := sdk.SealedObject{SealedObject: slabs.SealedObject{
			EncryptedDataKey: frand.Bytes(32),
			Slabs: []slabs.SlabSlice{{
				EncryptionKey: frand.Entropy256(),
				MinShards:     1,
				Length:        16,
				Sectors:       []slabs.PinnedSector{{Root: frand.Entropy256(), HostKey: frand.Entropy256()}},
			}},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}}
		if err := store.MarkObjectUploaded(bucket, name, versionID, md5, sealed, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		} else if !pinned {
			return
		} else if _, err := store.MarkObjectPinned(sealed.ID()); err != nil {
			t.Fatal(err)
		}
	}
	stage(plain, "pinned", true, true)
	stage(plain, "unpinned", true, false)
	stage(plain, "pending", false, false)
	stage(locked, "held", false, false)
	if err := store.PutObjectLegalHold(testAccessKeyID, locked, "held", s3.NoVersion(), s3.LegalHoldOn); err != nil {
		t.Fatal(err)
	}
	// a pending version on top of a pinned one
	stage(versioned, "versioned", true, true)
	stage(versioned, "versioned", false, false)
	uploadID := s3.NewUploadID()
	if err := store.CreateMultipartUpload(testAccessKeyID, plain, "multipart", uploadID, nil, nil); err != nil {
		t.Fatal(err)
	} else if _, _, err := store.AddMultipartPart(testAccessKeyID, plain, "multipart", uploadID, "part1", 1, frand.Entropy128(), 16); err != nil {
		t.Fatal(err)
	}

	users, err := store.ListUsers()
	if err != nil {
		t.Fatal(err)
	}
	accessKeys, err := store.ListAccessKeys(nil)
	if err != nil {
		t.Fatal(err)
	}
	buckets, err := store.ListBuckets(testAccessKeyID)
	if err != nil {
		t.Fatal(err)
	}
	live, err := store.UploadStats()
	if err != nil {
		t.Fatal(err)
	}

	checkDatabase := func(t *testing.T, fp string, objectCount int64) {
		t.Helper()

		backup, err := OpenDatabase(fp, log)
		if err != nil {
			t.Fatal(err)
		}
		defer backup.Close()

		if restored, err := backup.ListUsers(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(users, restored) {
			t.Fatalf("expected users %v, got %v", users, restored)
		}

		if restored, err := backup.ListAccessKeys(nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(accessKeys, restored) {
			t.Fatalf("expected access keys %v, got %v", accessKeys, restored)
		}

		if restored, err := backup.ListBuckets(testAccessKeyID); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(buckets, restored) {
			t.Fatalf("expected buckets %v, got %v", buckets, restored)
		}

		// only objects whose data reached Sia survive
		for _, missing := range []struct{ bucket, name string }{{plain, "pending"}, {plain, "unpinned"}, {locked, "held"}} {
			if _, err := backup.GetObject(aws.String(testAccessKeyID), missing.bucket, missing.name, s3.NoVersion(), nil, s3.ActionGetObject); !errors.Is(err, s3errs.ErrNoSuchKey) {
				t.Fatal("unexpected", missing.name, err)
			}
		}
		for _, kept := range []struct{ bucket, name string }{{plain, "pinned"}, {versioned, "versioned"}} {
			if obj, err := backup.GetObject(aws.String(testAccessKeyID), kept.bucket, kept.name, s3.NoVersion(), nil, s3.ActionGetObject); err != nil {
				t.Fatal(err)
			} else if obj.SiaObject == nil || obj.FileName != nil {
				t.Fatal("expected the pinned version", kept.name)
			}
		}
		if filenames, err := backup.AllFilenames(); err != nil {
			t.Fatal(err)
		} else if len(filenames) != 0 {
			t.Fatal("unexpected", filenames)
		}
		if stats, err := backup.UploadStats(); err != nil {
			t.Fatal(err)
		} else if stats.PendingObjects != 0 || stats.UnpinnedObjects != 0 || stats.MultipartUploads != 0 {
			t.Fatal("unexpected", stats)
		} else if stats.UploadedObjects != 2 || objectCount != 2 {
			t.Fatal("unexpected", stats.UploadedObjects, objectCount)
		}
	}

	t.Run("Backup", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		objectCount, err := store.Backup(t.Context(), destPath)
		if err != nil {
			t.Fatal(err)
		}
		checkDatabase(t, destPath, objectCount)

		// the source is untouched and no journal is left next to the copy
		if stats, err := store.UploadStats(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(stats, live) {
			t.Fatal("mismatch", stats)
		} else if entries, err := os.ReadDir(filepath.Dir(destPath)); err != nil {
			t.Fatal(err)
		} else if len(entries) != 1 {
			t.Fatal("unexpected files", entries)
		}
	})

	t.Run("EmptyDestination", func(t *testing.T) {
		if _, err := store.Backup(t.Context(), ""); err == nil {
			t.Fatal("expected error for empty destination path")
		}
	})

	t.Run("ExistingDestination", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.sqlite")
		if objectCount, err := store.Backup(t.Context(), destPath); err != nil {
			t.Fatal(err)
		} else if _, err := store.Backup(t.Context(), destPath); err == nil {
			t.Fatal("expected error for existing destination path")
		} else {
			checkDatabase(t, destPath, objectCount)
		}
	})
}

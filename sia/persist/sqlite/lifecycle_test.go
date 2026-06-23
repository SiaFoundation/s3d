package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestBucketLifecycleConfiguration(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// missing configuration returns ErrNoSuchLifecycleConfiguration; unknown
	// bucket returns ErrNoSuchBucket
	if _, err := store.GetBucketLifecycleConfiguration(accessKeyID, bucket); !errors.Is(err, s3errs.ErrNoSuchLifecycleConfiguration) {
		t.Fatalf("expected ErrNoSuchLifecycleConfiguration, got %v", err)
	} else if err := store.PutBucketLifecycleConfiguration(accessKeyID, "nope", "<x/>"); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatalf("expected ErrNoSuchBucket, got %v", err)
	}

	// store and read back
	const config = "<LifecycleConfiguration></LifecycleConfiguration>"
	if err := store.PutBucketLifecycleConfiguration(accessKeyID, bucket, config); err != nil {
		t.Fatal(err)
	} else if got, err := store.GetBucketLifecycleConfiguration(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	} else if got != config {
		t.Fatalf("expected %q, got %q", config, got)
	}

	// overwrite replaces the configuration
	const updated = "<LifecycleConfiguration><Rule/></LifecycleConfiguration>"
	if err := store.PutBucketLifecycleConfiguration(accessKeyID, bucket, updated); err != nil {
		t.Fatal(err)
	}
	store.assertCount(1, "bucket_lifecycle_configurations")
	if got, err := store.GetBucketLifecycleConfiguration(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	} else if got != updated {
		t.Fatalf("expected %q, got %q", updated, got)
	}

	// AllBucketLifecycleConfigurations returns the bucket and config
	all, err := store.AllBucketLifecycleConfigurations()
	if err != nil {
		t.Fatal(err)
	} else if len(all) != 1 || all[0].Bucket != bucket || all[0].Configuration != updated {
		t.Fatalf("unexpected configurations: %+v", all)
	}

	// delete removes the configuration and is idempotent
	if err := store.DeleteBucketLifecycleConfiguration(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "bucket_lifecycle_configurations")
	if err := store.DeleteBucketLifecycleConfiguration(accessKeyID, bucket); err != nil {
		t.Fatalf("expected delete to be idempotent, got %v", err)
	} else if _, err := store.GetBucketLifecycleConfiguration(accessKeyID, bucket); !errors.Is(err, s3errs.ErrNoSuchLifecycleConfiguration) {
		t.Fatalf("expected ErrNoSuchLifecycleConfiguration, got %v", err)
	}
}

func TestAbortMultipartUploads(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// old upload under the "logs/" prefix with a single part
	oldUpload := s3.NewUploadID()
	if err := store.CreateMultipartUpload(accessKeyID, bucket, "logs/a", oldUpload, nil); err != nil {
		t.Fatal(err)
	} else if _, _, err := store.AddMultipartPart(accessKeyID, bucket, "logs/a", oldUpload, "p1", 1, frand.Entropy128(), 500); err != nil {
		t.Fatal(err)
	}

	// recent upload under the "logs/" prefix
	newUpload := s3.NewUploadID()
	if err := store.CreateMultipartUpload(accessKeyID, bucket, "logs/b", newUpload, nil); err != nil {
		t.Fatal(err)
	}

	// upload under a different prefix
	otherUpload := s3.NewUploadID()
	if err := store.CreateMultipartUpload(accessKeyID, bucket, "data/c", otherUpload, nil); err != nil {
		t.Fatal(err)
	}

	// backdate the old upload by two days
	cutoff := time.Now().Add(-24 * time.Hour)
	if _, err := store.db.Exec("UPDATE multipart_uploads SET created_at = ? WHERE upload_id = ?",
		sqlTime(time.Now().Add(-48*time.Hour)), sqlUploadID(oldUpload)); err != nil {
		t.Fatal(err)
	}

	// only the old "logs/" upload should be aborted
	aborted, err := store.AbortMultipartUploads(bucket, "logs/", cutoff, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(aborted) != 1 {
		t.Fatalf("expected 1 aborted upload, got %d", len(aborted))
	} else if aborted[0].UploadID != oldUpload {
		t.Fatalf("expected upload %v, got %v", oldUpload, aborted[0].UploadID)
	} else if aborted[0].Size != 500 {
		t.Fatalf("expected size 500, got %d", aborted[0].Size)
	}
	store.assertCount(2, "multipart_uploads")
	store.assertCount(0, "multipart_parts") // part removed via cascade
}

func TestExpireObjects(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// pending on-disk object that should expire
	oldFile := "old.obj"
	if _, _, err := store.PutObject(accessKeyID, bucket, "logs/old", frand.Entropy128(), nil, 100, &oldFile); err != nil {
		t.Fatal(err)
	}
	// recent object under the same prefix that should survive
	newFile := "new.obj"
	if _, _, err := store.PutObject(accessKeyID, bucket, "logs/new", frand.Entropy128(), nil, 200, &newFile); err != nil {
		t.Fatal(err)
	}
	// object under a different prefix that should survive
	otherFile := "other.obj"
	if _, _, err := store.PutObject(accessKeyID, bucket, "data/keep", frand.Entropy128(), nil, 300, &otherFile); err != nil {
		t.Fatal(err)
	}

	// backdate the old object
	cutoff := time.Now().Add(-24 * time.Hour)
	if _, err := store.db.Exec("UPDATE objects SET updated_at = ? WHERE name = ?",
		sqlTime(time.Now().Add(-48*time.Hour)), "logs/old"); err != nil {
		t.Fatal(err)
	}

	deleted, orphans, err := store.ExpireObjects(bucket, "logs/", cutoff, 100)
	if err != nil {
		t.Fatal(err)
	} else if deleted != 1 {
		t.Fatalf("expected 1 deleted object, got %d", deleted)
	} else if len(orphans) != 1 || orphans[0].Filename != oldFile || orphans[0].Size != 100 {
		t.Fatalf("unexpected orphans: %+v", orphans)
	}
	store.assertCount(2, "objects")
}

func TestExpireObjectsVersions(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "versioned-bucket"
		key         = "logs/versioned"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	} else if err := store.PutBucketVersioning(accessKeyID, bucket, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	oldVersion, _, err := store.PutObject(accessKeyID, bucket, key, frand.Entropy128(), nil, 100, new(string))
	if err != nil {
		t.Fatal(err)
	}
	currentVersion, _, err := store.PutObject(accessKeyID, bucket, key, frand.Entropy128(), nil, 200, new(string))
	if err != nil {
		t.Fatal(err)
	}

	cutoff := time.Now().Add(-24 * time.Hour)
	if _, err := store.db.Exec("UPDATE objects SET updated_at = ? WHERE name = ? AND version_id = ?",
		sqlTime(time.Now().Add(-48*time.Hour)), key, oldVersion); err != nil {
		t.Fatal(err)
	}

	deleted, orphans, err := store.ExpireObjects(bucket, "logs/", cutoff, 100)
	if err != nil {
		t.Fatal(err)
	} else if deleted != 0 {
		t.Fatalf("expected no deleted objects, got %d", deleted)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans, got %+v", orphans)
	}
	if obj, err := store.GetObject(accessKeyID, bucket, key, s3.NoVersion(), nil); err != nil {
		t.Fatal(err)
	} else if obj.VersionID != currentVersion {
		t.Fatalf("expected current version %q, got %q", currentVersion, obj.VersionID)
	} else if _, err := store.GetObject(accessKeyID, bucket, key, s3.SpecificVersion(oldVersion), nil); err != nil {
		t.Fatalf("expected old noncurrent version to remain, got %v", err)
	}
}

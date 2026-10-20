package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestBucketLifecycleConfiguration(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID(bucket)

	// missing configuration returns ErrNoSuchLifecycleConfiguration; unknown
	// bucket returns ErrNoSuchBucket
	if _, err := store.GetBucketLifecycleConfiguration(bid); !errors.Is(err, s3errs.ErrNoSuchLifecycleConfiguration) {
		t.Fatalf("expected ErrNoSuchLifecycleConfiguration, got %v", err)
	} else if err := store.PutBucketLifecycleConfiguration(-1, "<x/>"); !errors.Is(err, s3errs.ErrNoSuchBucket) {
		t.Fatalf("expected ErrNoSuchBucket, got %v", err)
	}

	// store and read back
	const config = "<LifecycleConfiguration></LifecycleConfiguration>"
	if err := store.PutBucketLifecycleConfiguration(bid, config); err != nil {
		t.Fatal(err)
	} else if got, err := store.GetBucketLifecycleConfiguration(bid); err != nil {
		t.Fatal(err)
	} else if got != config {
		t.Fatalf("expected %q, got %q", config, got)
	}

	// overwrite replaces the configuration
	const updated = "<LifecycleConfiguration><Rule/></LifecycleConfiguration>"
	if err := store.PutBucketLifecycleConfiguration(bid, updated); err != nil {
		t.Fatal(err)
	}
	store.assertCount(1, "bucket_lifecycle_configurations")
	if got, err := store.GetBucketLifecycleConfiguration(bid); err != nil {
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
	if err := store.DeleteBucketLifecycleConfiguration(bid); err != nil {
		t.Fatal(err)
	}
	store.assertCount(0, "bucket_lifecycle_configurations")
	if err := store.DeleteBucketLifecycleConfiguration(bid); err != nil {
		t.Fatalf("expected delete to be idempotent, got %v", err)
	} else if _, err := store.GetBucketLifecycleConfiguration(bid); !errors.Is(err, s3errs.ErrNoSuchLifecycleConfiguration) {
		t.Fatalf("expected ErrNoSuchLifecycleConfiguration, got %v", err)
	}
}

func TestAbortMultipartUploads(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "test-bucket"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID(bucket)

	// old upload under the "logs/" prefix with a single part
	oldUpload := s3.NewUploadID()
	if err := store.CreateMultipartUpload(bid, "logs/a", oldUpload, nil, nil, objects.MultipartChecksum{}); err != nil {
		t.Fatal(err)
	} else if _, _, err := store.AddMultipartPart(bid, "logs/a", oldUpload, "p1", 1, frand.Entropy128(), 500, nil); err != nil {
		t.Fatal(err)
	}

	// recent upload under the "logs/" prefix
	newUpload := s3.NewUploadID()
	if err := store.CreateMultipartUpload(bid, "logs/b", newUpload, nil, nil, objects.MultipartChecksum{}); err != nil {
		t.Fatal(err)
	}

	// upload under a different prefix
	otherUpload := s3.NewUploadID()
	if err := store.CreateMultipartUpload(bid, "data/c", otherUpload, nil, nil, objects.MultipartChecksum{}); err != nil {
		t.Fatal(err)
	}

	// backdate the old upload by two days
	cutoff := time.Now().Add(-24 * time.Hour)
	if _, err := store.db.Exec("UPDATE multipart_uploads SET created_at = ? WHERE upload_id = ?",
		sqlTime(time.Now().Add(-48*time.Hour)), sqlUploadID(oldUpload)); err != nil {
		t.Fatal(err)
	}

	// only the old "logs/" upload should be aborted
	aborted, err := store.AbortMultipartUploads(bid, "logs/", cutoff, 100)
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
	if err := store.CreateBucket(accessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID(bucket)

	// pending on-disk object that should expire
	oldFile := "old.obj"
	if _, _, err := store.PutObject(bid, "logs/old", objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 100, FileName: &oldFile}); err != nil {
		t.Fatal(err)
	}
	// recent object under the same prefix that should survive
	newFile := "new.obj"
	if _, _, err := store.PutObject(bid, "logs/new", objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 200, FileName: &newFile}); err != nil {
		t.Fatal(err)
	}
	// object under a different prefix that should survive
	otherFile := "other.obj"
	if _, _, err := store.PutObject(bid, "data/keep", objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 300, FileName: &otherFile}); err != nil {
		t.Fatal(err)
	}

	// backdate the old object
	cutoff := time.Now().Add(-24 * time.Hour)
	if _, err := store.db.Exec("UPDATE objects SET updated_at = ? WHERE name = ?",
		sqlTime(time.Now().Add(-48*time.Hour)), "logs/old"); err != nil {
		t.Fatal(err)
	}

	deleted, orphans, err := store.ExpireObjects(bid, "logs/", nil, nil, cutoff, 100)
	if err != nil {
		t.Fatal(err)
	} else if deleted != 1 {
		t.Fatalf("expected 1 deleted object, got %d", deleted)
	} else if len(orphans) != 1 || orphans[0].Filename != oldFile || orphans[0].Size != 100 {
		t.Fatalf("unexpected orphans: %+v", orphans)
	}
	store.assertCount(2, "objects")

	// backdate what survived so only the size bounds decide the rest
	if _, err := store.db.Exec("UPDATE objects SET updated_at = ?", sqlTime(time.Now().Add(-48*time.Hour))); err != nil {
		t.Fatal(err)
	}

	// the bounds exclude their own value, so a lower bound of 200 skips the
	// 200 byte object and takes the 300 byte one
	size := func(n int64) *int64 { return &n }
	deleted, orphans, err = store.ExpireObjects(bid, "", size(200), nil, cutoff, 100)
	if err != nil {
		t.Fatal(err)
	} else if deleted != 1 {
		t.Fatalf("expected 1 deleted object, got %d", deleted)
	} else if len(orphans) != 1 || orphans[0].Filename != otherFile {
		t.Fatalf("unexpected orphans: %+v", orphans)
	}
	store.assertCount(1, "objects")

	// an upper bound of 200 excludes the 200 byte object as well
	if deleted, _, err := store.ExpireObjects(bid, "", nil, size(200), cutoff, 100); err != nil {
		t.Fatal(err)
	} else if deleted != 0 {
		t.Fatalf("expected no deleted objects, got %d", deleted)
	}
	store.assertCount(1, "objects")

	if deleted, _, err := store.ExpireObjects(bid, "", nil, size(201), cutoff, 100); err != nil {
		t.Fatal(err)
	} else if deleted != 1 {
		t.Fatalf("expected 1 deleted object, got %d", deleted)
	}
	store.assertCount(0, "objects")
}

func TestExpireObjectsVersions(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "versioned-bucket"
		key         = "logs/versioned"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID(bucket)
	if err := store.PutBucketVersioning(bid, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	oldVersion, _, err := store.PutObject(bid, key, objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 100, FileName: new(string)})
	if err != nil {
		t.Fatal(err)
	}
	currentVersion, _, err := store.PutObject(bid, key, objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 200, FileName: new(string)})
	if err != nil {
		t.Fatal(err)
	}

	cutoff := time.Now().Add(-24 * time.Hour)
	if _, err := store.db.Exec("UPDATE objects SET updated_at = ? WHERE name = ? AND version_id = ?",
		sqlTime(time.Now().Add(-48*time.Hour)), key, oldVersion); err != nil {
		t.Fatal(err)
	}

	deleted, orphans, err := store.ExpireObjects(bid, "logs/", nil, nil, cutoff, 100)
	if err != nil {
		t.Fatal(err)
	} else if deleted != 0 {
		t.Fatalf("expected no deleted objects, got %d", deleted)
	} else if len(orphans) != 0 {
		t.Fatalf("expected no orphans, got %+v", orphans)
	}
	if obj, err := store.GetObject(bid, key, s3.NoVersion(), nil); err != nil {
		t.Fatal(err)
	} else if obj.VersionID != currentVersion {
		t.Fatalf("expected current version %q, got %q", currentVersion, obj.VersionID)
	} else if _, err := store.GetObject(bid, key, s3.SpecificVersion(oldVersion), nil); err != nil {
		t.Fatalf("expected old noncurrent version to remain, got %v", err)
	}
}

func TestExpireNoncurrentVersions(t *testing.T) {
	const accessKeyID = testAccessKeyID

	store := initTestDB(t, zap.NewNop())

	// setup returns a versioned bucket holding four versions of one key, the
	// newest current and the other three noncurrent, oldest first.
	setup := func(bucket string) (bid s3.BucketID, key string, versions []string) {
		t.Helper()
		key = "logs/obj"
		if err := store.CreateBucket(accessKeyID, bucket, false); err != nil {
			t.Fatal(err)
		}
		bid = store.bucketID(bucket)
		if err := store.PutBucketVersioning(bid, s3.VersioningStatusEnabled); err != nil {
			t.Fatal(err)
		}
		for range 4 {
			v, _, err := store.PutObject(bid, key, objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 1, FileName: new(string)})
			if err != nil {
				t.Fatal(err)
			}
			versions = append(versions, v)
		}
		return bid, key, versions
	}
	backdate := func(bid s3.BucketID, key, version string, d time.Duration) {
		t.Helper()
		res, err := store.db.Exec(`UPDATE objects SET noncurrent_at = ? WHERE bucket_id = ? AND name = ? AND version_id = ?`,
			sqlTime(time.Now().Add(-d)), bid, key, version)
		if err != nil {
			t.Fatal(err)
		} else if n, err := res.RowsAffected(); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatalf("expected to backdate one row, got %d", n)
		}
	}
	exists := func(bid s3.BucketID, key, version string) bool {
		t.Helper()
		_, err := store.GetObject(bid, key, s3.SpecificVersion(version), nil)
		if err == nil {
			return true
		} else if errors.Is(err, s3errs.ErrNoSuchVersion) {
			return false
		}
		t.Fatal(err)
		return false
	}

	t.Run("Age", func(t *testing.T) {
		bid, key, v := setup("age")
		// only the two oldest noncurrent versions are past the cutoff
		backdate(bid, key, v[0], 48*time.Hour)
		backdate(bid, key, v[1], 48*time.Hour)

		deleted, _, err := store.ExpireNoncurrentVersions(bid, "logs/", nil, nil, 0, time.Now().Add(-24*time.Hour), 100)
		if err != nil {
			t.Fatal(err)
		} else if deleted != 2 {
			t.Fatalf("expected 2 expired versions, got %d", deleted)
		}
		if exists(bid, key, v[0]) || exists(bid, key, v[1]) {
			t.Fatal("expected the backdated versions to be expired")
		} else if !exists(bid, key, v[2]) || !exists(bid, key, v[3]) {
			t.Fatal("expected the recent noncurrent version and the current version to remain")
		}
	})

	t.Run("RetainCount", func(t *testing.T) {
		bid, key, v := setup("retain")
		// no age bound, so only the retain count decides
		deleted, _, err := store.ExpireNoncurrentVersions(bid, "logs/", nil, nil, 1, time.Now(), 100)
		if err != nil {
			t.Fatal(err)
		} else if deleted != 2 {
			t.Fatalf("expected 2 expired versions, got %d", deleted)
		}
		// v[2] is the newest noncurrent and is the one retained
		if exists(bid, key, v[0]) || exists(bid, key, v[1]) {
			t.Fatal("expected the two oldest noncurrent versions to be expired")
		} else if !exists(bid, key, v[2]) || !exists(bid, key, v[3]) {
			t.Fatal("expected the retained noncurrent version and the current version to remain")
		}
	})

	t.Run("AgeAndRetainCountBothApply", func(t *testing.T) {
		bid, key, v := setup("both")
		// every noncurrent version is old enough, so the count binds
		for _, version := range v[:3] {
			backdate(bid, key, version, 48*time.Hour)
		}
		deleted, _, err := store.ExpireNoncurrentVersions(bid, "logs/", nil, nil, 2, time.Now().Add(-24*time.Hour), 100)
		if err != nil {
			t.Fatal(err)
		} else if deleted != 1 {
			t.Fatalf("expected 1 expired version, got %d", deleted)
		}
		if exists(bid, key, v[0]) {
			t.Fatal("expected the oldest noncurrent version to be expired")
		} else if !exists(bid, key, v[1]) || !exists(bid, key, v[2]) {
			t.Fatal("expected the two retained noncurrent versions to remain")
		}

		// the age bound alone keeps a young version even when the count allows
		if deleted, _, err := store.ExpireNoncurrentVersions(bid, "logs/", nil, nil, 0, time.Now().Add(-72*time.Hour), 100); err != nil {
			t.Fatal(err)
		} else if deleted != 0 {
			t.Fatalf("expected nothing to expire before the cutoff, got %d", deleted)
		}
	})

	t.Run("PrefixAndSizeBounds", func(t *testing.T) {
		bid, key, v := setup("bounds")
		for _, version := range v[:3] {
			backdate(bid, key, version, 48*time.Hour)
		}
		// every version is 1 byte, so an exclusive lower bound of 1 matches none
		size := func(n int64) *int64 { return &n }
		if deleted, _, err := store.ExpireNoncurrentVersions(bid, "logs/", size(1), nil, 0, time.Now(), 100); err != nil {
			t.Fatal(err)
		} else if deleted != 0 {
			t.Fatalf("expected the size bound to exclude every version, got %d", deleted)
		}
		// a prefix that matches nothing also selects nothing
		if deleted, _, err := store.ExpireNoncurrentVersions(bid, "other/", nil, nil, 0, time.Now(), 100); err != nil {
			t.Fatal(err)
		} else if deleted != 0 {
			t.Fatalf("expected the prefix to exclude every version, got %d", deleted)
		}
	})
}

func TestExpireDeleteMarkers(t *testing.T) {
	const (
		accessKeyID = testAccessKeyID
		bucket      = "marker-bucket"
		key         = "logs/obj"
	)

	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket, false); err != nil {
		t.Fatal(err)
	}
	bid := store.bucketID(bucket)
	if err := store.PutBucketVersioning(bid, s3.VersioningStatusEnabled); err != nil {
		t.Fatal(err)
	}

	version, _, err := store.PutObject(bid, key, objects.PutOptions{ContentMD5: frand.Entropy128(), Length: 1, FileName: new(string)})
	if err != nil {
		t.Fatal(err)
	}
	// deleting without a version leaves a delete marker over the old version
	_, isMarker, _, err := store.DeleteObject(bid, s3.ObjectID{Key: key}, false)
	if err != nil {
		t.Fatal(err)
	} else if !isMarker {
		t.Fatal("expected a delete marker")
	}

	// the marker is not expired while another version of the key survives
	if deleted, _, err := store.ExpireDeleteMarkers(bid, "logs/", time.Now(), 100); err != nil {
		t.Fatal(err)
	} else if deleted != 0 {
		t.Fatalf("expected the marker to survive alongside another version, got %d", deleted)
	}

	// once the last version is gone the marker is an expired object delete marker
	if _, _, _, err := store.DeleteObject(bid, s3.ObjectID{Key: key, VersionID: &version}, false); err != nil {
		t.Fatal(err)
	}
	// a cutoff before the marker was created holds it back
	if deleted, _, err := store.ExpireDeleteMarkers(bid, "logs/", time.Now().Add(-time.Hour), 100); err != nil {
		t.Fatal(err)
	} else if deleted != 0 {
		t.Fatalf("expected the marker to survive a cutoff before its creation, got %d", deleted)
	}
	if deleted, _, err := store.ExpireDeleteMarkers(bid, "logs/", time.Now(), 100); err != nil {
		t.Fatal(err)
	} else if deleted != 1 {
		t.Fatalf("expected the marker to be removed, got %d", deleted)
	}
	store.assertCount(0, "objects")
}

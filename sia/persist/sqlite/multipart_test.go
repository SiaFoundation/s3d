package sqlite

import (
	"testing"

	"go.uber.org/zap"
)

func TestMultipart(t *testing.T) {
	const (
		unknownUID  = "a0188aceb938ca67b1d8ac03dfd361e9"
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
	)

	store := initTestDB(t, zap.NewNop())

	assertCount := func(expected int, table string) {
		t.Helper()
		var got int
		row := store.db.QueryRow("SELECT COUNT(*) FROM " + table)
		if err := row.Scan(&got); err != nil {
			t.Fatalf("failed to scan count from %s: %v", table, err)
		}
		if got != expected {
			t.Fatalf("expected %d rows in %s, got %d", expected, table, got)
		}
	}

	// create bucket
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create multipart upload (and assert no error on duplicate creation)
	uid, err := store.CreateMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if _, err := store.CreateMultipartUpload(bucket, object, nil); err != nil {
		t.Fatal(err)
	}
	assertCount(2, "multipart_uploads")

	// assert that the multipart upload exists
	if err := store.HasMultipartUpload(bucket, object, uid); err != nil {
		t.Fatal(err)
	} else if err := store.HasMultipartUpload(bucket, object, unknownUID); err == nil {
		t.Fatal("expected error for unknown upload ID")
	}

	// add a part (assert no error on duplicate part addition)
	if err := store.AddMultipartPart(uid, 1); err != nil {
		t.Fatal(err)
	} else if err := store.AddMultipartPart(uid, 1); err != nil {
		t.Fatal(err)
	}
	assertCount(1, "multipart_parts")

	// finish the part
	if err := store.FinishMultipartPart(uid, 1, [16]byte{1, 2, 3}, &[32]byte{4, 5, 6}, 1234); err != nil {
		t.Fatal(err)
	}

	// TODO: assert behaviour after completing the multipart upload
}

package sqlite

import (
	"fmt"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// GetBucketTagging returns the tag set of the bucket, or
// [s3errs.ErrNoSuchTagSet] when it has none.
func (s *Store) GetBucketTagging(bucket s3.BucketID) (tags s3.TagSet, err error) {
	err = s.transaction(func(tx *txn) error {
		bid, err := existingBucketID(tx, bucket)
		if err != nil {
			return err
		}
		tags, err = queryTags(tx, `SELECT tag_key, tag_value FROM bucket_tags WHERE bucket_id = $1 ORDER BY position`, bid)
		if err != nil {
			return err
		} else if len(tags) == 0 {
			return s3errs.ErrNoSuchTagSet
		}
		return nil
	})
	return
}

// PutBucketTagging replaces the tag set of the bucket. An empty set removes the
// tags.
func (s *Store) PutBucketTagging(bucket s3.BucketID, tags s3.TagSet) error {
	return s.transaction(func(tx *txn) error {
		bid, err := existingBucketID(tx, bucket)
		if err != nil {
			return err
		} else if _, err := tx.Exec(`DELETE FROM bucket_tags WHERE bucket_id = $1`, bid); err != nil {
			return fmt.Errorf("failed to delete bucket tags: %w", err)
		} else if err := insertTags(tx, `INSERT INTO bucket_tags (bucket_id, position, tag_key, tag_value) VALUES ($1, $2, $3, $4)`, tags, bid); err != nil {
			return fmt.Errorf("failed to insert bucket tags: %w", err)
		}
		return nil
	})
}

// DeleteBucketTagging removes the tag set of the bucket. It is not an error if
// the bucket has none.
func (s *Store) DeleteBucketTagging(bucket s3.BucketID) error {
	return s.PutBucketTagging(bucket, nil)
}

// GetObjectTagging returns the tag set of the addressed version.
func (s *Store) GetObjectTagging(bucket s3.BucketID, name string, version s3.VersionRequest) (tagging s3.ObjectTagging, err error) {
	err = s.transaction(func(tx *txn) error {
		tagging = s3.ObjectTagging{} // reset if the transaction retries

		bid, status, err := bucketIDAndVersioning(tx, bucket)
		if err != nil {
			return err
		}
		versionID, err := taggableVersion(tx, bid, name, version)
		if err != nil {
			return err
		}
		tags, err := objectTags(tx, bid, name, versionID)
		if err != nil {
			return err
		}
		tagging = s3.ObjectTagging{
			Tags:      tags,
			Version:   s3.SpecificVersion(versionID),
			VersionID: reportedTaggingVersion(status, versionID, version.Specified),
		}
		return nil
	})
	return
}

// PutObjectTagging replaces the tag set of the addressed version. It creates no
// version and changes neither the version's last-modified time nor its lock.
func (s *Store) PutObjectTagging(bucket s3.BucketID, name string, version s3.VersionRequest, tags s3.TagSet) error {
	return s.transaction(func(tx *txn) error {
		bid, err := existingBucketID(tx, bucket)
		if err != nil {
			return err
		}
		versionID, err := taggableVersion(tx, bid, name, version)
		if err != nil {
			return err
		}
		return setObjectTags(tx, bid, name, versionID, tags)
	})
}

// DeleteObjectTagging removes the tag set of the addressed version.
func (s *Store) DeleteObjectTagging(bucket s3.BucketID, name string, version s3.VersionRequest) error {
	return s.PutObjectTagging(bucket, name, version, nil)
}

// taggableVersion resolves the addressed version, or the current version when
// the request addresses none, to its internal version ID.
func taggableVersion(tx *txn, bid int64, name string, version s3.VersionRequest) (string, error) {
	versionID, _, _, _, err := objectLockRow(tx, bid, name, version)
	return versionID, err
}

// objectTags returns the tags of an object version.
func objectTags(tx *txn, bid int64, name, versionID string) (s3.TagSet, error) {
	return queryTags(tx, `SELECT tag_key, tag_value FROM object_tags WHERE bucket_id = $1 AND name = $2 AND version_id = $3 ORDER BY position`,
		bid, name, versionID)
}

// insertObjectTags stores the tags of a new object version.
func insertObjectTags(tx *txn, bid int64, name, versionID string, tags s3.TagSet) error {
	if err := insertTags(tx, `INSERT INTO object_tags (bucket_id, name, version_id, position, tag_key, tag_value) VALUES ($1, $2, $3, $4, $5, $6)`,
		tags, bid, name, versionID); err != nil {
		return fmt.Errorf("failed to insert object tags: %w", err)
	}
	return nil
}

// insertTags inserts each tag with a query taking the owner columns followed by
// position, tag_key and tag_value.
func insertTags(tx *txn, query string, tags s3.TagSet, owner ...any) error {
	for i, tag := range tags {
		if _, err := tx.Exec(query, append(owner, i, tag.Key, tag.Value)...); err != nil {
			return err
		}
	}
	return nil
}

// setObjectTags replaces the tags of an existing object version.
func setObjectTags(tx *txn, bid int64, name, versionID string, tags s3.TagSet) error {
	if _, err := tx.Exec(`DELETE FROM object_tags WHERE bucket_id = $1 AND name = $2 AND version_id = $3`, bid, name, versionID); err != nil {
		return fmt.Errorf("failed to delete object tags: %w", err)
	}
	return insertObjectTags(tx, bid, name, versionID, tags)
}

// queryTags reads a tag set from a query selecting tag_key and tag_value.
func queryTags(tx *txn, query string, args ...any) (tags s3.TagSet, err error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tag s3.Tag
		if err := rows.Scan(&tag.Key, &tag.Value); err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	return tags, rows.Err()
}

// reportedTaggingVersion is the version ID to report for a tagging request:
// none for the current version of a bucket that never had versioning.
func reportedTaggingVersion(status, v string, specified bool) string {
	if status == "" && !specified {
		return ""
	}
	return s3.FormatVersion(v)
}

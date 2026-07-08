package sqlite

import (
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
)

// CreateSnapshot bumps the snapshot generation and records a snapshot of the
// current uploaded object count. The Sia object ID is filled in later with
// MarkSnapshotPinned once the backup is uploaded.
func (s *Store) CreateSnapshot() (snap objects.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		var gen int64
		if err := tx.QueryRow("UPDATE global_settings SET snapshot_gen = snapshot_gen + 1 RETURNING snapshot_gen").Scan(&gen); err != nil {
			return err
		}
		return tx.QueryRow(`
			INSERT INTO snapshots (created_at, gen, object_count)
			VALUES ($1, $2, (SELECT stat_value FROM stats WHERE stat = $3))
			RETURNING id, created_at, object_count`, sqlTime(time.Now()), gen, statUploadedObjects).Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), &snap.ObjectCount)
	})
	return
}

// MarkSnapshotPinned records the Sia object ID for an uploaded snapshot.
func (s *Store) MarkSnapshotPinned(id int64, objectID types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("UPDATE snapshots SET sia_object_id = $1 WHERE id = $2", sqlHash256(objectID), id)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return objects.ErrSnapshotNotFound
		}
		return nil
	})
}

// ListSnapshots returns all uploaded snapshots ordered by id.
func (s *Store) ListSnapshots() (snapshots []objects.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		snapshots = snapshots[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT id, created_at, sia_object_id, object_count
			FROM snapshots
			WHERE sia_object_id IS NOT NULL
			ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var snap objects.Snapshot
			if err := rows.Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), (*sqlHash256)(&snap.SiaObjectID), &snap.ObjectCount); err != nil {
				return err
			}
			snapshots = append(snapshots, snap)
		}
		return rows.Err()
	})
	return
}

// DeleteSnapshot removes a snapshot from the store. Objects orphaned during its
// lifetime that no longer fall under any surviving snapshot's generation become
// eligible for unpinning on the next orphan loop. It does not unpin the
// snapshot's backup object from the Sia network. Callers exposing snapshot
// deletion must unpin that object themselves or it leaks.
func (s *Store) DeleteSnapshot(snapshotID int64) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM snapshots WHERE id = $1", snapshotID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return objects.ErrSnapshotNotFound
		}
		return nil
	})
}

// DeleteSnapshotsBySiaObject removes snapshots whose backup object matches one
// of the given Sia object IDs and returns the number of snapshots removed.
func (s *Store) DeleteSnapshotsBySiaObject(objectIDs []types.Hash256) (deleted int64, err error) {
	err = s.transaction(func(tx *txn) error {
		deleted = 0 // reset per transaction attempt

		stmt, err := tx.Prepare("DELETE FROM snapshots WHERE sia_object_id = $1")
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, id := range objectIDs {
			res, err := stmt.Exec(sqlHash256(id))
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			deleted += n
		}
		return nil
	})
	return
}

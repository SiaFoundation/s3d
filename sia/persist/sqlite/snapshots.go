package sqlite

import (
	"database/sql"
	"errors"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
)

// CheckSnapshotObject reports whether a snapshot references the given Sia
// object and whether any snapshot upload is still in flight. Both are
// evaluated in a single transaction so a snapshot recorded concurrently is
// always reported as known or in flight.
func (s *Store) CheckSnapshotObject(objectID types.Hash256) (known, inFlight bool, err error) {
	err = s.transaction(func(tx *txn) error {
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM snapshots WHERE sia_object_id = $1)", sqlHash256(objectID)).Scan(&known); err != nil {
			return err
		}
		return tx.QueryRow("SELECT EXISTS(SELECT 1 FROM snapshots WHERE sia_object_id IS NULL)").Scan(&inFlight)
	})
	return
}

// CreateSnapshot bumps the snapshot generation and records a snapshot of the
// current uploaded object count. The Sia object ID is filled in later with
// MarkSnapshotPinned once the snapshot is uploaded.
func (s *Store) CreateSnapshot() (snap s3.Snapshot, gen int64, err error) {
	err = s.transaction(func(tx *txn) error {
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

// AdoptSnapshot records a snapshot for an object discovered on the network,
// e.g. after restoring from a snapshot made by a previous database.
// The generation counter is raised to at least the adopted generation.
// Existing orphans are raised to it even when the counter is already past it:
// the adopted generation is from a different counter history, so it cannot be
// ordered against locally recorded orphans and the adopted snapshot may
// reference any of them. As in MarkSnapshotPinned the counter is then bumped
// once more and recorded as the completion generation. Adopting an object
// that already has a record returns the existing snapshot.
func (s *Store) AdoptSnapshot(objectID types.Hash256, createdAt time.Time, gen, objectCount int64) (snap s3.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		// return the existing record when the object was already adopted
		err := tx.QueryRow(`
			SELECT id, created_at, object_count, sia_object_id FROM snapshots WHERE sia_object_id = $1`,
			sqlHash256(objectID)).Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), &snap.ObjectCount, (*sqlHash256)(&snap.SiaObjectID))
		if err == nil {
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if _, err := tx.Exec("UPDATE orphaned_objects SET orphaned_at_gen = $1 WHERE orphaned_at_gen < $1", gen); err != nil {
			return err
		} else if _, err := tx.Exec("UPDATE global_settings SET snapshot_gen = $1 WHERE snapshot_gen < $1", gen); err != nil {
			return err
		}

		var completed int64
		if err := tx.QueryRow("UPDATE global_settings SET snapshot_gen = snapshot_gen + 1 RETURNING snapshot_gen").Scan(&completed); err != nil {
			return err
		}
		return tx.QueryRow(`
			INSERT INTO snapshots (created_at, gen, object_count, sia_object_id, gen_completed)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id, created_at, object_count, sia_object_id`, sqlTime(createdAt), gen, objectCount, sqlHash256(objectID), completed).Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), &snap.ObjectCount, (*sqlHash256)(&snap.SiaObjectID))
	})
	return
}

// MarkSnapshotPinned records the Sia object ID for an uploaded snapshot. The
// generation counter is bumped once more and recorded as the snapshot's
// completion generation, object rows created at or after it cannot appear in
// the snapshot.
func (s *Store) MarkSnapshotPinned(id int64, objectID types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		var completed int64
		if err := tx.QueryRow("UPDATE global_settings SET snapshot_gen = snapshot_gen + 1 RETURNING snapshot_gen").Scan(&completed); err != nil {
			return err
		}
		res, err := tx.Exec("UPDATE snapshots SET sia_object_id = $1, gen_completed = $2 WHERE id = $3", sqlHash256(objectID), completed, id)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return s3.ErrSnapshotNotFound
		}
		return nil
	})
}

// ListSnapshots returns all uploaded snapshots ordered by id.
func (s *Store) ListSnapshots() (snapshots []s3.Snapshot, err error) {
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
			var snap s3.Snapshot
			if err := rows.Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), (*sqlHash256)(&snap.SiaObjectID), &snap.ObjectCount); err != nil {
				return err
			}
			snapshots = append(snapshots, snap)
		}
		return rows.Err()
	})
	return
}

// DeleteSnapshot removes a snapshot from the store, releasing orphans only it
// was withholding. It does not unpin the snapshot's Sia object from the
// network, callers exposing snapshot deletion must unpin it themselves or it
// leaks. It returns [s3.ErrSnapshotNotFound] if no snapshot with the given
// id exists.
func (s *Store) DeleteSnapshot(snapshotID int64) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM snapshots WHERE id = $1", snapshotID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return s3.ErrSnapshotNotFound
		}
		return nil
	})
}

// DeleteSnapshotsBySiaObject removes snapshots whose Sia object matches the
// given object ID and returns the number of snapshots removed.
func (s *Store) DeleteSnapshotsBySiaObject(objectID types.Hash256) (deleted int64, err error) {
	err = s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM snapshots WHERE sia_object_id = $1", sqlHash256(objectID))
		if err != nil {
			return err
		}
		deleted, err = res.RowsAffected()
		return err
	})
	return
}

// DeleteIncompleteSnapshots removes snapshots that never finished uploading
// and returns the number removed. A snapshot rollback only runs in-process, so
// a row without an object id left over from a crash would withhold orphaned
// objects forever.
func (s *Store) DeleteIncompleteSnapshots() (deleted int64, err error) {
	err = s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM snapshots WHERE sia_object_id IS NULL")
		if err != nil {
			return err
		}
		deleted, err = res.RowsAffected()
		return err
	})
	return
}

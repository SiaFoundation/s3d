package sqlite

import (
	"database/sql"
	"errors"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
)

// Snapshot lifecycle states, transitions only move forward: created until
// the backup object is uploaded, pinning until the pin request succeeds,
// pinned, and deleting until the indexer confirms the object is gone. Every
// transition stamps state_since.
const (
	snapshotStateCreated int64 = iota
	snapshotStatePinning
	snapshotStatePinned
	snapshotStateDeleting
)

// CreateSnapshot bumps the snapshot generation and records a snapshot of the
// current uploaded object count in the created state.
func (s *Store) CreateSnapshot() (snap s3.Snapshot, gen int64, err error) {
	err = s.transaction(func(tx *txn) error {
		var err error
		if gen, err = bumpSnapshotGen(tx); err != nil {
			return err
		}
		return tx.QueryRow(`
			INSERT INTO snapshots (created_at, state, state_since, gen, object_count)
			VALUES ($1, $2, $1, $3, (SELECT stat_value FROM stats WHERE stat = $4))
			RETURNING id, created_at, object_count`, sqlTime(time.Now()), snapshotStateCreated, gen, statUploadedObjects).Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), &snap.ObjectCount)
	})
	return
}

// MarkSnapshotPinning records the Sia object ID for the created snapshot and
// marks it as awaiting its pin. It must be called before the pin request is
// issued.
func (s *Store) MarkSnapshotPinning(snapshotID int64, objectID types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("UPDATE snapshots SET state = $1, sia_object_id = $2, state_since = $3 WHERE id = $4 AND state = $5",
			snapshotStatePinning, sqlHash256(objectID), sqlTime(time.Now()), snapshotID, snapshotStateCreated)
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

// MarkSnapshotPinned marks the snapshot with the given Sia object ID pinned.
// The generation counter is bumped once more and recorded as the snapshot's
// completion generation, object rows created at or after it cannot appear in
// the backup. Only a snapshot awaiting its pin is completed.
func (s *Store) MarkSnapshotPinned(objectID types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		var id int64
		err := tx.QueryRow("SELECT id FROM snapshots WHERE sia_object_id = $1 AND state = $2",
			sqlHash256(objectID), snapshotStatePinning).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return objects.ErrSnapshotNotFound
		} else if err != nil {
			return err
		}
		completed, err := bumpSnapshotGen(tx)
		if err != nil {
			return err
		}
		_, err = tx.Exec("UPDATE snapshots SET state = $1, gen_completed = $2, state_since = $3 WHERE id = $4",
			snapshotStatePinned, completed, sqlTime(time.Now()), id)
		return err
	})
}

// AdoptSnapshot records a snapshot for a backup object discovered on the
// network, e.g. after restoring from a backup made by a previous database.
// The generation counter is raised to at least the adopted generation and
// existing orphans are raised to it even when the counter is already past it.
// The counter is then bumped once more and recorded as the completion
// generation. Adopting an object that already has a record returns the
// existing snapshot.
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

		completed, err := bumpSnapshotGen(tx)
		if err != nil {
			return err
		}
		return tx.QueryRow(`
			INSERT INTO snapshots (created_at, state, state_since, gen, object_count, sia_object_id, gen_completed)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING id, created_at, object_count, sia_object_id`, sqlTime(createdAt), snapshotStatePinned, sqlTime(time.Now()), gen, objectCount, sqlHash256(objectID), completed).Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), &snap.ObjectCount, (*sqlHash256)(&snap.SiaObjectID))
	})
	return
}

// RollbackSnapshot removes a snapshot whose backup was never uploaded and
// marks a snapshot whose pin may have reached the network for deletion
// instead.
func (s *Store) RollbackSnapshot(snapshotID int64) error {
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM snapshots WHERE id = $1 AND state = $2", snapshotID, snapshotStateCreated)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n > 0 {
			return nil
		}
		_, err = tx.Exec("UPDATE snapshots SET state = $1, state_since = $2 WHERE id = $3 AND state IN ($4, $5)",
			snapshotStateDeleting, sqlTime(time.Now()), snapshotID, snapshotStatePinning, snapshotStatePinned)
		return err
	})
}

// RollbackIncompleteSnapshots removes snapshots whose backup was never
// uploaded and returns the number removed. Snapshots awaiting their pin are
// left alone, the sync loop completes them or the deletion pass reaps them.
func (s *Store) RollbackIncompleteSnapshots() (deleted int64, err error) {
	err = s.transaction(func(tx *txn) error {
		res, err := tx.Exec("DELETE FROM snapshots WHERE state = $1", snapshotStateCreated)
		if err != nil {
			return err
		}
		deleted, err = res.RowsAffected()
		return err
	})
	return
}

// SnapshotsForDeletion returns the Sia object IDs of snapshots marked for
// deletion and of snapshots still awaiting their pin, if they entered that
// state at or before the given time.
func (s *Store) SnapshotsForDeletion(before time.Time) (ids []types.Hash256, err error) {
	err = s.transaction(func(tx *txn) error {
		ids = ids[:0] // reuse same slice if transaction retries
		rows, err := tx.Query("SELECT sia_object_id FROM snapshots WHERE state IN ($1, $2) AND state_since <= $3",
			snapshotStatePinning, snapshotStateDeleting, sqlTime(before))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id types.Hash256
			if err := rows.Scan((*sqlHash256)(&id)); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	})
	return
}

// DeleteSnapshotsBySiaObject removes snapshots whose backup object matches the
// given Sia object ID and returns the number of snapshots removed.
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

// ListSnapshots returns all pinned snapshots ordered by id.
func (s *Store) ListSnapshots() (snapshots []s3.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		snapshots = snapshots[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT id, created_at, sia_object_id, object_count
			FROM snapshots
			WHERE state = $1
			ORDER BY id`, snapshotStatePinned)
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

// HasSnapshotObject reports whether a snapshot in any state references the
// given Sia object.
func (s *Store) HasSnapshotObject(objectID types.Hash256) (known bool, err error) {
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow("SELECT EXISTS(SELECT 1 FROM snapshots WHERE sia_object_id = $1)",
			sqlHash256(objectID)).Scan(&known)
	})
	return
}

func bumpSnapshotGen(tx *txn) (gen int64, err error) {
	err = tx.QueryRow("UPDATE global_settings SET snapshot_gen = snapshot_gen + 1 RETURNING snapshot_gen").Scan(&gen)
	return
}

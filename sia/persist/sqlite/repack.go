package sqlite

import (
	"fmt"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.sia.tech/core/types"
)

// RepackSnapshot returns a read-only point-in-time view of slab occupancy and
// the Sia objects referencing each slab. Object sizes are their full logical
// size, not merely the bytes they occupy in a particular slab.
func (s *Store) RepackSnapshot() (snapshot objects.RepackSnapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		snapshot = objects.RepackSnapshot{}

		rows, err := tx.Query(`
			WITH slice_usage AS (
				SELECT slab_id, COALESCE(SUM(length), 0) AS live_bytes
				FROM sia_slab_slices
				GROUP BY slab_id
			), sector_usage AS (
				SELECT slab_id, COUNT(*) AS sectors
				FROM sia_slab_sectors
				GROUP BY slab_id
			)
			SELECT s.id, s.min_shards, COALESCE(sec.sectors, 0), COALESCE(su.live_bytes, 0)
			FROM sia_slabs s
			LEFT JOIN slice_usage su ON su.slab_id = s.id
			LEFT JOIN sector_usage sec ON sec.slab_id = s.id
			ORDER BY s.id
		`)
		if err != nil {
			return fmt.Errorf("failed to query slab occupancy: %w", err)
		}

		slabIndex := make(map[types.Hash256]int)
		for rows.Next() {
			var id sqlHash256
			var minShards, totalShards int64
			var liveBytes int64
			if err := rows.Scan(&id, &minShards, &totalShards, &liveBytes); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan slab occupancy: %w", err)
			}
			slabID := types.Hash256(id)
			slabIndex[slabID] = len(snapshot.Slabs)
			snapshot.Slabs = append(snapshot.Slabs, objects.RepackSlab{
				ID:          slabID,
				MinShards:   int(minShards),
				TotalShards: int(totalShards),
				LiveBytes:   liveBytes,
			})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("failed to read slab occupancy: %w", err)
		}
		rows.Close()

		rows, err = tx.Query(`
			WITH object_sizes AS (
				SELECT sia_object_id, COALESCE(SUM(length), 0) AS size
				FROM sia_slab_slices
				GROUP BY sia_object_id
			), object_ready AS (
				SELECT so.id,
					NOT EXISTS (
						SELECT 1 FROM unpinned_objects u WHERE u.sia_object_id = so.id
					)
					AND EXISTS (
						SELECT 1 FROM objects o WHERE o.sia_object_id = so.id
					)
					AND NOT EXISTS (
						SELECT 1 FROM objects o
						WHERE o.sia_object_id = so.id AND o.filename IS NOT NULL
					) AS ready
				FROM sia_objects so
			), refs AS (
				SELECT DISTINCT slab_id, sia_object_id
				FROM sia_slab_slices
			)
			SELECT
				r.slab_id,
				r.sia_object_id,
				COALESCE((
					SELECT b.name || '/' || o.name
					FROM objects o
					JOIN buckets b ON b.id = o.bucket_id
					WHERE o.sia_object_id = r.sia_object_id
					ORDER BY o.is_latest DESC, o.updated_at DESC
					LIMIT 1
				), '') AS label,
				os.size,
				rd.ready
			FROM refs r
			JOIN object_sizes os ON os.sia_object_id = r.sia_object_id
			JOIN object_ready rd ON rd.id = r.sia_object_id
			ORDER BY r.slab_id, r.sia_object_id
		`)
		if err != nil {
			return fmt.Errorf("failed to query slab object references: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var slabID, objectID sqlHash256
			var label string
			var size int64
			var ready bool
			if err := rows.Scan(&slabID, &objectID, &label, &size, &ready); err != nil {
				return fmt.Errorf("failed to scan slab object reference: %w", err)
			}
			idx, ok := slabIndex[types.Hash256(slabID)]
			if !ok {
				continue
			}
			snapshot.Slabs[idx].ObjectRefs = append(snapshot.Slabs[idx].ObjectRefs, objects.RepackObjectRef{
				ID:    types.Hash256(objectID),
				Label: label,
				Size:  size,
				Ready: ready,
			})
		}
		return rows.Err()
	})
	return
}

// SiaObject returns the sealed metadata for a Sia object by ID.
func (s *Store) SiaObject(id types.Hash256) (result objects.SiaObject, err error) {
	err = s.transaction(func(tx *txn) error {
		sealed, err := siaObject(tx, id)
		if err != nil {
			return err
		}
		result = objects.SiaObject{ID: id, Sealed: sealed}
		return nil
	})
	return
}

// ReplaceSiaObjects atomically switches every live S3 metadata reference from
// the old Sia object IDs to their newly pinned replacements. The old IDs are
// then handed to the normal snapshot-generation-aware orphan machinery.
func (s *Store) ReplaceSiaObjects(replacements []objects.RepackReplacement) error {
	if len(replacements) == 0 {
		return nil
	}

	oldIDs := make(map[types.Hash256]struct{}, len(replacements))
	for _, replacement := range replacements {
		if _, exists := oldIDs[replacement.OldID]; exists {
			return fmt.Errorf("duplicate repack source object %v", replacement.OldID)
		}
		oldIDs[replacement.OldID] = struct{}{}
	}
	for _, replacement := range replacements {
		newID := replacement.New.ID()
		if newID != replacement.OldID {
			if _, conflicts := oldIDs[newID]; conflicts {
				return fmt.Errorf("repack replacement %v targets another source object %v", replacement.OldID, newID)
			}
		}
	}

	return s.transaction(func(tx *txn) error {
		// Validate the complete plan before writing anything. If an object was
		// deleted or replaced while the repack upload was in flight, leave all
		// old metadata untouched and let the caller discard the new pins.
		for _, replacement := range replacements {
			var refs int64
			if err := tx.QueryRow(`
				SELECT COUNT(*) FROM objects WHERE sia_object_id = $1
			`, sqlHash256(replacement.OldID)).Scan(&refs); err != nil {
				return err
			} else if refs == 0 {
				return fmt.Errorf("%w: repack source %v", objects.ErrObjectNotFound, replacement.OldID)
			}
		}

		for _, replacement := range replacements {
			if replacement.New.ID() == replacement.OldID {
				continue
			}
			if err := upsertSiaObject(tx, replacement.New); err != nil {
				return fmt.Errorf("failed to persist repacked object %v: %w", replacement.OldID, err)
			}
		}

		for _, replacement := range replacements {
			newID := replacement.New.ID()
			if newID == replacement.OldID {
				continue
			}
			if _, err := tx.Exec(`
				UPDATE objects
				SET sia_object_id = $1
				WHERE sia_object_id = $2
			`, sqlHash256(newID), sqlHash256(replacement.OldID)); err != nil {
				return fmt.Errorf("failed to switch repacked object %v: %w", replacement.OldID, err)
			}
		}

		for _, replacement := range replacements {
			if replacement.New.ID() == replacement.OldID {
				continue
			}
			if err := insertOrphan(tx, replacement.OldID); err != nil {
				return fmt.Errorf("failed to queue old repacked object %v for cleanup: %w", replacement.OldID, err)
			}
		}
		return nil
	})
}

// QueueOrphanedObject records a pinned replacement that was never committed to
// live metadata. Both generation stamps use the current generation, which is
// conservative with respect to in-flight and completed snapshots.
func (s *Store) QueueOrphanedObject(id types.Hash256) error {
	if id == (types.Hash256{}) {
		return nil
	}
	return s.transaction(func(tx *txn) error {
		res, err := tx.Exec(`
			INSERT OR IGNORE INTO orphaned_objects (sia_object_id, orphaned_at_gen, created_at_gen)
			SELECT $1, snapshot_gen, snapshot_gen
			FROM global_settings
			LIMIT 1
		`, sqlHash256(id))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		} else if n == 0 {
			return nil
		}
		return incrementStat(tx, statOrphanedObjects, 1)
	})
}

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/SiaFoundation/s3d/sia/objects"
	"go.uber.org/zap"
)

// CreateSnapshot records a snapshot of the current objects in the store and
// writes a database backup to destPath. It bumps the snapshot generation so any
// object orphaned while the backup references it is withheld from the orphan
// loop until the snapshot is deleted. If the backup fails the snapshot is rolled
// back.
func (s *Store) CreateSnapshot(ctx context.Context, destPath string) error {
	if destPath == "" {
		return errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}

	// hold the single connection across the generation bump and the backup so
	// no writer can orphan an object the backup references before it is stamped
	// with this snapshot's generation
	conn, err := sqlConn(ctx, s.db)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	var snapshotID int64
	err = doTransaction(ctx, conn, s.log.Named("snapshot"), func(tx *txn) error {
		// bump the generation before the backup runs so every object orphaned
		// from now on is stamped at or above this snapshot's generation
		var gen int64
		if err := tx.QueryRow("UPDATE global_settings SET snapshot_generation = snapshot_generation + 1 RETURNING snapshot_generation").Scan(&gen); err != nil {
			return err
		}
		return tx.QueryRow(`
			INSERT INTO snapshots (created_at, path, gen, object_count)
			VALUES ($1, $2, $3, (SELECT stat_value FROM stats WHERE stat = $4))
			RETURNING id`, sqlTime(time.Now()), destPath, gen, statUploadedObjects).Scan(&snapshotID)
	})
	if err != nil {
		conn.Close()
		return err
	}

	backupErr := execBackup(ctx, conn, destPath)
	// release the held connection before DeleteSnapshot, which needs it back
	conn.Close()
	if backupErr != nil {
		if dErr := s.DeleteSnapshot(snapshotID); dErr != nil {
			s.log.Error("failed to roll back snapshot after backup error", zap.Int64("snapshotID", snapshotID), zap.Error(dErr))
		}
		return fmt.Errorf("failed to create backup: %w", backupErr)
	}
	return nil
}

// ListSnapshots returns all recorded snapshots ordered by id, each with the
// number of objects it captured.
func (s *Store) ListSnapshots() (snapshots []objects.Snapshot, err error) {
	err = s.transaction(func(tx *txn) error {
		snapshots = snapshots[:0] // reuse same slice if transaction retries
		rows, err := tx.Query(`
			SELECT id, created_at, path, object_count
			FROM snapshots
			ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var snap objects.Snapshot
			if err := rows.Scan(&snap.ID, (*sqlTime)(&snap.CreatedAt), &snap.Path, &snap.ObjectCount); err != nil {
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
// eligible for unpinning on the next orphan loop.
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

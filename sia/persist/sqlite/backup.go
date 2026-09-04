package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"
)

// Backup writes a consistent, compacted copy of the database to destPath
// using VACUUM INTO on a dedicated readonly connection, so the store keeps
// serving reads and writes while the backup runs. The copy is verified with
// an integrity check before it is moved to destPath.
func (s *Store) Backup(ctx context.Context, destPath string) (err error) {
	// prevent overwriting the destination file
	if destPath == "" {
		return errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}

	// a dedicated readonly connection leaves the store's connection free, a
	// reader never blocks the writer in WAL mode
	src, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", s.path, time.Minute.Milliseconds()))
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer src.Close()

	// vacuum into a temporary file so destPath only ever holds a verified copy
	tmpPath := destPath + ".tmp"
	defer func() {
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := src.ExecContext(ctx, "VACUUM INTO ?", tmpPath); err != nil {
		return fmt.Errorf("failed to vacuum into backup: %w", err)
	}

	// verify the copy before moving it into place
	backup, err := sql.Open("sqlite3", "file:"+tmpPath+"?mode=ro")
	if err != nil {
		return fmt.Errorf("failed to open backup: %w", err)
	}
	var result string
	err = backup.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result)
	backup.Close()
	if err != nil {
		return fmt.Errorf("failed to check backup integrity: %w", err)
	} else if result != "ok" {
		return fmt.Errorf("backup failed integrity check: %s", result)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("failed to move backup into place: %w", err)
	}
	return nil
}

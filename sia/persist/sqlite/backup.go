package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
)

// Backup writes a consistent copy of the database to destPath using VACUUM
// INTO. The copy runs over a dedicated connection, so it reads concurrently
// with the store's own connection and writes proceed for its duration. The
// destination is a freshly compacted database rather than a byte-for-byte image.
func (s *Store) Backup(ctx context.Context, destPath string) error {
	// prevent overwriting the destination file
	if destPath == "" {
		return errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}

	// open a dedicated connection to the source so VACUUM INTO runs as a
	// concurrent reader and the store's connection stays free for writers
	src, err := sql.Open("sqlite3", sqliteFilepath(s.path))
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer src.Close()

	if _, err := src.ExecContext(ctx, "VACUUM INTO ?", destPath); err != nil {
		// remove any partial output so the path can be reused
		_ = os.Remove(destPath)
		return fmt.Errorf("failed to back up database: %w", err)
	}
	return nil
}

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/mattn/go-sqlite3"
)

// sqlConn returns a dedicated connection from db.
func sqlConn(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}
	return conn, nil
}

// withSQLiteConn exposes conn's underlying SQLite connection for the duration
// of fn.
func withSQLiteConn(conn *sql.Conn, fn func(*sqlite3.SQLiteConn) error) error {
	return conn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return errors.New("connection is not a SQLiteConn")
		}
		return fn(c)
	})
}

// Backup creates a backup of the open database at destPath using the SQLite
// backup API. The backup runs over the store's own connection, so writes to
// the database are blocked for the duration of the backup but the snapshot is
// always consistent.
func (s *Store) Backup(ctx context.Context, destPath string) error {
	// initialize the source conn
	srcConn, err := sqlConn(ctx, s.db)
	if err != nil {
		return fmt.Errorf("failed to create source connection: %w", err)
	}
	defer srcConn.Close()
	return execBackup(ctx, srcConn, destPath)
}

// execBackup writes a backup of the database read through srcConn to destPath
// using the SQLite backup API. The caller retains ownership of srcConn, which
// lets a caller hold a single connection across an enclosing write and the
// backup so no other writer can interleave.
func execBackup(ctx context.Context, srcConn *sql.Conn, destPath string) (err error) {
	// prevent overwriting the destination file
	if destPath == "" {
		return errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); err == nil {
		return errors.New("destination file already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}

	// create the destination database
	dest, err := sql.Open("sqlite3", sqliteFilepath(destPath))
	if err != nil {
		return fmt.Errorf("failed to open destination database: %w", err)
	}
	defer func() {
		// errors are ignored
		dest.Close()
		if err != nil {
			// remove the destination file(s) if an error occurred during backup
			_ = os.Remove(destPath)
			_ = os.Remove(destPath + "-wal")
			_ = os.Remove(destPath + "-shm")
		}
	}()

	// initialize the destination conn
	destConn, err := sqlConn(ctx, dest)
	if err != nil {
		return fmt.Errorf("failed to create destination connection: %w", err)
	}
	defer destConn.Close()

	return withSQLiteConn(destConn, func(dc *sqlite3.SQLiteConn) error {
		return withSQLiteConn(srcConn, func(sc *sqlite3.SQLiteConn) (err error) {
			// start the backup
			backup, err := dc.Backup("main", sc, "main")
			if err != nil {
				return fmt.Errorf("failed to create backup: %w", err)
			}
			// ensure the backup is closed, surfacing the error only if the
			// backup itself otherwise succeeded
			defer func() {
				if ferr := backup.Finish(); ferr != nil && err == nil {
					err = fmt.Errorf("failed to finish backup: %w", ferr)
				}
			}()

			for step := 1; ; step++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				// copy a batch of pages per step rather than the whole
				// database at once so we can honor context cancellation
				if done, err := backup.Step(100); err != nil {
					return fmt.Errorf("backup step %d failed: %w", step, err)
				} else if done {
					break
				}
			}
			return nil
		})
	})
}

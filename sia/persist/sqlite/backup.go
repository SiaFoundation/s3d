package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/mattn/go-sqlite3"
)

// Backup creates a backup of the open database. The backup is created using
// the SQLite backup API, which is safe to use with a live database.
func (s *Store) Backup(ctx context.Context, destPath string) error {
	return Backup(ctx, s.path, destPath)
}

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

// backupDB is a helper function that creates a backup of the source database at
// the specified path. The backup is created using the SQLite backup API, which
// is safe to use with a live database.
func backupDB(ctx context.Context, src *sql.DB, destPath string) (err error) {
	// create the destination database
	dest, err := sql.Open("sqlite3", sqliteFilepath(destPath))
	if err != nil {
		return fmt.Errorf("failed to open destination database: %w", err)
	}
	defer func() {
		// errors are ignored
		dest.Close()
		if err != nil {
			// remove the destination file if an error occurred during backup
			os.Remove(destPath)
		}
	}()

	// initialize the source conn
	srcConn, err := sqlConn(ctx, src)
	if err != nil {
		return fmt.Errorf("failed to create source connection: %w", err)
	}
	defer srcConn.Close()

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

				if done, err := backup.Step(-1); err != nil {
					return fmt.Errorf("backup step %d failed: %w", step, err)
				} else if done {
					break
				}
			}
			return nil
		})
	})
}

// Backup creates a backup of the database at the specified path. The backup is
// created using the SQLite backup API, which is safe to use with a
// live database.
//
// This function should be used if the database is not already open in the
// current process. If the database is already open, use Store.Backup.
func Backup(ctx context.Context, srcPath, destPath string) (err error) {
	// ensure the source file exists
	if _, err := os.Stat(srcPath); err != nil {
		return fmt.Errorf("source file does not exist: %w", err)
	}

	// prevent overwriting the destination file
	if destPath == "" {
		return errors.New("empty destination path")
	} else if _, err := os.Stat(destPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("destination file already exists")
	}

	// open a new connection to the source database. We don't want to run
	// any migrations or other operations on the source database since it
	// might be open in another process.
	src, err := sql.Open("sqlite3", sqliteFilepath(srcPath))
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer src.Close()

	return backupDB(ctx, src, destPath)
}

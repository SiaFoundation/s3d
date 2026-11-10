package main

import (
	"flag"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/objects"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/jape"
)

func TestSnapshotScheduleCommands(t *testing.T) {
	backend, store := testutil.NewBackend(t)
	server := httptest.NewServer(jape.BasicAuth("password")(s3.NewAdmin(backend)))
	t.Cleanup(server.Close)
	orig, origFile := cfg, configFile
	t.Cleanup(func() { cfg, configFile = orig, origFile })
	configFile = "s3d.yml"
	cfg.AdminAddress = strings.TrimPrefix(server.URL, "http://")
	cfg.AdminPassword = "password"
	cfg.Directory = filepath.Join(t.TempDir(), "unused")

	cmd := flag.NewFlagSet("set", flag.ContinueOnError)
	if err := cmd.Parse([]string{"@daily"}); err != nil {
		t.Fatal(err)
	}
	runSnapshotsScheduleSet(t.Context(), cmd, s3.SnapshotSchedule{KeepDaily: 7})
	if sched, err := store.SnapshotSchedule(); err != nil {
		t.Fatal(err)
	} else if sched.Expression != "@daily" || sched.KeepDaily != 7 {
		t.Fatal("unexpected", sched)
	}

	runSnapshotsSchedule(t.Context(), flag.NewFlagSet("schedule", flag.ContinueOnError))
	runSnapshotsScheduleDisable(t.Context(), flag.NewFlagSet("disable", flag.ContinueOnError))
	if sched, err := store.SnapshotSchedule(); err != nil {
		t.Fatal(err)
	} else if sched.Expression != "" {
		t.Fatal("unexpected", sched)
	} else if _, err := os.Stat(cfg.Directory); !os.IsNotExist(err) {
		t.Fatal("unexpected", err)
	}
}

func TestPrepareRestoreDir(t *testing.T) {
	root := t.TempDir()

	// a missing directory is created, parents included
	dir := filepath.Join(root, "a", "b")
	if err := prepareRestoreDir(dir); err != nil {
		t.Fatal(err)
	} else if info, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	} else if !info.IsDir() {
		t.Fatal("expected a directory")
	}

	// an empty directory is accepted, so making one first still works
	if err := prepareRestoreDir(dir); err != nil {
		t.Fatal(err)
	}

	// anything already in the directory blocks the restore
	dbPath := filepath.Join(dir, "s3d.db")
	if err := os.WriteFile(dbPath, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareRestoreDir(dir); err == nil {
		t.Fatal("expected an error")
	} else if !strings.Contains(err.Error(), "not empty") {
		t.Fatal("unexpected", err)
	}

	// and the database that was there is left untouched
	if data, err := os.ReadFile(dbPath); err != nil {
		t.Fatal(err)
	} else if string(data) != "existing" {
		t.Fatal("unexpected", string(data))
	}

	// a path that is not a directory is rejected rather than written over
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareRestoreDir(file); err == nil {
		t.Fatal("expected an error")
	}
}

func TestCheckSnapshotVersion(t *testing.T) {
	remote := func(dbVersion int64) sia.RemoteSnapshot {
		return sia.RemoteSnapshot{
			Metadata: objects.SnapshotMetadata{
				Type:      objects.SnapshotType,
				CreatedAt: time.Now(),
				DBVersion: dbVersion,
				Encoding:  objects.SnapshotEncodingGzip,
			},
		}
	}

	// this build restores its own schema, and an older one that startup
	// migrates
	version := sqlite.SchemaVersion()
	if err := checkSnapshotVersion(remote(version)); err != nil {
		t.Fatal(err)
	} else if err := checkSnapshotVersion(remote(version - 1)); err != nil {
		t.Fatal(err)
	}

	// a snapshot written by a newer build cannot be restored
	if err := checkSnapshotVersion(remote(version + 1)); err == nil {
		t.Fatal("expected an error")
	} else if !strings.Contains(err.Error(), "newer") {
		t.Fatal("unexpected", err)
	}

	// nor one compressed with an encoding this build does not know
	foreign := remote(version)
	foreign.Metadata.Encoding = "zstd"
	if err := checkSnapshotVersion(foreign); err == nil {
		t.Fatal("expected an error")
	} else if !strings.Contains(err.Error(), "encoding") {
		t.Fatal("unexpected", err)
	}
}

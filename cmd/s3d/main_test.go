package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateServerConfig(t *testing.T) {
	valid := Config{
		ApiAddress:    "127.0.0.1:8000",
		AdminAddress:  "127.0.0.1:8001",
		AdminPassword: "password",
		Sia: Sia{
			DataShards:   10,
			ParityShards: 20,
		},
	}

	if err := validateServerConfig(valid); err != nil {
		t.Fatal(err)
	}

	// reject redundancy the SDK would fail on later
	invalid := []struct {
		dataShards   uint8
		parityShards uint8
	}{
		{0, 20},
		{10, 0},
		{10, 4},
		{10, 60},
		{200, 200},
	}

	for _, tc := range invalid {
		c := valid
		c.Sia.DataShards = tc.dataShards
		c.Sia.ParityShards = tc.parityShards
		if err := validateServerConfig(c); err == nil {
			t.Fatal("expected error", tc.dataShards, tc.parityShards)
		}
	}
}

func TestTryLoadConfig(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes do not restrict reads on windows")
	} else if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}

	orig := cfg
	t.Cleanup(func() { cfg = orig })

	unreadable := t.TempDir()
	if err := os.WriteFile(filepath.Join(unreadable, "s3d.yml"), []byte("directory: /unreadable\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Chdir(unreadable)

	dataDir := t.TempDir()
	readable := filepath.Join(dataDir, "s3d.yml")
	if err := os.WriteFile(readable, []byte("directory: /marker\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(configFileEnvVar, "")
	t.Setenv(dataDirEnvVar, dataDir)

	if fp := tryLoadConfig(); fp != readable {
		t.Fatal("unexpected path", fp)
	} else if cfg.Directory != "/marker" {
		t.Fatal("unexpected directory", cfg.Directory)
	}
}

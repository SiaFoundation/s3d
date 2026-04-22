//go:build !windows

package sia

import (
	"errors"
	"os"
)

func openFileAllowDelete(path string) (*os.File, error) {
	return os.Open(path)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

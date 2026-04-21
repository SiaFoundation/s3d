//go:build !windows

package sia

import "os"

func openFileAllowDelete(path string) (*os.File, error) {
	return os.Open(path)
}

package sia

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/SiaFoundation/s3d/sia/objects"
	"lukechampine.com/frand"
)

// TestOpenAndRemoveUpload tests that an upload file can be removed while it is
// still open, and the open handle can still be read from and matches the
// original data.
func TestOpenAndRemoveUpload(t *testing.T) {
	dir := t.TempDir()
	uploadsDir := filepath.Join(dir, UploadsDirectory)
	if err := os.MkdirAll(uploadsDir, 0700); err != nil {
		t.Fatal(err)
	}

	s := &Sia{directory: dir}

	// write random data to an upload file
	data := frand.Bytes(256)
	fileName := "test-upload"
	if err := os.WriteFile(filepath.Join(uploadsDir, fileName), data, 0600); err != nil {
		t.Fatal(err)
	}

	// open the upload
	obj := &objects.Object{FileName: &fileName}
	rc, err := s.openUpload("bucket", "name", obj, 0)
	if err != nil {
		t.Fatal(err)
	}

	// remove the upload while the file handle is still open
	if err := s.removeUpload(fileName); err != nil {
		t.Fatal(err)
	}

	// read from the still-open handle and compare
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()

	if !bytes.Equal(got, data) {
		t.Fatal("data read from open handle does not match original")
	}
}

package objects

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// MultipartReader provides a sequential reader over the parts of a multipart
// upload, it is not safe for concurrent use.
type MultipartReader struct {
	partsDir       string
	remainingParts []Part

	curr     *os.File
	currHash hash.Hash
	currPart Part
}

// NewReader creates a new Reader for the given multipart upload starting at
// the given byte offset. Parts that fall entirely before the offset are
// skipped, and within the first relevant part the file is seeked past the
// remaining bytes.
func NewReader(partsDir string, parts []Part, offset int64) (*MultipartReader, error) {
	for _, part := range parts {
		partPath := filepath.Join(partsDir, fmt.Sprintf("%d", part.PartNumber), part.Filename)
		if _, err := os.Stat(partPath); err != nil {
			return nil, fmt.Errorf("failed to stat part file %d: %w", part.PartNumber, err)
		}
	}

	// skip whole parts before offset
	for len(parts) > 0 && offset >= parts[0].Size {
		offset -= parts[0].Size
		parts = parts[1:]
	}

	r := &MultipartReader{
		remainingParts: parts,
		partsDir:       partsDir,
	}

	// discard remaining bytes within the first part so the hasher stays correct
	if offset > 0 && len(parts) > 0 {
		if _, err := io.CopyN(io.Discard, r, offset); err != nil {
			r.Close()
			return nil, fmt.Errorf("failed to skip to offset: %w", err)
		}
	}
	return r, nil
}

// Read reads data from the multipart upload parts sequentially.
func (r *MultipartReader) Read(p []byte) (int, error) {
	// no current part, open next
	if r.curr == nil {
		if err := r.openNext(); err != nil {
			return 0, err // io.EOF if no more parts
		}
	}

	n, err := r.curr.Read(p)
	if n > 0 {
		_, _ = r.currHash.Write(p[:n])
	}
	if errors.Is(err, io.EOF) {
		return n, r.finishPart() // ignore EOF, try next part
	}
	return n, err
}

// Close closes the reader and any open part file.
func (r *MultipartReader) Close() error {
	if r.curr == nil {
		return nil
	}
	return r.curr.Close()
}

func (r *MultipartReader) openNext() error {
	if len(r.remainingParts) == 0 {
		return io.EOF
	}

	part := r.remainingParts[0]
	r.remainingParts = r.remainingParts[1:]

	var err error
	path := filepath.Join(r.partsDir, strconv.Itoa(part.PartNumber), part.Filename)
	r.curr, err = os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open part at %s: %w", path, err)
	}

	stat, err := r.curr.Stat()
	if err != nil {
		r.curr.Close()
		return fmt.Errorf("failed to stat part %d: %w", part.PartNumber, err)
	}
	if stat.Size() != part.Size {
		r.curr.Close()
		return fmt.Errorf("part %d size mismatch: file is %d bytes, expected %d", part.PartNumber, stat.Size(), part.Size)
	}

	r.currHash = md5.New()
	r.currPart = part
	return nil
}

func (r *MultipartReader) finishPart() error {
	if err := r.curr.Close(); err != nil {
		return fmt.Errorf("failed to close part file: %w", err)
	}
	r.curr = nil

	if sum := r.currHash.Sum(nil); !bytes.Equal(sum, r.currPart.ContentMD5[:]) {
		return fmt.Errorf("part %d MD5 mismatch (expected %x, got %x): %w",
			r.currPart.PartNumber,
			r.currPart.ContentMD5,
			sum,
			s3errs.ErrBadDigest)
	}

	r.currHash = nil
	r.currPart = Part{}
	return nil
}

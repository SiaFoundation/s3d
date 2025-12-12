package multipart

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// Reader provides a sequential reader over the parts of a multipart upload, it
// is not safe for concurrent use.
type Reader struct {
	parts    []Part
	basePath string

	curr      *os.File
	currIdx   int
	bytesLeft int64
	totalSize int64

	partHash  hash.Hash
	totalHash hash.Hash
}

// NewReader creates a new Reader for the given multipart upload.
func NewReader(upload Upload, basePath string) *Reader {
	var total int64
	for _, p := range upload.Parts {
		total += p.Size
	}
	return &Reader{
		parts:     upload.Parts,
		basePath:  basePath,
		currIdx:   -1,
		totalSize: total,
		totalHash: md5.New(),
	}
}

// Read reads data from the multipart upload parts sequentially.
func (r *Reader) Read(p []byte) (int, error) {
	for {
		// no current part, open next
		if r.curr == nil || r.bytesLeft == 0 {
			if r.curr != nil && r.bytesLeft == 0 {
				if err := r.finishPart(); err != nil {
					return 0, err
				}
			}
			if err := r.openNext(); err != nil {
				return 0, err
			}
		}

		// limit read to remaining bytes in this part
		toRead := len(p)
		if int64(toRead) > r.bytesLeft {
			toRead = int(r.bytesLeft)
		}

		n, err := r.curr.Read(p[:toRead])
		if n > 0 {
			r.bytesLeft -= int64(n)
			if r.partHash != nil {
				_, _ = r.partHash.Write(p[:n])
			}
			if r.totalHash != nil {
				_, _ = r.totalHash.Write(p[:n])
			}
			if r.bytesLeft == 0 {
				if err := r.finishPart(); err != nil {
					return n, err
				}
			}
			return n, nil
		}
		if err == io.EOF {
			if r.bytesLeft > 0 {
				return n, io.ErrUnexpectedEOF
			}
			r.curr.Close()
			r.curr = nil
			continue
		}
		return n, err
	}
}

// Close closes the reader and any open part file.
func (r *Reader) Close() error {
	if r.curr != nil {
		return r.curr.Close()
	}
	return nil
}

// MD5Sum returns the MD5 hash of the bytes read so far.
func (r *Reader) MD5Sum() [16]byte {
	var sum [16]byte
	if r.totalHash != nil {
		copy(sum[:], r.totalHash.Sum(nil))
	}
	return sum
}

// Size returns the total size of all parts.
func (r *Reader) Size() int64 {
	return r.totalSize
}

func (r *Reader) openNext() error {
	if r.curr != nil {
		r.curr.Close()
		r.curr = nil
	}

	r.currIdx++
	if r.currIdx >= len(r.parts) {
		return io.EOF
	}

	p := r.parts[r.currIdx]
	path := filepath.Join(r.basePath, strconv.Itoa(p.PartNumber), p.Filename)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open part at %s: %w", path, err)
	}

	r.curr = f
	r.bytesLeft = p.Size
	r.partHash = md5.New()
	return nil
}

func (r *Reader) finishPart() error {
	if r.curr != nil {
		r.curr.Close()
		r.curr = nil
	}
	if r.partHash == nil {
		return nil
	}
	var sum [16]byte
	copy(sum[:], r.partHash.Sum(nil))
	if sum != r.parts[r.currIdx].MD5 {
		return s3errs.ErrBadDigest
	}
	r.partHash = nil
	return nil
}

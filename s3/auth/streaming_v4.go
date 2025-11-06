package auth

import (
	"bufio"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func handleAuthV4Streaming(req *http.Request) error {
	var size int64
	if sizeStr, ok := req.Header["X-Amz-Decoded-Content-Length"]; ok {
		if sizeStr[0] == "" {
			return s3errs.ErrInvalidArgument
		}
		var err error
		size, err = strconv.ParseInt(sizeStr[0], 10, 64)
		if err != nil {
			return s3errs.ErrInvalidArgument
		}
	}

	// parse x-amz-trailer which contains the expected trailer headers
	xAmzTrailer := req.Header.Get(HeaderXAMZTrailer)
	if xAmzTrailer == "" {
		return s3errs.ErrInvalidArgument
	}

	// parse the headers
	expectedHeaders := make(map[string]struct{})
	for h := range strings.SplitSeq(xAmzTrailer, ",") {
		expectedHeaders[http.CanonicalHeaderKey(h)] = struct{}{}
	}

	switch req.Header.Get(HeaderXAMZContentSHA256) {
	case ContentStreamingUnsignedPayloadTrailer:
		req.Body = newChunkedPayloadTrailerReader(req.Body, expectedHeaders)
	case ContentStreamingAWS4HMACSHA256Payload:
		return s3errs.ErrNotImplemented
	case ContentStreamingAWS4HMACSHA256PayloadTrailer:
		return s3errs.ErrNotImplemented
	default:
		// should not reach here
		return s3errs.ErrInternalError
	}

	req.ContentLength = size
	return nil
}

// chunkedPayloadTrailerReader reads an aws-chunked body where
// x-amz-content-sha256 = STREAMING-UNSIGNED-PAYLOAD-TRAILER.
// It implements io.Reader for the payload bytes.
type chunkedPayloadTrailerReader struct {
	br              *bufio.Reader
	r               io.Closer // io.Closer to avoid reading from it rather than br
	chunkRemain     int64
	done            bool
	expectedHeaders map[string]struct{}

	crc32Hasher  hash.Hash32
	crc32CHasher hash.Hash32
	sha1Hasher   hash.Hash
	sha256Hasher hash.Hash
}

// newChunkedPayloadTrailerReader wraps r. r should be the raw HTTP message body
// with Content-Encoding: aws-chunked.
func newChunkedPayloadTrailerReader(r io.ReadCloser, expectedHeaders map[string]struct{}) *chunkedPayloadTrailerReader {
	var crc32Hasher hash.Hash32
	if _, exists := expectedHeaders[xAmzChecksumCrc32]; exists {
		crc32Hasher = crc32.New(crc32.MakeTable(crc32.IEEE))
	}
	var crc32CHasher hash.Hash32
	if _, exists := expectedHeaders[xAmzChecksumCrc32C]; exists {
		crc32CHasher = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	}
	var sha1Hasher hash.Hash
	if _, exists := expectedHeaders[xAmzChecksumSha1]; exists {
		sha1Hasher = sha1.New()
	}
	var sha256Hasher hash.Hash
	if _, exists := expectedHeaders[xAmzChecksumSha256]; exists {
		sha256Hasher = sha256.New()
	}

	return &chunkedPayloadTrailerReader{
		r:               r,
		br:              bufio.NewReader(r),
		chunkRemain:     0,
		expectedHeaders: expectedHeaders,

		crc32Hasher:  crc32Hasher,
		crc32CHasher: crc32CHasher,
		sha1Hasher:   sha1Hasher,
		sha256Hasher: sha256Hasher,
	}
}

// Close closes the underlying reader.
func (r *chunkedPayloadTrailerReader) Close() error {
	return r.r.Close()
}

// Read streams only the payload bytes. Once the payload is exhausted,
// Read returns io.EOF (and trailers will have been parsed).
func (r *chunkedPayloadTrailerReader) Read(p []byte) (int, error) {
	// if we already finished payload, signal EOF.
	if r.done {
		return 0, io.EOF
	}

	// ensure we have an active chunk to read from.
	for r.chunkRemain == 0 {
		// Read next chunk-size line: "<hex-size>(;ext...)\r\n"
		line, err := readCRLFLine(r.br)
		if err != nil {
			return 0, err
		}
		size, err := parseChunkSize(line)
		if err != nil {
			return 0, err
		}
		if size == 0 {
			_, err := r.assertTrailerHeaders(r.expectedHeaders)
			if err != nil {
				return 0, err
			}
			r.done = true
			return 0, io.EOF
		}
		r.chunkRemain = size
	}

	// read up to min(len(p), chunkRemain)
	nwant := int64(len(p))
	if nwant == 0 {
		return 0, nil
	}
	if nwant > r.chunkRemain {
		nwant = r.chunkRemain
	}

	n, err := io.ReadFull(r.br, p[:nwant])
	r.chunkRemain -= int64(n)
	if err != nil {
		return n, err
	}

	// update hashers with read data
	r.updateHashers(p[:n])

	// if we just finished this chunk, expect a trailing CRLF.
	if r.chunkRemain == 0 {
		if err := expectCRLF(r.br); err != nil {
			return n, err
		}
	}
	return n, nil
}

// assertTrailerHeaders reads and asserts trailer headers from br match
// expectedHeaders.
func (r *chunkedPayloadTrailerReader) assertTrailerHeaders(expectedHeaders map[string]struct{}) (http.Header, error) {
	parsedHeaders := make(http.Header)

	for {
		// read a line from the buffer
		line, err := readCRLFLine(r.br)
		if err != nil {
			return nil, s3errs.ErrInvalidArgument
		}

		// an empty line indicates the end of the headers
		if line == "" {
			break
		}

		// split the line into key and value
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, s3errs.ErrInvalidArgument
		}

		key := http.CanonicalHeaderKey(strings.TrimSpace(parts[0]))
		value := strings.TrimSpace(parts[1])

		// check if the header is expected
		if _, ok := expectedHeaders[key]; !ok {
			return nil, s3errs.ErrInvalidArgument
		}

		// add the header to the parsed headers
		parsedHeaders.Add(key, value)
	}

	// ensure all expected headers are present
	for expected := range expectedHeaders {
		h, ok := parsedHeaders[expected]
		if !ok {
			return nil, fmt.Errorf("missing expected trailer header: %q", expected)
		}

		// verify checksum headers
		if expected == xAmzChecksumCrc32 {
			if chksum := base64.StdEncoding.EncodeToString(r.crc32Hasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
		if expected == xAmzChecksumCrc32C {
			if chksum := base64.StdEncoding.EncodeToString(r.crc32CHasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
		if expected == xAmzChecksumSha1 {
			if chksum := base64.StdEncoding.EncodeToString(r.sha1Hasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
		if expected == xAmzChecksumSha256 {
			if chksum := base64.StdEncoding.EncodeToString(r.sha256Hasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
	}
	return parsedHeaders, nil
}

// updateHashers updates all active hashers with data to later verify against
// trailer checksums.
func (r *chunkedPayloadTrailerReader) updateHashers(data []byte) {
	if r.crc32Hasher != nil {
		_, _ = r.crc32Hasher.Write(data)
	}
	if r.crc32CHasher != nil {
		_, _ = r.crc32CHasher.Write(data)
	}
	if r.sha1Hasher != nil {
		_, _ = r.sha1Hasher.Write(data)
	}
	if r.sha256Hasher != nil {
		_, _ = r.sha256Hasher.Write(data)
	}
}

func expectCRLF(br *bufio.Reader) error {
	b, err := br.Peek(2)
	if err != nil {
		return err
	}
	if len(b) < 2 || b[0] != '\r' || b[1] != '\n' {
		return fmt.Errorf("malformed chunk: missing CRLF after data")
	}
	_, _ = br.Discard(2)
	return nil
}

func parseChunkSize(line string) (int64, error) {
	// chunk size may have extensions: "ABC;foo=bar"
	sz := line
	if i := strings.IndexByte(line, ';'); i >= 0 {
		sz = line[:i]
	}
	sz = strings.TrimSpace(sz)
	if sz == "" {
		return 0, fmt.Errorf("empty chunk size")
	}
	n, err := strconv.ParseInt(sz, 16, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid chunk size %q: %w", sz, err)
	}
	return n, nil
}

func readCRLFLine(br *bufio.Reader) (string, error) {
	// read until '\n' and ensure it ends with "\r\n".
	b, err := br.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return "", fmt.Errorf("malformed chunk header: missing CRLF")
	}
	return string(b[:len(b)-2]), nil // strip CRLF
}

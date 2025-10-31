package auth

import (
	"bufio"
	"fmt"
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

	// TODO: This is where we should check if HeaderXAMZTrailer is set and parse
	// it. Then we'd add it to the chunked reader to make sure the reader can
	// validate that the trailer only contains headers previously specified.

	switch req.Header.Get(HeaderXAMZContentSHA256) {
	case ContentStreamingUnsignedPayloadTrailer:
		req.Body = io.NopCloser(newChunkedPayloadTrailerReader(req.Body))
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
//
// TODO: Eventually this should check if any checksum headers are to be expected
// in the trailer and compute the necessary checksum as we go. Finally it would
// compare the checksum in the trailer to the computed one and have Read return
// an appropriate error if there is a mismatch.
type chunkedPayloadTrailerReader struct {
	br             *bufio.Reader
	chunkRemain    int64
	donePayload    bool
	trailersParsed bool
}

// newChunkedPayloadTrailerReader wraps r. r should be the raw HTTP message body
// with Content-Encoding: aws-chunked.
func newChunkedPayloadTrailerReader(r io.Reader) *chunkedPayloadTrailerReader {
	return &chunkedPayloadTrailerReader{
		br:          bufio.NewReader(r),
		chunkRemain: 0,
	}
}

// Read streams only the payload bytes. Once the payload is exhausted,
// Read returns io.EOF (and trailers will have been parsed).
func (u *chunkedPayloadTrailerReader) Read(p []byte) (int, error) {
	// if we already finished payload, signal EOF.
	if u.donePayload {
		return 0, io.EOF
	}

	// ensure we have an active chunk to read from.
	for u.chunkRemain == 0 {
		// Read next chunk-size line: "<hex-size>(;ext...)\r\n"
		line, err := readCRLFLine(u.br)
		if err != nil {
			return 0, err
		}
		size, err := parseChunkSize(line)
		if err != nil {
			return 0, err
		}
		if size == 0 {
			// TODO: This is where the trailer should be parsed
			return 0, io.EOF
		}
		u.chunkRemain = size
	}

	// read up to min(len(p), chunkRemain)
	nwant := int64(len(p))
	if nwant == 0 {
		return 0, nil
	}
	if nwant > u.chunkRemain {
		nwant = u.chunkRemain
	}

	n, err := io.ReadFull(u.br, p[:nwant])
	u.chunkRemain -= int64(n)
	if err != nil {
		return n, err
	}

	// if we just finished this chunk, expect a trailing CRLF.
	if u.chunkRemain == 0 {
		if err := expectCRLF(u.br); err != nil {
			return n, err
		}
	}
	return n, nil
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

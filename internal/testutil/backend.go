package testutil

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"lukechampine.com/frand"
)

const (
	// ETagSize is the size of an ETag in bytes.
	ETagSize = 16
)

type (
	// MemoryBackendOption is a functional argument for configuring a
	// MemoryBackend.
	MemoryBackendOption func(*MemoryBackend)

	// MemoryBackend is an in-memory implementation of the s3 backend for testing.
	MemoryBackend struct {
		buckets          map[string]*bucket
		accessKeys       map[string]accessKey
		multipartUploads map[string]*multipartUpload
	}

	accessKey struct {
		owner  string
		secret auth.SecretAccessKey
	}

	bucket struct {
		owner   string // access key id of the owner
		objects map[string]*object
	}

	object struct {
		name         string
		data         []byte
		lastModified time.Time
		metadata     map[string]string
		contentMD5   [16]byte
		parts        map[int]objectMultipartPart
	}

	objectMultipartPart struct {
		offset     int64
		length     int64
		contentMD5 [16]byte
	}

	multipartUpload struct {
		bucket    string
		key       string
		metadata  map[string]string
		parts     map[int]*multipartPart
		createdAt time.Time
	}

	multipartPart struct {
		data         []byte
		contentMD5   [16]byte
		lastModified time.Time
	}
)

// WithKeyPair adds a key pair to the MemoryBackend.
func WithKeyPair(owner, accessKeyID, secretKey string) func(*MemoryBackend) {
	return func(mb *MemoryBackend) {
		mb.accessKeys[accessKeyID] = accessKey{
			owner:  owner,
			secret: auth.SecretAccessKey(secretKey),
		}
	}
}

// NewMemoryBackend creates a new MemoryBackend.
func NewMemoryBackend(opts ...MemoryBackendOption) *MemoryBackend {
	backend := &MemoryBackend{
		accessKeys:       make(map[string]accessKey),
		buckets:          make(map[string]*bucket),
		multipartUploads: make(map[string]*multipartUpload),
	}
	for _, opt := range opts {
		opt(backend)
	}
	return backend
}

// CopyObject copies an object from the source bucket/object to the destination.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
func (b *MemoryBackend) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, replace bool, meta map[string]string) (*s3.CopyObjectResult, error) {
	srcBkt, exists := b.buckets[srcBucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if srcBkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	dstBkt, exists := b.buckets[dstBucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if dstBkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	srcObjct, exists := srcBkt.objects[srcObject]
	if !exists {
		return nil, s3errs.ErrNoSuchKey
	}

	if !replace {
		for k, v := range srcObjct.metadata {
			if _, exists := meta[k]; !exists {
				meta[k] = v // merge metadata
			}
		}
	}
	if _, exists := dstBkt.objects[dstObject]; !exists {
		dstBkt.objects = make(map[string]*object)
	}
	dstObjct := &object{
		name:         dstObject,
		data:         slices.Clone(srcObjct.data),
		lastModified: time.Now().UTC(),
		metadata:     meta,
		contentMD5:   srcObjct.contentMD5,
		parts:        srcObjct.parts,
	}
	b.buckets[dstBucket].objects[dstObject] = dstObjct
	return &s3.CopyObjectResult{
		ContentMD5:   dstObjct.contentMD5,
		LastModified: dstObjct.lastModified,
		VersionID:    "", // versioning isn't supported
	}, nil
}

// CreateBucket creates a new bucket if it doesn't exist yet and returns an
// error otherwise.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (b *MemoryBackend) CreateBucket(ctx context.Context, accessKeyID, name string) error {
	if _, exists := b.accessKeys[accessKeyID]; !exists {
		return s3errs.ErrInvalidAccessKeyId
	} else if bkt, exists := b.buckets[name]; exists && bkt.owner == b.accessKeys[accessKeyID].owner {
		return s3errs.ErrBucketAlreadyOwnedByYou
	} else if exists {
		return s3errs.ErrBucketAlreadyExists
	}
	b.buckets[name] = &bucket{
		owner: b.accessKeys[accessKeyID].owner,
	}
	return nil
}

// DeleteBucket deletes the specified bucket if it exists, is owned by the user
// requesting deletion and is empty.
func (b *MemoryBackend) DeleteBucket(ctx context.Context, accessKeyID, name string) error {
	bkt, exists := b.buckets[name]
	if !exists {
		return s3errs.ErrNoSuchBucket
	} else if bkt.owner != b.accessKeys[accessKeyID].owner {
		return s3errs.ErrAccessDenied
	} else if len(bkt.objects) > 0 {
		return s3errs.ErrBucketNotEmpty
	}
	delete(b.buckets, name)
	return nil
}

// DeleteObject deletes the specified object from the given bucket.
func (b *MemoryBackend) DeleteObject(ctx context.Context, accessKeyID, bucket, object string) (*s3.DeleteObjectResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	if _, exists := bkt.objects[object]; !exists {
		return nil, s3errs.ErrNoSuchKey
	}
	delete(bkt.objects, object)
	return &s3.DeleteObjectResult{
		IsDeleteMarker: false,
		VersionID:      "",
	}, nil
}

// DeleteObjects deletes multiple objects from the specified bucket.
func (b *MemoryBackend) DeleteObjects(ctx context.Context, accessKeyID, bucket string, objects []s3.ObjectID) (*s3.ObjectsDeleteResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	var res s3.ObjectsDeleteResult
	for _, obj := range objects {
		o, exists := bkt.objects[obj.Key]
		if exists &&
			((obj.ETag != nil && s3.FormatETag(o.contentMD5[:]) != *obj.ETag) ||
				(obj.Size != nil && int64(len(o.data)) != *obj.Size) ||
				(obj.LastModifiedTime != nil && !o.lastModified.Round(time.Second).Equal(obj.LastModifiedTime.StdTime()))) {
			res.Error = append(res.Error, s3.ErrorResult{
				Key:     obj.Key,
				Code:    s3errs.ErrPreconditionFailed.Code,
				Message: s3errs.ErrPreconditionFailed.Description,
			})
			continue
		}

		delete(bkt.objects, obj.Key)
		res.Deleted = append(res.Deleted, s3.ObjectID{
			Key:       obj.Key,
			VersionID: "", // versioning isn't supported
		})
	}
	return &res, nil
}

// GetObject retrieves an object from the specified bucket.
func (b *MemoryBackend) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest, partNumber *int32) (*s3.Object, error) {
	return b.headOrGetObject(ctx, accessKeyID, bucket, object, requestedRange, partNumber, false)
}

// HeadBucket checks if the specified bucket exists and is owned by the user.
func (b *MemoryBackend) HeadBucket(ctx context.Context, accessKeyID, bucket string) error {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return s3errs.ErrNoSuchBucket
	} else if bkt.owner != b.accessKeys[accessKeyID].owner {
		return s3errs.ErrAccessDenied
	}
	return nil
}

// HeadObject retrieves metadata about the specified object without returning
// the object's data.
func (b *MemoryBackend) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest, partNumber *int32) (*s3.Object, error) {
	return b.headOrGetObject(ctx, accessKeyID, bucket, object, requestedRange, partNumber, true)
}

// ListObjects lists objects in the specified bucket that match the given prefix
// and pagination settings.
func (b *MemoryBackend) ListObjects(ctx context.Context, accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if accessKeyID == nil || bkt.owner != b.accessKeys[*accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}

	// flatten the objects into a slice and sort them lexicographically
	objects := slices.Collect(maps.Values(bkt.objects))
	slices.SortFunc(objects, func(a, b *object) int {
		return strings.Compare(a.name, b.name)
	})

	result := s3.NewObjectsListResult(page.MaxKeys)
	if page.MaxKeys == 0 {
		return result, nil
	}

	var lastMatchedPart string

	for _, obj := range objects {
		match := match(prefix, obj.name)
		switch {
		case match == nil:
			continue
		case match.CommonPrefix:
			if page.Marker != nil && strings.Compare(*page.Marker, match.MatchedPart) >= 0 {
				continue
			}
			if match.MatchedPart == lastMatchedPart {
				continue // should not count towards keys
			}
			result.AddPrefix(match.MatchedPart)
			lastMatchedPart = match.MatchedPart
		default:
			if page.Marker != nil && strings.Compare(*page.Marker, obj.name) >= 0 {
				continue
			}
			result.Add(&s3.Content{
				Key:          obj.name,
				LastModified: s3.NewContentTime(obj.lastModified),
				ETag:         s3.FormatETag(obj.contentMD5[:]),
				Owner: &s3.UserInfo{
					ID: bkt.owner,
				},
				Size: int64(len(obj.data)),
			})
		}

		if result.IsTruncated {
			break
		}
	}
	if !result.IsTruncated {
		result.NextMarker = ""
	}

	return result, nil
}

// PutObject puts an object into the specified bucket with the given data and
// metadata.
func (b *MemoryBackend) PutObject(_ context.Context, accessKeyID, bucket, obj string, r io.Reader, opts s3.PutObjectOptions) (*s3.PutObjectResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	if bkt.objects == nil {
		bkt.objects = make(map[string]*object)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	} else if len(data) != int(opts.ContentLength) {
		return nil, s3errs.ErrIncompleteBody
	}

	contentMD5 := md5.Sum(data)
	if opts.ContentMD5 != nil && *opts.ContentMD5 != contentMD5 {
		return nil, s3errs.ErrBadDigest
	}
	contentSHA256 := sha256.Sum256(data)
	if opts.ContentSHA256 != nil && *opts.ContentSHA256 != contentSHA256 {
		return nil, s3errs.ErrBadDigest
	}

	bkt.objects[obj] = &object{
		name:         obj,
		data:         slices.Clone(data),
		contentMD5:   contentMD5,
		lastModified: time.Now().UTC(),
		metadata:     opts.Meta,
		parts:        make(map[int]objectMultipartPart),
	}
	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
	}, nil
}

// CreateMultipartUpload creates a new multipart upload.
func (b *MemoryBackend) CreateMultipartUpload(_ context.Context, accessKeyID, bucket, key string, opts s3.CreateMultipartUploadOptions) (*s3.CreateMultipartUploadResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}

	var entropy [8]byte
	frand.Read(entropy[:])
	uploadID := hex.EncodeToString(entropy[:])

	b.multipartUploads[uploadID] = &multipartUpload{
		bucket:    bucket,
		key:       key,
		metadata:  opts.Meta,
		parts:     make(map[int]*multipartPart),
		createdAt: time.Now().UTC(),
	}

	return &s3.CreateMultipartUploadResult{
		UploadID: uploadID,
	}, nil
}

// ListMultipartUploads lists in-progress multipart uploads for the given bucket.
func (b *MemoryBackend) ListMultipartUploads(_ context.Context, accessKeyID, bucket string, opts s3.ListMultipartUploadsOptions) (*s3.ListMultipartUploadsResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}

	type entry struct {
		key      string
		uploadID string
		created  time.Time
	}

	var entries []entry
	for id, upload := range b.multipartUploads {
		if upload.bucket != bucket {
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(upload.key, opts.Prefix) {
			continue
		}
		entries = append(entries, entry{
			key:      upload.key,
			uploadID: id,
			created:  upload.createdAt,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].key == entries[j].key {
			return entries[i].uploadID < entries[j].uploadID
		}
		return entries[i].key < entries[j].key
	})

	// apply markers
	if opts.KeyMarker != "" {
		i := 0
		for i < len(entries) {
			cmp := strings.Compare(entries[i].key, opts.KeyMarker)
			if cmp < 0 {
				i++
				continue
			}
			if cmp == 0 && opts.UploadIDMarker != "" && entries[i].uploadID <= opts.UploadIDMarker {
				i++
				continue
			}
			break
		}
		entries = entries[i:]
	}

	// sanitize max uploads
	if opts.MaxUploads <= 0 || opts.MaxUploads > s3.MaxMultipartUploads {
		opts.MaxUploads = s3.MaxMultipartUploads
	}

	// collect results
	uploads := make([]s3.MultipartUploadInfo, 0, len(entries))
	for _, entry := range entries {
		uploads = append(uploads, s3.MultipartUploadInfo{
			Key:       entry.key,
			UploadID:  entry.uploadID,
			Initiated: entry.created,
		})
		if int64(len(uploads)) == opts.MaxUploads {
			break
		}
	}

	// determine if truncated
	isTruncated := len(entries) > len(uploads)
	var nextKeyMarker, nextUploadIDMarker string
	if isTruncated && len(uploads) > 0 {
		last := uploads[len(uploads)-1]
		nextKeyMarker = last.Key
		nextUploadIDMarker = last.UploadID
	}

	return &s3.ListMultipartUploadsResult{
		Uploads:            uploads,
		IsTruncated:        isTruncated,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIDMarker: nextUploadIDMarker,
	}, nil
}

// AbortMultipartUpload aborts an in-progress multipart upload and discards
// any uploaded parts.
func (b *MemoryBackend) AbortMultipartUpload(_ context.Context, accessKeyID, bucket, key, uploadID string) error {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return s3errs.ErrNoSuchBucket
	}
	if bkt.owner != b.accessKeys[accessKeyID].owner {
		return s3errs.ErrAccessDenied
	}
	upload, exists := b.multipartUploads[uploadID]
	if !exists {
		return s3errs.ErrNoSuchUpload
	}
	if upload.bucket != bucket || upload.key != key {
		return s3errs.ErrNoSuchUpload
	}
	delete(b.multipartUploads, uploadID)
	return nil
}

// UploadPart uploads a single part for a multipart upload.
func (b *MemoryBackend) UploadPart(_ context.Context, accessKeyID, bucket, key, uploadID string, r io.Reader, opts s3.UploadPartOptions) (*s3.UploadPartResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	upload, exists := b.multipartUploads[uploadID]
	if !exists {
		return nil, s3errs.ErrNoSuchUpload
	}
	if upload.bucket != bucket || upload.key != key {
		return nil, s3errs.ErrNoSuchUpload
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != opts.ContentLength {
		return nil, s3errs.ErrIncompleteBody
	}

	contentMD5 := md5.Sum(data)
	if opts.ContentMD5 != nil && *opts.ContentMD5 != contentMD5 {
		return nil, s3errs.ErrBadDigest
	}
	contentSHA256 := sha256.Sum256(data)
	if opts.ContentSHA256 != nil && *opts.ContentSHA256 != contentSHA256 {
		return nil, s3errs.ErrBadDigest
	}

	upload.parts[opts.PartNumber] = &multipartPart{
		data:         data,
		contentMD5:   contentMD5,
		lastModified: time.Now(),
	}

	return &s3.UploadPartResult{
		ContentMD5: contentMD5,
	}, nil
}

// UploadPartCopy copies a single part from an existing object as part of a
// multipart upload.
func (b *MemoryBackend) UploadPartCopy(_ context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject, uploadID string, opts s3.UploadPartCopyOptions) (*s3.UploadPartCopyResult, error) {
	srcBkt, exists := b.buckets[srcBucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if srcBkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	srcObjct, exists := srcBkt.objects[srcObject]
	if !exists {
		return nil, s3errs.ErrNoSuchKey
	}

	dstBkt, exists := b.buckets[dstBucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if dstBkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	upload, exists := b.multipartUploads[uploadID]
	if !exists {
		return nil, s3errs.ErrNoSuchUpload
	}
	if upload.bucket != dstBucket || upload.key != dstObject {
		return nil, s3errs.ErrNoSuchUpload
	}

	start := opts.Range.Start
	length := opts.Range.Length
	if start < 0 || length <= 0 || start+length > int64(len(srcObjct.data)) {
		return nil, s3errs.ErrInvalidRange
	}

	partData := slices.Clone(srcObjct.data[start : start+length])
	contentMD5 := md5.Sum(partData)

	upload.parts[opts.PartNumber] = &multipartPart{
		data:         partData,
		contentMD5:   contentMD5,
		lastModified: time.Now(),
	}

	return &s3.UploadPartCopyResult{
		ContentMD5:   contentMD5,
		LastModified: srcObjct.lastModified,
	}, nil
}

// ListParts lists uploaded parts for an in-progress multipart upload.
func (b *MemoryBackend) ListParts(_ context.Context, accessKeyID, bucket, key, uploadID string, page s3.ListPartsPage) (*s3.ListPartsResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	upload, exists := b.multipartUploads[uploadID]
	if !exists {
		return nil, s3errs.ErrNoSuchUpload
	}
	if upload.bucket != bucket || upload.key != key {
		return nil, s3errs.ErrNoSuchUpload
	}

	partNumbers := make([]int, 0, len(upload.parts))
	for number := range upload.parts {
		partNumbers = append(partNumbers, number)
	}
	sort.Ints(partNumbers)

	result := &s3.ListPartsResult{
		OwnerID:              bkt.owner,
		InitiatorID:          accessKeyID,
		OwnerDisplayName:     "",
		InitiatorDisplayName: "",
	}

	var listed int64
	for _, number := range partNumbers {
		if number <= page.PartNumberMarker {
			continue
		}
		part := upload.parts[number]
		if part == nil {
			continue
		}
		if listed >= page.MaxParts {
			result.IsTruncated = true
			if len(result.Parts) > 0 {
				last := result.Parts[len(result.Parts)-1].PartNumber
				result.NextPartNumberMarker = strconv.Itoa(int(last))
			}
			break
		}

		result.Parts = append(result.Parts, s3.UploadPart{
			PartNumber:   number,
			LastModified: part.lastModified,
			Size:         int64(len(part.data)),
			ContentMD5:   part.contentMD5,
		})
		listed++
	}

	return result, nil
}

// CompleteMultipartUpload assembles the uploaded parts into the final object.
func (b *MemoryBackend) CompleteMultipartUpload(_ context.Context, accessKeyID, bucket, key, uploadID string, parts []s3.CompletedPart) (*s3.CompleteMultipartUploadResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if bkt.owner != b.accessKeys[accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	upload, exists := b.multipartUploads[uploadID]
	if !exists {
		return nil, s3errs.ErrNoSuchUpload
	}
	if upload.bucket != bucket || upload.key != key {
		return nil, s3errs.ErrNoSuchUpload
	}

	if len(parts) == 0 {
		return nil, s3errs.ErrInvalidRequest
	}

	// build object hash and validate parts
	var totalSize int
	objHash := make([]byte, 0, len(parts)*ETagSize)
	objParts := make(map[int]objectMultipartPart, len(parts))
	for i, completed := range parts {
		part, found := upload.parts[completed.PartNumber]
		if !found {
			return nil, s3errs.ErrInvalidPart
		} else if part.contentMD5 != completed.ETag {
			return nil, s3errs.ErrInvalidPart
		}

		lastPart := i == len(parts)-1
		if !lastPart && int64(len(part.data)) < s3.MinUploadPartSize {
			return nil, s3errs.ErrEntityTooSmall
		}

		objHash = append(objHash, part.contentMD5[:]...)
		objParts[completed.PartNumber] = objectMultipartPart{
			offset:     int64(totalSize),
			length:     int64(len(part.data)),
			contentMD5: part.contentMD5,
		}

		totalSize += len(part.data)
	}

	// collect object data
	objData := make([]byte, 0, totalSize)
	for _, completed := range parts {
		objData = append(objData, upload.parts[completed.PartNumber].data...)
	}
	objMD5 := md5.Sum(objData)

	// calculate final ETag
	var etag string
	if len(parts) == 1 {
		etag = s3.FormatETag(objMD5[:])
	} else {
		multipartMD5 := md5.Sum(objHash)
		etag = s3.FormatMultipartETag(multipartMD5[:], len(parts))
	}

	// store the object
	if bkt.objects == nil {
		bkt.objects = make(map[string]*object)
	}
	bkt.objects[key] = &object{
		name:         key,
		data:         objData,
		lastModified: time.Now(),
		metadata:     upload.metadata,
		contentMD5:   objMD5,
		parts:        objParts,
	}
	delete(b.multipartUploads, uploadID)

	return &s3.CompleteMultipartUploadResult{
		ETag:       etag,
		ContentMD5: objMD5,
	}, nil
}

// ListBuckets lists all available buckets.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (b *MemoryBackend) ListBuckets(ctx context.Context, accessKeyID string) ([]s3.BucketInfo, error) {
	var buckets []s3.BucketInfo
	for name, bucket := range b.buckets {
		if bucket.owner == b.accessKeys[accessKeyID].owner {
			buckets = append(buckets, s3.BucketInfo{
				Name:         name,
				CreationDate: s3.NewContentTime(time.Now()),
			})
		}
	}
	return buckets, nil
}

// LoadSecret loads the secret access key for the given access key ID.
func (b *MemoryBackend) LoadSecret(ctx context.Context, accessKeyID string) (auth.SecretAccessKey, error) {
	if ak, exists := b.accessKeys[accessKeyID]; exists {
		return slices.Clone(ak.secret), nil // return a copy to prevent modification
	}
	return nil, s3errs.ErrInvalidAccessKeyId
}

func (b *MemoryBackend) headOrGetObject(_ context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest, partNumber *int32, head bool) (*s3.Object, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if accessKeyID == nil || bkt.owner != b.accessKeys[*accessKeyID].owner {
		return nil, s3errs.ErrAccessDenied
	}
	obj, exists := bkt.objects[object]
	if !exists {
		return nil, s3errs.ErrNoSuchKey
	}
	size := int64(len(obj.data))
	rnge, err := requestedRange.Range(size)
	if err != nil {
		return nil, err
	} else if rnge != nil {
		partNumber = nil // ignore part number when a range is requested
	}

	var partCount *int32
	var contentMD5 [16]byte
	if partNumber != nil {
		if len(obj.parts) == 0 {
			if *partNumber != 1 {
				return nil, s3errs.ErrInvalidPart
			}
			pc := int32(1)
			partCount = &pc
			contentMD5 = obj.contentMD5
		} else {
			partInfo, exists := obj.parts[int(*partNumber)]
			if !exists {
				return nil, s3errs.ErrInvalidPart
			}
			rnge = &s3.ObjectRange{
				Start:  partInfo.offset,
				Length: partInfo.length,
			}
			contentMD5 = partInfo.contentMD5
			pc := int32(len(obj.parts))
			partCount = &pc
		}
	} else {
		contentMD5 = obj.contentMD5
	}

	var body io.ReadCloser
	if !head {
		if rnge == nil {
			body = io.NopCloser(bytes.NewReader(obj.data))
		} else {
			body = io.NopCloser(bytes.NewReader(obj.data[rnge.Start : rnge.Start+rnge.Length]))
		}
	}
	return &s3.Object{
		Body:         body,
		ContentMD5:   contentMD5,
		LastModified: obj.lastModified,
		Metadata:     obj.metadata,
		Range:        rnge,
		Size:         size,
		PartsCount:   partCount,
	}, nil
}

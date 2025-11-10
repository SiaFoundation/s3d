package testutils

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

type (
	// MemoryBackend is an in-memory implementation of the s3 backend for testing.
	MemoryBackend struct {
		buckets    map[string]*bucket
		accessKeys map[string]auth.SecretAccessKey
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
	}
)

// NewMemoryBackend creates a new MemoryBackend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		accessKeys: make(map[string]auth.SecretAccessKey),
		buckets:    make(map[string]*bucket),
	}
}

// AddAccessKey adds a new access key to the backend for authentication.
func (b *MemoryBackend) AddAccessKey(ctx context.Context, accessKeyID, secretAccessKey string) error {
	if _, exists := b.accessKeys[accessKeyID]; exists {
		return errors.New("access key already exists")
	}
	b.accessKeys[accessKeyID] = auth.SecretAccessKey(secretAccessKey)
	return nil
}

// CopyObject copies an object from the source bucket/object to the destination.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
func (b *MemoryBackend) CopyObject(ctx context.Context, accessKeyID, srcBucket, srcObject, dstBucket, dstObject string, meta map[string]string) (*s3.CopyObjectResult, error) {
	srcBkt, exists := b.buckets[srcBucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if srcBkt.owner != accessKeyID {
		return nil, s3errs.ErrAccessDenied
	}
	dstBkt, exists := b.buckets[dstBucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if dstBkt.owner != accessKeyID {
		return nil, s3errs.ErrAccessDenied
	}
	srcObjct, exists := srcBkt.objects[srcObject]
	if !exists {
		return nil, s3errs.ErrNoSuchKey
	}

	for k, v := range srcObjct.metadata {
		if _, exists := meta[k]; !exists {
			meta[k] = v // merge metadata
		}
	}
	if _, exists := dstBkt.objects[dstObject]; !exists {
		dstBkt.objects = make(map[string]*object)
	}
	dstObjct := &object{
		name:         dstObject,
		data:         slices.Clone(srcObjct.data),
		lastModified: time.Now(),
		metadata:     meta,
		contentMD5:   srcObjct.contentMD5,
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
	} else if bkt, exists := b.buckets[name]; exists && bkt.owner == accessKeyID {
		return s3errs.ErrBucketAlreadyOwnedByYou
	} else if exists {
		return s3errs.ErrBucketAlreadyExists
	}
	b.buckets[name] = &bucket{
		owner: accessKeyID,
	}
	return nil
}

// DeleteBucket deletes the specified bucket if it exists, is owned by the user
// requesting deletion and is empty.
func (b *MemoryBackend) DeleteBucket(ctx context.Context, accessKeyID, name string) error {
	bkt, exists := b.buckets[name]
	if !exists {
		return s3errs.ErrNoSuchBucket
	} else if bkt.owner != accessKeyID {
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
	} else if bkt.owner != accessKeyID {
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
	} else if bkt.owner != accessKeyID {
		return nil, s3errs.ErrAccessDenied
	}
	var res s3.ObjectsDeleteResult
	for _, obj := range objects {
		delete(bkt.objects, obj.Key)
		res.Deleted = append(res.Deleted, s3.ObjectID{
			Key:       obj.Key,
			VersionID: "", // versioning isn't supported
		})
	}
	return &res, nil
}

// GetObject retrieves an object from the specified bucket.
func (b *MemoryBackend) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest) (*s3.Object, error) {
	return b.headOrGetObject(ctx, accessKeyID, bucket, object, requestedRange, false)
}

// HeadBucket checks if the specified bucket exists and is owned by the user.
func (b *MemoryBackend) HeadBucket(ctx context.Context, accessKeyID, bucket string) error {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return s3errs.ErrNoSuchBucket
	} else if bkt.owner != accessKeyID {
		return s3errs.ErrAccessDenied
	}
	return nil
}

// HeadObject retrieves metadata about the specified object without returning
// the object's data.
func (b *MemoryBackend) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest) (*s3.Object, error) {
	return b.headOrGetObject(ctx, accessKeyID, bucket, object, requestedRange, true)
}

// ListObjects lists objects in the specified bucket that match the given prefix
// and pagination settings.
func (b *MemoryBackend) ListObjects(ctx context.Context, accessKeyID *string, bucket string, prefix s3.Prefix, page s3.ListObjectsPage) (*s3.ObjectsListResult, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	} else if accessKeyID == nil || bkt.owner != *accessKeyID {
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
				Size:         int64(len(obj.data)),
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
	if bkt.owner != accessKeyID {
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
		lastModified: time.Now(),
		metadata:     opts.Meta,
	}
	return &s3.PutObjectResult{
		ContentMD5: contentMD5,
	}, nil
}

// ListBuckets lists all available buckets.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (b *MemoryBackend) ListBuckets(ctx context.Context, accessKeyID string) ([]s3.BucketInfo, error) {
	var buckets []s3.BucketInfo
	for name := range b.buckets {
		buckets = append(buckets, s3.BucketInfo{
			Name:         name,
			CreationDate: s3.NewContentTime(time.Now()),
		})
	}
	return buckets, nil
}

// LoadSecret loads the secret access key for the given access key ID.
func (b *MemoryBackend) LoadSecret(ctx context.Context, accessKeyID string) (auth.SecretAccessKey, error) {
	if secret, exists := b.accessKeys[accessKeyID]; exists {
		return slices.Clone(secret), nil // return a copy to prevent modification
	}
	return nil, s3errs.ErrInvalidAccessKeyId
}

func (b *MemoryBackend) headOrGetObject(_ context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest, head bool) (*s3.Object, error) {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return nil, s3errs.ErrNoSuchBucket
	}
	if accessKeyID == nil || bkt.owner != *accessKeyID {
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
		ContentMD5:   obj.contentMD5,
		LastModified: obj.lastModified,
		Metadata:     obj.metadata,
		Range:        rnge,
		Size:         size,
	}, nil
}

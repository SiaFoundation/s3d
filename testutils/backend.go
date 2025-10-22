package testutils

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"io"
	"slices"
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
		data     []byte
		metadata map[string]string
		hash     []byte
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

func (b *MemoryBackend) GetObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest) (*s3.Object, error) {
	return b.headOrGetObject(ctx, accessKeyID, bucket, object, requestedRange, false)
}

func (b *MemoryBackend) HeadObject(ctx context.Context, accessKeyID *string, bucket, object string, requestedRange *s3.ObjectRangeRequest) (*s3.Object, error) {
	return b.headOrGetObject(ctx, accessKeyID, bucket, object, requestedRange, true)
}

func (b *MemoryBackend) PutMemObject(accessKeyID string, bucket, obj string, data []byte, metadata map[string]string) error {
	bkt, exists := b.buckets[bucket]
	if !exists {
		return s3errs.ErrNoSuchBucket
	}
	if bkt.owner != accessKeyID {
		return s3errs.ErrAccessDenied
	}
	if bkt.objects == nil {
		bkt.objects = make(map[string]*object)
	}
	hash := md5.Sum(data)
	bkt.objects[obj] = &object{
		data:     slices.Clone(data),
		hash:     hash[:],
		metadata: metadata,
	}
	return nil
}

// ListBuckets lists all available buckets.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (b *MemoryBackend) ListBuckets(ctx context.Context, accessKeyID string) ([]s3.BucketInfo, error) {
	var buckets []s3.BucketInfo
	for name := range b.buckets {
		buckets = append(buckets, s3.BucketInfo{
			Name:         name,
			CreationDate: time.Now().UTC(),
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
		body = io.NopCloser(bytes.NewReader(obj.data))
	}
	return &s3.Object{
		Body:     body,
		Hash:     obj.hash,
		Metadata: obj.metadata,
		Range:    rnge,
		Size:     size,
	}, nil
}

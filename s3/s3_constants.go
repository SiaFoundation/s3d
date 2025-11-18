package s3

const (
	// KeySizeLimit defines the maximum size of an S3 object key's name.
	//
	// From https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html:
	// The name for a key is a sequence of Unicode characters whose UTF-8
	// encoding is at most 1024 bytes long.
	KeySizeLimit = 1024

	// MaxUploadPartNumber defines the maximum allowed part number in a multipart
	// upload. AWS allows part numbers between 1 and 10,000 inclusive.
	MaxUploadPartNumber = 10000

	// MinUploadPartSize defines the minimum allowed size for a multipart upload
	// part, except for the last part, which may be smaller.
	MinUploadPartSize int64 = 5 << 20 // 5 MiB

	// MaxUploadPartSize is the maximum allowed size for a single multipart
	// part.
	MaxUploadPartSize int64 = 5 << 30 // 5 GiB

	// MaxUploadListParts defines the maximum number of parts returned in a
	// single ListParts response.
	MaxUploadListParts = 1000

	// DefaultMaxUploadListParts is the default number of parts returned when
	// no limit is specified.
	DefaultMaxUploadListParts = MaxUploadListParts

	// MetadataSizeLimit defines the maximum size of the metadata associated
	// with an S3 object.
	//
	// From https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html:
	// Within the PUT request header, the user-defined metadata is limited to 2
	// KB in size. The size of user-defined metadata is measured by taking the
	// sum of the number of bytes in the UTF-8 encoding of each key and value.
	MetadataSizeLimit = 2000
)

const (
	// MaxBucketKeys is the maximum number of object keys from a bucket that can
	// be retrieved in one call to ListObjects or deleted by DeleteObjects.
	MaxBucketKeys = 1000

	// DefaultMaxBucketKeys is the default number of object keys from a bucket
	// retrieved by ListObjects if no limit is specified.
	DefaultMaxBucketKeys = 1000
)

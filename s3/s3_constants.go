package s3

const (
	// From https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html:
	// The name for a key is a sequence of Unicode characters whose UTF-8
	// encoding is at most 1024 bytes long."
	keySizeLimit = 1024

	// From https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html:
	// Within the PUT request header, the user-defined metadata is limited to 2
	// KB in size. The size of user-defined metadata is measured by taking the
	// sum of the number of bytes in the UTF-8 encoding of each key and value.
	defaultMetadataSizeLimit = 2000
)

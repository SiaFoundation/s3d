package sqlite

import (
	"testing"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestGetObject(t *testing.T) {
	const (
		accessKeyID = "test-accesskey"
		bucket      = "test-bucket"
		object      = "test-object"
		multipart   = "test-multipart"
	)

	var (
		objID     = frand.Entropy256()
		objMD5    = frand.Entropy128()
		objMeta   = map[string]string{"foo": "bar"}
		objLength = frand.Intn(10) + 1

		multipartID       = frand.Entropy256()
		multipartMD5      = frand.Entropy128()
		multipartUploadID = s3.NewUploadID()
		multipartMeta     = map[string]string{"baz": "qux"}
	)

	// create bucket
	store := initTestDB(t, zap.NewNop())
	if err := store.CreateBucket(accessKeyID, bucket); err != nil {
		t.Fatal(err)
	}

	// create object
	err := store.PutObject(accessKeyID, bucket, object, objID, objMeta, objMD5, int64(objLength))
	if err != nil {
		t.Fatal(err)
	}

	// create multipart object
	err = store.CreateMultipartUpload(bucket, multipart, multipartUploadID, multipartMeta)
	if err != nil {
		t.Fatal(err)
	}
	// add parts
	part1MD5 := frand.Entropy128()
	part2MD5 := frand.Entropy128()
	if _, err := store.AddMultipartPart(bucket, multipart, multipartUploadID, "part-1", 1, part1MD5, s3.MinUploadPartSize); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMultipartPart(bucket, multipart, multipartUploadID, "part-2", 2, part2MD5, 2); err != nil {
		t.Fatal(err)
	}
	// complete
	totalSize := int64(s3.MinUploadPartSize + 2)
	err = store.CompleteMultipartUpload(bucket, multipart, multipartUploadID, multipartID, multipartMD5, totalSize)
	if err != nil {
		t.Fatal(err)
	}

	// get object without part number
	obj, err := store.GetObject(aws.String(accessKeyID), bucket, object, nil)
	if err != nil {
		t.Fatal(err)
	} else if obj.ID != objID {
		t.Fatalf("expected object ID %v, got %v", objID, obj.ID)
	} else if obj.Length != int64(objLength) {
		t.Fatalf("expected object length %d, got %d", objLength, obj.Length)
	} else if obj.ContentMD5 != objMD5 {
		t.Fatalf("expected object MD5 %v, got %v", objMD5, obj.ContentMD5)
	} else if len(obj.Meta) != len(objMeta) || obj.Meta["foo"] != "bar" {
		t.Fatalf("expected object metadata %v, got %v", objMeta, obj.Meta)
	}

	// get object with part number 1
	objPart1, err := store.GetObject(aws.String(accessKeyID), bucket, object, aws.Int32(1))
	if err != nil {
		t.Fatal(err)
	} else if objPart1.ID != objID {
		t.Fatalf("expected object ID %v, got %v", objID, objPart1.ID)
	} else if objPart1.Offset != 0 {
		t.Fatalf("expected object offset 0, got %d", objPart1.Offset)
	} else if objPart1.Length != int64(objLength) {
		t.Fatalf("expected object length %d, got %d", objLength, objPart1.Length)
	} else if objPart1.ContentMD5 != objMD5 {
		t.Fatalf("expected object MD5 %v, got %v", objMD5, objPart1.ContentMD5)
	} else if len(objPart1.Meta) != len(objMeta) || objPart1.Meta["foo"] != "bar" {
		t.Fatalf("expected object metadata %v, got %v", objMeta, objPart1.Meta)
	}

	// get multipart object with part number 2
	multipartPart2, err := store.GetObject(aws.String(accessKeyID), bucket, multipart, aws.Int32(2))
	if err != nil {
		t.Fatal(err)
	} else if multipartPart2.ID != multipartID {
		t.Fatalf("expected object ID %v, got %v", multipartID, multipartPart2.ID)
	} else if multipartPart2.Offset != int64(s3.MinUploadPartSize) {
		t.Fatalf("expected object offset %d, got %d", s3.MinUploadPartSize, multipartPart2.Offset)
	} else if multipartPart2.Length != 2 {
		t.Fatalf("expected object length %d, got %d", 2, multipartPart2.Length)
	} else if multipartPart2.ContentMD5 != part2MD5 {
		t.Fatalf("expected object MD5 %v, got %v", part2MD5, multipartPart2.ContentMD5)
	} else if len(multipartPart2.Meta) != len(multipartMeta) || multipartPart2.Meta["baz"] != "qux" {
		t.Fatalf("expected object metadata %v, got %v", multipartMeta, multipartPart2.Meta)
	}
}

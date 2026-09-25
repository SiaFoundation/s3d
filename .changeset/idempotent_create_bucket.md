---
default: patch
---

# Answer CreateBucket on an owned bucket with 200 in the default region

Creating a bucket the caller already owns answered 409 BucketAlreadyOwnedByYou.
Amazon S3 answers 200 in us-east-1 for legacy compatibility and returns the
error elsewhere, so s3d answers 200 only when it serves the default region.

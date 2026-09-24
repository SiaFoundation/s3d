---
default: patch
---

# Answer CreateBucket on an owned bucket with 200

Creating a bucket the caller already owns answered 409
BucketAlreadyOwnedByYou. AWS answers 200 in us-east-1, which is the region s3d
reports, and the Backend interface already documented the operation as
idempotent.

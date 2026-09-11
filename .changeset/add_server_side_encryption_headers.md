---
default: minor
---

# Add server side encryption headers

The bucket encryption endpoints are implemented for `AES256`, and object
responses report `x-amz-server-side-encryption: AES256`. Customer provided keys
and KMS are rejected.

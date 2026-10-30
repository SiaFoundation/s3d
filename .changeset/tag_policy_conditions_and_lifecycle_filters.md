---
default: minor
---

# Support tag policy conditions and lifecycle tag filters

Bucket policies can now condition object actions on `s3:ExistingObjectTag/<key>`,
`s3:RequestObjectTag/<key>` and `s3:RequestObjectTagKeys`, and lifecycle
expiration rules can filter on tags.

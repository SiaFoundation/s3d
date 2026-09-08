---
default: minor
---

# Added support for public buckets via bucket policies

`PutBucketPolicy`, `GetBucketPolicy`, `DeleteBucketPolicy` and
`GetBucketPolicyStatus` are now implemented for policies that grant read access
to everyone. A policy may allow `s3:GetObject`, `s3:GetObjectVersion`,
`s3:ListBucket` and `s3:ListBucketVersions` to the `*` principal, which lets
any caller respectively read an object, read a specific version of one, list
the bucket, and list its versions. The grant covers unsigned requests as well
as signed requests from other users. Each action is granted independently, and
every other policy is rejected rather than partially applied.

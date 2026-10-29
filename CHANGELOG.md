## 0.2.2 (2026-10-29)

### Features

- Add bucket policies
- Added object size filters to bucket lifecycle rules
- Replace the Go library with the new Rust-based library
- Fixed the reported Sia upload rate reading zero during an upload
- Added `s3d snapshots create`, `list` and `delete`
- Added `s3d snapshots restore`

#### Auto flush objects after configurable interval

`sia.uploadDeadline` sets how long after an object was last modified its group
is uploaded to Sia regardless of wasted space, so small objects no longer wait
indefinitely for enough data to fill a slab. It defaults to `24h`, and `0s`
disables the deadline.

#### Implement POST Object

A bucket now accepts a browser form upload. The credential, policy and
signature are read from the form fields, the policy's conditions are enforced,
and `success_action_status` and `success_action_redirect` decide the response.
Such a request was previously refused with `AccessDenied`.

#### Added support for S3 Object Lock

Buckets can now be created with object lock enabled, or have it turned on once
versioning is enabled. Object versions can be protected by a retention period in
`GOVERNANCE` or `COMPLIANCE` mode, by a legal hold, or by the bucket's default
retention rule, and a locked version cannot be deleted until its protection
ends. `x-amz-bypass-governance-retention` lets the bucket owner shorten, clear
or delete through a `GOVERNANCE` retention. Event holds are not supported and
are rejected with `NotImplemented`.

### Fixes

- Pin uploaded objects in parallel
- Leave objects still buffered on disk out of snapshots

#### Fix a panic on a malformed Content-MD5

A `Content-MD5` that did not decode to a 16 byte digest panicked the request
handler and dropped the connection. It is now refused with `InvalidDigest`.

## 0.2.1 (2026-10-05)

### Features

- Report live transfer stats

### Fixes

- Add missing CHECK constraint on global_settings
- Fixed config file lookup aborting when a location cannot be read
- Always include KeyCount in ListObjectsV2 responses
- Match the AWS wording for NoSuchLifecycleConfiguration
- Accept a percent encoded CopyObject source

#### Report an empty bucket location instead of "null"

GetBucketLocation returned the literal string "null" for us-east-1. Amazon S3 represents the absence of a location constraint as an empty LocationConstraint element; SDKs expose this as null. Some clients were treating the literal "null" as the region name and using it for request signing.

## 0.2.0 (2026-09-25)

### Breaking Changes

- Replaced POST /system/sqlite3/backup with POST /snapshots

### Features

#### Add log flags

`-log.file.enabled` and `-log.stdout.enableANSI` set the two log options from the
command line, so a service manager that already captures stdout can turn off the
log file and the color codes without editing the config file. Neither default
changed.

#### Make redundancy configurable

`sia.dataShards` and `sia.parityShards` set the erasure coding scheme used for
uploads, defaulting to the previous 10 of 30. An invalid combination is rejected
before s3d opens its listeners or database. Objects already stored keep their
original scheme and remain readable.

#### Validate X-Amz-Checksum-* request headers

`PutObject` now verifies the `CRC32`, `CRC32C`, `CRC64NVME`, `MD5`, `SHA1`,
`SHA256` and `SHA512` checksum headers rather than storing them unchecked, and
refuses a mismatch with `BadDigest`. A request naming more than one checksum is
refused with `InvalidRequest`. Checksums sent as streaming `X-Amz-Trailer`
values are verified the same way, which they previously were not.

### Fixes

#### Filter request headers from object metadata

Object writes now store only object metadata headers. Request control headers
such as signing, copy, tagging, ACL and encryption headers are no longer saved
as metadata or replayed on later reads, and metadata stored by earlier versions
is filtered when an object is read. A migration prunes it from the database so
a later `CopyObject` does not carry it forward. `Content-Language` is now stored
and returned with the object, which it previously was not.

#### Sync the upload directory after staging an object

Staged uploads were written and synced, but the directory holding them was
not, so an unclean shutdown could leave the database referencing a file that
no longer had a directory entry.

## 0.1.4 (2026-09-11)

### Features

- Add presigned URLs
- Authorize HeadBucket with s3:ListBucket
- Make number of upload threads configurable

#### Added support for public buckets via bucket policies

`PutBucketPolicy`, `GetBucketPolicy`, `DeleteBucketPolicy` and
`GetBucketPolicyStatus` are now implemented for policies that grant read access
to everyone. A policy may allow `s3:GetObject`, `s3:GetObjectVersion`,
`s3:ListBucket` and `s3:ListBucketVersions` to the `*` principal, which lets
any caller respectively read an object, read a specific version of one, list
the bucket, and list its versions. The grant covers unsigned requests as well
as signed requests from other users. Each action is granted independently, and
every other policy is rejected rather than partially applied.

### Fixes

#### Answer service root requests with an S3 error instead of a plain 404

Non-GET requests to the service root returned Go's plain-text `404 page not
found`. AWS dispatches on the method before authenticating there, answering 405
with an `Allow: GET` header, and clients that probe the root before logging in,
such as Cyberduck sending an unsigned `HEAD /`, cannot connect otherwise.

## 0.1.3 (2026-08-31)

### Features

- Support conditional operations

### Fixes

- Fixed an issue where uploads on a slow connection might fail with "no more hosts available"

## 0.1.2 (2026-08-12)

### Features

- Add S3 versioning support
- Added `flush` CLI command
- Added a `POST /objects/flush` admin endpoint that uploads all pending objects to Sia immediately, regardless of padding.
- Add an option to serve the S3 API over HTTPS

### Fixes

- Add lifecycle support
- Add sqlite backup
- Add stats table
- Added a `status` command that prints a basic overview of the background upload pipeline by querying the admin API.
- Check remaining account storage before starting an upload group and skip groups that would exceed the available space, avoiding failed pin attempts after upload.
- Don't log clients disconnecting as errors.
- Implement io.WriterTo for custom readers
- Update the sia-storage-go SDK for slab versioning support
- Normalize sealed Sia objects into sia_objects, sia_slabs, sia_slab_slices and sia_slab_sectors tables
- Only log errors on the error level if they are s3d's fault and not the client's to avoid spam.
- Redact secrets in logs
- Update siastorage to v0.2.0

#### Fail puts with SlowDown when waiting for disk space times out

Requests that wait for disk usage to drop below the limit now give up after
two minutes and return a 503 SlowDown error instead of blocking forever.
Previously a blocked put could pin its handler goroutine indefinitely, even
after the client disconnected, and prevent graceful shutdown from completing.

## 0.1.1 (2026-06-11)

### Features

- Implicily handle localhost as a host base.

### Fixes

- Correctly handle virtual host style routing without a port.

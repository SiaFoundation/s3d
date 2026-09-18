---
default: minor
---

# Validate X-Amz-Checksum-* request headers

`PutObject` now verifies the `CRC32`, `CRC32C`, `CRC64NVME`, `MD5`, `SHA1`,
`SHA256` and `SHA512` checksum headers rather than storing them unchecked, and
refuses a mismatch with `BadDigest`. A request naming more than one checksum is
refused with `InvalidRequest`. Checksums sent as streaming `X-Amz-Trailer`
values are verified the same way, which they previously were not.

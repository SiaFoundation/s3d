---
default: minor
---

# Validate X-Amz-Checksum-* request headers

`PutObject` now verifies the `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1` and
`SHA256` checksum headers rather than storing them unchecked, and refuses a
mismatch with `BadDigest`. A request naming more than one checksum is refused
with `InvalidRequest`.

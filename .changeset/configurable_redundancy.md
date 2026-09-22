---
default: minor
---

# Make redundancy configurable

`sia.dataShards` and `sia.parityShards` set the erasure coding scheme used for
uploads, defaulting to the previous 10 of 30. An invalid combination is rejected
before s3d opens its listeners or database. Objects already stored keep their
original scheme and remain readable.

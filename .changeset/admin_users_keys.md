---
default: major
---

# Added user and access key management to the admin API

`s3d users` and `s3d keys` now manage the running instance through the admin
API instead of opening the database directly, so `s3d` has to be running when
they are used and `s3d keys list` no longer prints secret keys.

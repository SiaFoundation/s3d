---
default: minor
---

# `s3d users` and `s3d keys` manage the running instance through the admin API

The commands no longer open the database directly, so `s3d` has to be running
when they are used and `s3d keys list` no longer prints secret keys.

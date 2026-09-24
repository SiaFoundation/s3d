---
default: minor
---

# Report live transfer stats

`s3d status` and the admin API now report how fast data is arriving from S3
clients and going out to Sia, how much of the local buffer is in use, and the
progress of the upload groups currently streaming to Sia.

---
default: minor
---

# Support pre-authorized keys in `s3d login` and `s3d -env`

`s3d login` approves the app connection with a pre-authorized key from
`S3D_PREAUTHORIZED_KEY` instead of a URL to visit, and `s3d -env` registers on
first start from `S3D_PREAUTHORIZED_KEY`, `S3D_RECOVERY_PHRASE` and
`S3D_INDEXER_URL` without running `s3d login` at all.

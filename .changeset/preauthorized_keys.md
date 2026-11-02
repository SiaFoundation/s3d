---
default: minor
---

# Support pre-authorized keys in `s3d login` and `s3d -env`

`s3d login` reads `S3D_PREAUTHORIZED_KEY`, `S3D_RECOVERY_PHRASE` and
`S3D_INDEXER_URL` and prompts only for what is not set, with the key replacing
the URL to visit. `s3d -env` registers from the same variables on first start
without running `s3d login` at all.

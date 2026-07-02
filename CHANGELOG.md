## 0.1.2 (2026-07-02)

### Features

- Add S3 versioning support
- Added `flush` CLI command
- Added a `POST /objects/flush` admin endpoint that uploads all pending objects to Sia immediately, regardless of padding.

### Fixes

- Add lifecycle support
- Add sqlite backup
- Add stats table
- Added a `status` command that prints a basic overview of the background upload pipeline by querying the admin API.
- Check remaining account storage before starting an upload group and skip groups that would exceed the available space, avoiding failed pin attempts after upload.
- Don't log clients disconnecting as errors.
- Implement io.WriterTo for custom readers
- Only log errors on the error level if they are s3d's fault and not the client's to avoid spam.

## 0.1.1 (2026-06-11)

### Features

- Implicily handle localhost as a host base.

### Fixes

- Correctly handle virtual host style routing without a port.

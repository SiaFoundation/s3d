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

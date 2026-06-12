# [![S3d](https://sia.tech/api/media/file/banner-s3d.png)](https://sia.tech)

[![GoDoc](https://pkg.go.dev/badge/github.com/SiaFoundation/s3d)](https://pkg.go.dev/github.com/SiaFoundation/s3d)

A lightweight, S3-compatible Renter for the Sia network

## Overview

`s3d` is a lightweight daemon built by the Sia Foundation that translates AWS
S3 API calls into operations on the Sia decentralized storage network. Any
application or tool that speaks S3 can store data on Sia without modification.
It supports AWS Signature V4 authentication, path-style and virtual-hosted-style
bucket addressing, multipart uploads, and upload packing for small objects.

`s3d` is built on top of the [Sia Storage SDK](https://pkg.go.dev/go.sia.tech/siastorage).
All data is encrypted client-side by the SDK and distributed across the Sia
network. The server stores lightweight metadata in a local SQLite database.
Objects are buffered on disk before being uploaded to Sia in the background;
see [Upload Packing](#upload-packing) for details.

To build your own app on Sia, take a look at the
[Sia Developer Portal](https://devs.sia.storage).

## Building

`s3d` uses SQLite for its persistence. A C compiler toolchain is required.

```sh
git clone https://github.com/SiaFoundation/s3d.git
cd s3d
go generate ./...
CGO_ENABLED=1 go build -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w' ./cmd/s3d
```

## Getting Started

To get started, run

```sh
s3d login
```

`s3d login` will guide you through the initial configuration, prompt for your
12-word recovery phrase (or generate a new one if you leave it blank), and
register `s3d` with the Sia indexer. Visit the printed URL in your browser to
approve the app connection and complete setup.

To update the configuration later, run

```sh
s3d config
```

For more information on configuration options, see the
[Configuration](#configuration) section below.

Next, create a user and generate an access key pair:

```sh
s3d users create <username>
s3d keys create [--access-key <id> --secret-key <secret>] <username>
```

The access key pair is auto-generated when both flags are omitted. Save the
printed credentials. The secret key is only shown once.

Once logged in and credentials are set up, start `s3d` with

```sh
s3d
```

## Docker

`s3d` is available as a Docker image at `ghcr.io/siafoundation/s3d`.

### 1. Create the compose file

Create a new file named `docker-compose.yml`. You can use the following as a
template. The `/data` mount is where `s3d` stores its metadata database, config
file, and logs.

```yml
services:
  s3d:
    image: ghcr.io/siafoundation/s3d:master
    restart: unless-stopped
    ports:
      - 8000:8000/tcp
    volumes:
      - s3d:/data

volumes:
  s3d:
```

### 2. Configure and log in

`s3d login` will run the configuration wizard if there is no config file detected. Then it will register `s3d` with the indexer. `s3d` will print a URL that must be visited to approve the connection to the indexer.

```sh
docker compose run --rm s3d login
```

### 3. Create a user and access key

```sh
docker compose run --rm s3d users create <username>
docker compose run --rm s3d keys create [--access-key <id> --secret-key <secret>] <username>
```

The access key pair is auto-generated when both flags are omitted. Save the
printed credentials. The secret key is only shown once.

### 4. Start `s3d`

```sh
docker compose up -d
```

### Building the image

```sh
docker build -t s3d .
```

## Upload Packing

Sia stores data in fixed-size slabs made up of 4 MiB sectors. Uploading small
objects individually wastes storage because the unused remainder of each slab
sits empty.

All objects are first written to a local uploads directory on disk. A background
loop periodically collects pending objects and groups them together using a
bin-packing algorithm. A group is uploaded to Sia once its waste falls below the
configured threshold (default 10%). This ensures objects are packed efficiently
into slabs regardless of size, minimizing wasted space on the network.

## Multipart Uploads

`s3d` supports S3 multipart uploads, allowing large files to be uploaded in
parts and assembled server-side on completion.

| Constraint | Value |
|------------|-------|
| Minimum part size | 5 MiB |
| Maximum part size | 5 GiB |
| Maximum parts per upload | 10,000 |

Each part is uploaded individually to the Sia network. On completion, the parts
are assembled into the final object. `UploadPartCopy` is supported for
assembling objects from existing data without re-uploading.

## Admin API

`s3d` serves an admin API on a separate HTTP address for monitoring. It is
always enabled and defaults to `127.0.0.1:8001`. The admin password is set
during `s3d config`, or via `adminAddress` and `adminPassword` in the config
file:

```yaml
adminAddress: 127.0.0.1:8001 # must differ from apiAddress
adminPassword: change-me # required
```

Requests are authenticated via HTTP Basic authentication using the configured
password; the username is ignored.

### `GET /prometheus`

Returns metrics about the background upload pipeline in the Prometheus text
exposition format:

```sh
curl -u ":change-me" http://127.0.0.1:8001/prometheus
```

| Metric | Description |
|--------|-------------|
| `s3d_upload_pending_objects` | Objects buffered on disk waiting to be uploaded to Sia |
| `s3d_upload_pending_size_bytes` | Total size of pending objects in bytes |
| `s3d_upload_uploaded_objects` | Objects fully uploaded to the Sia network |
| `s3d_upload_uploaded_size_bytes` | Total size of uploaded objects in bytes |
| `s3d_upload_failed_uploads` | Failed upload attempts since the process started |
| `s3d_upload_orphaned_objects` | Deleted or overwritten objects pending cleanup |
| `s3d_upload_multipart_uploads` | In-progress multipart uploads |

## Compatibility

`s3d` aims to be as compatible as possible with the S3 API. Authentication uses
AWS Signature V4 exclusively. SigV4A is not implemented. Supported
`x-amz-content-sha256` modes include `UNSIGNED-PAYLOAD`,
`STREAMING-UNSIGNED-PAYLOAD-TRAILER`, `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, and
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`. Both path-style
(`s3.example.com/bucket/object`) and virtual-hosted-style
(`bucket.s3.example.com/object`) addressing are supported.

Bucket lifecycle configuration supports prefix-based `AbortIncompleteMultipartUpload` rules and current-object `Expiration` rules.

### Operations

| Operation | Status |
|-----------|--------|
| **Buckets** | |
| CreateBucket | ✓ |
| DeleteBucket | ✓ |
| HeadBucket | ✓ |
| ListBuckets | ✓ |
| GetBucketLocation | ✓ |
| GetBucketVersioning | ✗ |
| PutBucketVersioning | ✗ |
| GetBucketAcl | ✗ |
| PutBucketAcl | ✗ |
| GetBucketPolicy | ✗ |
| PutBucketPolicy | ✗ |
| GetBucketLifecycle | ✓ |
| PutBucketLifecycle | ✓ |
| DeleteBucketLifecycle | ✓ |
| GetBucketCors | ✗ |
| PutBucketCors | ✗ |
| GetBucketTagging | ✗ |
| PutBucketTagging | ✗ |
| GetBucketEncryption | ✗ |
| PutBucketEncryption | ✗ |
| **Objects** | |
| PutObject | ✓ |
| GetObject | ✓ |
| HeadObject | ✓ |
| DeleteObject | ✓ |
| DeleteObjects | ✓ |
| CopyObject | ✓ |
| ListObjects (v1) | ✓ |
| ListObjects (v2) | ✓ |
| GetObjectAcl | ✗ |
| PutObjectAcl | ✗ |
| GetObjectTagging | ✗ |
| PutObjectTagging | ✗ |
| GetObjectLock | ✗ |
| PutObjectLock | ✗ |
| SelectObjectContent | ✗ |
| **Multipart** | |
| CreateMultipartUpload | ✓ |
| UploadPart | ✓ |
| UploadPartCopy | ✓ |
| CompleteMultipartUpload | ✓ |
| AbortMultipartUpload | ✓ |
| ListParts | ✓ |
| ListMultipartUploads | ✓ |

## Configuration

`s3d` is configured via a YAML config file. Run `s3d config` to interactively
generate one.

### Command-Line Flags

| Flag | Description |
|------|-------------|
| `-api.s3` | Address to serve the S3 API on (default `127.0.0.1:8000`) |

### Subcommands

| Command | Description |
|---------|-------------|
| `version` | Print the version, commit hash, and build date |
| `config` | Launch the interactive configuration wizard |
| `login` | Prompts for recovery phrase and registers `s3d` with the Sia indexer |
| `users` | Manage S3 users (create, delete, list) |
| `keys` | Manage S3 access keys (create, delete, list) |

### Default Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 8000 | TCP | S3 API |
| 8001 | TCP | Admin API (disabled by default, see [Admin API](#admin-api)) |

### Default Paths

| Platform | Config File | Data Directory |
|----------|-------------|----------------|
| Linux | `/etc/s3d/s3d.yml` | `/var/lib/s3d` |
| macOS | `~/Library/Application Support/s3d/s3d.yml` | `~/Library/Application Support/s3d` |
| Windows | `%APPDATA%/s3d/s3d.yml` | `%APPDATA%/s3d` |
| Docker | `/data/s3d.yml` | `/data` |

### Environment Variables

Environment variables take the highest precedence, overriding both the config
file and CLI flags. The order of precedence from lowest to highest is:

1. Code defaults
2. Config file (`s3d.yml`)
3. CLI flags
4. Environment variables

| Variable | Description |
|----------|-------------|
| `S3D_CONFIG_FILE` | Override the config file path |
| `S3D_DATA_DIR` | Override the data directory |

### Example Config

```yaml
apiAddress: 127.0.0.1:8000
adminAddress: 127.0.0.1:8001 # serve the admin API on this address (must differ from apiAddress)
adminPassword: change-me # required to access the admin API
directory: /var/lib/s3d
log:
  stdout:
    enabled: true # enable logging to stdout
    level: info # log level (debug, info, warn, error)
    format: human # log format (human, json)
    enableANSI: true # enable ANSI color codes (disabled on Windows)
  file:
    enabled: true # enable logging to file
    level: info # log level (debug, info, warn, error)
    format: json # log format (human, json)
    path: /var/log/s3d/s3d.log # log file path (defaults to <directory>/s3d.log)
s3:
  hostBases: # bases for virtual-hosted-style addressing ("localhost" is always included)
    - s3.example.com
```

Access keys are managed via the `s3d users` and `s3d keys` CLI commands and
stored in the SQLite database. See [Getting Started](#getting-started) for
details.

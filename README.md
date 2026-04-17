# [![S3d](https://sia.tech/api/media/file/banner-s3d.png)](http://sia.tech)

[![GoDoc](https://godoc.org/github.com/SiaFoundation/s3d?status.svg)](https://godoc.org/github.com/SiaFoundation/s3d)

An S3-compatible gateway for the [Sia](https://sia.tech) network.

## Overview

`s3d` is a lightweight daemon built by the Sia Foundation that translates AWS
S3 API calls into operations on the Sia decentralized storage network. Any
application or tool that speaks S3 can store data on Sia without modification.
It supports AWS Signature V4 authentication, path-style and virtual-hosted-style
bucket addressing, multipart uploads, and upload packing for small objects.

`s3d` is built on top of the [Sia Storage SDK](https://pkg.go.dev/go.sia.tech/siastorage).
All data is encrypted client-side by the SDK and distributed across the Sia
network. The server stores lightweight metadata in a local SQLite database.
Small objects may be temporarily buffered on disk before being uploaded to Sia;
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

A convenient way to configure `s3d` for the first time is to run

```sh
s3d config
```

which will guide you through an interactive configuration process and generate a
config file. For more information on configuration options, see the
[Configuration](#configuration) section below.

Once configured, start `s3d` with

```sh
./s3d
```

On first launch, `s3d` will register itself with the Sia indexer and print a URL
to approve the app connection. Visit the URL in your browser to complete setup.
If no recovery phrase is configured, a new one will be generated automatically.
Store it in a safe place as it is required to recover your account and data.

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

### 2. Configure `s3d`

Run the configuration wizard to generate a config file.

```sh
docker compose run --rm -it s3d config
```

### 3. Start `s3d`

```sh
docker compose up -d
```

### 4. Approve the app connection

On first run, `s3d` will print a URL that must be visited to approve the
connection to the indexer.

```sh
docker compose logs -f s3d
```

### Building the image

```sh
docker build -t s3d .
```

## Upload Packing

Sia stores data in fixed-size slabs made up of 4 MiB sectors. Uploading small
objects individually wastes storage because the unused remainder of each slab
sits empty.

`s3d` uses a waste threshold to decide whether an object should be packed. When
uploading an object directly would waste more than the configured percentage of
slab space (default 10%), the object is written to a local packing directory
instead. A background loop periodically collects buffered objects and packs them
together using a bin-packing algorithm, uploading the result as a single
operation. Objects that fit slabs efficiently bypass packing and stream directly
to Sia.

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

## Compatibility

`s3d` aims to be as compatible as possible with the S3 API. Authentication uses
AWS Signature V4 exclusively. Both path-style (`s3.example.com/bucket/object`)
and virtual-hosted-style (`bucket.s3.example.com/object`) addressing are
supported.

### Supported Operations

| Category | Operation | Status |
|----------|-----------|--------|
| **Buckets** | CreateBucket | Supported |
| | DeleteBucket | Supported |
| | HeadBucket | Supported |
| | ListBuckets | Supported |
| | GetBucketLocation | Supported |
| **Objects** | PutObject | Supported |
| | GetObject | Supported |
| | HeadObject | Supported |
| | DeleteObject | Supported |
| | DeleteObjects | Supported |
| | CopyObject | Supported |
| | ListObjects (v1) | Supported |
| | ListObjects (v2) | Supported |
| **Multipart** | CreateMultipartUpload | Supported |
| | UploadPart | Supported |
| | UploadPartCopy | Supported |
| | CompleteMultipartUpload | Supported |
| | AbortMultipartUpload | Supported |
| | ListParts | Supported |
| | ListMultipartUploads | Supported |

### Not Supported

Versioning, ACLs, bucket policies, lifecycle rules, object locking, tagging,
server-side encryption configuration, CORS configuration, website hosting,
replication, inventory, analytics, metrics, event notifications, and select
object content.

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

### Default Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 8000 | TCP | S3 API |

### Default Paths

| | Config File | Data Directory |
|---------|-------------|----------------|
| Linux | `/etc/s3d/s3d.yml` | `/var/lib/s3d` |
| macOS | `~/Library/Application Support/s3d/s3d.yml` | `~/Library/Application Support/s3d` |
| Windows | `%APPDATA%/s3d/s3d.yml` | `%APPDATA%/s3d` |
| Docker | `/data/s3d.yml` | `/data` |

### Environment Variables

The following environment variables may be used to override the default
configuration and take precedence over the config file settings.

| Variable | Description |
|----------|-------------|
| `S3D_CONFIG_FILE` | Override the config file path |
| `S3D_DATA_DIR` | Override the data directory |
| `S3D_RECOVERY_PHRASE` | Set the wallet recovery phrase |

### Example Config

```yaml
apiAddress: :8000
directory: /var/lib/s3d
recoveryPhrase: your twelve word recovery phrase goes right here in this field
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
  hostBases: [] # virtual-hosted-style bucket bases (e.g. ["s3.example.com"])
sia:
  indexerURL: https://sia.storage # Sia indexer URL
  keyPairs:
    - accessKey: your-access-key-id # 16 to 128 characters
      secretKey: your-secret-access-key-change-me-please # 32 to 128 characters
```

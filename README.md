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

Run a single `s3d` instance per app key and data directory. Each instance keeps
its own record of what it has stored and runs cleanup passes against the shared
account on the Sia network, so two instances sharing either will interfere with
each other and can unpin each other's data.

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

The erasure coding scheme is applied when a group is uploaded, not when an
object is received. Changing `sia.dataShards` or `sia.parityShards` takes
effect on the next restart and applies to every object still waiting in the
uploads directory. Objects already uploaded keep the scheme they were stored
with and remain readable.

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

The admin API is documented in [`openapi.yml`](openapi.yml) and rendered at
[api.sia.tech/s3d](https://api.sia.tech/s3d). It serves upload pipeline metrics
in the Prometheus text exposition format.

```sh
curl -u ":change-me" http://127.0.0.1:8001/prometheus
```

## Snapshots

**Snapshots are experimental.** The commands and the format written to Sia may
still change.

A snapshot is a copy of the SQLite metadata database, compressed and uploaded to
Sia as a pinned object. It captures the mapping from S3 keys to Sia objects
along with the users and access keys, so restoring one brings back an instance
that can serve the data already stored on the network. The object data itself is
never copied, it already lives on Sia and the database only references it.

Creating a snapshot first flushes the objects still buffered on disk, so the
call blocks for as long as that upload takes. Objects that fail to flush are
logged and the snapshot is taken without them.

Each snapshot is identified by its Sia object ID, printed on creation and by
`s3d snapshots list`. It is the only identifier that survives losing the
database, so keep it somewhere outside the data directory.

Snapshots assume one `s3d` instance owns the app key, as described in the
[Overview](#overview). Snapshotting or restoring while a second instance runs
against the same account will produce a database that does not match what is
stored on the network.

### Creating and listing

```sh
s3d snapshots create
s3d snapshots list
```

`list` reads the running instance's own records, so a snapshot shows up once its
pin is confirmed. With `--remote` it enumerates the account on the Sia network
instead, which needs nothing but the app key and is the way to find a snapshot
after losing the database.

Enumeration pages through every object event in the account, 500 per request,
each waiting on the one before it. The cost is those round trips, so it grows
with everything ever stored rather than with the number of snapshots. An account
of 850,000 events takes around 25 minutes. Nothing is printed until it finishes,
so give it time.

```sh
s3d snapshots list --remote
```

### Restoring

Stop the daemon first, the restored database replaces the configured one.

```sh
s3d snapshots restore 0d4f2c9a1b7e3568af0c21d9e4b85730c6a91f42db38e7051c9a6b24f80d3e17
```

Passing `latest` instead of an object ID enumerates the account to find the
newest snapshot, at the same cost as `list --remote` above. Restoring a known
object ID fetches it in a single request, which is why the ID is worth keeping.

```sh
s3d snapshots restore latest
```

An existing database is moved aside with its write ahead log before the restored
one takes its place, so a restore can be undone. Pass `--out` to write the
restored database to another directory and leave the configured one untouched.
The app key is read from the configured data directory, so this requires an
instance that has already run `s3d login`.

### Deleting

Deleting unpins the snapshot's Sia object and removes its record, releasing the
objects it was withholding from cleanup.

```sh
s3d snapshots delete 0d4f2c9a1b7e3568af0c21d9e4b85730c6a91f42db38e7051c9a6b24f80d3e17
```

## HTTPS

`s3d` can serve the S3 API over TLS in addition to plain HTTP. Set
`apiHTTPSAddress` in the config file or pass `-api.s3.https`:

```yaml
apiAddress: 127.0.0.1:8000
apiHTTPSAddress: 127.0.0.1:8443
```

In the configuration wizard, press Enter to keep the current HTTPS setting or
enter `none` to disable HTTPS.

The certificate is a self-signed certificate for `localhost`, `127.0.0.1`, and
`::1`. It is generated on every start and never written to disk, so clients must
skip certificate verification. While this is supported, it is recommended to use
a reverse proxy with a real certificate in production.

```sh
aws --endpoint-url https://127.0.0.1:8443 --no-verify-ssl s3 ls
```

## Compatibility

`s3d` aims to be as compatible as possible with the S3 API. Authentication uses
AWS Signature V4 exclusively. SigV4A is not implemented. Supported
`x-amz-content-sha256` modes include `UNSIGNED-PAYLOAD`,
`STREAMING-UNSIGNED-PAYLOAD-TRAILER`, `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, and
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`. Both path-style
(`s3.example.com/bucket/object`) and virtual-hosted-style
(`bucket.s3.example.com/object`) addressing are supported.

Bucket lifecycle configuration supports prefix-based `AbortIncompleteMultipartUpload` rules and current-object `Expiration` rules.

Bucket policies are supported only to grant read access to everyone.
`PutBucketPolicy` accepts `Allow` statements whose principal is `*` and whose
actions are drawn from this set:

| Action | Resource | Grants |
|--------|----------|--------|
| `s3:GetObject` | `arn:aws:s3:::<bucket>/*` | `GetObject`, `HeadObject` |
| `s3:GetObjectVersion` | `arn:aws:s3:::<bucket>/*` | `GetObject`, `HeadObject` for a specific `versionId` |
| `s3:ListBucket` | `arn:aws:s3:::<bucket>` | `ListObjects` v1 and v2, `HeadBucket` |
| `s3:ListBucketVersions` | `arn:aws:s3:::<bucket>` | `ListObjectVersions` |

Each action is granted on its own, so a policy that allows listing does not also
allow reading. An action paired with the wrong resource grants nothing, as in
S3. A policy granting everything looks like this:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::<bucket>/*"
    },
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:ListBucket", "s3:ListBucketVersions"],
      "Resource": "arn:aws:s3:::<bucket>"
    }
  ]
}
```

A `*` principal covers every caller, so the grant applies to signed requests
from other users as well as to unsigned ones. Nothing beyond those four actions
follows from it: writes, deletes, and the bucket's own configuration all still
require the owner's credentials, only the owner can read, change or remove the
policy, and the bucket does not appear in anyone else's `ListBuckets`. A
listing reports the bucket's owner, which is not necessarily the caller.

Any policy s3d cannot honor exactly is rejected rather than partially applied,
so an accepted policy never grants more access than it describes: `Deny`
statements, `Condition` blocks, named principals, other actions, resources
scoped to a key prefix, and unrecognised members of any kind are all refused.
ACLs remain unimplemented, as does `PublicAccessBlock`.

Conditional requests are supported. `GetObject` and `HeadObject` honor
`If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`, and
the copy source of `CopyObject` and `UploadPartCopy` honors the matching
`x-amz-copy-source-if-*` headers.

`PutObject`, `CopyObject` and `CompleteMultipartUpload` honor `If-Match` and
`If-None-Match` against the current version of the destination, evaluated
atomically with the write. `DeleteObject` honors `If-Match` (including the `*`
wildcard), `x-amz-if-match-size` and `x-amz-if-match-last-modified-time`, and
each object of a multi delete honors its `ETag`, `Size` and `LastModifiedTime`
elements, where a failed precondition fails only that object. A conditional
delete with nothing to delete is a no-op, as an unconditional one is.

### Operations

✓ fully supported, ◐ supported with the limitations listed, ✗ not implemented.

| Operation | Status | Limitations |
|-----------|--------|-------------|
| **Buckets** | | |
| CreateBucket | ✓ | |
| DeleteBucket | ✓ | |
| HeadBucket | ✓ | |
| ListBuckets | ✓ | |
| GetBucketLocation | ✓ | |
| GetBucketVersioning | ✓ | |
| PutBucketVersioning | ◐ | `MfaDelete` is rejected; `Enabled` and `Suspended` are supported |
| GetBucketAcl | ✗ | |
| PutBucketAcl | ✗ | |
| GetBucketPolicy | ✓ | |
| PutBucketPolicy | ◐ | `Allow` only, principal `*` only, and only the four read actions listed above |
| DeleteBucketPolicy | ✓ | |
| GetBucketPolicyStatus | ✓ | |
| GetBucketLifecycle | ✓ | |
| PutBucketLifecycle | ◐ | Prefix-based `AbortIncompleteMultipartUpload` and current-object `Expiration` rules only |
| DeleteBucketLifecycle | ✓ | |
| GetBucketCors | ✗ | |
| PutBucketCors | ✗ | |
| GetBucketTagging | ✗ | |
| PutBucketTagging | ✗ | |
| GetBucketEncryption | ✗ | |
| PutBucketEncryption | ✗ | |
| PublicAccessBlock | ✗ | |
| **Objects** | | |
| PutObject | ✓ | |
| GetObject | ✓ | |
| HeadObject | ✓ | |
| DeleteObject | ✓ | |
| DeleteObjects | ✓ | |
| CopyObject | ✓ | |
| ListObjects (v1) | ✓ | |
| ListObjects (v2) | ✓ | |
| ListObjectVersions | ✓ | |
| GetObjectAcl | ✗ | |
| PutObjectAcl | ✗ | |
| GetObjectTagging | ✗ | |
| PutObjectTagging | ✗ | |
| GetObjectLock | ✗ | |
| PutObjectLock | ✗ | |
| SelectObjectContent | ✗ | |
| **Multipart** | | |
| CreateMultipartUpload | ✓ | |
| UploadPart | ✓ | |
| UploadPartCopy | ✓ | |
| CompleteMultipartUpload | ✓ | |
| AbortMultipartUpload | ✓ | |
| ListParts | ✓ | |
| ListMultipartUploads | ✓ | |

## Configuration

`s3d` is configured via a YAML config file. Run `s3d config` to interactively
generate one.

### Command-Line Flags

| Flag | Description |
|------|-------------|
| `-api.s3` | Address to serve the S3 API on (default `127.0.0.1:8000`) |
| `-api.s3.https` | Address to serve the S3 API on over HTTPS (disabled by default, see [HTTPS](#https)) |
| `-log.file.enabled` | Enable logging to a file (default `true`) |
| `-log.stdout.enableANSI` | Enable ANSI color codes on stdout (default `true`, `false` on Windows) |

### Subcommands

| Command | Description |
|---------|-------------|
| `version` | Print the version, commit hash, and build date |
| `config` | Launch the interactive configuration wizard |
| `login` | Prompts for recovery phrase and registers `s3d` with the Sia indexer |
| `status` | Print a basic overview of the background upload pipeline |
| `users` | Manage S3 users (create, delete, list) |
| `keys` | Manage S3 access keys (create, delete, list) |
| `snapshots` | Manage database snapshots backed up to Sia. Experimental, see [Snapshots](#snapshots) |

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
apiHTTPSAddress: "" # also serve the S3 API over HTTPS on this address (disabled when blank, see HTTPS)
adminAddress: 127.0.0.1:8001 # serve the admin API on this address (must differ from both S3 API addresses)
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
sia:
  diskUsageLimit: 10737418240 # max bytes buffered on disk pending upload (0 disables the limit, default 10 GiB)
  uploadThreads: 1 # object groups uploaded to Sia concurrently by the background upload loop (default 4)
  dataShards: 10 # shards each slab is split into (default 10)
  parityShards: 20 # recovery shards added to each slab (default 20)
s3:
  hostBases: # bases for virtual-hosted-style addressing ("localhost" is always included)
    - s3.example.com
```

Access keys are managed via the `s3d users` and `s3d keys` CLI commands and
stored in the SQLite database. See [Getting Started](#getting-started) for
details.

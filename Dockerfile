FROM golang:1.26 AS builder

WORKDIR /s3d

# get dependencies
COPY go.mod go.sum ./
RUN go mod download

# copy source
COPY . .

# mark as safe in git
RUN git config --global --add safe.directory .

# codegen
RUN go generate ./...

# build
RUN CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w -linkmode external -extldflags "-static"'  ./cmd/s3d

FROM debian:bookworm-slim

LABEL maintainer="The Sia Foundation <info@sia.tech>" \
    org.opencontainers.image.vendor="The Sia Foundation" \
    org.opencontainers.image.description="A S3-compatible gateway for Sia" \
    org.opencontainers.image.source="https://github.com/SiaFoundation/s3d" \
    org.opencontainers.image.licenses=MIT

# copy binary and certificates
COPY --from=builder /s3d/bin/s3d /usr/bin/s3d
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENV S3D_DATA_DIR=/data
ENV S3D_CONFIG_FILE=/data/s3d.yml

VOLUME [ "/data" ]

# S3 API port
EXPOSE 8000/tcp

ENTRYPOINT [ "s3d", "-api.s3", ":8000" ]

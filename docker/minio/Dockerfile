# MinIO server and client built from pinned, patched source tags.
#
# Why a source build: the upstream project is archived and its Community Edition is source-only.
# The free Docker Hub images were removed, and the last release (the fix for GHSA-jjjj-jwhf-8rgr,
# a privilege escalation in service accounts and STS) was never published as an image. The advisory
# itself says to install that tag with `go install`. See docs/decisions/0015-minio-local-s3.md.
#
# This is a local development stand-in for S3 holding synthetic data. It is not for production.

FROM golang:1.24.8-alpine3.22 AS build
# Release tags of github.com/minio/minio (patched) and github.com/minio/mc (last release).
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MC_TAG=RELEASE.2025-08-13T08-35-41Z
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOBIN=/out GOPROXY=https://proxy.golang.org,direct
RUN apk add --no-cache git
# The caches are BuildKit cache mounts, so a retry after an interrupted download (or a rebuild with
# another tag) reuses every module already fetched instead of starting from zero.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go install -ldflags "-s -w" github.com/minio/minio@${MINIO_TAG}
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go install -ldflags "-s -w" github.com/minio/mc@${MC_TAG}

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 minio \
 && mkdir -p /data && chown minio /data
COPY --from=build /out/minio /out/mc /usr/local/bin/
USER minio
ENV MINIO_UPDATE=off
EXPOSE 9000 9001
VOLUME /data
CMD ["minio", "server", "/data", "--address", ":9000", "--console-address", ":9001"]

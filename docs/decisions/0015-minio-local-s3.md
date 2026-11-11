# ADR 0015: MinIO as a local S3 stand-in for the raw zone

- Status: accepted
- Date: 2026-11-11

## Context

The raw zone has two copies: S3 and GCS. Until now both were local folders (`s3-local`, `gcs-local`), and the
S3 adapter had only ever been tested against `moto`, an in-process fake. A folder cannot fail the way a network
service does: no timeouts, no permission errors, no "connection refused". The specification names MinIO as the
local object store, so this ADR adds it for the S3 side, to exercise the real adapter over real HTTP.

## Decision

- **Only the S3 side moves to MinIO; GCS and the control store stay local directories.** MinIO speaks the S3
  API, so it can stand in for S3 but not for GCS. Choosing the backend is one environment variable,
  `ECOMM_S3_BACKEND=local|minio` (default `local`, so nothing changes unless asked), read by one factory
  (`ecomm.lake.stores`) that every host command goes through. The destination is named `s3-minio` so its origin
  is never mistaken for AWS. Selecting `minio` without its settings is an error, never a silent fallback.
- **The same adapter, not a new one.** `S3ObjectStore` is what will talk to real S3; MinIO only changes the
  endpoint. It gained settings a self-hosted server needs (path-style addressing, an explicit default region,
  checksums only when required) and **bounded timeouts** (connect 5 s, read 30 s, 2 attempts), so an unreachable
  endpoint fails in seconds and the commit layer's own retry stays in charge.
- **Upstream is archived, so the image is built from source.** `minio/minio` was archived (read-only) on
  2026-04-25, its Community Edition is source-only, and the free Docker Hub images were removed. The final release,
  `RELEASE.2025-10-15T17-29-55Z`, fixes **GHSA-jjjj-jwhf-8rgr** (high: a restricted service account or STS account
  can escalate to its parent's privileges) and was never published as an image. The advisory's own instruction is to
  install that tag with `go install`, so `docker/minio/Dockerfile` does exactly that, for the server and for
  `mc` (also archived; last release `RELEASE.2025-08-13T08-35-41Z`), from pinned tags and pinned base images, as a
  non-root user. This avoids both an unpatched old image and a dependency on a registry that has already removed
  images once. The cost is a compile (about 25 minutes on this 5 GB machine, several minutes on a CI runner),
  cached afterwards.
- **No service accounts or STS.** The pipeline uses a plain IAM user with an attached policy, so the advisory's
  attack surface is not used even in principle. A test forbids service accounts in the init script.
- **Least privilege, mirrored from Terraform.** `docker/minio/pipeline-policy.json` carries the same statements as
  the Terraform pipeline role: list/get/put under `raw/ manifests/ quarantine/ ledger/ reports/`, no delete, one
  bucket. A test parses `infra/terraform/aws/main.tf` and fails if the actions or prefixes drift apart. The
  bucket is private and versioned (like the Terraform bucket). Root credentials exist only in the server and the
  one-shot init job, in a separate git-ignored file; the pipeline only ever gets the pipeline user's key.
- **Loopback only, generated secrets.** API on `127.0.0.1:9000`, console on `127.0.0.1:9001`; credentials are
  random and generated, and MinIO refuses to start without them rather than using its default login.
- **License.** MinIO is AGPL-3.0. It is used here only as a separate, unmodified local process; nothing from it is
  linked into or redistributed with this repository (only the build recipe).
- **Swapping the server is a settings change.** Nothing in the pipeline is MinIO-specific. If a maintained
  S3-compatible server is preferred later (Garage, SeaweedFS and RustFS are candidates, none evaluated here),
  point `ECOMM_S3_ENDPOINT_URL` at it and replace the compose service and init script.

## What the real server found

Running against a real server found two defects that no fake had:

1. **The commit crashed on an unreachable destination.** `commit_batch` read the destination's manifest to see
   whether the batch was already there, outside its retry logic. A read that failed (not "not found") escaped the
   whole call, so there was no PARTIAL status and the healthy destination was never written. Local folders cannot
   fail reads and the test double only failed writes, so every earlier test passed. An unreadable manifest now means
   "not known to be committed" and takes the idempotent write path, which retries and records the failure per
   destination. Covered by two unit tests (written first and seen failing) and a live outage test.
2. **My own refactor disabled the "no lake yet" checks.** Opening a local store creates its directory, so checking
   afterwards always passed and a mistyped `--demo-dir` created an empty lake. A pure `lake_layout()` is now
   checked first, with tests for every reader.

## What MinIO cannot tell us (so it is not claimed)

- **403 versus 404 on a missing object.** AWS documents that `GetObject`/`HeadObject` on a missing key returns 403,
  not 404, when the caller lacks an unconditioned `s3:ListBucket`. The Terraform pipeline role grants `ListBucket`
  only under a `s3:prefix` condition. MinIO returned 404 in the same situation, so the live tests pass but do **not**
  prove the policy works on real AWS. If AWS answers 403, the commit layer's "is this batch new?" check would fail
  against it. This needs a run against a real account (the same `test_minio.py` assertions can be pointed at S3);
  until then it is a recorded risk, not a validated design. Likely fixes: drop the prefix condition from the pipeline
  `ListBucket` statement, or probe existence with a prefixed list.
- `ListBuckets` is refused by AWS but filtered by MinIO; the test asserts the property that matters (no other bucket
  is visible) rather than the status code.
- IAM evaluation, KMS/SSE encryption, the `DenyInsecureTransport` bucket policy (local MinIO is plain HTTP) and
  lifecycle rules are AWS features that are not exercised.
- Throughput, large-object (multipart) behaviour and concurrency are untested.

## Consequences

- Compose gains `minio` and `minio-init` under the opt-in `minio` profile (about 0.2 GB of memory; a 206 MB image).
- The Spark container, the Airflow DAGs and the metrics exporter still use local directories; they run in
  containers and are not yet wired to MinIO. Only the host commands follow the backend setting.
- The MinIO project itself is not maintained. For a local, loopback, synthetic-data stand-in that is acceptable;
  for anything else it would not be.

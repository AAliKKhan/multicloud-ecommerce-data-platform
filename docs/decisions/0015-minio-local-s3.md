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
3. **The Spark sink's pre-write conflict check let a raw storage error escape** (found when wiring Spark to
   MinIO, see the addendum below). It reads each destination before writing so it cannot overwrite different data;
   a read that *failed* (as opposed to "not found") escaped as an unhandled `StorageError`. It is now a retryable
   `BatchCommitError` naming the destination, raised before anything is written anywhere.

## What MinIO cannot tell us (so it is not claimed)

- **403 versus 404 on a missing object: mitigated in code, still unproven on real AWS.** AWS documents that
  `GetObject`/`HeadObject` on a missing key returns 403, not 404, when the caller lacks an unconditioned
  `s3:ListBucket`. The Terraform pipeline role grants `ListBucket` only under a `s3:prefix` condition, which a plain
  GET or HEAD does not carry. MinIO answers 404 here (the live run showed it does so even for a caller with no
  `ListBucket` at all), so the live tests alone could never have caught it. The commit layer, the Spark sink and the
  loader all decide "is this batch new?" from a missing object, so a 403 would have failed every new batch.
  **The fix (adapter, Terraform unchanged):** when a read or existence check answers 403/`AccessDenied`, the adapter
  lists the prefix equal to that exact key, which the role may do everywhere it may write. The key is present only
  if it is the first entry returned; absent means "not found". If the key is present but unreadable, or if the
  probe is refused too, the result is a *permission error*, never "missing": guessing "missing" could make a batch
  overwrite committed data. A present key costs no extra request. **Verified** against a client double that
  reproduces AWS's worst case (403 for every missing key, prefix list allowed), whose permissions a test compares
  with the Terraform role so the two cannot drift; against the commit path, the Spark sink's conflict check and
  the loader's reads; and live on MinIO for the two permission-error branches (an existing key the role may not
  read, with the probe allowed and with it refused). **Not verified:** that real AWS really answers 403 here, and
  that its prefixed `ListBucket` condition accepts the probe as designed. Both need a real account (the same
  `test_minio.py` assertions can be pointed at S3). Widening the role's `ListBucket` was rejected: it would loosen
  least privilege to work around a client-side ambiguity the client can resolve itself.
- `ListBuckets` is refused by AWS but filtered by MinIO; the test asserts the property that matters (no other bucket
  is visible) rather than the status code.
- IAM evaluation, KMS/SSE encryption, the `DenyInsecureTransport` bucket policy (local MinIO is plain HTTP) and
  lifecycle rules are AWS features that are not exercised.
- Throughput, large-object (multipart) behaviour and concurrency are untested.

## Consequences

- Compose gains `minio` and `minio-init` under the opt-in `minio` profile (about 0.2 GB of memory; a 206 MB image).
- The Spark container follows the backend setting too (see the addendum). The Airflow DAGs and the metrics-exporter
  container still use local directories and are not wired to MinIO.
- The MinIO project itself is not maintained. For a local, loopback, synthetic-data stand-in that is acceptable;
  for anything else it would not be.

## Addendum: the Spark streaming job writes to MinIO (2026-11-11)

The Spark container is the component that writes the raw zone in the streaming path, so it follows the same
`ECOMM_S3_BACKEND` setting through the same factory (`open_lake_in`, for the job's `--lake-dir`).

- **Credentials and network.** The container reads the generated pipeline-user file (`docker/minio/.env`, optional)
  and is given `ECOMM_S3_ENDPOINT_URL=http://minio:9000`, a host name on the compose network (the file holds the
  loopback address the host uses). The root credentials never reach it, and no key is written in compose. The
  `spark` and `minio` profiles stay independent: a local run needs no MinIO. `boto3` and `botocore` are added to the
  image at the versions in `uv.lock` as the last layer (a test checks the versions), so the container runs the
  adapter the tests ran against and the large PySpark layers keep their build cache.
- **Fail before Spark starts.** The job lists both destinations first. An unreachable one exits `1` in seconds,
  a missing MinIO configuration exits `2` with the fix, and neither boots the JVM. The alternative was a minute of
  start-up followed by a failure on the first write.
- **Unreadable is not empty.** See finding 3. Choosing to fail rather than assume "nothing there" is deliberate: the
  conflict check exists to stop a checkpoint reset from silently overwriting different data.
- **Verification.** `tests/integration/test_spark_minio.py` runs the real container against Kafka and MinIO: crash
  after a commit, MinIO stopped, restart, rerun. Result equals the reference ingest, both destinations are
  byte-identical, offsets are gapless and the warehouse loaded from MinIO has no double counting (13 checks passing,
  about 18 minutes on this machine). With MinIO stopped the job failed fast and the local GCS copy was untouched.
- **Not verified:** MinIO failing in the middle of a running micro-batch (not reproducible enough to test; the sink
  is unit-tested against an unreadable and an unwritable destination), real AWS's 403 behaviour (see above), and
  anything about throughput.

## Addendum 2: the Airflow DAGs and the exporter container read the bucket too (2026-11-12)

The last two containers that touch the raw zone now follow `ECOMM_S3_BACKEND` as well, so no component is
left on a local-only path. Each gets the pipeline user's key and `http://minio:9000`, never the root credentials;
`boto3`/`botocore` are added at the locked versions (the Airflow one in its isolated project virtualenv, not
Airflow's own constrained environment). A static test checks the env files, endpoints, absence of `depends_on`
(independent profiles) and the pinned versions for all four services that need them.

- **Readiness check.** The DAG's `check_lake_ready` was a shell `find` on a local folder. It is now
  `ecomm.lake.ready_cli` (0 = batches present, 99 = nothing yet so skip, 1 = missing, misconfigured or
  unreachable). The design choice worth recording: an unreachable lake exits 1 and **never 99**, because a skipped
  run looks healthy and would hide an outage. Tested for each outcome, for the contract with the DAG's skip code,
  for a typo in `--demo-dir` (nothing is created), and live inside the Airflow container with the bucket
  unreachable.
- **Verification.** Airflow: 7 live checks. All four DAGs succeed against a bucket-backed raw zone, the readiness
  check read the bucket, the warehouse holds each committed event once, the reconciliation compared MinIO with the
  GCS copy, and every bucket object is byte-identical afterwards (the DAGs only read). Exporter: 5 live checks.
  Numbers read from the bucket equal what was committed, parity agrees, the exporter never writes, and with MinIO
  stopped it keeps serving and names the failing sections (`lake`, `parity`) and then recovers by itself.
- **A finding that was not about MinIO.** The first exporter run took 167 s per refresh and the cadence check
  failed. Profiling inside the image, step by step, showed MinIO answering in 0.02 to 1.5 s while the *local*
  store took 10 to 56 s per listing, because `LocalObjectStore.list_objects` walked and `stat`-ed every file under
  its root and only then filtered by prefix; one refresh does several such listings. It now walks only the
  subtree the prefix can match (`os.walk`, no per-file `stat`). Measured on the same data and filesystem: one
  refresh 188 to 237 s down to about 15 s on a local lake of 1,772 files, and 31 to 36 s down to 5 to 14 s with the S3
  side in MinIO. The cadence assertion was kept at 30 s, not loosened. Behaviour is pinned by 24 equivalence tests
  (every kind of prefix against a brute-force reference, hostile prefixes) written before the change and passing
  on the old code, plus two that prove other branches are never opened and failed on the old code.
- **Test hygiene found on the way.** The earlier MinIO test never removed its own batches, so a later test that
  needs a clean bucket skipped instead of running; and a Docker crash during the Spark test left objects behind.
  The fixture now cleans up after itself, and the new tests refuse to run (skip, with the reason) rather than
  delete data they did not create.
- **Not verified:** the exporter against a bucket holding many thousands of batches (every refresh reads every
  manifest over HTTP, so it is O(batches); fine at demo scale, not measured beyond it), a MinIO outage in the middle
  of a DAG task (the readiness check is tested unreachable, the DAG tasks themselves are not), and real AWS.

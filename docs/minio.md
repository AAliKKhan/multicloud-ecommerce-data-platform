# MinIO: a local S3 for the raw zone

MinIO stands in for **S3** so the real S3 adapter runs over real HTTP, with real permissions, instead of against a
folder. Only the S3 copy of the raw zone moves; the GCS copy and the control store (ledger, statistics, reports)
stay local directories. Design, the upstream-status story and the limits: [ADR 0015](decisions/0015-minio-local-s3.md).

Everything runs on loopback with synthetic data. It is a development stand-in, not a production store.

## Start it

```bash
python docker/minio/generate_env.py --print-login   # once: random secrets, git-ignored
docker compose --profile minio up -d --build        # first build compiles MinIO from source: be patient
docker compose --profile minio run --rm minio-init  # bucket, policy, pipeline user (safe to repeat)
```

| What | Where |
|---|---|
| S3 API | `http://127.0.0.1:9000` |
| Console (object browser) | `http://127.0.0.1:9001`; log in with the root user in `docker/minio/.env.root` |
| Bucket | `ecomm-raw-minio`, private and versioned |
| Pipeline credentials | `docker/minio/.env` (a least-privilege user, never root) |

The image is built from the pinned, patched source tag because upstream is archived and no longer publishes images.
The first build took about 25 minutes on a 5 GB laptop (it compiles about two thousand Go packages; the output is
quiet while it does, so do not mistake the silence for a hang) and about 200 MB of memory afterwards; later builds
reuse the Go caches.

## Use it

Select the backend with one variable. The default is the local directory, so nothing changes unless you ask:

```powershell
$env:ECOMM_S3_BACKEND = "minio"                 # PowerShell;  cmd: set ECOMM_S3_BACKEND=minio
uv run ecomm-demo --out data/minio-demo         # the S3 side of the raw zone is written to MinIO
uv run ecomm-reconcile --demo-dir data/minio-demo
uv run ecomm-quality   --demo-dir data/minio-demo
```

These commands follow the setting: `ecomm-demo`, `ecomm-load`, `ecomm-reconcile`, `ecomm-quality`,
`ecomm-quarantine`, `ecomm-metrics`. The destination is then called `s3-minio` in manifests, reports and metrics.
Settings come from `docker/minio/.env` and the process environment (which wins):
`ECOMM_S3_ENDPOINT_URL`, `ECOMM_S3_BUCKET`, `ECOMM_S3_ACCESS_KEY`, `ECOMM_S3_SECRET_KEY`.

The **Spark streaming job** follows the same setting, in its container (as the pipeline user, over the compose
network): [spark.md](spark.md#landing-the-s3-side-in-minio). So do the **Airflow DAGs**
([airflow.md](airflow.md#against-a-minio-raw-zone)) and the **metrics exporter**
([monitoring.md](monitoring.md#against-a-minio-raw-zone)): every container that touches the raw zone now follows
the one setting. Each gets only the pipeline user's key, never the root credentials.

## What is verified

`tests/integration/test_minio.py` runs 21 checks against the live server (also in CI, job `minio`):

- **Adapter contract over real HTTP:** round trip, overwrite, sorted prefix listing, binary data, the real key
  layout, "missing object" semantics, invalid keys rejected before any request.
- **Least privilege:** the pipeline user can read, write and list under the five pipeline prefixes, and cannot delete
  an object or a version, write or list elsewhere, see or reach another bucket, or change the bucket's versioning.
  Anonymous access is refused. The policy is checked, by a unit test, to be identical to the Terraform pipeline role.
- **The whole pipeline, compared with the local backend:** same counts, **byte-identical raw, manifest and quarantine
  objects**, and the same events in the warehouse loaded from MinIO.
- **Replay:** running the same batches again writes no new objects and loads zero rows.
- **Outage:** an unreachable endpoint gives PARTIAL within seconds (not a hang), the healthy side is written, and the
  retry completes only the failed side. Wrong credentials fail fast without echoing the secret.

## Spark, end to end

`tests/integration/test_spark_minio.py` (13 checks, also in CI as job `spark-minio`) runs the real Spark container
against Kafka and MinIO: crash after a commit, MinIO stopped, restart, rerun. The result equals the reference
ingest, the MinIO and local-GCS copies are byte-identical, and the warehouse loaded from MinIO has no double
counting. With MinIO stopped the job exits in seconds before Spark starts and writes nothing. It took about 18
minutes on this 5 GB machine (four Spark runs).

## Found by running it

Three real defects, plus one documented risk now mitigated, all fixed test-first and covered by unit tests: `commit_batch` crashed when a destination could
not be *read* (so one unreachable cloud lost the whole commit instead of giving PARTIAL), a refactor had made the
"no lake yet" checks always pass, and the Spark sink's pre-write conflict check let a raw storage error escape
when a destination was unreadable. Details in the ADR.

## What it cannot prove

MinIO is not AWS. In particular, AWS is documented to answer 403 (not 404) for a missing object when the caller lacks
an unconditioned `s3:ListBucket`, and the Terraform pipeline role grants `ListBucket` only under a prefix condition.
MinIO answers 404 there (even for a caller with no `ListBucket`), so live tests on MinIO can never show it.

The adapter now handles it: on a 403 it lists the prefix equal to that key (allowed under the role) to tell a
missing object from a forbidden one, and reports an unresolved case as a permission error, never as "missing".
That is verified against a client double that reproduces AWS's worst case, with its permissions checked against the
Terraform role, and live on MinIO for the permission-error branches. What stays unproven until a real account is
used is that AWS really answers 403 here and accepts the probe under the prefix condition.

Encryption (KMS/SSE), the HTTPS-only bucket policy, lifecycle rules and IAM itself are not exercised either, and
nothing here is load-tested.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ECOMM_S3_BACKEND=minio needs ...` | Run `python docker/minio/generate_env.py`. |
| `AccessDenied` on a write | The pipeline user may only write under `raw/ manifests/ quarantine/ ledger/ reports/`. |
| The integration test skips "bucket already holds other batches" | It needs a clean bucket: `docker compose --profile minio down -v`, then up and init again. |
| `connection refused` | MinIO is not running: `docker compose --profile minio up -d`. |
| `minio` refuses to start | The root secrets are missing; the container says how to create them. |
| `pull access denied` for MinIO images | Expected: upstream removed them. Use the built `ecomm-minio` image (`--build`). |
| Rotating secrets | `generate_env.py --force`, then `down -v` (the old user lives in the volume). |

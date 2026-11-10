# Infrastructure (Terraform)

Terraform describes the cloud side of the platform. **Nothing here has ever been
applied**: there is no AWS or GCP account behind this project yet. The
configuration is validated (`terraform validate`, `terraform fmt`) and checked by
static tests, but a real `plan` and `apply` are unverified.

Local mode needs none of this. Cloud resources are opt-in, small and easy to
destroy.

## What each stack creates

| Stack | Resources |
|---|---|
| `infra/terraform/aws` | Raw S3 bucket (public access blocked, ACLs disabled, SSE-S3 or your KMS key, TLS-only policy, optional versioning, lifecycle rules); least-privilege IAM **policies** (pipeline read/write without delete, analyst read-only); optional pipeline **role**; Glue database and `raw_events` table using partition projection (no crawler); optional monthly budget |
| `infra/terraform/gcp` | Raw GCS bucket (uniform access, public access prevention enforced, versioning, lifecycle); raw and `_analytics` BigQuery datasets; one pipeline service account with narrow roles; optional analyst read access; optional budget; APIs enabled but never disabled on destroy |

Deliberately **not** created: IAM users, access keys, service-account keys,
crawlers, Dataflow, Kubernetes, or anything with public access.

## Configuration contract

Terraform outputs map directly to the application settings in `.env`:

| Output | Setting |
|---|---|
| AWS `raw_bucket_name` | `AWS_S3_BUCKET` |
| GCP `raw_bucket_name` | `GCS_BUCKET` |
| GCP `raw_dataset_id` | `BIGQUERY_DATASET` (dbt writes to `<dataset>_analytics`) |
| AWS/GCP region variables | `AWS_REGION`, `GCP_REGION` |

Credentials are never in Terraform or `.env`. Use the AWS credential chain (SSO,
profile, assumed role) and Google Application Default Credentials or service
account impersonation.

## Locations and regions

- Keep the GCS bucket and both BigQuery datasets in the **same location** (`location`
  variable) so loads and queries do not cross locations.
- Pick the AWS region closest to where the pipeline runs. Copying data between
  AWS and GCP incurs data-transfer charges, so keep the synthetic datasets small.
- S3 and GCS bucket names are global: choose unique names.

## Before you apply (checklist)

1. Create a **separate** AWS account/profile and GCP project for `dev`. Never reuse
   production credentials.
2. Copy `dev.tfvars.example` to `dev.tfvars` (git-ignored) in each stack and edit it.
3. For anything shared, copy `backend.tf.example` to `backend.tf` and use remote
   state. Local state is acceptable only for a personal sandbox and must never be committed.
4. Review the cost notes below and set the budget variables.

## Commands

```bash
cd infra/terraform/aws        # or gcp
terraform init
terraform plan  -var-file=dev.tfvars -out=tfplan   # read the plan carefully
terraform apply tfplan                              # only with explicit approval
```

Destroy (sandbox buckets need `force_destroy = true` if they contain data):

```bash
terraform destroy -var-file=dev.tfvars
```

Teardown checklist: run `destroy` in both stacks, confirm the buckets, the
datasets, the Glue database and the IAM resources are gone in the consoles, and
check the billing pages the next day. Budgets alert, they do not stop spending.

## Cost notes (no figures on purpose)

Costs depend on region, volume, retention, query patterns and current pricing,
so none are claimed here. The configuration limits exposure by default:

- Demo-sized synthetic data; `raw_expiration_days` expires demo objects.
- Short non-current-version retention; incomplete uploads aborted after 7 days.
- No crawlers, no streaming inserts, bulk load jobs only.
- BigQuery tables partitioned by `event_date`; the dbt BigQuery profile sets
  `maximum_bytes_billed` per query; `table_expiration_days` for dev.
- Free-tier eligibility is not guaranteed free usage; quotas and prices change.

## Verification status

| Check | Status |
|---|---|
| `terraform fmt -check`, `terraform validate` (both stacks) | Run locally and in CI |
| Static safety tests (`tests/unit/infra`) | Run in CI: no secrets/keys, private encrypted buckets, narrow roles, nothing billable by default |
| `terraform plan` / `apply` against a real account | **Not done** (no credentials, no approval) |
| IAM policy behaviour with the real S3/GCS adapters | **Unverified** |

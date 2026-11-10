locals {
  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
    data        = "synthetic"
  }

  # Prefixes the pipeline may touch (see ADR 0006 for the layout).
  pipeline_prefixes = ["raw/", "manifests/", "quarantine/", "ledger/", "reports/"]
  analyst_prefixes  = ["raw/", "manifests/"]

  event_types = [
    "order_created", "order_updated", "order_cancelled",
    "payment_authorized", "payment_captured", "payment_failed", "payment_refunded",
    "stock_received", "stock_reserved", "stock_adjusted",
    "customer_created", "customer_updated",
  ]

  glue_database = replace("${var.project_name}_${var.environment}_raw", "-", "_")
  name_prefix   = "${var.project_name}-${var.environment}"
}

# --- Raw-zone bucket ---------------------------------------------------------

resource "aws_s3_bucket" "raw" {
  bucket        = var.raw_bucket_name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    object_ownership = "BucketOwnerEnforced" # disables ACLs entirely
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    bucket_key_enabled = var.kms_key_arn != null

    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn != null ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_arn
    }
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  # Versioning must exist before lifecycle rules that reference versions.
  depends_on = [aws_s3_bucket_versioning.raw]

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  dynamic "rule" {
    for_each = var.enable_versioning ? [1] : []

    content {
      id     = "expire-noncurrent-versions"
      status = "Enabled"

      filter {}

      noncurrent_version_expiration {
        noncurrent_days = var.noncurrent_version_expiration_days
      }
    }
  }

  dynamic "rule" {
    for_each = var.raw_expiration_days > 0 ? [1] : []

    content {
      id     = "expire-demo-data"
      status = "Enabled"

      filter {}

      expiration {
        days = var.raw_expiration_days
      }
    }
  }
}

data "aws_iam_policy_document" "bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "raw" {
  bucket = aws_s3_bucket.raw.id
  policy = data.aws_iam_policy_document.bucket.json

  depends_on = [aws_s3_bucket_public_access_block.raw]
}

# --- Least-privilege IAM -----------------------------------------------------
# No IAM users and no access keys are created: use roles / SSO / OIDC.
# The pipeline policy has NO delete permission; overwriting an object (the
# retry protocol) only needs PutObject.

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListPipelinePrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = concat(local.pipeline_prefixes, [for p in local.pipeline_prefixes : "${p}*"])
    }
  }

  statement {
    sid       = "ReadWritePipelineObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = [for p in local.pipeline_prefixes : "${aws_s3_bucket.raw.arn}/${p}*"]
  }

  statement {
    sid       = "BucketLocation"
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn]
  }

  dynamic "statement" {
    for_each = var.kms_key_arn != null ? [1] : []

    content {
      sid       = "UseCustomerManagedKey"
      actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
      resources = [var.kms_key_arn]
    }
  }
}

data "aws_iam_policy_document" "analyst" {
  statement {
    sid       = "ListReadablePrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = concat(local.analyst_prefixes, [for p in local.analyst_prefixes : "${p}*"])
    }
  }

  statement {
    sid       = "ReadObjects"
    actions   = ["s3:GetObject"]
    resources = [for p in local.analyst_prefixes : "${aws_s3_bucket.raw.arn}/${p}*"]
  }

  dynamic "statement" {
    for_each = var.kms_key_arn != null ? [1] : []

    content {
      sid       = "DecryptWithCustomerManagedKey"
      actions   = ["kms:Decrypt"]
      resources = [var.kms_key_arn]
    }
  }
}

resource "aws_iam_policy" "pipeline" {
  name        = "${local.name_prefix}-raw-pipeline"
  description = "Read/write the pipeline prefixes of the raw bucket. No delete."
  policy      = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_policy" "analyst" {
  name        = "${local.name_prefix}-raw-read"
  description = "Read-only access to raw data and manifests."
  policy      = data.aws_iam_policy_document.analyst.json
}

data "aws_iam_policy_document" "pipeline_trust" {
  count = length(var.pipeline_principal_arns) > 0 ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = var.pipeline_principal_arns
    }
  }
}

resource "aws_iam_role" "pipeline" {
  count = length(var.pipeline_principal_arns) > 0 ? 1 : 0

  name                 = "${local.name_prefix}-pipeline"
  assume_role_policy   = data.aws_iam_policy_document.pipeline_trust[0].json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "pipeline" {
  count = length(var.pipeline_principal_arns) > 0 ? 1 : 0

  role       = aws_iam_role.pipeline[0].name
  policy_arn = aws_iam_policy.pipeline.arn
}

# --- Glue Data Catalog -------------------------------------------------------
# Partition projection means Athena finds partitions from the key layout, so no
# crawler (and no crawler cost) is needed. Only the event envelope is exposed;
# the payload is analysed in the warehouse.

resource "aws_glue_catalog_database" "raw" {
  count = var.create_glue_catalog ? 1 : 0

  name        = local.glue_database
  description = "Raw e-commerce events (synthetic). Catalogue for the S3 copy."
}

resource "aws_glue_catalog_table" "raw_events" {
  count = var.create_glue_catalog ? 1 : 0

  name          = "raw_events"
  database_name = aws_glue_catalog_database.raw[0].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"               = "json"
    "EXTERNAL"                     = "TRUE"
    "projection.enabled"           = "true"
    "projection.event_type.type"   = "enum"
    "projection.event_type.values" = join(",", local.event_types)
    "projection.event_date.type"   = "date"
    "projection.event_date.range"  = "2026-01-01,NOW"
    "projection.event_date.format" = "yyyy-MM-dd"
    "projection.event_hour.type"   = "integer"
    "projection.event_hour.range"  = "0,23"
    "projection.event_hour.digits" = "2"
    "storage.location.template"    = "s3://${aws_s3_bucket.raw.bucket}/raw/${var.source_name}/$${event_type}/event_date=$${event_date}/event_hour=$${event_hour}"
  }

  partition_keys {
    name = "event_type"
    type = "string"
  }

  partition_keys {
    name = "event_date"
    type = "string"
  }

  partition_keys {
    name = "event_hour"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.bucket}/raw/${var.source_name}/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "ingest"
      type = "struct<run_id:string,batch_id:string,source_topic:string,kafka_partition:int,kafka_offset:bigint,ingested_at:string,is_late:boolean>"
    }

    columns {
      name = "event"
      type = "struct<event_id:string,event_type:string,schema_version:int,event_time:string,producer:string,correlation_id:string>"
    }
  }
}

# --- Budget ------------------------------------------------------------------

resource "aws_budgets_budget" "monthly" {
  count = var.monthly_budget_usd > 0 ? 1 : 0

  name         = "${local.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.budget_alert_emails
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.budget_alert_emails
  }

  lifecycle {
    precondition {
      condition     = length(var.budget_alert_emails) > 0
      error_message = "Set budget_alert_emails when monthly_budget_usd is greater than 0."
    }
  }
}

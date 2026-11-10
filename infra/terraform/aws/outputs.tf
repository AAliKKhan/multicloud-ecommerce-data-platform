output "raw_bucket_name" {
  description = "Value for AWS_S3_BUCKET."
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "pipeline_policy_arn" {
  description = "Attach to the identity that runs the pipeline."
  value       = aws_iam_policy.pipeline.arn
}

output "analyst_policy_arn" {
  description = "Attach to read-only users or roles."
  value       = aws_iam_policy.analyst.arn
}

output "pipeline_role_arn" {
  description = "Null unless pipeline_principal_arns was set."
  value       = try(aws_iam_role.pipeline[0].arn, null)
}

output "glue_database" {
  description = "Glue database name, or null when the catalog is disabled."
  value       = try(aws_glue_catalog_database.raw[0].name, null)
}

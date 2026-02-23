
output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.main.endpoint
}

output "landing_bucket" {
  value = aws_s3_bucket.landing.bucket
}

output "glue_scripts_bucket" {
  value = aws_s3_bucket.glue_scripts.bucket
}

output "redshift_secret_arn" {
  value = aws_secretsmanager_secret.redshift_password.arn
}

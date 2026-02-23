# -------------------------------------------------------
# Glue — CSV ingestion
#
# We upload 3 things to S3:
#   1. The Glue script itself
#   2. sales.csv
#   3. customers.csv
#   4. products.csv
#
# etag = filemd5(...) means Terraform re-uploads the file
# automatically if its contents change.
# -------------------------------------------------------

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.glue_scripts.bucket
  key    = "scripts/csv_ingest.py"
  source = "${path.module}/../../ingestion/glue/csv_ingest.py"
  etag   = filemd5("${path.module}/../../ingestion/glue/csv_ingest.py")
}

resource "aws_s3_object" "sales_csv" {
  bucket = aws_s3_bucket.landing.bucket
  key    = "uploads/sales/sales.csv"
  source = "${path.module}/../../ingestion/sample_data/sales.csv"
  etag   = filemd5("${path.module}/../../ingestion/sample_data/sales.csv")
}

resource "aws_s3_object" "customers_csv" {
  bucket = aws_s3_bucket.landing.bucket
  key    = "uploads/customers/customers.csv"
  source = "${path.module}/../../ingestion/sample_data/customers.csv"
  etag   = filemd5("${path.module}/../../ingestion/sample_data/customers.csv")
}

resource "aws_s3_object" "products_csv" {
  bucket = aws_s3_bucket.landing.bucket
  key    = "uploads/products/products.csv"
  source = "${path.module}/../../ingestion/sample_data/products.csv"
  etag   = filemd5("${path.module}/../../ingestion/sample_data/products.csv")
}

resource "aws_glue_job" "csv_ingest" {
  name     = "${var.project_name}-csv-ingest"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "pythonshell"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/csv_ingest.py"
  }

  default_arguments = {
    "--S3_BUCKET"    = aws_s3_bucket.landing.bucket
    "--job-language" = "python"
  }
  max_capacity = 0.0625 # minimum — costs almost nothing
  timeout      = 10
}

output "glue_job_name" {
  value = aws_glue_job.csv_ingest.name
}

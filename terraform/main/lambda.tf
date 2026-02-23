data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../../ingestion/lambda/fakestore_ingest.py"
  output_path = "${path.module}/../../ingestion/lambda/fakestore_ingest.zip"
}

resource "aws_lambda_function" "fakestore_ingest" {
  function_name    = "${var.project_name}-fakestore-ingest"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.11"
  handler          = "fakestore_ingest.handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60
  memory_size      = 128

  # No vpc_config block — Lambda runs outside VPC
  # It can reach FakeStore API directly via internet
  # S3 is reachable via AWS internal network regardless

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.landing.bucket
    }
  }
}

resource "aws_cloudwatch_event_rule" "daily_ingest" {
  name                = "${var.project_name}-daily-ingest"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule = aws_cloudwatch_event_rule.daily_ingest.name
  arn  = aws_lambda_function.fakestore_ingest.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fakestore_ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_ingest.arn
}

output "lambda_function_name" {
  value = aws_lambda_function.fakestore_ingest.function_name
}

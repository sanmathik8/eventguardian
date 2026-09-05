resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/eventguardian-processor"
  retention_in_days = 7
}

resource "aws_lambda_function" "processor" {
  function_name = "eventguardian-processor"

  filename         = "../lambda_function.zip"
  source_code_hash = filebase64sha256("../lambda_function.zip")

  role    = aws_iam_role.lambda_role.arn
  handler = "app.lambda_handler"
  runtime = "python3.13"

  timeout     = 30
  memory_size = 256

  environment {
    variables = {
      IDEMPOTENCY_TABLE = aws_dynamodb_table.idempotency.name
      OUTPUT_BUCKET     = aws_s3_bucket.processed.bucket
    }
  }

  tags = {
    Project     = "EventGuardian"
    Environment = "Dev"
    Owner       = "Sanmathi"
  }

  depends_on = [
    aws_iam_role_policy.lambda_policy,
    aws_cloudwatch_log_group.lambda_logs
  ]
}

resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.events.arn
  function_name    = aws_lambda_function.processor.arn

  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 50
  }
}

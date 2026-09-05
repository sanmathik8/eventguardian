resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name        = "eventguardian-dlq-messages"
  alarm_description = "Alert when messages appear in the EventGuardian DLQ"

  namespace   = "AWS/SQS"
  metric_name = "ApproximateNumberOfMessagesVisible"

  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  alarm_actions = [
    aws_sns_topic.dlq_alerts.arn
  ]

  treat_missing_data = "notBreaching"

  tags = {
    Project     = "EventGuardian"
    Environment = "Dev"
    Owner       = "Sanmathi"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name        = "eventguardian-lambda-errors"
  alarm_description = "Alert when Lambda function errors occur"

  namespace   = "AWS/Lambda"
  metric_name = "Errors"

  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"

  dimensions = {
    FunctionName = aws_lambda_function.processor.function_name
  }

  alarm_actions = [
    aws_sns_topic.dlq_alerts.arn
  ]

  treat_missing_data = "notBreaching"

  tags = {
    Project     = "EventGuardian"
    Environment = "Dev"
    Owner       = "Sanmathi"
  }
}

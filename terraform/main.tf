# terraform/main.tf
# Full infrastructure: Lambda + ESM + MSK + IAM + DLQ
# terraform init && terraform apply -var="environment=dev"

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region"       { default = "us-east-1" }
variable "environment"      { default = "dev" }
variable "vpc_id"           { description = "VPC where MSK lives" }
variable "subnet_ids"       { type = list(string) }
variable "msk_cluster_arn"  { description = "Existing MSK cluster ARN" }
variable "kafka_topics"     { default = ["orders.created", "payments.processed", "user.events"] }

locals {
  name = "kafka-consumer-${var.environment}"
  tags = { Environment = var.environment, ManagedBy = "terraform", Service = "kafka-consumer" }
}

# ── IAM Role ───────────────────────────────────────────────────────────────
resource "aws_iam_role" "lambda" {
  name = "${local.name}-role"
  tags = local.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "kafka_and_dlq" {
  name = "${local.name}-kafka-dlq"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka:DescribeCluster",
          "kafka:GetBootstrapBrokers",
          "kafka:ListTopics",
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData",
        ]
        Resource = [var.msk_cluster_arn, "${var.msk_cluster_arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.dlq.arn
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      }
    ]
  })
}

# ── Dead Letter Queue ──────────────────────────────────────────────────────
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.name}-dlq"
  message_retention_seconds = 1209600  # 14 days
  tags                      = local.tags
}

# ── Security Group ─────────────────────────────────────────────────────────
resource "aws_security_group" "lambda" {
  name   = "${local.name}-sg"
  vpc_id = var.vpc_id
  tags   = local.tags

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── Lambda Function ────────────────────────────────────────────────────────
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../lambda.zip"
}

resource "aws_lambda_function" "consumer" {
  function_name    = local.name
  role             = aws_iam_role.lambda.arn
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  runtime          = "nodejs20.x"
  handler          = "handler.handler"
  memory_size      = 512
  timeout          = 300
  tags             = local.tags

  environment {
    variables = {
      LOG_LEVEL          = var.environment == "prod" ? "INFO" : "DEBUG"
      ENVIRONMENT        = var.environment
      SERVICE_NAME       = "kafka-lambda-consumer"
      METRICS_NAMESPACE  = "KafkaLambdaConsumer/${var.environment}"
    }
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }
}

# ── Event Source Mapping ───────────────────────────────────────────────────
resource "aws_lambda_event_source_mapping" "kafka" {
  function_name     = aws_lambda_function.consumer.arn
  event_source_arn  = var.msk_cluster_arn
  topics            = var.kafka_topics
  starting_position = "LATEST"
  batch_size        = 100

  # Collect up to 5 seconds of messages before triggering (improves efficiency)
  maximum_batching_window_in_seconds = 5

  # On total failure → send batch metadata to DLQ
  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }
}

# ── CloudWatch Alarm: Consumer Lag ────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  alarm_name          = "${local.name}-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "OffsetLag"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Maximum"
  threshold           = 10000
  alarm_description   = "Kafka consumer lag exceeded 10k messages"
  tags                = local.tags

  dimensions = {
    FunctionName = aws_lambda_function.consumer.function_name
    EventSourceMappingID = aws_lambda_event_source_mapping.kafka.uuid
  }
}

# ── Outputs ────────────────────────────────────────────────────────────────
output "function_arn"       { value = aws_lambda_function.consumer.arn }
output "esm_uuid"           { value = aws_lambda_event_source_mapping.kafka.uuid }
output "dlq_url"            { value = aws_sqs_queue.dlq.url }
output "security_group_id"  { value = aws_security_group.lambda.id }

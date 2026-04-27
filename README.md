# Kafka → AWS Lambda ESM Consumer

Production-ready serverless Kafka consumer using AWS Lambda Event Source Mapping (ESM).

## Project Structure

```
kafka-lambda/
├── src/
│   ├── handler.mjs      # Lambda entrypoint — decodes, routes, orchestrates
│   ├── processor.mjs    # Domain handlers per topic (add yours here)
│   ├── logger.mjs       # Structured JSON logger (CloudWatch / Datadog ready)
│   └── metrics.mjs      # CloudWatch EMF metrics (zero extra API calls)
├── tests/
│   └── test-local.mjs   # Local test runner (no AWS needed)
├── logs/
│   └── sample-cloudwatch-output.log  # Sample CW log output + Insights queries
├── scripts/
│   └── deploy.sh        # CLI packaging & deployment script
├── terraform/
│   └── main.tf          # Full IaC: Lambda + ESM + IAM + DLQ + Alarm
└── package.json
```

## Quick Start

### 1. Test locally

```bash
# All scenarios
npm run test:all

# Individual scenarios
npm run test:order    # Single order event
npm run test:payment  # Payment with idempotency check
npm run test:batch    # Multi-topic, multi-partition batch
npm run test:unknown  # Unknown topic (graceful skip)
```

### 2. Deploy to AWS

```bash
# Prerequisites: AWS CLI configured, Lambda function already created
npm run deploy:dev
npm run deploy:prod
```

### 3. Wire up the ESM

Edit `scripts/deploy.sh` and uncomment the `create-event-source-mapping` block.  
Set your `MSK_CLUSTER_ARN` environment variable and run:

```bash
MSK_CLUSTER_ARN=arn:aws:kafka:... ./scripts/deploy.sh dev
```

Or use Terraform:

```bash
cd terraform
terraform init
terraform apply \
  -var="environment=dev" \
  -var="vpc_id=vpc-xxx" \
  -var='subnet_ids=["subnet-a","subnet-b"]' \
  -var="msk_cluster_arn=arn:aws:kafka:..."
```

## Adding a New Topic

1. Write a handler function in `src/processor.mjs`
2. Register it in `TOPIC_HANDLERS`
3. Add a test scenario in `tests/test-local.mjs`
4. Add the topic to `var.kafka_topics` in Terraform

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `ENVIRONMENT` | `unknown` | Injected into every log line |
| `SERVICE_NAME` | `kafka-lambda-consumer` | Log tag |
| `METRICS_NAMESPACE` | `KafkaLambdaConsumer` | CloudWatch namespace |

## CloudWatch Logs Insights

```
# All errors
fields @timestamp, message, topic, offset, error
| filter level = "ERROR"
| sort @timestamp desc

# Throughput by topic (5-min buckets)
filter ispresent(topic) and level = "INFO"
| stats count() by topic, bin(5m)

# Average invocation duration
filter message = "ESM invocation complete"
| stats avg(durationMs), max(durationMs) by bin(1h)
```

## IAM Permissions Required

- `AWSLambdaBasicExecutionRole`
- `AWSLambdaVPCAccessExecutionRole`
- `kafka-cluster:Connect`, `ReadData`, `DescribeTopic`, `DescribeGroup`, `AlterGroup`
- `sqs:SendMessage` on your DLQ

## Production Checklist

- [ ] Idempotency checks implemented (DynamoDB / Redis)
- [ ] DLQ configured on ESM destination
- [ ] CloudWatch consumer lag alarm set
- [ ] `LOG_LEVEL=INFO` in prod (not DEBUG)
- [ ] VPC security group allows egress to MSK broker ports (9092 / 9094 / 9096)
- [ ] Memory tuned (start 512 MB, adjust based on actual usage)
- [ ] Batch size tuned for your throughput vs latency tradeoff

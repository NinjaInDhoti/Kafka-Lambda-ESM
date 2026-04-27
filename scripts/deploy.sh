#!/usr/bin/env bash
# deploy.sh — Packages and deploys the Kafka Lambda consumer to AWS
# Usage: ./scripts/deploy.sh [dev|staging|prod]
set -euo pipefail

ENVIRONMENT="${1:-dev}"
FUNCTION_NAME="kafka-consumer-${ENVIRONMENT}"
REGION="${AWS_REGION:-us-east-1}"
RUNTIME="nodejs20.x"
MEMORY_MB=512
TIMEOUT_SEC=300      # ESM: max 5 min per invocation
HANDLER="src/handler.handler"

echo "🚀 Deploying ${FUNCTION_NAME} to ${REGION} (${ENVIRONMENT})"

# ── 1. Package ─────────────────────────────────────────────────────────────
echo "📦 Packaging Lambda..."
rm -f lambda.zip
zip -r lambda.zip src/ package.json --exclude "*.test.*" --exclude "__tests__/*"

# ── 2. Update Function Code ────────────────────────────────────────────────
echo "📤 Uploading code..."
aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file fileb://lambda.zip \
  --region "${REGION}" \
  --output json | jq '{FunctionArn, CodeSize, LastModified}'

# Wait for update to propagate
aws lambda wait function-updated \
  --function-name "${FUNCTION_NAME}" \
  --region "${REGION}"

# ── 3. Update Config ───────────────────────────────────────────────────────
echo "⚙️  Updating configuration..."
aws lambda update-function-configuration \
  --function-name "${FUNCTION_NAME}" \
  --runtime "${RUNTIME}" \
  --handler "${HANDLER}" \
  --memory-size "${MEMORY_MB}" \
  --timeout "${TIMEOUT_SEC}" \
  --environment "Variables={
    LOG_LEVEL=INFO,
    ENVIRONMENT=${ENVIRONMENT},
    SERVICE_NAME=kafka-lambda-consumer,
    METRICS_NAMESPACE=KafkaLambdaConsumer/${ENVIRONMENT}
  }" \
  --region "${REGION}"

echo "✅ Deployment complete: ${FUNCTION_NAME}"

# ── 4. Create / Update ESM ─────────────────────────────────────────────────
# Uncomment and set MSK_CLUSTER_ARN to wire up the event source
#
# MSK_CLUSTER_ARN="arn:aws:kafka:${REGION}:123456789012:cluster/prod-cluster/abc"
# TOPICS="orders.created,payments.processed,user.events"
#
# aws lambda create-event-source-mapping \
#   --function-name "${FUNCTION_NAME}" \
#   --event-source-arn "${MSK_CLUSTER_ARN}" \
#   --topics ${TOPICS} \
#   --starting-position LATEST \
#   --batch-size 100 \
#   --maximum-batching-window-in-seconds 5 \
#   --destination-config '{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:123456789012:kafka-dlq"}}' \
#   --region "${REGION}"

/**
 * Kafka → AWS Lambda ESM Consumer
 * Production-ready handler with full observability and error handling
 *
 * Supports: Amazon MSK, MSK Serverless, Self-managed Kafka (via VPC)
 */

import { processMessage } from "./processor.mjs";
import { logger } from "./logger.mjs";
import { metrics } from "./metrics.mjs";

/**
 * Lambda ESM Kafka Handler
 *
 * Event shape:
 * {
 *   "eventSource": "aws:kafka",
 *   "eventSourceArn": "arn:aws:kafka:...",
 *   "records": {
 *     "<topic>-<partition>": [
 *       {
 *         "topic": "string",
 *         "partition": 0,
 *         "offset": 0,
 *         "timestamp": 1234567890,
 *         "timestampType": "CREATE_TIME",
 *         "key": "<base64>",
 *         "value": "<base64>",
 *         "headers": [{ "<key>": [<bytes>] }]
 *       }
 *     ]
 *   }
 * }
 */
export const handler = async (event, context) => {
  const startTime = Date.now();
  const results = {
    processed: 0,
    failed: 0,
    skipped: 0,
    errors: [],
  };

  logger.info("ESM invocation started", {
    requestId: context.awsRequestId,
    functionName: context.functionName,
    remainingMs: context.getRemainingTimeInMillis(),
    topicPartitions: Object.keys(event.records),
    eventSourceArn: event.eventSourceArn,
  });

  // Iterate over each topic-partition batch
  for (const [topicPartition, messages] of Object.entries(event.records)) {
    logger.info(`Processing partition`, {
      topicPartition,
      messageCount: messages.length,
    });

    for (const message of messages) {
      try {
        // Decode base64 key & value (AWS always base64-encodes them)
        const decodedKey = message.key
          ? Buffer.from(message.key, "base64").toString("utf-8")
          : null;

        const decodedValue = message.value
          ? Buffer.from(message.value, "base64").toString("utf-8")
          : null;

        // Decode headers (each header value is a byte array)
        const headers = (message.headers || []).reduce((acc, header) => {
          for (const [k, v] of Object.entries(header)) {
            acc[k] = Buffer.from(v).toString("utf-8");
          }
          return acc;
        }, {});

        const enrichedMessage = {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          timestamp: new Date(message.timestamp).toISOString(),
          key: decodedKey,
          value: decodedValue,
          headers,
          rawMessage: message,
        };

        logger.debug("Decoded message", {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          key: decodedKey,
        });

        // Parse JSON payload (if applicable)
        let parsedPayload = null;
        if (decodedValue) {
          try {
            parsedPayload = JSON.parse(decodedValue);
          } catch {
            logger.warn("Non-JSON message value, treating as raw string", {
              topic: message.topic,
              offset: message.offset,
            });
            parsedPayload = decodedValue;
          }
        }

        // Hand off to your business logic
        await processMessage({
          ...enrichedMessage,
          payload: parsedPayload,
        });

        results.processed++;
        metrics.increment("messages.processed", {
          topic: message.topic,
          partition: String(message.partition),
        });
      } catch (err) {
        results.failed++;
        results.errors.push({
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          error: err.message,
        });

        logger.error("Failed to process message", {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          error: err.message,
          stack: err.stack,
        });

        metrics.increment("messages.failed", {
          topic: message.topic,
          errorType: err.constructor.name,
        });

        // IMPORTANT: Re-throw to signal ESM to retry the batch
        // Remove this if you want partial failure tolerance (log & continue)
        throw err;
      }
    }
  }

  const duration = Date.now() - startTime;

  logger.info("ESM invocation complete", {
    ...results,
    durationMs: duration,
    requestId: context.awsRequestId,
  });

  metrics.gauge("invocation.duration_ms", duration);

  return {
    statusCode: 200,
    body: results,
  };
};

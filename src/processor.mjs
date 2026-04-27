/**
 * processor.mjs
 * Business logic layer — called once per decoded Kafka message.
 * Implement your domain-specific logic here.
 *
 * Supports routing by topic, message type, or header.
 */

import { logger } from "./logger.mjs";

// Map topic names to their handler functions
const TOPIC_HANDLERS = {
  "orders.created": handleOrderCreated,
  "payments.processed": handlePaymentProcessed,
  "user.events": handleUserEvent,
};

/**
 * Entry point — routes to the correct handler based on topic
 * @param {Object} message - Enriched & decoded message from handler.mjs
 */
export async function processMessage(message) {
  const { topic, partition, offset, key, payload, headers } = message;

  logger.debug("Routing message", { topic, partition, offset });

  const handler = TOPIC_HANDLERS[topic];

  if (!handler) {
    logger.warn("No handler registered for topic", { topic });
    // Return without throwing — treat unknown topics as skipped
    return;
  }

  await handler({ topic, partition, offset, key, payload, headers });
}

// ---------------------------------------------------------------------------
// Domain Handlers
// ---------------------------------------------------------------------------

async function handleOrderCreated({ topic, partition, offset, key, payload }) {
  logger.info("Processing order", {
    orderId: payload?.orderId,
    customerId: payload?.customerId,
    amount: payload?.totalAmount,
    offset,
  });

  // Example: validate, enrich, persist
  if (!payload?.orderId) {
    throw new Error(`Missing orderId in order event at offset ${offset}`);
  }

  // Simulate async work (DB write, downstream API call, etc.)
  await simulateAsyncWork(50);

  logger.info("Order processed successfully", { orderId: payload.orderId });
}

async function handlePaymentProcessed({
  topic,
  partition,
  offset,
  key,
  payload,
}) {
  logger.info("Processing payment", {
    paymentId: payload?.paymentId,
    status: payload?.status,
    offset,
  });

  // Idempotency check — critical for payment events
  // In production, check a DynamoDB/Redis store before processing
  const alreadyProcessed = await checkIdempotency(payload?.paymentId);
  if (alreadyProcessed) {
    logger.warn("Duplicate payment event skipped", {
      paymentId: payload?.paymentId,
    });
    return;
  }

  await simulateAsyncWork(30);
  logger.info("Payment processed", { paymentId: payload?.paymentId });
}

async function handleUserEvent({ topic, partition, offset, key, payload }) {
  logger.info("Processing user event", {
    eventType: payload?.eventType,
    userId: payload?.userId,
    offset,
  });

  await simulateAsyncWork(20);
  logger.info("User event processed", { eventType: payload?.eventType });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function checkIdempotency(id) {
  // Stub: replace with DynamoDB / Redis / ElastiCache lookup in production
  // Returns false (not seen before) for demonstration
  return false;
}

async function simulateAsyncWork(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

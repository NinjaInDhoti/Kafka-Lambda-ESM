/**
 * test-local.mjs
 * Run the Lambda handler locally without deploying to AWS.
 *
 * Usage:
 *   node tests/test-local.mjs
 *   node tests/test-local.mjs --scenario payment
 *   node tests/test-local.mjs --scenario batch
 */

import { handler } from "../src/handler.mjs";

// ── Mock Lambda Context ────────────────────────────────────────────────────
const mockContext = {
  awsRequestId: "local-test-" + Date.now(),
  functionName: "kafka-consumer-dev",
  functionVersion: "$LATEST",
  getRemainingTimeInMillis: () => 300000,
  logGroupName: "/aws/lambda/kafka-consumer-dev",
  logStreamName: "2024/01/01/[$LATEST]abc123",
};

// ── Message Factory ────────────────────────────────────────────────────────
function encodeMessage(payload, key = null) {
  return {
    key: key ? Buffer.from(key).toString("base64") : null,
    value: Buffer.from(JSON.stringify(payload)).toString("base64"),
    headers: [
      { "content-type": Array.from(Buffer.from("application/json")) },
      { "x-source": Array.from(Buffer.from("test-producer")) },
    ],
  };
}

// ── Test Scenarios ─────────────────────────────────────────────────────────
const SCENARIOS = {
  // Single order event
  order: {
    eventSource: "aws:kafka",
    eventSourceArn: "arn:aws:kafka:us-east-1:123456789:cluster/test/abc",
    records: {
      "orders.created-0": [
        {
          topic: "orders.created",
          partition: 0,
          offset: 42,
          timestamp: Date.now(),
          timestampType: "CREATE_TIME",
          ...encodeMessage({
            orderId: "ORD-20240115-001",
            customerId: "CUST-789",
            totalAmount: 249.99,
            currency: "USD",
            items: [
              { sku: "ITEM-A", qty: 2, price: 99.99 },
              { sku: "ITEM-B", qty: 1, price: 50.01 },
            ],
            createdAt: new Date().toISOString(),
          }, "ORD-20240115-001"),
        },
      ],
    },
  },

  // Payment event (tests idempotency path)
  payment: {
    eventSource: "aws:kafka",
    eventSourceArn: "arn:aws:kafka:us-east-1:123456789:cluster/test/abc",
    records: {
      "payments.processed-0": [
        {
          topic: "payments.processed",
          partition: 0,
          offset: 17,
          timestamp: Date.now(),
          timestampType: "CREATE_TIME",
          ...encodeMessage({
            paymentId: "PAY-20240115-XYZ",
            orderId: "ORD-20240115-001",
            status: "SUCCESS",
            amount: 249.99,
            gateway: "stripe",
            processedAt: new Date().toISOString(),
          }, "PAY-20240115-XYZ"),
        },
      ],
    },
  },

  // Multi-partition, multi-topic batch
  batch: {
    eventSource: "aws:kafka",
    eventSourceArn: "arn:aws:kafka:us-east-1:123456789:cluster/test/abc",
    records: {
      "orders.created-0": Array.from({ length: 5 }, (_, i) => ({
        topic: "orders.created",
        partition: 0,
        offset: i,
        timestamp: Date.now() - i * 1000,
        timestampType: "CREATE_TIME",
        ...encodeMessage({ orderId: `ORD-BATCH-${i}`, customerId: `CUST-${i}`, totalAmount: (i + 1) * 10 }, `ORD-BATCH-${i}`),
      })),
      "user.events-1": [
        {
          topic: "user.events",
          partition: 1,
          offset: 88,
          timestamp: Date.now(),
          timestampType: "CREATE_TIME",
          ...encodeMessage({ eventType: "PAGE_VIEW", userId: "USR-001", page: "/checkout" }, "USR-001"),
        },
      ],
    },
  },

  // Unknown topic (tests graceful skip)
  unknown: {
    eventSource: "aws:kafka",
    eventSourceArn: "arn:aws:kafka:us-east-1:123456789:cluster/test/abc",
    records: {
      "some.unknown.topic-0": [
        {
          topic: "some.unknown.topic",
          partition: 0,
          offset: 0,
          timestamp: Date.now(),
          timestampType: "CREATE_TIME",
          ...encodeMessage({ data: "hello world" }, "key-1"),
        },
      ],
    },
  },
};

// ── Runner ─────────────────────────────────────────────────────────────────
async function run() {
  const scenarioArg = process.argv.find((a) => a.startsWith("--scenario="))?.split("=")[1]
    || (process.argv.includes("--scenario") && process.argv[process.argv.indexOf("--scenario") + 1])
    || "order";

  const scenario = SCENARIOS[scenarioArg];
  if (!scenario) {
    console.error(`Unknown scenario: "${scenarioArg}". Available: ${Object.keys(SCENARIOS).join(", ")}`);
    process.exit(1);
  }

  console.log(`\n${"─".repeat(60)}`);
  console.log(`▶  Running scenario: ${scenarioArg.toUpperCase()}`);
  console.log(`${"─".repeat(60)}\n`);

  try {
    const result = await handler(scenario, mockContext);
    console.log(`\n${"─".repeat(60)}`);
    console.log("✅  Handler completed successfully");
    console.log("Result:", JSON.stringify(result, null, 2));
    console.log(`${"─".repeat(60)}\n`);
  } catch (err) {
    console.error(`\n${"─".repeat(60)}`);
    console.error("❌  Handler threw an error:", err.message);
    console.error(err.stack);
    console.error(`${"─".repeat(60)}\n`);
    process.exit(1);
  }
}

run();

/**
 * logger.mjs
 * Structured JSON logger for AWS Lambda.
 *
 * Outputs JSON lines compatible with CloudWatch Logs Insights,
 * Datadog, OpenSearch, and most log aggregation platforms.
 *
 * Log levels: DEBUG < INFO < WARN < ERROR
 * Controlled via LOG_LEVEL environment variable.
 */

const LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
const CURRENT_LEVEL = LEVELS[process.env.LOG_LEVEL?.toUpperCase() ?? "INFO"] ?? LEVELS.INFO;

function log(level, message, context = {}) {
  if (LEVELS[level] < CURRENT_LEVEL) return;

  const entry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    service: process.env.SERVICE_NAME ?? "kafka-lambda-consumer",
    environment: process.env.ENVIRONMENT ?? "unknown",
    ...context,
  };

  // CloudWatch structured logging (Lambda writes stdout → CW Logs)
  console[level === "ERROR" ? "error" : level === "WARN" ? "warn" : "log"](
    JSON.stringify(entry)
  );
}

export const logger = {
  debug: (msg, ctx) => log("DEBUG", msg, ctx),
  info:  (msg, ctx) => log("INFO",  msg, ctx),
  warn:  (msg, ctx) => log("WARN",  msg, ctx),
  error: (msg, ctx) => log("ERROR", msg, ctx),
};

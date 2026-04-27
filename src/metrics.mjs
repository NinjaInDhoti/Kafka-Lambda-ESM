/**
 * metrics.mjs
 * CloudWatch EMF (Embedded Metric Format) compatible metrics.
 *
 * EMF lets you publish custom CloudWatch metrics directly from
 * Lambda logs — zero extra API calls, sub-second resolution.
 *
 * Docs: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format.html
 */

const NAMESPACE = process.env.METRICS_NAMESPACE ?? "KafkaLambdaConsumer";
const metricBuffer = [];

function emitEMF(metricName, value, unit = "Count", dimensions = {}) {
  const emfLog = {
    _aws: {
      Timestamp: Date.now(),
      CloudWatchMetrics: [
        {
          Namespace: NAMESPACE,
          Dimensions: [Object.keys(dimensions)],
          Metrics: [{ Name: metricName, Unit: unit }],
        },
      ],
    },
    [metricName]: value,
    ...dimensions,
  };

  // EMF metrics are emitted as structured log lines
  console.log(JSON.stringify(emfLog));
}

export const metrics = {
  /**
   * Increment a counter (e.g., messages processed)
   * @param {string} name
   * @param {Object} dimensions - e.g., { topic: 'orders', partition: '0' }
   */
  increment(name, dimensions = {}) {
    emitEMF(name, 1, "Count", dimensions);
  },

  /**
   * Record a gauge value (e.g., duration in ms)
   * @param {string} name
   * @param {number} value
   * @param {string} unit - CloudWatch unit string
   * @param {Object} dimensions
   */
  gauge(name, value, unit = "Milliseconds", dimensions = {}) {
    emitEMF(name, value, unit, dimensions);
  },
};

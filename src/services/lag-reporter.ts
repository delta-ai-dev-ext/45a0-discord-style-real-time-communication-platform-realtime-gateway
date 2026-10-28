/**
 * Consumer lag sampling (architecture §7.2 `kafka_consumer_lag`).
 *
 * Offsets are never committed (ADR-001), so lag is derived from the partition high
 * watermark and the last offset this pod actually consumed. Records for partitions we
 * have not started reading yet count as zero: the consumer joined at `latest`.
 */

import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";

export interface LagSource {
  /** Partition → high watermark (next offset to be produced), keyed by partition. */
  highWatermarks(topic: string): Promise<Record<string, number>>;
  /** Partition → last consumed offset for this pod, if any. */
  lastOffsets(topic: string): Record<string, number> | undefined;
}

export interface LagReporterOptions {
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
  readonly topics: readonly string[];
  readonly source: LagSource;
}

/**
 * Lag for a topic: for every partition with a known consumed offset,
 * `high - lastConsumed - 1` pending records, clamped at zero.
 */
export function computeTopicLag(
  highWatermarks: Record<string, number>,
  lastOffsets: Record<string, number> | undefined,
): number {
  if (!lastOffsets) {
    return 0;
  }

  let lag = 0;

  for (const [partition, high] of Object.entries(highWatermarks)) {
    const last = lastOffsets[partition];

    if (last === undefined) {
      continue;
    }

    lag += Math.max(high - last - 1, 0);
  }

  return lag;
}

export class LagReporter {
  private readonly metrics: MetricsRegistry;
  private readonly logger: Logger;
  private readonly topics: readonly string[];
  private readonly source: LagSource;

  constructor(options: LagReporterOptions) {
    this.metrics = options.metrics;
    this.logger = options.logger;
    this.topics = options.topics;
    this.source = options.source;
  }

  async sampleAll(): Promise<void> {
    for (const topic of this.topics) {
      await this.sample(topic);
    }
  }

  async sample(topic: string): Promise<void> {
    try {
      const highWatermarks = await this.source.highWatermarks(topic);
      const lag = computeTopicLag(highWatermarks, this.source.lastOffsets(topic));
      this.metrics.setConsumerLag(topic, lag);
    } catch (error) {
      this.logger.warn("consumer lag sample failed", {
        operation: "lag_report",
        status: "error",
        topic,
        detail: error instanceof Error ? error.message : "unknown",
      });
    }
  }
}

/**
 * Lag adapter: admin-client high watermarks + the consumer's local read positions.
 * Kept separate from the consumer runner so the lag arithmetic stays pure/testable.
 */

import type { AdminLike, KafkaLike } from "./kafka-consumer";
import type { LagSource } from "../services/lag-reporter";

export interface ConsumerPositionSource {
  lastOffsets(topic: string): Record<string, number> | undefined;
}

export async function createKafkaLagSource(options: {
  readonly kafka: KafkaLike;
  readonly positions: ConsumerPositionSource;
}): Promise<LagSource> {
  const admin = options.kafka.admin();
  await admin.connect();

  return {
    highWatermarks: async (topic: string) => {
      const offsets = await admin.fetchTopicOffsets(topic);
      const highWatermarks: Record<string, number> = {};

      for (const entry of offsets) {
        const high = Number(entry.high);

        if (!Number.isNaN(high)) {
          highWatermarks[String(entry.partition)] = high;
        }
      }

      return highWatermarks;
    },
    lastOffsets: (topic) => options.positions.lastOffsets(topic),
  };
}

export type { AdminLike };

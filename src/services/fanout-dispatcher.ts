/**
 * Fan-out dispatcher (architecture §2 "Fan-out dispatcher", §7.1/§7.2).
 *
 * Serializes each outbound event frame exactly once, writes it into the target
 * sessions' bounded queues, and records delivery + latency metrics. Fan-out latency
 * is measured from the envelope `occurred_at` to the actual socket write.
 */

import type { Clock } from "../lib/clock";
import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";
import { serializeServerFrame, type EventFrame } from "../models/frames";
import type { Session } from "./session-registry";

export interface DeliveryMetadata {
  /** Kafka topic the event came from; the `source_topic` latency label. */
  readonly topic: string;
  /** Envelope `occurred_at`, epoch milliseconds. */
  readonly occurredAt: number;
}

export interface FanoutDispatcherOptions {
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
  readonly clock: Clock;
}

export class FanoutDispatcher {
  private readonly metrics: MetricsRegistry;
  private readonly logger: Logger;
  private readonly clock: Clock;

  constructor(options: FanoutDispatcherOptions) {
    this.metrics = options.metrics;
    this.logger = options.logger;
    this.clock = options.clock;
  }

  /**
   * Deliver one event frame to one session.
   * Returns false when the frame was dropped (overflow); the session is then closed
   * by the queue's overflow handler, so the router must not retry.
   */
  deliver(session: Session, frame: EventFrame, meta: DeliveryMetadata): boolean {
    if (session.closing) {
      return false;
    }

    const payload = serializeServerFrame(frame);
    const eventType = frame.event_type;

    return session.outbound.push(payload, () => {
      this.metrics.recordDelivery(eventType);
      this.metrics.recordFanoutLatency(
        meta.topic,
        (this.clock.now() - meta.occurredAt) / 1_000,
      );
    });
  }

  /** Deliver one event frame to many sessions; returns the number of accepted writes. */
  deliverTo(sessions: readonly Session[], frame: EventFrame, meta: DeliveryMetadata): number {
    let accepted = 0;

    for (const session of sessions) {
      if (this.deliver(session, frame, meta)) {
        accepted += 1;
      }
    }

    this.logger.debug("fan-out complete", {
      operation: "fanout",
      status: accepted > 0 ? "delivered" : "no_recipients",
      topic: meta.topic,
    });

    return accepted;
  }
}

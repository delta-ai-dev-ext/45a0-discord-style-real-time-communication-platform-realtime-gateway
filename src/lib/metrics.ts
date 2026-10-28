/**
 * Prometheus metric set from docs/architecture.md §7.
 *
 * Names and label sets are contractual. Labels are strictly low-cardinality:
 * never user_id, community_id, channel_id or request identifiers.
 */

import { Counter, Gauge, Histogram, Registry } from "prom-client";
import { CONSUMED_TOPICS } from "../models/events";
import type { ReconnectMetricReason } from "../models/errors";

export type IdentificationResult = "ok" | "invalid_token";

export interface MetricsRegistryOptions {
  /** Topics to pre-initialise on the lag gauge so scrapes see a zero sample. */
  readonly topics?: readonly string[];
}

export class MetricsRegistry {
  readonly registry: Registry;

  readonly connectionsActive: Gauge;
  readonly sessionsIdentified: Counter<"result">;
  readonly dailyActiveUsersSignal: Counter;
  readonly eventsDelivered: Counter<"event_type">;
  readonly fanoutLatency: Histogram<"source_topic">;
  readonly reconnects: Counter<"reason">;
  readonly accessFetchDuration: Histogram;
  readonly kafkaConsumerLag: Gauge<"topic">;

  constructor(options: MetricsRegistryOptions = {}) {
    this.registry = new Registry();

    this.connectionsActive = new Gauge({
      name: "gateway_connections_active",
      help: "Open WebSocket sessions on this pod",
      registers: [this.registry],
    });

    this.sessionsIdentified = new Counter({
      name: "gateway_sessions_identified_total",
      help: "Identification outcomes",
      labelNames: ["result"] as const,
      registers: [this.registry],
    });

    this.dailyActiveUsersSignal = new Counter({
      name: "gateway_daily_active_users_signal_total",
      help: "First identify per user per UTC day signal",
      registers: [this.registry],
    });

    this.eventsDelivered = new Counter({
      name: "gateway_events_delivered_total",
      help: "Events successfully written to sockets",
      labelNames: ["event_type"] as const,
      registers: [this.registry],
    });

    this.fanoutLatency = new Histogram({
      name: "gateway_fanout_latency_seconds",
      help: "Event occurred_at to socket write latency",
      labelNames: ["source_topic"] as const,
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers: [this.registry],
    });

    this.reconnects = new Counter({
      name: "gateway_reconnects_total",
      help: "Reconnect prompts and causes",
      labelNames: ["reason"] as const,
      registers: [this.registry],
    });

    this.accessFetchDuration = new Histogram({
      name: "gateway_access_fetch_duration_seconds",
      help: "community_service access call latency",
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1],
      registers: [this.registry],
    });

    this.kafkaConsumerLag = new Gauge({
      name: "kafka_consumer_lag",
      help: "Consumer lag per topic",
      labelNames: ["topic"] as const,
      registers: [this.registry],
    });

    for (const topic of options.topics ?? CONSUMED_TOPICS) {
      this.kafkaConsumerLag.set({ topic }, 0);
    }
  }

  recordIdentification(result: IdentificationResult): void {
    this.sessionsIdentified.inc({ result });
  }

  recordDailyActiveUserSignal(): void {
    this.dailyActiveUsersSignal.inc();
  }

  recordDelivery(eventType: string): void {
    this.eventsDelivered.inc({ event_type: eventType });
  }

  recordFanoutLatency(sourceTopic: string, seconds: number): void {
    this.fanoutLatency.observe({ source_topic: sourceTopic }, Math.max(seconds, 0));
  }

  recordReconnect(reason: ReconnectMetricReason): void {
    this.reconnects.inc({ reason });
  }

  recordAccessFetchDuration(seconds: number): void {
    this.accessFetchDuration.observe(Math.max(seconds, 0));
  }

  setConsumerLag(topic: string, lag: number): void {
    this.kafkaConsumerLag.set({ topic }, Math.max(lag, 0));
  }

  setConnectionsActive(count: number): void {
    this.connectionsActive.set(count);
  }
}

export const metrics = new MetricsRegistry();

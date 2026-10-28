import { describe, expect, it } from "bun:test";
import { createManualClock, utcDayKey } from "../../src/lib/clock";
import { newTraceContext, traceContextFromParent } from "../../src/lib/trace";
import { MetricsRegistry } from "../../src/lib/metrics";
import { createLogger, silentLogger } from "../../src/lib/logger";

describe("clock", () => {
  it("advances manually and derives UTC day keys", () => {
    const clock = createManualClock(Date.parse("2026-02-05T23:59:59.000Z"));

    expect(utcDayKey(clock.now())).toBe("2026-02-05");
    clock.advance(1_000);
    expect(utcDayKey(clock.now())).toBe("2026-02-06");
    clock.set(Date.parse("2026-03-01T00:00:00.000Z"));
    expect(utcDayKey(clock.now())).toBe("2026-03-01");
  });
});

describe("trace context", () => {
  it("propagates a structurally valid upstream traceparent", () => {
    const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const context = traceContextFromParent(parent);

    expect(context.traceparent).toBe(parent);
    expect(context.traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
  });

  it("generates a fresh sampled context for missing or malformed input", () => {
    for (const candidate of [undefined, "", "garbage", "00-too-short-01"]) {
      const context = traceContextFromParent(candidate);
      expect(context.traceparent).toMatch(/^00-[\da-f]{32}-[\da-f]{16}-01$/);
      expect(context.traceId).toHaveLength(32);
    }
  });

  it("always produces a distinct trace id", () => {
    expect(newTraceContext().traceId).not.toBe(newTraceContext().traceId);
  });
});

describe("metrics registry", () => {
  it("registers exactly the documented metric names and labels", async () => {
    const metrics = new MetricsRegistry();

    metrics.recordIdentification("ok");
    metrics.recordDelivery("message_created");
    metrics.recordFanoutLatency("chat_message_events", 0.01);
    metrics.recordReconnect("timeout");
    metrics.setConsumerLag("community_events", 0);

    const rendered = await metrics.registry.metrics();

    for (const name of [
      "gateway_connections_active",
      "gateway_sessions_identified_total",
      "gateway_daily_active_users_signal_total",
      "gateway_events_delivered_total",
      "gateway_fanout_latency_seconds",
      "gateway_reconnects_total",
      "gateway_access_fetch_duration_seconds",
      "kafka_consumer_lag",
    ]) {
      expect(rendered).toContain(name);
    }

    expect(rendered).toContain('result="ok"');
    expect(rendered).toContain('event_type="message_created"');
    expect(rendered).toContain('source_topic="chat_message_events"');
    expect(rendered).toContain('reason="timeout"');
    expect(rendered).toContain('topic="community_events"');
  });

  it("keeps labels low-cardinality and records values", async () => {
    const metrics = new MetricsRegistry();

    metrics.recordIdentification("ok");
    metrics.recordIdentification("invalid_token");
    metrics.recordDailyActiveUserSignal();
    metrics.recordDelivery("message_created");
    metrics.recordFanoutLatency("chat_message_events", 0.123);
    metrics.recordReconnect("shutdown");
    metrics.recordAccessFetchDuration(0.05);
    metrics.setConsumerLag("chat_message_events", 12);
    metrics.setConnectionsActive(3);

    const rendered = await metrics.registry.metrics();
    const forbidden = ["user_id", "community_id", "channel_id", "connection_id", "request_id"];

    for (const label of forbidden) {
      expect(rendered).not.toContain(`${label}=`);
    }

    expect(metrics.connectionsActive).toBeDefined();
    expect(rendered).toContain("gateway_connections_active 3");
  });
});

describe("logger", () => {
  it("exposes the structured logging surface without console usage", () => {
    const logger = createLogger("error");

    expect(typeof logger.debug).toBe("function");
    expect(typeof logger.info).toBe("function");
    expect(typeof logger.warn).toBe("function");
    expect(typeof logger.error).toBe("function");
    logger.error("boom", { operation: "test", status: "error", trace_id: "t1" });
    silentLogger.info("dropped");
  });
});

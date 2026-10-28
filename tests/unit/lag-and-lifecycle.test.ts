import { describe, expect, it } from "bun:test";
import { silentLogger } from "../../src/lib/logger";
import { MetricsRegistry } from "../../src/lib/metrics";
import { LagReporter, computeTopicLag, type LagSource } from "../../src/services/lag-reporter";
import { LifecycleState } from "../../src/services/lifecycle";
import { CONSUMED_TOPICS } from "../../src/models/events";

describe("lag arithmetic", () => {
  it("sums pending records per partition", () => {
    expect(computeTopicLag({ "0": 10, "1": 5 }, { "0": 5, "1": 4 })).toBe(4);
  });

  it("clamps and ignores unknown partitions", () => {
    expect(computeTopicLag({ "0": 3 }, { "0": 3 })).toBe(0);
    expect(computeTopicLag({ "0": 1 }, { "0": 10 })).toBe(0);
    expect(computeTopicLag({ "0": 100 }, { "1": 5 })).toBe(0);
  });

  it("reports zero lag before any read position is known", () => {
    expect(computeTopicLag({ "0": 100 }, undefined)).toBe(0);
  });
});

describe("lag reporter", () => {
  const source = (overrides: Partial<LagSource> = {}): LagSource => ({
    highWatermarks: async () => ({ "0": 10 }),
    lastOffsets: () => ({ "0": 3 }),
    ...overrides,
  });

  it("publishes the gauge for every consumed topic", async () => {
    const metrics = new MetricsRegistry();
    const reporter = new LagReporter({
      metrics,
      logger: silentLogger,
      topics: CONSUMED_TOPICS,
      source: source(),
    });

    await reporter.sampleAll();

    const rendered = await metrics.registry.metrics();
    for (const topic of CONSUMED_TOPICS) {
      expect(rendered).toContain(`kafka_consumer_lag{topic="${topic}"} 6`);
    }
  });

  it("logs and keeps the last value when sampling fails", async () => {
    const metrics = new MetricsRegistry();
    const reporter = new LagReporter({
      metrics,
      logger: silentLogger,
      topics: ["chat_message_events"],
      source: source({
        highWatermarks: async () => {
          throw new Error("admin unavailable");
        },
      }),
    });

    await reporter.sample("chat_message_events");

    const rendered = await metrics.registry.metrics();
    expect(rendered).toContain('kafka_consumer_lag{topic="chat_message_events"} 0');
  });
});

describe("lifecycle readiness", () => {
  it("is not ready until every consumer is assigned and keys are loaded", () => {
    const lifecycle = new LifecycleState();

    expect(lifecycle.isReady()).toBe(false);

    for (const topic of CONSUMED_TOPICS) {
      lifecycle.markConsumerAssigned(topic, true);
    }
    expect(lifecycle.isReady()).toBe(false);

    lifecycle.markAuthKeysReady(true);
    expect(lifecycle.isReady()).toBe(true);
  });

  it("is not ready while a single consumer lacks an assignment", () => {
    const lifecycle = new LifecycleState();

    lifecycle.markAuthKeysReady(true);
    lifecycle.markConsumerAssigned(CONSUMED_TOPICS[0], true);
    lifecycle.markConsumerAssigned(CONSUMED_TOPICS[1], true);
    lifecycle.markConsumerAssigned(CONSUMED_TOPICS[2], false);

    expect(lifecycle.isReady()).toBe(false);
    expect(lifecycle.snapshot()).toMatchObject({ ready: false, authKeysReady: true });
  });

  it("fails readiness immediately when draining starts", () => {
    const lifecycle = new LifecycleState();

    lifecycle.markAuthKeysReady(true);
    for (const topic of CONSUMED_TOPICS) {
      lifecycle.markConsumerAssigned(topic, true);
    }
    expect(lifecycle.isReady()).toBe(true);

    lifecycle.beginDraining();
    expect(lifecycle.isDraining).toBe(true);
    expect(lifecycle.isReady()).toBe(false);
    expect(lifecycle.snapshot().draining).toBe(true);
  });

  it("is never ready with an empty topic set", () => {
    const lifecycle = new LifecycleState([]);
    lifecycle.markAuthKeysReady(true);

    expect(lifecycle.isReady()).toBe(false);
  });
});

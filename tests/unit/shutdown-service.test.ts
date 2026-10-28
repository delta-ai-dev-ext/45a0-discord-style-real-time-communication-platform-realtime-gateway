import { describe, expect, it } from "bun:test";
import { silentLogger } from "../../src/lib/logger";
import { CONSUMED_TOPICS } from "../../src/models/events";
import { LifecycleState } from "../../src/services/lifecycle";
import { ShutdownService } from "../../src/services/shutdown-service";
import { createRegistryHarness } from "../mocks/harness";
import { createMockConsumer } from "../mocks/mock-consumer";
import { sessionFor } from "../mocks/mock-token-verifier";

function setup(options: { timeoutMs?: number; delay?: (ms: number) => Promise<void> } = {}) {
  const harness = createRegistryHarness();
  const lifecycle = new LifecycleState();
  const consumers = createMockConsumer();

  lifecycle.markAuthKeysReady(true);
  for (const topic of CONSUMED_TOPICS) {
    lifecycle.markConsumerAssigned(topic, true);
  }

  const shutdown = new ShutdownService({
    lifecycle,
    registry: harness.registry,
    consumers,
    metrics: harness.metrics,
    logger: silentLogger,
    clock: harness.clock,
    timeoutMs: options.timeoutMs ?? 20_000,
    delay: options.delay ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))),
  });

  return { ...harness, lifecycle, consumers, shutdown };
}

describe("graceful shutdown", () => {
  it("fails readiness first, asks every session to reconnect, then drains", async () => {
    const harness = setup();
    const first = harness.createSession();
    const second = harness.createSession();
    harness.registry.identify(first.session.connectionId, sessionFor("u1"));
    harness.registry.identify(second.session.connectionId, sessionFor("u2"));

    await harness.shutdown.shutdown();

    expect(harness.lifecycle.isDraining).toBe(true);
    expect(harness.lifecycle.isReady()).toBe(false);
    expect(harness.consumers.paused).toBe(true);
    expect(harness.consumers.stopped).toBe(true);

    for (const { socket } of [first, second]) {
      expect(socket.frames()[0]).toEqual({ type: "reconnect", reason: "shutdown" });
      expect(socket.closed).toEqual({ code: 1001, reason: "server shutdown" });
    }
  });

  it("records the shutdown reconnect metric per session", async () => {
    const harness = setup();
    harness.createSession();
    harness.createSession();

    await harness.shutdown.shutdown();

    const rendered = await harness.metrics.registry.metrics();
    expect(rendered).toContain('gateway_reconnects_total{reason="shutdown"} 2');
  });

  it("force-closes sessions when the drain window expires", async () => {
    const harness = setup({ timeoutMs: 5, delay: () => Promise.resolve() });
    const { socket } = harness.createSession();

    // Simulate a queue that never drains: buffer a frame behind backpressure.
    socket.send = () => -1;
    harness.registry.get(harness.registry.all()[0]!.connectionId)?.outbound.push("blocked");
    harness.registry.get(harness.registry.all()[0]!.connectionId)?.outbound.push("blocked-2");

    await harness.shutdown.shutdown();

    expect(socket.terminated).toBe(true);
  });

  it("is idempotent and tolerates consumer failures", async () => {
    const harness = setup();
    harness.consumers.pause = async () => {
      throw new Error("pause failed");
    };
    harness.consumers.stop = async () => {
      throw new Error("stop failed");
    };

    await harness.shutdown.shutdown();
    await harness.shutdown.shutdown();

    expect(harness.shutdown.isRunning).toBe(true);
    expect(harness.lifecycle.isDraining).toBe(true);
  });

  it("leaves no sessions open after a clean drain", async () => {
    const harness = setup();
    const { session } = harness.createSession();

    await harness.shutdown.shutdown();

    expect(session.outbound.isClosed).toBe(true);
    expect(harness.registry.size).toBe(1);
  });
});

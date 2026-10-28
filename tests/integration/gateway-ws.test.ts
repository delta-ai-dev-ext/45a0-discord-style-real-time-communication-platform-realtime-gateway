/**
 * End-to-end gateway behaviour over a real in-process WebSocket and real Elysia app.
 * Kafka, community_service and the JWKS boundary are doubles (see tests/mocks), so the
 * suite is network-free and deterministic.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { createManualClock } from "../../src/lib/clock";
import { silentLogger } from "../../src/lib/logger";
import { MetricsRegistry } from "../../src/lib/metrics";
import { createApp } from "../../src/app";
import { createFrameController } from "../../src/controllers/frames-controller";
import { createHealthController } from "../../src/controllers/health-controller";
import { CHAT_MESSAGE_EVENTS_TOPIC, CONSUMED_TOPICS } from "../../src/models/events";
import { EventRouter } from "../../src/services/event-router";
import { FanoutDispatcher } from "../../src/services/fanout-dispatcher";
import { LifecycleState } from "../../src/services/lifecycle";
import { SessionRegistry } from "../../src/services/session-registry";
import { SessionService } from "../../src/services/session-service";
import type { ServerFrame } from "../../src/models/frames";
import { createMockAccessClient } from "../mocks/mock-access-client";
import { createMockTokenVerifier, sessionFor } from "../mocks/mock-token-verifier";

const OCCURRED_AT = "2026-02-05T10:00:00.000Z";

interface TestClient {
  send(frame: unknown): void;
  next(predicate?: (frame: ServerFrame) => boolean): Promise<ServerFrame>;
  close(): void;
  readonly closed: Promise<{ code: number }>;
}

function buildHarness() {
  const clock = createManualClock();
  const metrics = new MetricsRegistry();
  const lifecycle = new LifecycleState();
  const registry = new SessionRegistry({
    clock,
    metrics,
    logger: silentLogger,
    maxConnections: 10,
    outboundQueueCapacity: 32,
  });
  const accessClient = createMockAccessClient();
  const verifier = createMockTokenVerifier({
    good: sessionFor("u1", { username: "alice", expiresAt: clock.now() + 300_000 }),
    other: sessionFor("u2", { username: "bob", expiresAt: clock.now() + 300_000 }),
  });
  const sessionService = new SessionService({
    registry,
    verifier,
    accessClient,
    metrics,
    logger: silentLogger,
    clock,
  });
  const dispatcher = new FanoutDispatcher({ metrics, logger: silentLogger, clock });
  const router = new EventRouter({
    registry,
    dispatcher,
    accessClient,
    logger: silentLogger,
    clock,
  });
  const app = createApp({
    lifecycle,
    healthController: createHealthController({ lifecycle, clock }),
    frameController: createFrameController({ registry, sessionService, metrics, logger: silentLogger }),
  });

  return { app, lifecycle, registry, accessClient, router, metrics };
}

const servers: Array<{ stop: () => void }> = [];

afterEach(() => {
  while (servers.length > 0) {
    servers.pop()?.stop();
  }
});

function listen(harness: ReturnType<typeof buildHarness>): { base: string; ws: string } {
  const server = harness.app.listen(0);
  servers.push({ stop: () => server.stop(true) });

  const port = server.server?.port;
  if (!port) {
    throw new Error("server did not bind");
  }

  return { base: `http://localhost:${port}`, ws: `ws://localhost:${port}/v1/gateway` };
}

async function connect(url: string): Promise<TestClient> {
  const socket = new WebSocket(url);
  const received: ServerFrame[] = [];
  const waiters: Array<(frame: ServerFrame) => void> = [];
  let resolveClosed: (value: { code: number }) => void = () => {};
  const closed = new Promise<{ code: number }>((resolve) => {
    resolveClosed = resolve;
  });

  socket.onmessage = (event) => {
    const frame = JSON.parse(String(event.data)) as ServerFrame;
    received.push(frame);
    for (const waiter of [...waiters]) {
      waiter(frame);
    }
  };
  socket.onclose = (event) => resolveClosed({ code: event.code });

  await new Promise<void>((resolve, reject) => {
    socket.onopen = () => resolve();
    socket.onerror = () => reject(new Error("websocket failed to open"));
  });

  return {
    send: (frame) => socket.send(JSON.stringify(frame)),
    next(predicate = () => true) {
      const existing = received.find(predicate);
      if (existing) {
        return Promise.resolve(existing);
      }

      return new Promise<ServerFrame>((resolve) => {
        const timeout = setTimeout(() => resolve({ type: "error", code: "internal_error", message: "timeout" }), 2_000);
        waiters.push((frame) => {
          if (predicate(frame)) {
            clearTimeout(timeout);
            resolve(frame);
          }
        });
      });
    },
    close: () => socket.close(),
    closed,
  };
}

describe("gateway integration", () => {
  it("identifies, heartbeats, subscribes and receives fan-out events", async () => {
    const harness = buildHarness();
    const { base, ws } = listen(harness);
    const client = await connect(ws);

    client.send({ type: "identify", token: "good" });
    expect(await client.next((frame) => frame.type === "ready")).toEqual({
      type: "ready",
      user_id: "u1",
      username: "alice",
    });

    client.send({ type: "heartbeat" });
    expect(await client.next((frame) => frame.type === "heartbeat_ack")).toEqual({
      type: "heartbeat_ack",
    });

    harness.accessClient.allow("c1", "u1", ["ch1"]);
    client.send({ type: "subscribe_community", community_id: "c1" });
    expect(await client.next((frame) => frame.type === "event" && frame.event_type === "subscribed")).toEqual({
      type: "event",
      event_type: "subscribed",
      data: { community_id: "c1", visible_channel_ids: ["ch1"] },
    });

    await harness.router.handle(
      CHAT_MESSAGE_EVENTS_TOPIC,
      JSON.stringify({
        event_id: "evt-1",
        event_type: "message_created",
        occurred_at: OCCURRED_AT,
        data: {
          message_id: "m1",
          channel_id: "ch1",
          community_id: "c1",
          author_user_id: "u2",
          author_username: "bob",
          content: "hello",
          created_at: OCCURRED_AT,
        },
      }),
    );

    const delivered = await client.next(
      (frame) => frame.type === "event" && frame.event_type === "message_created",
    );
    expect(delivered).toMatchObject({
      type: "event",
      event_type: "message_created",
      data: { message_id: "m1" },
    });

    const health = await fetch(`${base}/healthz`);
    expect(health.status).toBe(200);
    expect(await health.json()).toMatchObject({ status: "ok" });

    client.close();
    await client.closed;
  });

  it("rejects unauthenticated frames and invalid frames", async () => {
    const harness = buildHarness();
    const { ws } = listen(harness);

    const unauthenticated = await connect(ws);
    unauthenticated.send({ type: "heartbeat" });
    expect(await unauthenticated.next()).toMatchObject({
      type: "error",
      code: "not_identified",
    });

    const malformed = await connect(ws);
    malformed.send({ type: "unknown_frame" });
    expect(await malformed.next()).toMatchObject({ type: "error", code: "invalid_frame" });
  });

  it("serves probes and rejects non-upgrade requests", async () => {
    const harness = buildHarness();
    const { base } = listen(harness);

    const notUpgrade = await fetch(`${base}/v1/gateway`);
    expect(notUpgrade.status).toBe(426);
    expect(await notUpgrade.json()).toMatchObject({ error: { code: "upgrade_required" } });

    const unready = await fetch(`${base}/readyz`);
    expect(unready.status).toBe(503);
    expect(await unready.json()).toMatchObject({ status: "degraded" });

    harness.lifecycle.markAuthKeysReady(true);
    for (const topic of CONSUMED_TOPICS) {
      harness.lifecycle.markConsumerAssigned(topic, true);
    }
    const ready = await fetch(`${base}/readyz`);
    expect(ready.status).toBe(200);
    expect(await ready.json()).toMatchObject({ status: "ok" });

    const missing = await fetch(`${base}/nope`);
    expect(missing.status).toBe(404);
    expect(await missing.json()).toMatchObject({ error: { code: "not_found" } });
  });

  it("stops accepting upgrades and reports unready while draining", async () => {
    const harness = buildHarness();
    const { base, ws } = listen(harness);
    harness.lifecycle.markAuthKeysReady(true);
    for (const topic of CONSUMED_TOPICS) {
      harness.lifecycle.markConsumerAssigned(topic, true);
    }

    harness.lifecycle.beginDraining();

    expect((await fetch(`${base}/readyz`)).status).toBe(503);
    expect((await fetch(`${base}/healthz`)).status).toBe(200);

    const rejected = await fetch(`${base}/v1/gateway`);
    expect(rejected.status).toBe(503);
    expect(await rejected.json()).toMatchObject({ error: { code: "unavailable" } });

    await expect(connect(ws)).rejects.toThrow();
  });
});

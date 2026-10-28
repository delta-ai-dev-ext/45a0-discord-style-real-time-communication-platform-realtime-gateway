import { describe, expect, it } from "bun:test";
import { asUserId } from "../../src/models/ids";
import { silentLogger } from "../../src/lib/logger";
import { SessionService } from "../../src/services/session-service";
import { createRegistryHarness } from "../mocks/harness";
import { createFakeSocket } from "../mocks/fake-socket";
import { createMockAccessClient } from "../mocks/mock-access-client";
import { createMockTokenVerifier, sessionFor } from "../mocks/mock-token-verifier";

function setup(options: { tokens?: Parameters<typeof createMockTokenVerifier>[0] } = {}) {
  const harness = createRegistryHarness();
  const tokens = options.tokens ?? {
    good: sessionFor("u1", { expiresAt: harness.clock.now() + 300_000 }),
    other: sessionFor("u2", { expiresAt: harness.clock.now() + 300_000 }),
    invalid: "invalid_token" as const,
    expired: "token_expired" as const,
  };
  const verifier = createMockTokenVerifier(tokens);
  const accessClient = createMockAccessClient();
  const service = new SessionService({
    registry: harness.registry,
    verifier,
    accessClient,
    metrics: harness.metrics,
    logger: silentLogger,
    clock: harness.clock,
  });

  return { ...harness, verifier, accessClient, service };
}

describe("session service — identify", () => {
  it("registers the session, sends ready and signals the daily-active metric", async () => {
    const { service, createSession, metrics } = setup();
    const { session, socket } = createSession();

    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });

    expect(session.userId).toBe(asUserId("u1"));
    expect(session.username).toBe("u1-name");
    expect(socket.frames()).toEqual([{ type: "ready", user_id: "u1", username: "u1-name" }]);
    expect(socket.closed).toBeNull();
    expect(metrics.dailyActiveUsersSignal).toBeDefined();
  });

  it("rejects an invalid token with invalid_token and closes", async () => {
    const { service, createSession } = setup();
    const { session, socket } = createSession();

    await service.handleFrame(session.connectionId, { type: "identify", token: "invalid" });

    expect(socket.frames()[0]).toMatchObject({ type: "error", code: "invalid_token" });
    expect(socket.closed).not.toBeNull();
    expect(session.userId).toBeNull();
  });

  it("rejects an expired token with token_expired and closes", async () => {
    const { service, createSession } = setup();
    const { session, socket } = createSession();

    await service.handleFrame(session.connectionId, { type: "identify", token: "expired" });

    expect(socket.frames()[0]).toMatchObject({ type: "error", code: "token_expired" });
    expect(socket.closed).not.toBeNull();
  });

  it("reports already_identified without changing the session", async () => {
    const { service, createSession } = setup();
    const { session, socket } = createSession();

    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });
    await service.handleFrame(session.connectionId, { type: "identify", token: "other" });

    expect(session.userId).toBe(asUserId("u1"));
    expect(socket.frames()[1]).toMatchObject({ type: "error", code: "already_identified" });
    expect(socket.closed).toBeNull();
  });

  it("ignores frames for unknown connections", async () => {
    const { service } = setup();
    await service.handleFrame("missing", { type: "heartbeat" });
  });
});

describe("session service — heartbeat, refresh and unsubscribe guards", () => {
  it("requires identify before any other frame and closes", async () => {
    for (const frame of [
      { type: "heartbeat" },
      { type: "subscribe_community", community_id: "c1" },
      { type: "unsubscribe_community", community_id: "c1" },
      { type: "refresh_token", token: "good" },
    ] as const) {
      const { service, createSession } = setup();
      const { session, socket } = createSession();

      await service.handleFrame(session.connectionId, frame);

      expect(socket.frames()[0]).toMatchObject({ type: "error", code: "not_identified" });
      expect(socket.closed).not.toBeNull();
    }
  });

  it("acknowledges heartbeats", async () => {
    const { service, createSession } = setup();
    const { session, socket } = createSession();

    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });
    await service.handleFrame(session.connectionId, { type: "heartbeat" });

    expect(socket.frames()[1]).toEqual({ type: "heartbeat_ack" });
  });

  it("extends the session on a valid refresh and closes on an invalid one", async () => {
    const { service, createSession, clock } = setup();
    const identified = createSession();
    await service.handleFrame(identified.session.connectionId, { type: "identify", token: "good" });

    clock.advance(1_000);
    await service.handleFrame(identified.session.connectionId, { type: "refresh_token", token: "other" });

    expect(identified.session.userId).toBe(asUserId("u2"));
    expect(identified.socket.closed).toBeNull();

    await service.handleFrame(identified.session.connectionId, { type: "refresh_token", token: "invalid" });
    expect(identified.socket.frames().at(-1)).toMatchObject({
      type: "error",
      code: "invalid_token",
    });
    expect(identified.socket.closed).not.toBeNull();
  });

  it("reports not_subscribed when unsubscribing from an unknown community", async () => {
    const { service, createSession } = setup();
    const { session, socket } = createSession();

    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });
    await service.handleFrame(session.connectionId, {
      type: "unsubscribe_community",
      community_id: "c1",
    });

    expect(socket.frames()[1]).toMatchObject({ type: "error", code: "not_subscribed" });
    expect(socket.closed).toBeNull();
  });
});

describe("session service — subscribe authorization", () => {
  it("confirms an authorized subscription with visible channels", async () => {
    const { service, createSession, accessClient } = setup();
    const { session, socket } = createSession();

    accessClient.allow("c1", "u1", ["ch1", "ch2"]);
    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });
    await service.handleFrame(session.connectionId, { type: "subscribe_community", community_id: "c1" });

    expect(socket.frames()[1]).toEqual({
      type: "event",
      event_type: "subscribed",
      data: { community_id: "c1", visible_channel_ids: ["ch1", "ch2"] },
    });
    expect(session.subscriptions.get("c1")?.visibleChannelIds).toEqual(new Set(["ch1", "ch2"]));
    expect(accessClient.calls[0]?.traceparent).toMatch(/^00-[\da-f]{32}-[\da-f]{16}-01$/);
  });

  it("fails closed with not_authorized when membership is denied", async () => {
    const { service, createSession, accessClient } = setup();
    const { session, socket } = createSession();

    accessClient.deny("c1", "u1");
    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });
    await service.handleFrame(session.connectionId, { type: "subscribe_community", community_id: "c1" });

    expect(socket.frames()[1]).toMatchObject({ type: "error", code: "not_authorized" });
    expect(session.subscriptions.size).toBe(0);
  });

  it("maps an unavailable access service to internal_error and registers nothing", async () => {
    const { service, createSession, accessClient } = setup();
    const { session, socket } = createSession();

    accessClient.unavailable("c1", "u1");
    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });
    await service.handleFrame(session.connectionId, { type: "subscribe_community", community_id: "c1" });

    expect(socket.frames()[1]).toMatchObject({ type: "error", code: "internal_error" });
    expect(session.subscriptions.size).toBe(0);
  });

  it("removes an active subscription and stops delivery", async () => {
    const { service, createSession, accessClient, registry } = setup();
    const { session } = createSession();

    accessClient.allow("c1", "u1", ["ch1"]);
    await service.handleFrame(session.connectionId, { type: "identify", token: "good" });
    await service.handleFrame(session.connectionId, { type: "subscribe_community", community_id: "c1" });
    await service.handleFrame(session.connectionId, {
      type: "unsubscribe_community",
      community_id: "c1",
    });

    expect(session.subscriptions.size).toBe(0);
    expect(registry.sessionsForCommunity("c1")).toHaveLength(0);
  });
});

describe("session service — transport edge cases", () => {
  it("drops further frames once the outbound queue is closed", async () => {
    const harness = createRegistryHarness();
    const socket = createFakeSocket();
    const session = harness.registry.create(socket);
    expect(session).not.toBeNull();

    const service = new SessionService({
      registry: harness.registry,
      verifier: createMockTokenVerifier({ bad: "invalid_token" }),
      accessClient: createMockAccessClient(),
      metrics: harness.metrics,
      logger: silentLogger,
      clock: harness.clock,
    });

    await service.handleFrame(session!.connectionId, { type: "identify", token: "bad" });
    expect(socket.closed).not.toBeNull();

    await service.handleFrame(session!.connectionId, { type: "heartbeat" });
    expect(socket.frames()).toHaveLength(1);
  });
});

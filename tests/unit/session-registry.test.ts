import { describe, expect, it } from "bun:test";
import { computeExpiryLead, MAX_EXPIRY_LEAD_MS } from "../../src/services/session-registry";
import { asUserId } from "../../src/models/ids";
import { createRegistryHarness } from "../mocks/harness";
import { createFakeSocket } from "../mocks/fake-socket";
import { sessionFor } from "../mocks/mock-token-verifier";

describe("session registry", () => {
  it("creates sessions, indexes them and tracks the active-connection gauge", () => {
    const { registry, metrics, createSession } = createRegistryHarness();
    const first = createSession();
    const second = createSession();

    expect(registry.size).toBe(2);
    expect(registry.get(first.session.connectionId)).toBe(first.session);
    expect(registry.all()).toHaveLength(2);
    expect(metrics.connectionsActive).toBeDefined();
    expect(first.session.connectionId).not.toBe(second.session.connectionId);
  });

  it("rejects connections past WS_MAX_CONNECTIONS_PER_POD", () => {
    const { registry, createSession } = createRegistryHarness({ maxConnections: 1 });
    createSession();

    expect(registry.isAtCapacity).toBe(true);
    expect(registry.create(createFakeSocket())).toBeNull();
  });

  it("indexes identified sessions by user and supports refresh", () => {
    const { registry, createSession, clock } = createRegistryHarness();
    const { session } = createSession();
    const identity = sessionFor("u1", { expiresAt: clock.now() + 60_000 });

    registry.identify(session.connectionId, identity);
    expect(session.userId).toBe(asUserId("u1"));
    expect(session.username).toBe("u1-name");
    expect(registry.sessionsForUser(asUserId("u1"))).toHaveLength(1);
    // A quarter of the 60s token lifetime, below the one-minute cap.
    expect(session.expiryNoticeLeadMs).toBe(15_000);
    expect(computeExpiryLead(clock.now(), clock.now() + 240_000)).toBe(MAX_EXPIRY_LEAD_MS);

    registry.refresh(session.connectionId, sessionFor("u1", { expiresAt: clock.now() + 240_000 }));
    expect(session.tokenExpiresAt).toBe(clock.now() + 240_000);
    expect(session.expiryNoticeSent).toBe(false);
  });

  it("re-identifying a different user re-indexes the session", () => {
    const { registry, createSession } = createRegistryHarness();
    const { session } = createSession();

    registry.identify(session.connectionId, sessionFor("u1"));
    registry.identify(session.connectionId, sessionFor("u2"));

    expect(registry.sessionsForUser(asUserId("u1"))).toHaveLength(0);
    expect(registry.sessionsForUser(asUserId("u2"))).toHaveLength(1);
  });

  it("subscribes, updates visibility and unsubscribes with index cleanup", () => {
    const { registry, createSession } = createRegistryHarness();
    const { session } = createSession();

    registry.identify(session.connectionId, sessionFor("u1"));
    const subscription = registry.subscribe(session.connectionId, "c1", ["ch1", "ch2"]);

    expect(subscription?.visibleChannelIds).toEqual(new Set(["ch1", "ch2"]));
    expect(registry.sessionsForCommunity("c1")).toHaveLength(1);

    registry.updateVisibility(session.connectionId, "c1", ["ch1"]);
    expect(session.subscriptions.get("c1")?.visibleChannelIds).toEqual(new Set(["ch1"]));
    expect(registry.updateVisibility(session.connectionId, "missing", ["ch1"])).toBe(false);

    expect(registry.unsubscribe(session.connectionId, "c1")).toBe(true);
    expect(registry.sessionsForCommunity("c1")).toHaveLength(0);
    expect(registry.unsubscribe(session.connectionId, "c1")).toBe(false);
  });

  it("force-unsubscribes one user without touching other members", () => {
    const { registry, createSession } = createRegistryHarness();
    const kicked = createSession();
    const other = createSession();

    registry.identify(kicked.session.connectionId, sessionFor("kicked"));
    registry.identify(other.session.connectionId, sessionFor("other"));
    registry.subscribe(kicked.session.connectionId, "c1", ["ch1"]);
    registry.subscribe(other.session.connectionId, "c1", ["ch1"]);

    expect(registry.forceUnsubscribeUser(asUserId("kicked"), "c1")).toBe(1);
    expect(kicked.session.subscriptions.has("c1")).toBe(false);
    expect(other.session.subscriptions.has("c1")).toBe(true);
    expect(registry.sessionsForCommunity("c1")).toHaveLength(1);
  });

  it("removes a community from every session on community_deleted", () => {
    const { registry, createSession } = createRegistryHarness();
    const first = createSession();
    const second = createSession();

    registry.subscribe(first.session.connectionId, "c1", ["ch1"]);
    registry.subscribe(second.session.connectionId, "c1", ["ch1"]);

    expect(registry.removeCommunityFromAll("c1")).toBe(2);
    expect(registry.sessionsForCommunity("c1")).toHaveLength(0);
  });

  it("detaches every index when a session closes", () => {
    const { registry, createSession } = createRegistryHarness();
    const { session } = createSession();

    registry.identify(session.connectionId, sessionFor("u1"));
    registry.subscribe(session.connectionId, "c1", ["ch1"]);
    registry.remove(session.connectionId);

    expect(registry.size).toBe(0);
    expect(registry.sessionsForUser(asUserId("u1"))).toHaveLength(0);
    expect(registry.sessionsForCommunity("c1")).toHaveLength(0);
    expect(registry.remove("unknown")).toBeUndefined();
  });

  it("signals the daily-active metric once per user per UTC day", () => {
    const { registry, clock, createSession } = createRegistryHarness();
    createSession();
    const userId = asUserId("u1");

    expect(registry.markDailyActiveUser(userId)).toBe(true);
    expect(registry.markDailyActiveUser(userId)).toBe(false);

    clock.advance(24 * 60 * 60 * 1_000);
    expect(registry.markDailyActiveUser(userId)).toBe(true);
  });

  it("resets the daily-active dedupe set at capacity", () => {
    const { registry } = createRegistryHarness({ dailyActiveCapacity: 1 });

    expect(registry.markDailyActiveUser(asUserId("a"))).toBe(true);
    expect(registry.markDailyActiveUser(asUserId("b"))).toBe(true);
  });

  it("touches activity timestamps and ignores unknown connections", () => {
    const { registry, createSession, clock } = createRegistryHarness();
    const { session } = createSession();

    clock.advance(5_000);
    registry.touch(session.connectionId);
    expect(session.lastActivityAt).toBe(clock.now());

    registry.touch("unknown");
  });

  it("derives the expiry lead from the token lifetime", () => {
    expect(computeExpiryLead(0, 600_000)).toBe(MAX_EXPIRY_LEAD_MS);
    expect(computeExpiryLead(0, 8_000)).toBe(2_000);
    expect(computeExpiryLead(10_000, 5_000)).toBe(0);
  });
});

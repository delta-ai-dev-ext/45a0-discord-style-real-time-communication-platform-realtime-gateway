import { describe, expect, it } from "bun:test";
import { silentLogger } from "../../src/lib/logger";
import { SessionSweeper, startSweeper } from "../../src/services/session-sweeper";
import { createRegistryHarness } from "../mocks/harness";
import { sessionFor } from "../mocks/mock-token-verifier";

const IDLE_TIMEOUT_MS = 30_000;

function setup() {
  const harness = createRegistryHarness();
  const sweeper = new SessionSweeper({
    registry: harness.registry,
    metrics: harness.metrics,
    logger: silentLogger,
    clock: harness.clock,
    idleTimeoutMs: IDLE_TIMEOUT_MS,
  });

  return { ...harness, sweeper };
}

describe("session sweeper", () => {
  it("closes a session that missed its heartbeat deadline", () => {
    const { sweeper, createSession, clock } = setup();
    const { session, socket } = createSession();

    clock.advance(IDLE_TIMEOUT_MS + 1);
    sweeper.sweep();

    expect(session.closing).toBe(true);
    expect(socket.closed).toEqual({ code: 1001, reason: "idle timeout" });
  });

  it("keeps a session that keeps sending frames", () => {
    const { sweeper, createSession, clock, registry } = setup();
    const { session, socket } = createSession();

    clock.advance(IDLE_TIMEOUT_MS - 1);
    registry.touch(session.connectionId);
    clock.advance(IDLE_TIMEOUT_MS - 1);
    sweeper.sweep();

    expect(session.closing).toBe(false);
    expect(socket.closed).toBeNull();
  });

  it("sends one proactive reconnect near expiry and closes at expiry", () => {
    const { sweeper, createSession, clock, registry } = setup();
    const { session, socket } = createSession();
    const expiresAt = clock.now() + 8_000;

    registry.identify(session.connectionId, sessionFor("u1", { expiresAt }));

    // Lead is a quarter of the lifetime (2s): no notice yet after one sweep at t+0.
    sweeper.sweep();
    expect(socket.sent).toHaveLength(0);

    clock.advance(6_000);
    sweeper.sweep();
    expect(socket.frames()).toEqual([{ type: "reconnect", reason: "token_expiry" }]);
    expect(session.expiryNoticeSent).toBe(true);

    clock.advance(1_000);
    sweeper.sweep();
    expect(socket.frames().filter((frame) => frame.type === "reconnect")).toHaveLength(1);

    clock.advance(2_000);
    sweeper.sweep();
    expect(socket.frames().at(-1)).toMatchObject({ type: "error", code: "token_expired" });
    expect(socket.closed).not.toBeNull();
  });

  it("skips unauthenticated and already-closing sessions", () => {
    const { sweeper, createSession, clock } = setup();
    const anonymous = createSession();
    const closing = createSession();

    closing.session.closing = true;
    clock.advance(IDLE_TIMEOUT_MS * 2);
    sweeper.sweep();

    expect(anonymous.socket.closed).not.toBeNull();
    expect(closing.socket.closed).toBeNull();
  });

  it("drives sweeps from the interval handle", () => {
    const { sweeper, createSession, clock } = setup();
    const { socket } = createSession();
    const scheduled: Array<() => void> = [];
    let cleared = false;

    const handle = startSweeper(
      sweeper,
      IDLE_TIMEOUT_MS,
      (task) => {
        scheduled.push(task);
        return 1;
      },
      () => {
        cleared = true;
      },
    );

    clock.advance(IDLE_TIMEOUT_MS + 1);
    handle.tick();
    expect(socket.closed).not.toBeNull();

    scheduled.forEach((task) => task());
    handle.stop();
    expect(cleared).toBe(true);
    expect(scheduled.length).toBeGreaterThan(0);
  });
});

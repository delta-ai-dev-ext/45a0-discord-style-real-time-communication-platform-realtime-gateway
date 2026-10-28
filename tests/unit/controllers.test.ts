import { describe, expect, it } from "bun:test";
import { createManualClock } from "../../src/lib/clock";
import { silentLogger } from "../../src/lib/logger";
import { CONSUMED_TOPICS } from "../../src/models/events";
import { createFrameController } from "../../src/controllers/frames-controller";
import { createHealthController } from "../../src/controllers/health-controller";
import { LifecycleState } from "../../src/services/lifecycle";
import { SessionService } from "../../src/services/session-service";
import { createFakeSocket } from "../mocks/fake-socket";
import { createRegistryHarness } from "../mocks/harness";
import { createMockAccessClient } from "../mocks/mock-access-client";
import { createMockTokenVerifier, sessionFor } from "../mocks/mock-token-verifier";

describe("health controller", () => {
  it("always reports liveness and gates readiness on state", () => {
    const clock = createManualClock(Date.parse("2026-02-05T10:00:00.000Z"));
    const lifecycle = new LifecycleState();
    const controller = createHealthController({ lifecycle, clock });

    expect(controller.liveness()).toEqual({
      status: "ok",
      checked_at: "2026-02-05T10:00:00.000Z",
    });
    expect(controller.readiness()).toEqual({
      httpStatus: 503,
      body: { status: "degraded", checked_at: "2026-02-05T10:00:00.000Z" },
    });

    lifecycle.markAuthKeysReady(true);
    for (const topic of CONSUMED_TOPICS) {
      lifecycle.markConsumerAssigned(topic, true);
    }
    expect(controller.readiness().httpStatus).toBe(200);
    expect(controller.readiness().body.status).toBe("ok");

    lifecycle.beginDraining();
    expect(controller.readiness()).toMatchObject({
      httpStatus: 503,
      body: { status: "degraded" },
    });
  });
});

describe("frame controller", () => {
  function setup(options: { maxConnections?: number } = {}) {
    const harness = createRegistryHarness(options);
    const verifier = createMockTokenVerifier({
      good: sessionFor("u1", { expiresAt: harness.clock.now() + 300_000 }),
    });
    const sessionService = new SessionService({
      registry: harness.registry,
      verifier,
      accessClient: createMockAccessClient(),
      metrics: harness.metrics,
      logger: silentLogger,
      clock: harness.clock,
    });
    const controller = createFrameController({
      registry: harness.registry,
      sessionService,
      metrics: harness.metrics,
      logger: silentLogger,
    });

    return { ...harness, controller, verifier };
  }

  it("registers an upgraded connection and delegates valid frames", async () => {
    const { controller, registry } = setup();
    const socket = createFakeSocket();

    const session = controller.open(socket);
    expect(session).not.toBeNull();

    await controller.message(session!.connectionId, JSON.stringify({ type: "identify", token: "good" }));

    expect(socket.frames()[0]).toMatchObject({ type: "ready", user_id: "u1" });
    expect(registry.size).toBe(1);

    controller.close(session!.connectionId);
    expect(registry.size).toBe(0);
  });

  it("rejects new connections with internal_error past pod capacity", () => {
    const { controller } = setup({ maxConnections: 1 });
    const accepted = createFakeSocket();
    const rejected = createFakeSocket();

    expect(controller.open(accepted)).not.toBeNull();
    expect(controller.open(rejected)).toBeNull();
    expect(rejected.frames()[0]).toMatchObject({ type: "error", code: "internal_error" });
    expect(rejected.closed).toEqual({ code: 1013, reason: "capacity" });
  });

  it("closes the connection with invalid_frame for malformed input", async () => {
    const { controller } = setup();
    const socket = createFakeSocket();
    const session = controller.open(socket);

    await controller.message(session!.connectionId, "not-json");

    expect(socket.frames()[0]).toMatchObject({ type: "error", code: "invalid_frame" });
    expect(socket.closed).not.toBeNull();
  });

  it("ignores messages for unknown connections and drains safely", async () => {
    const { controller } = setup();

    await controller.message("missing", JSON.stringify({ type: "heartbeat" }));
    controller.close("missing");
    controller.drain("missing");
  });

  it("resumes buffered frames on transport drain", async () => {
    const { controller } = setup();
    const socket = createFakeSocket({ statuses: [-1] });

    const session = controller.open(socket);
    controller.drain(session!.connectionId);

    expect(session!.outbound.isBackpressured).toBe(false);
  });
});

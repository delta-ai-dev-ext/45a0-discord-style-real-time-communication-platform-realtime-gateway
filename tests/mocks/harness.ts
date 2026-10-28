import { createManualClock, type ManualClock } from "../../src/lib/clock";
import { silentLogger } from "../../src/lib/logger";
import { MetricsRegistry } from "../../src/lib/metrics";
import { SessionRegistry } from "../../src/services/session-registry";
import { createFakeSocket, type FakeSocket } from "./fake-socket";

export interface RegistryHarness {
  readonly clock: ManualClock;
  readonly metrics: MetricsRegistry;
  readonly registry: SessionRegistry;
  createSession(): { session: NonNullable<ReturnType<SessionRegistry["create"]>>; socket: FakeSocket };
}

export function createRegistryHarness(
  options: {
    maxConnections?: number;
    queueCapacity?: number;
    dailyActiveCapacity?: number;
  } = {},
): RegistryHarness {
  const clock = createManualClock();
  const metrics = new MetricsRegistry();
  const registry = new SessionRegistry({
    clock,
    metrics,
    logger: silentLogger,
    maxConnections: options.maxConnections ?? 100,
    outboundQueueCapacity: options.queueCapacity ?? 8,
    ...(options.dailyActiveCapacity !== undefined
      ? { dailyActiveCapacity: options.dailyActiveCapacity }
      : {}),
  });

  return {
    clock,
    metrics,
    registry,
    createSession() {
      const socket = createFakeSocket();
      const session = registry.create(socket);
      if (!session) {
        throw new Error("expected a session");
      }
      return { session, socket };
    },
  };
}

/** Parse the last frame a fake socket received. */
export function lastFrame(socket: FakeSocket) {
  const frames = socket.frames();
  return frames[frames.length - 1];
}

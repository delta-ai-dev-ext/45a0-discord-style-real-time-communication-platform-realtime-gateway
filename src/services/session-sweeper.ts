/**
 * Periodic session sweeper (architecture §3.2, §6.2).
 *
 * Two deadlines are enforced per session without any per-session timer:
 *  - idle: no frame within `WS_HEARTBEAT_INTERVAL_MS` → the session is dead, close it;
 *  - token: near expiry → proactively ask the client to reconnect and refresh the
 *    token; at expiry → close with `error {code: token_expired}`.
 *
 * The sweeper is driven by an injectable tick (see `startSweeper`), so tests call
 * `sweep()` directly instead of waiting on wall-clock timers.
 */

import type { Clock } from "../lib/clock";
import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";
import { errorFrame, reconnectFrame, serializeServerFrame } from "../models/frames";
import type { Session, SessionRegistry } from "./session-registry";

export interface SessionSweeperOptions {
  readonly registry: SessionRegistry;
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
  readonly clock: Clock;
  readonly idleTimeoutMs: number;
}

const CLOSE_GOING_AWAY = 1001;
const CLOSE_POLICY_VIOLATION = 1008;

export class SessionSweeper {
  private readonly registry: SessionRegistry;
  private readonly metrics: MetricsRegistry;
  private readonly logger: Logger;
  private readonly clock: Clock;
  private readonly idleTimeoutMs: number;

  constructor(options: SessionSweeperOptions) {
    this.registry = options.registry;
    this.metrics = options.metrics;
    this.logger = options.logger;
    this.clock = options.clock;
    this.idleTimeoutMs = options.idleTimeoutMs;
  }

  sweep(): void {
    const now = this.clock.now();

    for (const session of this.registry.all()) {
      if (session.closing) {
        continue;
      }

      if (this.sweepIdle(session, now)) {
        continue;
      }

      this.sweepToken(session, now);
    }
  }

  /** Returns true when the session was closed as dead. */
  private sweepIdle(session: Session, now: number): boolean {
    if (now - session.lastActivityAt <= this.idleTimeoutMs) {
      return false;
    }

    session.closing = true;
    this.metrics.recordReconnect("timeout");
    this.logger.info("closing idle session", {
      operation: "session_sweep",
      status: "timeout",
      connection_id: session.connectionId,
      user_id: session.userId ?? undefined,
    });
    // The socket is presumed dead (no frames for a full heartbeat interval), so a
    // protocol frame would not be read; the reconnect counter still records the cause.
    session.outbound.closeNow(CLOSE_GOING_AWAY, "idle timeout");
    return true;
  }

  private sweepToken(session: Session, now: number): void {
    if (session.userId === null || session.tokenExpiresAt === null) {
      return;
    }

    if (now >= session.tokenExpiresAt) {
      session.closing = true;
      this.metrics.recordReconnect("token_expiry");
      this.logger.info("closing session on token expiry", {
        operation: "session_sweep",
        status: "token_expired",
        connection_id: session.connectionId,
        user_id: session.userId,
      });
      session.outbound.closeWith(
        serializeServerFrame(errorFrame("token_expired", "session token has expired")),
        CLOSE_POLICY_VIOLATION,
        "token_expired",
      );
      return;
    }

    if (!session.expiryNoticeSent && now >= session.tokenExpiresAt - session.expiryNoticeLeadMs) {
      session.expiryNoticeSent = true;
      this.metrics.recordReconnect("token_expiry");
      session.outbound.push(serializeServerFrame(reconnectFrame("token_expiry")));
      this.logger.debug("advisory reconnect sent before token expiry", {
        operation: "session_sweep",
        status: "token_expiry",
        connection_id: session.connectionId,
        user_id: session.userId,
      });
    }
  }
}

export interface SweeperHandle {
  stop(): void;
  tick(): void;
}

/**
 * Start the sweep loop. The interval is derived from the heartbeat window so the
 * deadline is enforced with acceptable granularity.
 */
export function startSweeper(
  sweeper: SessionSweeper,
  intervalMs: number,
  schedule: (task: () => void, ms: number) => unknown = (task, ms) => setInterval(task, ms),
  cancel: (handle: unknown) => void = (handle) => clearInterval(handle as Timer),
): SweeperHandle {
  const period = Math.max(Math.min(Math.floor(intervalMs / 2), 5_000), 50);
  const handle = schedule(() => sweeper.sweep(), period);

  return {
    stop: () => cancel(handle),
    tick: () => sweeper.sweep(),
  };
}

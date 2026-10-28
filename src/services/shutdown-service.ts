/**
 * Graceful shutdown / drain (architecture §6.3, US-010).
 *
 * Order is contractual: readiness fails first, every session is told to reconnect,
 * Kafka consumption stops, queues flush inside `SHUTDOWN_TIMEOUT_MS`, and whatever is
 * left is force-closed. The whole sequence is bounded so the pod always terminates.
 */

import type { Clock } from "../lib/clock";
import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";
import { reconnectFrame, serializeServerFrame } from "../models/frames";
import type { SessionRegistry } from "./session-registry";
import type { ConsumerController, LifecycleState } from "./lifecycle";

export interface ShutdownServiceOptions {
  readonly lifecycle: LifecycleState;
  readonly registry: SessionRegistry;
  readonly consumers: ConsumerController;
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
  readonly clock: Clock;
  readonly timeoutMs: number;
  /** Timer used by the deadline race; injectable so tests stay deterministic. */
  readonly delay?: (ms: number) => Promise<void>;
}

const CLOSE_GOING_AWAY = 1001;

export class ShutdownService {
  private running = false;

  private readonly lifecycle: LifecycleState;
  private readonly registry: SessionRegistry;
  private readonly consumers: ConsumerController;
  private readonly metrics: MetricsRegistry;
  private readonly logger: Logger;
  private readonly clock: Clock;
  private readonly timeoutMs: number;
  private readonly delay: (ms: number) => Promise<void>;

  constructor(options: ShutdownServiceOptions) {
    this.lifecycle = options.lifecycle;
    this.registry = options.registry;
    this.consumers = options.consumers;
    this.metrics = options.metrics;
    this.logger = options.logger;
    this.clock = options.clock;
    this.timeoutMs = options.timeoutMs;
    this.delay = options.delay ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  }

  get isRunning(): boolean {
    return this.running;
  }

  async shutdown(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;
    const deadline = this.clock.now() + this.timeoutMs;

    // 1. Readiness fails immediately so Kubernetes stops routing new sessions.
    this.lifecycle.beginDraining();
    this.logger.info("shutdown started", { operation: "shutdown", status: "draining" });

    // 2. Tell every session to reconnect; the client reconnects to a healthy pod.
    const sessions = this.registry.all();
    const reconnectPayload = serializeServerFrame(reconnectFrame("shutdown"));

    for (const session of sessions) {
      this.metrics.recordReconnect("shutdown");
      session.outbound.push(reconnectPayload);
    }

    // 3. Stop consuming; no new dispatch work is accepted.
    await this.bounded(this.consumers.pause(), deadline, "consumer pause");

    // 4. Flush bounded queues inside the remaining window.
    const drained = await this.bounded(
      Promise.allSettled(sessions.map((session) => session.outbound.whenDrained())),
      deadline,
      "queue drain",
    );

    if (drained.ok) {
      for (const session of sessions) {
        session.outbound.closeNow(CLOSE_GOING_AWAY, "server shutdown");
      }
    } else {
      for (const session of sessions) {
        session.outbound.terminate();
      }
    }

    // 5. Close Kafka connections; the caller exits the process with code 0.
    await this.bounded(this.consumers.stop(), deadline, "consumer stop");

    this.logger.info("shutdown complete", {
      operation: "shutdown",
      status: drained.ok ? "drained" : "forced",
      detail: `${sessions.length} sessions`,
    });
  }

  private async bounded(
    task: Promise<unknown>,
    deadline: number,
    label: string,
  ): Promise<{ ok: boolean }> {
    const remaining = Math.max(deadline - this.clock.now(), 0);

    try {
      const result = await Promise.race([
        task.then(() => "completed" as const),
        this.delay(remaining).then(() => "timeout" as const),
      ]);

      if (result === "timeout") {
        this.logger.warn("shutdown step timed out", {
          operation: "shutdown",
          status: "timeout",
          detail: label,
        });
        return { ok: false };
      }

      return { ok: true };
    } catch (error) {
      this.logger.warn("shutdown step failed", {
        operation: "shutdown",
        status: "error",
        detail: `${label}: ${error instanceof Error ? error.message : "unknown"}`,
      });
      return { ok: false };
    }
  }
}

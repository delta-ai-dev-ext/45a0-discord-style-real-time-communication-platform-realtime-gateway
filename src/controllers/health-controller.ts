/**
 * Probe controller: shapes the liveness/readiness responses (`HealthStatus` schema).
 *
 * Liveness is deliberately dependency-free — a Kafka or community_service blip must
 * never restart the pod. Readiness reflects consumer assignments and draining only.
 */

import type { Clock } from "../lib/clock";
import type { LifecycleState } from "../services/lifecycle";

export interface HealthStatusBody {
  status: "ok" | "degraded";
  checked_at: string;
}

export interface HealthControllerOptions {
  readonly lifecycle: LifecycleState;
  readonly clock: Clock;
}

export interface HealthController {
  liveness(): HealthStatusBody;
  readiness(): { httpStatus: 200 | 503; body: HealthStatusBody };
}

export function createHealthController(options: HealthControllerOptions): HealthController {
  const { lifecycle, clock } = options;

  return {
    liveness() {
      return { status: "ok", checked_at: new Date(clock.now()).toISOString() };
    },

    readiness() {
      const ready = lifecycle.isReady();

      return {
        httpStatus: ready ? 200 : 503,
        body: {
          status: ready ? "ok" : "degraded",
          checked_at: new Date(clock.now()).toISOString(),
        },
      };
    },
  };
}

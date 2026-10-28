/**
 * Elysia application factory.
 *
 * Dependencies are injected so tests can build the same app with doubles (mock Kafka,
 * mock access client, locally signed JWTs) and no network. `src/index.ts` is the only
 * place that wires real adapters and starts the process.
 */

import { Elysia } from "elysia";
import type { HealthController } from "./controllers/health-controller";
import type { FrameController } from "./controllers/frames-controller";
import type { LifecycleState } from "./services/lifecycle";
import { createGatewayRoutes } from "./routes/gateway";
import { createHealthRoutes } from "./routes/health";

export interface AppDependencies {
  readonly lifecycle: LifecycleState;
  readonly healthController: HealthController;
  readonly frameController: FrameController;
  readonly maxPayloadLength?: number;
}

const GATEWAY_PATH = "/v1/gateway";
const SERVICE_UNAVAILABLE = 503;

export function createApp(dependencies: AppDependencies) {
  return new Elysia({ name: "realtime_gateway" })
    .onRequest(({ request, set }) => {
      if (!dependencies.lifecycle.isDraining) {
        return;
      }

      const { pathname } = new URL(request.url);

      if (pathname !== GATEWAY_PATH) {
        return;
      }

      // Draining: refuse new sessions before the WebSocket handshake completes.
      set.status = SERVICE_UNAVAILABLE;
      return {
        error: {
          code: "unavailable",
          message: "gateway is shutting down, reconnect to another instance",
        },
      };
    })
    .use(createHealthRoutes(dependencies.healthController))
    .use(
      createGatewayRoutes({
        controller: dependencies.frameController,
        ...(dependencies.maxPayloadLength !== undefined
          ? { maxPayloadLength: dependencies.maxPayloadLength }
          : {}),
      }),
    )
    .onError(({ code, set }) => {
      if (code === "NOT_FOUND") {
        set.status = 404;
        return { error: { code: "not_found", message: "route not found" } };
      }

      set.status = 500;
      return { error: { code: "internal_error", message: "unexpected server error" } };
    });
}

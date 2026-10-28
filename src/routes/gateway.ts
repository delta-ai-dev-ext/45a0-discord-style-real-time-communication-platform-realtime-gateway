/**
 * Gateway upgrade route (docs/openapi_spec.yaml `GET /v1/gateway`).
 *
 * Elysia's built-in WebSocket handles the upgrade; this router only wires socket
 * callbacks to the frame controller and keeps the connection id per socket. A plain
 * (non-upgrade) GET answers `426 Upgrade Required`; a draining pod is rejected with
 * `503` by the app-level `onRequest` guard before the handshake.
 */

import { Elysia } from "elysia";
import type { FrameController } from "../controllers/frames-controller";
import type { BunSocketLike } from "../gateways/ws-sender";

export interface GatewayRouteOptions {
  readonly controller: FrameController;
  /** Upper bound on a single client frame; protects the parse path. */
  readonly maxPayloadLength?: number;
}

const DEFAULT_MAX_PAYLOAD = 64 * 1024;

/**
 * Elysia hands each callback a fresh wrapper object for the same connection, so object
 * identity of the wrapper is not stable. `ws.data` is the per-connection context and is
 * shared by every callback of one socket, which makes it a reliable registry key.
 */
function connectionKey(socket: { data?: unknown }): object {
  return (socket.data ?? socket) as object;
}

export function createGatewayRoutes(options: GatewayRouteOptions) {
  const { controller } = options;
  const connectionIds = new WeakMap<object, string>();

  return new Elysia({ name: "gateway-routes" })
    .ws("/v1/gateway", {
      maxPayloadLength: options.maxPayloadLength ?? DEFAULT_MAX_PAYLOAD,
      open(ws) {
        const session = controller.open(ws as unknown as BunSocketLike);

        if (session) {
          connectionIds.set(connectionKey(ws), session.connectionId);
        }
      },
      async message(ws, raw) {
        const connectionId = connectionIds.get(connectionKey(ws));

        if (!connectionId) {
          return;
        }

        // The payload may already be JSON-parsed by Elysia's WS message parser.
        await controller.message(connectionId, raw);
      },
      close(ws) {
        const connectionId = connectionIds.get(connectionKey(ws));

        if (!connectionId) {
          return;
        }

        connectionIds.delete(connectionKey(ws));
        controller.close(connectionId);
      },
      drain(ws) {
        const connectionId = connectionIds.get(connectionKey(ws));

        if (connectionId) {
          controller.drain(connectionId);
        }
      },
    })
    .get("/v1/gateway", ({ set }) => {
      set.status = 426;
      return {
        error: {
          code: "upgrade_required",
          message: "this endpoint requires a websocket upgrade",
        },
      };
    });
}

/**
 * WebSocket frame controller.
 *
 * Thin by design (docs/engineering_guide.md layering): it owns no protocol rules, it
 * validates frames at the boundary and delegates every decision to `SessionService`.
 */

import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";
import { errorFrame, parseClientFrame, serializeServerFrame } from "../models/frames";
import type { Session, SessionRegistry } from "../services/session-registry";
import type { SessionService } from "../services/session-service";
import { createWsSender, type BunSocketLike } from "../gateways/ws-sender";

export interface FrameControllerOptions {
  readonly registry: SessionRegistry;
  readonly sessionService: SessionService;
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
}

const CLOSE_TOO_BUSY = 1013;
const CLOSE_POLICY_VIOLATION = 1008;

export interface FrameController {
  /** Registers the connection; returns null when the pod is at capacity. */
  open(socket: BunSocketLike): Session | null;
  message(connectionId: string, raw: unknown): Promise<void>;
  close(connectionId: string): void;
  drain(connectionId: string): void;
}

export function createFrameController(options: FrameControllerOptions): FrameController {
  const { registry, sessionService, metrics, logger } = options;

  return {
    open(socket) {
      const session = registry.create(createWsSender(socket));

      if (!session) {
        metrics.recordReconnect("error");
        const sender = createWsSender(socket);
        sender.send(
          serializeServerFrame(
            errorFrame("internal_error", "gateway is at capacity, retry shortly"),
          ),
        );
        sender.close(CLOSE_TOO_BUSY, "capacity");
        return null;
      }

      return session;
    },

    async message(connectionId, raw) {
      const session = registry.get(connectionId);

      if (!session) {
        return;
      }

      const parsed = parseClientFrame(raw);

      if (!parsed.ok) {
        logger.warn("invalid client frame", {
          operation: "frame_message",
          status: "invalid_frame",
          connection_id: connectionId,
          detail: parsed.detail,
        });
        metrics.recordReconnect("error");
        session.outbound.closeWith(
          serializeServerFrame(errorFrame("invalid_frame", "frame did not match the protocol")),
          CLOSE_POLICY_VIOLATION,
          "invalid_frame",
        );
        return;
      }

      await sessionService.handleFrame(connectionId, parsed.frame);
    },

    close(connectionId) {
      registry.remove(connectionId);
      logger.debug("session closed", {
        operation: "session_close",
        status: "closed",
        connection_id: connectionId,
      });
    },

    drain(connectionId) {
      registry.get(connectionId)?.outbound.handleDrain();
    },
  };
}

/**
 * Client/server frame protocol (docs/openapi_spec.yaml `components/schemas`).
 *
 * Inbound frames are validated at the transport boundary; anything that does not
 * match the discriminated union becomes `invalid_frame` and the connection is closed.
 * Outbound frames are plain objects serialized exactly once by the transport adapter.
 */

import { z } from "zod";
import type { GatewayErrorCode, ReconnectReason } from "./errors";

const nonEmptyString = z.string().min(1);

export const identifyFrameSchema = z.object({
  type: z.literal("identify"),
  token: nonEmptyString,
});

export const heartbeatFrameSchema = z.object({
  type: z.literal("heartbeat"),
});

export const subscribeCommunityFrameSchema = z.object({
  type: z.literal("subscribe_community"),
  community_id: nonEmptyString,
});

export const unsubscribeCommunityFrameSchema = z.object({
  type: z.literal("unsubscribe_community"),
  community_id: nonEmptyString,
});

export const refreshTokenFrameSchema = z.object({
  type: z.literal("refresh_token"),
  token: nonEmptyString,
});

export const clientFrameSchema = z.discriminatedUnion("type", [
  identifyFrameSchema,
  heartbeatFrameSchema,
  subscribeCommunityFrameSchema,
  unsubscribeCommunityFrameSchema,
  refreshTokenFrameSchema,
]);

export type IdentifyFrame = z.infer<typeof identifyFrameSchema>;
export type HeartbeatFrame = z.infer<typeof heartbeatFrameSchema>;
export type SubscribeCommunityFrame = z.infer<typeof subscribeCommunityFrameSchema>;
export type UnsubscribeCommunityFrame = z.infer<typeof unsubscribeCommunityFrameSchema>;
export type RefreshTokenFrame = z.infer<typeof refreshTokenFrameSchema>;
export type ClientFrame = z.infer<typeof clientFrameSchema>;

export interface ReadyFrame {
  type: "ready";
  user_id: string;
  username: string;
}

export interface HeartbeatAckFrame {
  type: "heartbeat_ack";
}

export interface EventFrame {
  type: "event";
  event_type: string;
  data: Record<string, unknown>;
}

export interface ErrorFrame {
  type: "error";
  code: GatewayErrorCode;
  message: string;
}

export interface ReconnectFrame {
  type: "reconnect";
  reason: ReconnectReason;
}

export type ServerFrame = ReadyFrame | HeartbeatAckFrame | EventFrame | ErrorFrame | ReconnectFrame;

/** Event type emitted when a community subscription is confirmed. */
export const SUBSCRIBED_EVENT_TYPE = "subscribed";

/** Event type emitted for moderation actions affecting the session's own user. */
export const MODERATION_NOTICE_EVENT_TYPE = "moderation_notice";

export type ClientFrameParseResult =
  | { ok: true; frame: ClientFrame }
  | { ok: false; reason: "invalid_frame"; detail: string };

/**
 * Parse one client frame from whatever the transport handed us.
 *
 * Bun/Elysia may deliver the frame as a string, as bytes, or already JSON-parsed when
 * the payload starts like an object literal; all three shapes must validate identically.
 * Returns a typed failure instead of throwing so the caller can emit `invalid_frame`.
 */
export function parseClientFrame(raw: unknown): ClientFrameParseResult {
  let parsed: unknown;

  if (typeof raw === "string") {
    try {
      parsed = JSON.parse(raw);
    } catch {
      return { ok: false, reason: "invalid_frame", detail: "frame is not valid JSON" };
    }
  } else if (raw instanceof Uint8Array) {
    try {
      parsed = JSON.parse(new TextDecoder().decode(raw));
    } catch {
      return { ok: false, reason: "invalid_frame", detail: "frame is not valid JSON" };
    }
  } else if (raw !== null && typeof raw === "object") {
    // Transport already parsed the JSON payload.
    parsed = raw;
  } else {
    return { ok: false, reason: "invalid_frame", detail: "frame must be a JSON object" };
  }

  const result = clientFrameSchema.safeParse(parsed);
  if (!result.success) {
    const detail = result.error.issues
      .map((issue) => `${issue.path.join(".") || "<root>"}: ${issue.message}`)
      .join("; ");
    return { ok: false, reason: "invalid_frame", detail };
  }

  return { ok: true, frame: result.data };
}

export function serializeServerFrame(frame: ServerFrame): string {
  return JSON.stringify(frame);
}

export function readyFrame(userId: string, username: string): ReadyFrame {
  return { type: "ready", user_id: userId, username };
}

export function heartbeatAckFrame(): HeartbeatAckFrame {
  return { type: "heartbeat_ack" };
}

export function eventFrame(eventType: string, data: Record<string, unknown>): EventFrame {
  return { type: "event", event_type: eventType, data };
}

export function errorFrame(code: GatewayErrorCode, message: string): ErrorFrame {
  return { type: "error", code, message };
}

export function reconnectFrame(reason: ReconnectReason): ReconnectFrame {
  return { type: "reconnect", reason };
}

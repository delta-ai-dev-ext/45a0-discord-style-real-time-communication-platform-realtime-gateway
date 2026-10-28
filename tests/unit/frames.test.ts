import { describe, expect, it } from "bun:test";
import {
  errorFrame,
  eventFrame,
  heartbeatAckFrame,
  parseClientFrame,
  readyFrame,
  reconnectFrame,
  serializeServerFrame,
} from "../../src/models/frames";
import { GATEWAY_ERROR_CODES } from "../../src/models/errors";

describe("client frame codec", () => {
  it("accepts every documented client frame", () => {
    const cases = [
      { type: "identify", token: "jwt" },
      { type: "heartbeat" },
      { type: "subscribe_community", community_id: "c1" },
      { type: "unsubscribe_community", community_id: "c1" },
      { type: "refresh_token", token: "jwt2" },
    ] as const;

    for (const frame of cases) {
      const parsed = parseClientFrame(JSON.stringify(frame));
      expect(parsed.ok).toBe(true);
      if (parsed.ok) {
        expect(parsed.frame.type).toBe(frame.type);
      }
    }
  });

  it("rejects non-JSON payloads as invalid_frame", () => {
    const parsed = parseClientFrame("not-json");
    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toBe("invalid_frame");
      expect(parsed.detail).toContain("JSON");
    }
  });

  it("rejects unknown frame types, missing fields and extra discriminators", () => {
    for (const payload of [
      { type: "subscribe_channel", channel_id: "x" },
      { type: "identify" },
      { type: "identify", token: "" },
      { type: 42 },
      { heartbeat: true },
      {},
    ]) {
      const parsed = parseClientFrame(JSON.stringify(payload));
      expect(parsed.ok).toBe(false);
    }
  });

  it("ignores unknown extra properties on a valid frame", () => {
    const parsed = parseClientFrame(JSON.stringify({ type: "heartbeat", extra: 1 }));

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.frame).toEqual({ type: "heartbeat" });
    }
  });

  it("decodes binary frames from the transport", () => {
    const bytes = new TextEncoder().encode(JSON.stringify({ type: "heartbeat" }));
    const parsed = parseClientFrame(bytes);
    expect(parsed.ok).toBe(true);
  });

  it("parses boolean and array payloads without throwing", () => {
    expect(parseClientFrame("true").ok).toBe(false);
    expect(parseClientFrame("[]").ok).toBe(false);
    expect(parseClientFrame("null").ok).toBe(false);
  });
});

describe("server frame codec", () => {
  it("serializes each server frame type to the documented shape", () => {
    expect(JSON.parse(serializeServerFrame(readyFrame("u1", "alice")))).toEqual({
      type: "ready",
      user_id: "u1",
      username: "alice",
    });
    expect(JSON.parse(serializeServerFrame(heartbeatAckFrame()))).toEqual({ type: "heartbeat_ack" });
    expect(JSON.parse(serializeServerFrame(eventFrame("message_created", { message_id: "m" })))).toEqual({
      type: "event",
      event_type: "message_created",
      data: { message_id: "m" },
    });
    expect(JSON.parse(serializeServerFrame(errorFrame("not_subscribed", "nope")))).toEqual({
      type: "error",
      code: "not_subscribed",
      message: "nope",
    });
    expect(JSON.parse(serializeServerFrame(reconnectFrame("shutdown")))).toEqual({
      type: "reconnect",
      reason: "shutdown",
    });
  });

  it("emits exactly the documented error code vocabulary", () => {
    expect([...GATEWAY_ERROR_CODES]).toEqual([
      "invalid_token",
      "token_expired",
      "not_identified",
      "already_identified",
      "not_authorized",
      "not_subscribed",
      "invalid_frame",
      "internal_error",
    ]);
  });
});

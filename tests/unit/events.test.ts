import { describe, expect, it } from "bun:test";
import {
  ACCESS_CHANGING_COMMUNITY_EVENTS,
  CHAT_MESSAGE_EVENTS_TOPIC,
  COMMUNITY_EVENTS_TOPIC,
  IDENTITY_FRIEND_EVENTS_TOPIC,
  parseTopicEvent,
} from "../../src/models/events";

const occurredAt = "2026-02-05T10:00:00.000Z";

const chatCreated = {
  event_id: "e1",
  event_type: "message_created",
  occurred_at: occurredAt,
  trace_parent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  data: {
    message_id: "m1",
    channel_id: "ch1",
    community_id: "c1",
    author_user_id: "u1",
    author_username: "alice",
    content: "hi",
    created_at: occurredAt,
    extra_field: "preserved",
  },
};

describe("kafka event parsing", () => {
  it("parses a chat message event and preserves upstream payload fields", () => {
    const parsed = parseTopicEvent(CHAT_MESSAGE_EVENTS_TOPIC, JSON.stringify(chatCreated));

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.event.eventId).toBe("e1");
      expect(parsed.event.eventType).toBe("message_created");
      expect(parsed.event.occurredAt).toBe(Date.parse(occurredAt));
      expect(parsed.event.traceParent).toBe(chatCreated.trace_parent);
      expect((parsed.event.data as Record<string, unknown>).extra_field).toBe("preserved");
    }
  });

  it("parses community structure, moderation and friend events", () => {
    const communityEvent = parseTopicEvent(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify({
        event_id: "e2",
        event_type: "member_timed_out",
        occurred_at: occurredAt,
        data: { community_id: "c1", user_id: "u2", member_message: "cool down" },
      }),
    );
    expect(communityEvent.ok).toBe(true);

    const friendEvent = parseTopicEvent(
      IDENTITY_FRIEND_EVENTS_TOPIC,
      JSON.stringify({
        event_id: "e3",
        event_type: "friend_request_sent",
        occurred_at: occurredAt,
        data: { request_id: "r1", sender_user_id: "u1", recipient_user_id: "u2" },
      }),
    );
    expect(friendEvent.ok).toBe(true);
  });

  it("rejects unparseable, unknown-topic and non-conforming events", () => {
    expect(parseTopicEvent(CHAT_MESSAGE_EVENTS_TOPIC, "{").ok).toBe(false);
    expect(parseTopicEvent("some_other_topic", JSON.stringify(chatCreated)).ok).toBe(false);

    const missingField = parseTopicEvent(
      CHAT_MESSAGE_EVENTS_TOPIC,
      JSON.stringify({ ...chatCreated, data: { message_id: "m1" } }),
    );
    expect(missingField.ok).toBe(false);

    const badTimestamp = parseTopicEvent(
      CHAT_MESSAGE_EVENTS_TOPIC,
      JSON.stringify({ ...chatCreated, occurred_at: "yesterday" }),
    );
    expect(badTimestamp.ok).toBe(false);
  });

  it("treats every access-changing community event as access-changing", () => {
    for (const eventType of [
      "channel_created",
      "channel_updated",
      "channel_deleted",
      "member_roles_changed",
      "roles_changed",
      "permission_overwrites_changed",
    ] as const) {
      expect(ACCESS_CHANGING_COMMUNITY_EVENTS.has(eventType)).toBe(true);
    }
  });
});

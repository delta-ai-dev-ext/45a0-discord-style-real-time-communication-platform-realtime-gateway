/**
 * Kafka event contracts for the three consumed topics (docs/architecture.md §4.2).
 *
 * The gateway never reinterprets payloads: it validates the envelope plus the fields
 * it needs for routing, then forwards the original `data` object to clients so that
 * fields owned by upstream services (e.g. message content shape) pass through intact.
 */

import { z } from "zod";

export const CHAT_MESSAGE_EVENTS_TOPIC = "chat_message_events";
export const COMMUNITY_EVENTS_TOPIC = "community_events";
export const IDENTITY_FRIEND_EVENTS_TOPIC = "identity_friend_events";

export const CONSUMED_TOPICS = [
  CHAT_MESSAGE_EVENTS_TOPIC,
  COMMUNITY_EVENTS_TOPIC,
  IDENTITY_FRIEND_EVENTS_TOPIC,
] as const;

export type ConsumedTopic = (typeof CONSUMED_TOPICS)[number];

const isoTimestamp = z
  .string()
  .refine((value) => !Number.isNaN(Date.parse(value)), "must be an ISO-8601 timestamp");

const envelopeBase = {
  event_id: z.string().min(1),
  occurred_at: isoTimestamp,
  trace_parent: z.string().min(1).optional(),
};

export const chatMessageEventSchema = z.discriminatedUnion("event_type", [
  z.object({
    ...envelopeBase,
    event_type: z.literal("message_created"),
    data: z.object({
      message_id: z.string().min(1),
      channel_id: z.string().min(1),
      community_id: z.string().min(1),
      author_user_id: z.string().min(1),
      author_username: z.string(),
      content: z.string(),
      created_at: isoTimestamp,
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("message_edited"),
    data: z.object({
      message_id: z.string().min(1),
      channel_id: z.string().min(1),
      community_id: z.string().min(1),
      content: z.string(),
      edited_at: isoTimestamp,
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("message_deleted"),
    data: z.object({
      message_id: z.string().min(1),
      channel_id: z.string().min(1),
      community_id: z.string().min(1),
    }),
  }),
]);

const memberNoticeBase = {
  community_id: z.string().min(1),
  user_id: z.string().min(1),
  member_message: z.string().optional(),
};

export const communityEventSchema = z.discriminatedUnion("event_type", [
  z.object({
    ...envelopeBase,
    event_type: z.literal("channel_created"),
    data: z.object({
      community_id: z.string().min(1),
      channel_id: z.string().min(1),
      name: z.string().optional(),
      type: z.enum(["text", "voice"]).optional(),
      position: z.number().int().optional(),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("channel_updated"),
    data: z.object({
      community_id: z.string().min(1),
      channel_id: z.string().min(1),
      name: z.string().optional(),
      type: z.enum(["text", "voice"]).optional(),
      position: z.number().int().optional(),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("channel_deleted"),
    data: z.object({
      community_id: z.string().min(1),
      channel_id: z.string().min(1),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_joined"),
    data: z.object({ community_id: z.string().min(1), user_id: z.string().min(1) }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_left"),
    data: z.object({ community_id: z.string().min(1), user_id: z.string().min(1) }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_roles_changed"),
    data: z.object({
      community_id: z.string().min(1),
      user_id: z.string().min(1),
      role_ids: z.array(z.string()),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("roles_changed"),
    data: z.object({ community_id: z.string().min(1) }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("permission_overwrites_changed"),
    data: z.object({
      community_id: z.string().min(1),
      channel_id: z.string().min(1).optional(),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_timed_out"),
    data: z.object({
      ...memberNoticeBase,
      timed_out_until: isoTimestamp.optional(),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_timeout_cleared"),
    data: z.object({ ...memberNoticeBase }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_kicked"),
    data: z.object({ ...memberNoticeBase }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_banned"),
    data: z.object({
      ...memberNoticeBase,
      ban_expires_at: isoTimestamp.optional(),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("member_unbanned"),
    data: z.object({ ...memberNoticeBase }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("community_updated"),
    data: z.object({ community_id: z.string().min(1), name: z.string().optional() }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.literal("community_deleted"),
    data: z.object({
      community_id: z.string().min(1),
      channel_ids: z.array(z.string()).optional(),
    }),
  }),
]);

export const friendEventSchema = z.discriminatedUnion("event_type", [
  z.object({
    ...envelopeBase,
    event_type: z.literal("friend_request_sent"),
    data: z.object({
      request_id: z.string().min(1),
      sender_user_id: z.string().min(1),
      recipient_user_id: z.string().min(1),
      sender_username: z.string().optional(),
      recipient_username: z.string().optional(),
    }),
  }),
  z.object({
    ...envelopeBase,
    event_type: z.enum([
      "friend_request_accepted",
      "friend_request_declined",
      "friend_request_cancelled",
      "friendship_removed",
    ]),
    data: z.object({
      request_id: z.string().min(1),
      sender_user_id: z.string().min(1),
      recipient_user_id: z.string().min(1),
    }),
  }),
]);

export type ChatMessageEvent = z.infer<typeof chatMessageEventSchema>;
export type CommunityEvent = z.infer<typeof communityEventSchema>;
export type FriendEvent = z.infer<typeof friendEventSchema>;

export type TopicEvent = ChatMessageEvent | CommunityEvent | FriendEvent;

export type CommunityEventType = CommunityEvent["event_type"];

/** Community events that imply the member's visible channel set may have changed. */
export const ACCESS_CHANGING_COMMUNITY_EVENTS: ReadonlySet<CommunityEventType> = new Set([
  "channel_created",
  "channel_updated",
  "channel_deleted",
  "member_joined",
  "member_left",
  "member_roles_changed",
  "roles_changed",
  "permission_overwrites_changed",
]);

export const MODERATION_COMMUNITY_EVENTS = [
  "member_timed_out",
  "member_timeout_cleared",
  "member_kicked",
  "member_banned",
  "member_unbanned",
] as const;

export type ModerationCommunityEventType = (typeof MODERATION_COMMUNITY_EVENTS)[number];

/** Moderation actions after which the member loses access to the community entirely. */
export const REMOVAL_COMMUNITY_EVENTS: ReadonlySet<ModerationCommunityEventType> = new Set([
  "member_kicked",
  "member_banned",
]);

export interface RoutedEvent<TEvent extends TopicEvent = TopicEvent> {
  readonly eventId: string;
  readonly eventType: TEvent["event_type"];
  readonly occurredAt: number;
  readonly traceParent?: string;
  /** Original payload object; typed by `eventType` but forwarded verbatim. */
  readonly data: TEvent["data"];
}

export type EventParseResult<TEvent extends TopicEvent = TopicEvent> =
  | { ok: true; event: RoutedEvent<TEvent> }
  | { ok: false; detail: string };

const schemasByTopic = {
  [CHAT_MESSAGE_EVENTS_TOPIC]: chatMessageEventSchema,
  [COMMUNITY_EVENTS_TOPIC]: communityEventSchema,
  [IDENTITY_FRIEND_EVENTS_TOPIC]: friendEventSchema,
} as const;

const topicSchemaFor = (topic: string): z.ZodType<TopicEvent> | undefined =>
  schemasByTopic[topic as ConsumedTopic] as z.ZodType<TopicEvent> | undefined;

/**
 * Validate a raw Kafka value against the schema for its topic.
 * Malformed events are reported (never thrown) so the consumer can log and continue.
 */
export function parseTopicEvent(topic: string, raw: string | Uint8Array): EventParseResult {
  const schema = topicSchemaFor(topic);
  if (!schema) {
    return { ok: false, detail: `topic ${topic} is not consumed by this service` };
  }

  const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { ok: false, detail: "event value is not valid JSON" };
  }

  const result = schema.safeParse(parsed);
  if (!result.success) {
    const detail = result.error.issues
      .map((issue) => `${issue.path.join(".") || "<root>"}: ${issue.message}`)
      .join("; ");
    return { ok: false, detail };
  }

  const event = result.data;
  const occurredAt = Date.parse(event.occurred_at);
  if (Number.isNaN(occurredAt)) {
    return { ok: false, detail: "occurred_at is not a parseable timestamp" };
  }

  const rawData = (parsed as { data?: unknown }).data;

  return {
    ok: true,
    event: {
      eventId: event.event_id,
      eventType: event.event_type,
      occurredAt,
      traceParent: "trace_parent" in event ? event.trace_parent : undefined,
      data: (rawData ?? {}) as TopicEvent["data"],
    } as RoutedEvent,
  };
}

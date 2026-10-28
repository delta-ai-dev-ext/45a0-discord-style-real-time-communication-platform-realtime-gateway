import { describe, expect, it } from "bun:test";
import { silentLogger } from "../../src/lib/logger";
import { asUserId } from "../../src/models/ids";
import {
  CHAT_MESSAGE_EVENTS_TOPIC,
  COMMUNITY_EVENTS_TOPIC,
  IDENTITY_FRIEND_EVENTS_TOPIC,
} from "../../src/models/events";
import { EventRouter } from "../../src/services/event-router";
import { FanoutDispatcher } from "../../src/services/fanout-dispatcher";
import type { ServerFrame } from "../../src/models/frames";
import { createRegistryHarness } from "../mocks/harness";
import { createMockAccessClient } from "../mocks/mock-access-client";
import { sessionFor } from "../mocks/mock-token-verifier";
import type { FakeSocket } from "../mocks/fake-socket";

const OCCURRED_AT = "2026-02-05T10:00:00.000Z";

function setup(options: { dedupeCapacity?: number; refetchDedupeMs?: number } = {}) {
  const harness = createRegistryHarness({ queueCapacity: 64 });
  const accessClient = createMockAccessClient();
  const dispatcher = new FanoutDispatcher({
    metrics: harness.metrics,
    logger: silentLogger,
    clock: harness.clock,
  });
  const router = new EventRouter({
    registry: harness.registry,
    dispatcher,
    accessClient,
    logger: silentLogger,
    clock: harness.clock,
    ...(options.dedupeCapacity !== undefined ? { dedupeCapacity: options.dedupeCapacity } : {}),
    ...(options.refetchDedupeMs !== undefined
      ? { accessRefetchDedupeMs: options.refetchDedupeMs }
      : {}),
  });

  return { ...harness, accessClient, dispatcher, router };
}

type Harness = ReturnType<typeof setup>;

function subscribe(
  harness: Harness,
  userId: string,
  communityId: string,
  visibleChannelIds: string[],
): { socket: FakeSocket; id: string } {
  const { session, socket } = harness.createSession();
  harness.registry.identify(session.connectionId, sessionFor(userId));
  harness.registry.subscribe(session.connectionId, communityId, visibleChannelIds);
  return { socket, id: session.connectionId };
}

function chatCreated(overrides: { event_id?: string; channel?: string; community?: string } = {}) {
  return {
    event_id: overrides.event_id ?? "evt-1",
    event_type: "message_created",
    occurred_at: OCCURRED_AT,
    data: {
      message_id: "m1",
      channel_id: overrides.channel ?? "ch1",
      community_id: overrides.community ?? "c1",
      author_user_id: "author",
      author_username: "author-name",
      content: "hello",
      created_at: OCCURRED_AT,
    },
  };
}

function communityEvent(
  eventType: string,
  data: Record<string, unknown>,
  eventId = `evt-${eventType}`,
) {
  return {
    event_id: eventId,
    event_type: eventType,
    occurred_at: OCCURRED_AT,
    trace_parent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    data,
  };
}

describe("event router — chat delivery", () => {
  it("delivers to subscribers that can see the channel only", async () => {
    const harness = setup();
    const visible = subscribe(harness, "u1", "c1", ["ch1"]);
    const hidden = subscribe(harness, "u2", "c1", ["ch2"]);
    const elsewhere = subscribe(harness, "u3", "c2", ["ch1"]);

    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, JSON.stringify(chatCreated()));

    expect(visible.socket.frames()).toHaveLength(1);
    expect(visible.socket.frames()[0]).toMatchObject({
      type: "event",
      event_type: "message_created",
      data: { message_id: "m1", channel_id: "ch1" },
    });
    expect(hidden.socket.frames()).toHaveLength(0);
    expect(elsewhere.socket.frames()).toHaveLength(0);
  });

  it("records delivered and latency metrics for socket writes", async () => {
    const harness = setup();
    subscribe(harness, "u1", "c1", ["ch1"]);
    harness.clock.advance(250);

    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, JSON.stringify(chatCreated()));

    const rendered = await harness.metrics.registry.metrics();
    expect(rendered).toContain("gateway_events_delivered_total");
    expect(rendered).toContain('event_type="message_created"');
    expect(rendered).toContain('source_topic="chat_message_events"');
    expect(rendered).toContain('gateway_fanout_latency_seconds_count{source_topic="chat_message_events"} 1');
  });

  it("delivers message_edited and message_deleted in order", async () => {
    const harness = setup();
    const { socket } = subscribe(harness, "u1", "c1", ["ch1"]);

    await harness.router.handle(
      CHAT_MESSAGE_EVENTS_TOPIC,
      JSON.stringify({
        ...chatCreated({ event_id: "e1" }),
        event_type: "message_edited",
        data: {
          message_id: "m1",
          channel_id: "ch1",
          community_id: "c1",
          content: "edited",
          edited_at: OCCURRED_AT,
        },
      }),
    );
    await harness.router.handle(
      CHAT_MESSAGE_EVENTS_TOPIC,
      JSON.stringify({
        ...chatCreated({ event_id: "e2" }),
        event_type: "message_deleted",
        data: { message_id: "m1", channel_id: "ch1", community_id: "c1" },
      }),
    );

    expect(socket.frames().map((frame) => frame.type === "event" && frame.event_type)).toEqual([
      "message_edited",
      "message_deleted",
    ]);
  });

  it("dedupes by event_id and prunes the window", async () => {
    const harness = setup({ dedupeCapacity: 2 });
    const { socket } = subscribe(harness, "u1", "c1", ["ch1"]);
    const event = JSON.stringify(chatCreated({ event_id: "same" }));

    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, event);
    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, event);
    expect(socket.frames()).toHaveLength(1);

    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, JSON.stringify(chatCreated({ event_id: "a" })));
    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, JSON.stringify(chatCreated({ event_id: "b" })));
    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, JSON.stringify(chatCreated({ event_id: "c" })));

    expect(socket.frames()).toHaveLength(4);
  });

  it("ignores malformed payloads and unknown topics without throwing", async () => {
    const harness = setup();
    const { socket } = subscribe(harness, "u1", "c1", ["ch1"]);

    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, "{not-json");
    await harness.router.handle(CHAT_MESSAGE_EVENTS_TOPIC, JSON.stringify({ event_id: "x" }));
    await harness.router.handle("unknown_topic", JSON.stringify(chatCreated({ event_id: "y" })));

    expect(socket.frames()).toHaveLength(0);
  });
});

describe("event router — community structure", () => {
  it("broadcasts structure events and refreshes cached visibility", async () => {
    const harness = setup();
    const { socket } = subscribe(harness, "u1", "c1", ["ch1"]);
    harness.accessClient.allow("c1", "u1", ["ch1", "ch2"]);

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(communityEvent("channel_created", { community_id: "c1", channel_id: "ch2" })),
    );

    expect(socket.frames()[0]).toMatchObject({ type: "event", event_type: "channel_created" });
    expect(harness.accessClient.calls).toHaveLength(1);
    expect(harness.accessClient.calls[0]?.traceparent).toBe(
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    );

    const session = harness.registry.get(subscribe(harness, "u1", "c1", ["ch1"]).id);
    void session;
    const subscribed = harness.registry.sessionsForCommunity("c1");
    expect(subscribed[0]?.subscriptions.get("c1")?.visibleChannelIds).toEqual(
      new Set(["ch1", "ch2"]),
    );
  });

  it("fails closed by clearing visibility when the access service is unavailable", async () => {
    const harness = setup();
    const { id } = subscribe(harness, "u1", "c1", ["ch1"]);
    harness.accessClient.unavailable("c1", "u1");

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(communityEvent("roles_changed", { community_id: "c1" })),
    );

    expect(harness.registry.get(id)?.subscriptions.get("c1")?.visibleChannelIds.size).toBe(0);
  });

  it("drops the subscription when the re-fetch denies membership", async () => {
    const harness = setup();
    const { id } = subscribe(harness, "u1", "c1", ["ch1"]);
    harness.accessClient.deny("c1", "u1");

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(communityEvent("member_left", { community_id: "c1", user_id: "u1" })),
    );

    expect(harness.registry.get(id)?.subscriptions.has("c1")).toBe(false);
  });

  it("dedupes access re-fetches per user per burst", async () => {
    const harness = setup({ refetchDedupeMs: 1_000 });
    subscribe(harness, "u1", "c1", ["ch1"]);
    harness.accessClient.allow("c1", "u1", ["ch1"]);

    for (let index = 0; index < 3; index += 1) {
      await harness.router.handle(
        COMMUNITY_EVENTS_TOPIC,
        JSON.stringify(
          communityEvent(
            "channel_updated",
            { community_id: "c1", channel_id: "ch1" },
            `evt-${index}`,
          ),
        ),
      );
    }

    expect(harness.accessClient.calls).toHaveLength(1);

    harness.clock.advance(1_500);
    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(
        communityEvent("channel_updated", { community_id: "c1", channel_id: "ch1" }, "evt-late"),
      ),
    );
    expect(harness.accessClient.calls).toHaveLength(2);
  });

  it("removes subscriptions on community_deleted", async () => {
    const harness = setup();
    const first = subscribe(harness, "u1", "c1", ["ch1"]);
    const second = subscribe(harness, "u2", "c1", ["ch1"]);

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(communityEvent("community_deleted", { community_id: "c1" })),
    );

    expect(first.socket.frames()[0]).toMatchObject({ event_type: "community_deleted" });
    expect(second.socket.frames()).toHaveLength(1);
    expect(harness.registry.sessionsForCommunity("c1")).toHaveLength(0);
    expect(harness.registry.get(first.id)?.subscriptions.size).toBe(0);
  });

  it("delivers community_updated without an access re-fetch", async () => {
    const harness = setup();
    const { socket } = subscribe(harness, "u1", "c1", ["ch1"]);

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(communityEvent("community_updated", { community_id: "c1", name: "C" })),
    );

    expect(socket.frames()[0]).toMatchObject({ event_type: "community_updated" });
    expect(harness.accessClient.calls).toHaveLength(0);
  });
});

describe("event router — moderation notices", () => {
  it("notices every session of the affected user and narrows visibility on timeout", async () => {
    const harness = setup();
    const laptop = subscribe(harness, "u2", "c1", ["ch1", "ch2"]);
    const phone = subscribe(harness, "u2", "c1", ["ch1", "ch2"]);
    const bystander = subscribe(harness, "u1", "c1", ["ch1"]);

    harness.accessClient.allow("c1", "u2", ["ch1"]);

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(
        communityEvent("member_timed_out", {
          community_id: "c1",
          user_id: "u2",
          timed_out_until: "2026-02-05T12:00:00.000Z",
          member_message: "You can post again after a short cooldown.",
        }),
      ),
    );

    const notice: ServerFrame = {
      type: "event",
      event_type: "moderation_notice",
      data: {
        community_id: "c1",
        user_id: "u2",
        action: "member_timed_out",
        timed_out_until: "2026-02-05T12:00:00.000Z",
        member_message: "You can post again after a short cooldown.",
      },
    };

    expect(laptop.socket.frames()).toEqual([notice]);
    expect(phone.socket.frames()).toEqual([notice]);
    expect(bystander.socket.frames()).toHaveLength(0);
    expect(harness.registry.get(laptop.id)?.subscriptions.get("c1")?.visibleChannelIds).toEqual(
      new Set(["ch1"]),
    );
    expect(harness.registry.get(laptop.id)?.subscriptions.has("c1")).toBe(true);
  });

  it("force-unsubscribes on member_kicked and member_banned", async () => {
    for (const action of ["member_kicked", "member_banned"] as const) {
      const harness = setup();
      const { socket, id } = subscribe(harness, "u2", "c1", ["ch1"]);

      await harness.router.handle(
        COMMUNITY_EVENTS_TOPIC,
        JSON.stringify(
          communityEvent(
            action,
            { community_id: "c1", user_id: "u2", member_message: "You were removed." },
            `evt-${action}`,
          ),
        ),
      );

      expect(socket.frames()[0]).toMatchObject({
        type: "event",
        event_type: "moderation_notice",
        data: { action },
      });
      expect(harness.registry.get(id)?.subscriptions.has("c1")).toBe(false);
    }
  });

  it("notices member_unbanned without re-subscribing", async () => {
    const harness = setup();
    const { socket, id } = subscribe(harness, "u2", "c1", ["ch1"]);

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(communityEvent("member_unbanned", { community_id: "c1", user_id: "u2" })),
    );

    expect(socket.frames()[0]).toMatchObject({ event_type: "moderation_notice" });
    expect(harness.registry.get(id)?.subscriptions.has("c1")).toBe(true);
    expect(harness.accessClient.calls).toHaveLength(0);
  });

  it("ignores moderation events without an affected user", async () => {
    const harness = setup();
    const { socket } = subscribe(harness, "u2", "c1", ["ch1"]);

    await harness.router.handle(
      COMMUNITY_EVENTS_TOPIC,
      JSON.stringify(communityEvent("member_kicked", { community_id: "c1" })),
    );

    expect(socket.frames()).toHaveLength(0);
  });
});

describe("event router — friend events", () => {
  const friendPayload = {
    request_id: "r1",
    sender_user_id: "u1",
    recipient_user_id: "u2",
    sender_username: "alice",
    recipient_username: "bob",
  };

  it("delivers friend_request_sent to the recipient only", async () => {
    const harness = setup();
    const sender = subscribe(harness, "u1", "c1", ["ch1"]);
    const recipientSessions = [
      subscribe(harness, "u2", "c1", ["ch1"]),
      subscribe(harness, "u2", "c1", ["ch1"]),
    ];

    await harness.router.handle(
      IDENTITY_FRIEND_EVENTS_TOPIC,
      JSON.stringify({
        event_id: "f1",
        event_type: "friend_request_sent",
        occurred_at: OCCURRED_AT,
        data: friendPayload,
      }),
    );

    expect(sender.socket.frames()).toHaveLength(0);
    for (const session of recipientSessions) {
      expect(session.socket.frames()[0]).toMatchObject({
        event_type: "friend_request_sent",
        data: { request_id: "r1" },
      });
    }
  });

  it("delivers accepted/declined/cancelled/removed events to both users once", async () => {
    for (const eventType of [
      "friend_request_accepted",
      "friend_request_declined",
      "friend_request_cancelled",
      "friendship_removed",
    ]) {
      const harness = setup();
      const sender = subscribe(harness, "u1", "c1", ["ch1"]);
      const recipient = subscribe(harness, "u2", "c1", ["ch1"]);

      await harness.router.handle(
        IDENTITY_FRIEND_EVENTS_TOPIC,
        JSON.stringify({
          event_id: `f-${eventType}`,
          event_type: eventType,
          occurred_at: OCCURRED_AT,
          data: { request_id: "r1", sender_user_id: "u1", recipient_user_id: "u2" },
        }),
      );

      expect(sender.socket.frames()).toHaveLength(1);
      expect(recipient.socket.frames()).toHaveLength(1);
    }
  });

  it("drops events with no local recipients", async () => {
    const harness = setup();
    subscribe(harness, "u9", "c9", ["ch9"]);

    await harness.router.handle(
      IDENTITY_FRIEND_EVENTS_TOPIC,
      JSON.stringify({
        event_id: "f2",
        event_type: "friend_request_sent",
        occurred_at: OCCURRED_AT,
        data: friendPayload,
      }),
    );

    expect(harness.registry.sessionsForUser(asUserId("u2"))).toHaveLength(0);
  });
});

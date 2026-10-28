/**
 * Kafka event router (architecture §4.2, §4.3).
 *
 * The only component aware of Kafka topics and event contracts. It maps each consumed
 * envelope to the sessions that must receive it, applies the per-topic delivery rules,
 * dedupes by `event_id`, and enforces "access updates before message dispatch" for the
 * same community by awaiting in-flight access re-fetches before delivering chat events.
 */

import type { Clock } from "../lib/clock";
import type { Logger } from "../lib/logger";
import { traceContextFromParent, type TraceContext } from "../lib/trace";
import { MODERATION_NOTICE_EVENT_TYPE, eventFrame, type EventFrame } from "../models/frames";
import { asUserId, type UserId } from "../models/ids";
import {
  ACCESS_CHANGING_COMMUNITY_EVENTS,
  CHAT_MESSAGE_EVENTS_TOPIC,
  COMMUNITY_EVENTS_TOPIC,
  IDENTITY_FRIEND_EVENTS_TOPIC,
  MODERATION_COMMUNITY_EVENTS,
  REMOVAL_COMMUNITY_EVENTS,
  parseTopicEvent,
  type ChatMessageEvent,
  type CommunityEvent,
  type FriendEvent,
  type ModerationCommunityEventType,
  type RoutedEvent,
} from "../models/events";
import type { AccessClient } from "../gateways/access-client";
import type { FanoutDispatcher } from "./fanout-dispatcher";
import type { Session, SessionRegistry } from "./session-registry";

export interface EventRouterOptions {
  readonly registry: SessionRegistry;
  readonly dispatcher: FanoutDispatcher;
  readonly accessClient: AccessClient;
  readonly logger: Logger;
  readonly clock: Clock;
  /** Size of the per-pod `event_id` dedupe window. */
  readonly dedupeCapacity?: number;
  /** Minimum gap between access re-fetches for the same user/community. */
  readonly accessRefetchDedupeMs?: number;
}

interface ChatPayload {
  readonly community_id: string;
  readonly channel_id: string;
}

interface CommunityPayload {
  readonly community_id: string;
  readonly channel_id?: string;
  readonly user_id?: string;
  readonly channel_ids?: string[];
  readonly timed_out_until?: string;
  readonly ban_expires_at?: string;
  readonly member_message?: string;
}

interface FriendPayload {
  readonly sender_user_id: string;
  readonly recipient_user_id: string;
}

const DEFAULT_DEDUPE_CAPACITY = 10_000;
const DEFAULT_ACCESS_REFETCH_DEDUPE_MS = 1_000;

export class EventRouter {
  private readonly registry: SessionRegistry;
  private readonly dispatcher: FanoutDispatcher;
  private readonly accessClient: AccessClient;
  private readonly logger: Logger;
  private readonly clock: Clock;
  private readonly dedupeCapacity: number;
  private readonly accessRefetchDedupeMs: number;

  /** Insertion-ordered `event_id` window for at-least-once dedupe. */
  private readonly seenEvents = new Map<string, number>();
  /** `${communityId}|${userId}` → in-flight re-fetch promise. */
  private readonly inFlightRefreshes = new Map<string, Promise<void>>();
  /** `${communityId}|${userId}` → last re-fetch completion time. */
  private readonly lastRefreshedAt = new Map<string, number>();
  /** `communityId` → in-flight re-fetches, awaited before chat dispatch. */
  private readonly pendingByCommunity = new Map<string, Set<Promise<void>>>();

  constructor(options: EventRouterOptions) {
    this.registry = options.registry;
    this.dispatcher = options.dispatcher;
    this.accessClient = options.accessClient;
    this.logger = options.logger;
    this.clock = options.clock;
    this.dedupeCapacity = options.dedupeCapacity ?? DEFAULT_DEDUPE_CAPACITY;
    this.accessRefetchDedupeMs = options.accessRefetchDedupeMs ?? DEFAULT_ACCESS_REFETCH_DEDUPE_MS;
  }

  /** Consume one Kafka record. Never throws: a bad event must not stop the consumer. */
  async handle(topic: string, raw: string | Uint8Array): Promise<void> {
    const parsed = parseTopicEvent(topic, raw);

    if (!parsed.ok) {
      this.logger.warn("discarding unparseable event", {
        operation: "event_route",
        status: "invalid",
        topic,
        detail: parsed.detail,
      });
      return;
    }

    const event = parsed.event;

    if (this.isDuplicate(event.eventId)) {
      this.logger.debug("discarding duplicate event", {
        operation: "event_route",
        status: "duplicate",
        topic,
        event_id: event.eventId,
      });
      return;
    }

    const trace = traceContextFromParent(event.traceParent);

    try {
      switch (topic) {
        case CHAT_MESSAGE_EVENTS_TOPIC:
          await this.routeChat(event as RoutedEvent<ChatMessageEvent>, trace);
          return;
        case COMMUNITY_EVENTS_TOPIC:
          await this.routeCommunity(event as RoutedEvent<CommunityEvent>, trace);
          return;
        case IDENTITY_FRIEND_EVENTS_TOPIC:
          this.routeFriend(event as RoutedEvent<FriendEvent>, trace);
          return;
        default:
          this.logger.warn("ignoring event from unexpected topic", {
            operation: "event_route",
            status: "unknown_topic",
            topic,
            event_id: event.eventId,
          });
      }
    } catch (error) {
      this.logger.error("event routing failed", {
        operation: "event_route",
        status: "error",
        topic,
        event_id: event.eventId,
        trace_id: trace.traceId,
        detail: error instanceof Error ? error.message : "unknown",
      });
    }
  }

  /* ------------------------------- chat topic ------------------------------- */

  private async routeChat(event: RoutedEvent<ChatMessageEvent>, trace: TraceContext): Promise<void> {
    const data = event.data as ChatPayload;

    // Access updates for this community are applied before any of its messages.
    await this.awaitCommunityAccessRefresh(data.community_id);

    const recipients = this.registry
      .sessionsForCommunity(data.community_id)
      .filter((session) => this.canSeeChannel(session, data.community_id, data.channel_id));

    this.dispatcher.deliverTo(recipients, this.frameFor(event), {
      topic: CHAT_MESSAGE_EVENTS_TOPIC,
      occurredAt: event.occurredAt,
    });

    this.logger.debug("chat event routed", {
      operation: "event_route",
      status: recipients.length > 0 ? "delivered" : "no_recipients",
      topic: CHAT_MESSAGE_EVENTS_TOPIC,
      event_id: event.eventId,
      trace_id: trace.traceId,
    });
  }

  /** Fail closed: no cached visibility for the channel means no delivery. */
  private canSeeChannel(session: Session, communityId: string, channelId: string): boolean {
    const subscription = session.subscriptions.get(communityId);
    return subscription !== undefined && subscription.visibleChannelIds.has(channelId);
  }

  /* ----------------------------- community topic ----------------------------- */

  private async routeCommunity(
    event: RoutedEvent<CommunityEvent>,
    trace: TraceContext,
  ): Promise<void> {
    const data = event.data as CommunityPayload;
    const eventType = event.eventType as CommunityEvent["event_type"];

    if (isModerationEvent(eventType)) {
      await this.routeModeration(eventType, data, event.occurredAt, event.eventId, trace);
      return;
    }

    this.dispatcher.deliverTo(
      this.registry.sessionsForCommunity(data.community_id),
      this.frameFor(event),
      { topic: COMMUNITY_EVENTS_TOPIC, occurredAt: event.occurredAt },
    );

    if (eventType === "community_deleted") {
      const removed = this.registry.removeCommunityFromAll(data.community_id);
      this.logger.info("community removed from local sessions", {
        operation: "event_route",
        status: "community_deleted",
        event_id: event.eventId,
        community_id: data.community_id,
        trace_id: trace.traceId,
        detail: `${removed} subscriptions dropped`,
      });
      return;
    }

    if (ACCESS_CHANGING_COMMUNITY_EVENTS.has(eventType)) {
      await this.refreshSubscribers(data.community_id, trace);
    }
  }

  private async routeModeration(
    eventType: ModerationCommunityEventType,
    data: CommunityPayload,
    occurredAt: number,
    eventId: string,
    trace: TraceContext,
  ): Promise<void> {
    if (!data.user_id) {
      this.logger.warn("moderation event without affected user", {
        operation: "event_route",
        status: "invalid",
        event_id: eventId,
        community_id: data.community_id,
      });
      return;
    }

    const affectedUserId = asUserId(data.user_id);
    const notice = buildModerationNotice(eventType, data);

    this.dispatcher.deliverTo(
      this.registry.sessionsForUser(affectedUserId),
      eventFrame(MODERATION_NOTICE_EVENT_TYPE, notice),
      { topic: COMMUNITY_EVENTS_TOPIC, occurredAt },
    );

    if (REMOVAL_COMMUNITY_EVENTS.has(eventType)) {
      const removed = this.registry.forceUnsubscribeUser(affectedUserId, data.community_id);
      this.logger.info("member removed from community, subscriptions dropped", {
        operation: "event_route",
        status: eventType,
        event_id: eventId,
        community_id: data.community_id,
        user_id: data.user_id,
        trace_id: trace.traceId,
        detail: `${removed} subscriptions dropped`,
      });
      return;
    }

    if (eventType === "member_timed_out" || eventType === "member_timeout_cleared") {
      // Timeouts narrow visibility, clearing widens it; never auto-unsubscribe.
      await this.refreshUserAccess(data.user_id, data.community_id, trace, true);
    }
  }

  /* ------------------------------ friend topic ------------------------------ */

  private routeFriend(event: RoutedEvent<FriendEvent>, trace: TraceContext): void {
    const data = event.data as FriendPayload;
    const eventType = event.eventType as FriendEvent["event_type"];

    const recipients =
      eventType === "friend_request_sent"
        ? this.registry.sessionsForUser(asUserId(data.recipient_user_id))
        : this.sessionsForUsers([data.sender_user_id, data.recipient_user_id]);

    this.dispatcher.deliverTo(recipients, this.frameFor(event), {
      topic: IDENTITY_FRIEND_EVENTS_TOPIC,
      occurredAt: event.occurredAt,
    });

    this.logger.debug("friend event routed", {
      operation: "event_route",
      status: recipients.length > 0 ? "delivered" : "no_recipients",
      event_id: event.eventId,
      trace_id: trace.traceId,
    });
  }

  private sessionsForUsers(userIds: readonly string[]): Session[] {
    const seen = new Set<string>();
    const sessions: Session[] = [];

    for (const userId of userIds) {
      for (const session of this.registry.sessionsForUser(asUserId(userId))) {
        if (seen.has(session.connectionId)) {
          continue;
        }
        seen.add(session.connectionId);
        sessions.push(session);
      }
    }

    return sessions;
  }

  /* ------------------------------ access refresh ----------------------------- */

  private async refreshSubscribers(communityId: string, trace: TraceContext): Promise<void> {
    const userIds = new Set<string>();

    for (const session of this.registry.sessionsForCommunity(communityId)) {
      if (session.userId !== null) {
        userIds.add(session.userId);
      }
    }

    await Promise.all(
      [...userIds].map((userId) => this.refreshUserAccess(userId, communityId, trace, false)),
    );
  }

  /**
   * Re-fetch one member's access and rewrite cached visibility for every local session
   * of that user. Fails closed: any non-definitive answer clears visibility (and drops
   * an outright denial) so an event is never delivered on stale permissions.
   */
  private async refreshUserAccess(
    userId: string,
    communityId: string,
    trace: TraceContext,
    force: boolean,
  ): Promise<void> {
    const key = `${communityId}|${userId}`;
    const inFlight = this.inFlightRefreshes.get(key);

    if (inFlight) {
      await inFlight;
      return;
    }

    if (!force && this.wasRecentlyRefreshed(key)) {
      return;
    }

    const task = (async (): Promise<void> => {
      const decision = await this.accessClient.fetchAccess({
        communityId,
        userId,
        traceparent: trace.traceparent,
      });

      const brandUserId: UserId = asUserId(userId);
      const sessions = this.registry.sessionsForUser(brandUserId);

      if (!decision.ok) {
        for (const session of sessions) {
          this.registry.updateVisibility(session.connectionId, communityId, []);
        }

        if (decision.reason === "unavailable") {
          this.logger.warn("access re-fetch unavailable, visibility cleared", {
            operation: "access_refresh",
            status: "fail_closed",
            community_id: communityId,
            user_id: userId,
            trace_id: trace.traceId,
            detail: decision.detail,
          });
          return;
        }

        this.registry.forceUnsubscribeUser(brandUserId, communityId);
        this.logger.info("access re-fetch denied, subscription dropped", {
          operation: "access_refresh",
          status: decision.reason,
          community_id: communityId,
          user_id: userId,
          trace_id: trace.traceId,
        });
        return;
      }

      for (const session of sessions) {
        this.registry.updateVisibility(session.connectionId, communityId, decision.visibleChannelIds);
      }

      this.logger.debug("access re-fetch applied", {
        operation: "access_refresh",
        status: "ok",
        community_id: communityId,
        user_id: userId,
        trace_id: trace.traceId,
      });
    })();

    this.inFlightRefreshes.set(key, task);
    this.trackCommunityRefresh(communityId, task);

    try {
      await task;
      this.lastRefreshedAt.set(key, this.clock.now());
      this.pruneRefreshHistory();
    } finally {
      this.inFlightRefreshes.delete(key);
      this.pendingByCommunity.get(communityId)?.delete(task);
    }
  }

  private async awaitCommunityAccessRefresh(communityId: string): Promise<void> {
    const pending = this.pendingByCommunity.get(communityId);

    if (!pending || pending.size === 0) {
      return;
    }

    await Promise.allSettled([...pending]);
  }

  private trackCommunityRefresh(communityId: string, task: Promise<void>): void {
    let pending = this.pendingByCommunity.get(communityId);

    if (!pending) {
      pending = new Set();
      this.pendingByCommunity.set(communityId, pending);
    }

    pending.add(task);
  }

  private wasRecentlyRefreshed(key: string): boolean {
    const last = this.lastRefreshedAt.get(key);
    return last !== undefined && this.clock.now() - last < this.accessRefetchDedupeMs;
  }

  private pruneRefreshHistory(): void {
    if (this.lastRefreshedAt.size <= this.dedupeCapacity) {
      return;
    }

    const cutoff = this.clock.now() - this.accessRefetchDedupeMs;

    for (const [key, timestamp] of this.lastRefreshedAt) {
      if (timestamp < cutoff) {
        this.lastRefreshedAt.delete(key);
      }
    }
  }

  /* --------------------------------- helpers -------------------------------- */

  private frameFor(event: RoutedEvent): EventFrame {
    return eventFrame(event.eventType, event.data as Record<string, unknown>);
  }

  private isDuplicate(eventId: string): boolean {
    if (this.seenEvents.has(eventId)) {
      return true;
    }

    this.seenEvents.set(eventId, this.clock.now());

    if (this.seenEvents.size > this.dedupeCapacity) {
      const oldest = this.seenEvents.keys().next();
      if (!oldest.done) {
        this.seenEvents.delete(oldest.value);
      }
    }

    return false;
  }
}

function isModerationEvent(
  eventType: CommunityEvent["event_type"],
): eventType is ModerationCommunityEventType {
  return (MODERATION_COMMUNITY_EVENTS as readonly string[]).includes(eventType);
}

/** Build the member-facing moderation notice payload (never punitive wording). */
export function buildModerationNotice(
  action: ModerationCommunityEventType,
  data: CommunityPayload,
): Record<string, unknown> {
  const notice: Record<string, unknown> = {
    community_id: data.community_id,
    user_id: data.user_id,
    action,
  };

  if (data.timed_out_until) {
    notice.timed_out_until = data.timed_out_until;
  }

  if (data.ban_expires_at) {
    notice.ban_expires_at = data.ban_expires_at;
  }

  if (data.member_message) {
    notice.member_message = data.member_message;
  }

  return notice;
}

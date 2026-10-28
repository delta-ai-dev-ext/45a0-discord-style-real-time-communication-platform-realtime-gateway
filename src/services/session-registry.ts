/**
 * In-memory session registry (architecture §2 "Session registry").
 *
 * All state here is ephemeral and per pod: losing it only loses this pod's sessions;
 * clients reconnect anywhere and re-subscribe (ADR-001). Indexes by user and by
 * community keep fan-out O(subscribers) instead of O(all sessions).
 */

import { utcDayKey, type Clock } from "../lib/clock";
import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";
import type { VerifiedSession } from "../lib/jwt";
import {
  asConnectionId,
  asCommunityId,
  type CommunityId,
  type ConnectionId,
  type UserId,
} from "../models/ids";
import { reconnectFrame, serializeServerFrame } from "../models/frames";
import { OutboundQueue, type OutboundSender } from "./outbound-queue";

export interface SessionSubscription {
  readonly communityId: CommunityId;
  /** Cached answer from community_service; absence of a channel means "not visible". */
  visibleChannelIds: Set<string>;
  updatedAt: number;
}

export interface Session {
  readonly connectionId: ConnectionId;
  readonly outbound: OutboundQueue;
  readonly subscriptions: Map<string, SessionSubscription>;
  readonly createdAt: number;
  userId: UserId | null;
  username: string | null;
  /** Token expiry in epoch milliseconds; `null` until the session identifies. */
  tokenExpiresAt: number | null;
  lastActivityAt: number;
  /** Guards against re-sending the proactive expiry `reconnect` frame. */
  expiryNoticeSent: boolean;
  /**
   * Lead time used for the proactive reconnect: a quarter of the token lifetime,
   * capped at one minute, so short-lived test tokens are not warned immediately.
   */
  expiryNoticeLeadMs: number;
  /** Set once the transport is closed, so sweeps skip the session. */
  closing: boolean;
}

export interface SessionRegistryOptions {
  readonly clock: Clock;
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
  readonly maxConnections: number;
  readonly outboundQueueCapacity: number;
  /** Upper bound on the per-pod daily-active dedupe set. */
  readonly dailyActiveCapacity?: number;
}

const CONNECTION_ID_BYTES = 12;
const DEFAULT_DAILY_ACTIVE_CAPACITY = 100_000;

export class SessionRegistry {
  private readonly sessions = new Map<string, Session>();
  private readonly byUserId = new Map<string, Set<string>>();
  private readonly byCommunityId = new Map<string, Set<string>>();
  private readonly clock: Clock;
  private readonly metrics: MetricsRegistry;
  private readonly logger: Logger;
  private readonly maxConnections: number;
  private readonly outboundQueueCapacity: number;
  private readonly dailyActiveCapacity: number;

  private dailyActiveDay = "";
  private dailyActiveUsers = new Set<string>();

  constructor(options: SessionRegistryOptions) {
    this.clock = options.clock;
    this.metrics = options.metrics;
    this.logger = options.logger;
    this.maxConnections = options.maxConnections;
    this.outboundQueueCapacity = options.outboundQueueCapacity;
    this.dailyActiveCapacity = options.dailyActiveCapacity ?? DEFAULT_DAILY_ACTIVE_CAPACITY;
  }

  get size(): number {
    return this.sessions.size;
  }

  get isAtCapacity(): boolean {
    return this.sessions.size >= this.maxConnections;
  }

  /**
   * Register a freshly upgraded connection. Returns `null` when the pod is at
   * `WS_MAX_CONNECTIONS_PER_POD`; the caller rejects the session with `internal_error`.
   */
  create(sender: OutboundSender): Session | null {
    if (this.sessions.size >= this.maxConnections) {
      this.logger.warn("connection rejected: pod capacity reached", {
        operation: "session_create",
        status: "capacity_exceeded",
      });
      return null;
    }

    const now = this.clock.now();
    const connectionId = asConnectionId(generateConnectionId());

    const session: Session = {
      connectionId,
      outbound: new OutboundQueue({
        capacity: this.outboundQueueCapacity,
        sender,
        onOverflow: (queue) => {
          this.logger.warn("session queue overflow, forcing reconnect", {
            operation: "session_overflow",
            status: "dropped",
            connection_id: connectionId,
          });
          this.metrics.recordReconnect("error");
          queue.closeWith(
            serializeServerFrame(reconnectFrame("error")),
            1013,
            "queue overflow",
          );
        },
        onWriteError: (error) => {
          this.logger.warn("session write failed", {
            operation: "session_write",
            status: "error",
            connection_id: connectionId,
            detail: error instanceof Error ? error.message : "unknown",
          });
        },
      }),
      subscriptions: new Map(),
      createdAt: now,
      userId: null,
      username: null,
      tokenExpiresAt: null,
      lastActivityAt: now,
      expiryNoticeSent: false,
      expiryNoticeLeadMs: 0,
      closing: false,
    };

    this.sessions.set(connectionId, session);
    this.metrics.setConnectionsActive(this.sessions.size);
    this.logger.debug("session opened", {
      operation: "session_create",
      status: "ok",
      connection_id: connectionId,
    });

    return session;
  }

  get(connectionId: string): Session | undefined {
    return this.sessions.get(connectionId);
  }

  all(): Session[] {
    return [...this.sessions.values()];
  }

  sessionsForUser(userId: UserId): Session[] {
    const connections = this.byUserId.get(userId);
    if (!connections) {
      return [];
    }
    return this.collect(connections);
  }

  sessionsForCommunity(communityId: string): Session[] {
    const connections = this.byCommunityId.get(communityId);
    if (!connections) {
      return [];
    }
    return this.collect(connections);
  }

  /** Record inbound activity for the idle-heartbeat deadline sweep. */
  touch(connectionId: string, at: number = this.clock.now()): void {
    const session = this.sessions.get(connectionId);
    if (session) {
      session.lastActivityAt = at;
    }
  }

  /** Attach verified identity claims to a session (idempotent for the same user). */
  identify(connectionId: string, identity: VerifiedSession): Session | undefined {
    const session = this.sessions.get(connectionId);
    if (!session) {
      return undefined;
    }

    if (session.userId !== null) {
      this.byUserId.get(session.userId)?.delete(connectionId);
    }

    session.userId = identity.userId;
    session.username = identity.username;
    session.tokenExpiresAt = identity.expiresAt;
    session.expiryNoticeSent = false;
    session.expiryNoticeLeadMs = computeExpiryLead(this.clock.now(), identity.expiresAt);

    let connections = this.byUserId.get(identity.userId);
    if (!connections) {
      connections = new Set();
      this.byUserId.set(identity.userId, connections);
    }
    connections.add(connectionId);

    return session;
  }

  /** Extend the session after a successful `refresh_token`. */
  refresh(connectionId: string, identity: VerifiedSession): Session | undefined {
    return this.identify(connectionId, identity);
  }

  remove(connectionId: string): Session | undefined {
    const session = this.sessions.get(connectionId);
    if (!session) {
      return undefined;
    }

    session.closing = true;

    if (session.userId !== null) {
      const connections = this.byUserId.get(session.userId);
      connections?.delete(connectionId);
      if (connections && connections.size === 0) {
        this.byUserId.delete(session.userId);
      }
    }

    for (const communityId of [...session.subscriptions.keys()]) {
      session.subscriptions.delete(communityId);
      this.detachFromCommunity(communityId, connectionId);
    }

    this.sessions.delete(connectionId);
    this.metrics.setConnectionsActive(this.sessions.size);
    return session;
  }

  subscribe(
    connectionId: string,
    communityId: string,
    visibleChannelIds: readonly string[],
  ): SessionSubscription | undefined {
    const session = this.sessions.get(connectionId);
    if (!session) {
      return undefined;
    }

    const subscription: SessionSubscription = {
      communityId: asCommunityId(communityId),
      visibleChannelIds: new Set(visibleChannelIds),
      updatedAt: this.clock.now(),
    };

    session.subscriptions.set(communityId, subscription);

    let connections = this.byCommunityId.get(communityId);
    if (!connections) {
      connections = new Set();
      this.byCommunityId.set(communityId, connections);
    }
    connections.add(connectionId);

    return subscription;
  }

  unsubscribe(connectionId: string, communityId: string): boolean {
    const session = this.sessions.get(connectionId);
    if (!session?.subscriptions.has(communityId)) {
      return false;
    }

    session.subscriptions.delete(communityId);
    this.detachFromCommunity(communityId, connectionId);
    return true;
  }

  /** Replace cached visibility after an access re-fetch. Absent subscription is a no-op. */
  updateVisibility(
    connectionId: string,
    communityId: string,
    visibleChannelIds: readonly string[],
  ): boolean {
    const session = this.sessions.get(connectionId);
    const subscription = session?.subscriptions.get(communityId);

    if (!subscription) {
      return false;
    }

    subscription.visibleChannelIds = new Set(visibleChannelIds);
    subscription.updatedAt = this.clock.now();
    return true;
  }

  /**
   * Remove a community from every local session of one user.
   * Used by `member_kicked` / `member_banned` force-unsubscribe.
   */
  forceUnsubscribeUser(userId: UserId, communityId: string): number {
    let removed = 0;

    for (const session of this.sessionsForUser(userId)) {
      if (session.subscriptions.delete(communityId)) {
        removed += 1;
        this.detachFromCommunity(communityId, session.connectionId);
      }
    }

    return removed;
  }

  /** Remove a community subscription from every local session (community_deleted). */
  removeCommunityFromAll(communityId: string): number {
    const connections = [...(this.byCommunityId.get(communityId) ?? [])];
    let removed = 0;

    for (const connectionId of connections) {
      const session = this.sessions.get(connectionId);
      if (session?.subscriptions.delete(communityId)) {
        removed += 1;
      }
      this.detachFromCommunity(communityId, connectionId);
    }

    this.byCommunityId.delete(communityId);
    return removed;
  }

  /**
   * Daily-active-user signal dedupe: true exactly once per user per UTC day per pod.
   */
  markDailyActiveUser(userId: UserId): boolean {
    const day = utcDayKey(this.clock.now());

    if (this.dailyActiveDay !== day) {
      this.dailyActiveDay = day;
      this.dailyActiveUsers = new Set();
    }

    if (this.dailyActiveUsers.has(userId)) {
      return false;
    }

    if (this.dailyActiveUsers.size >= this.dailyActiveCapacity) {
      this.logger.warn("daily active dedupe set reset at capacity", {
        operation: "daily_active_dedupe",
        status: "reset",
      });
      this.dailyActiveUsers.clear();
    }

    this.dailyActiveUsers.add(userId);
    return true;
  }

  private collect(connections: Set<string>): Session[] {
    const sessions: Session[] = [];

    for (const connectionId of connections) {
      const session = this.sessions.get(connectionId);
      if (session) {
        sessions.push(session);
      }
    }

    return sessions;
  }

  private detachFromCommunity(communityId: string, connectionId: string): void {
    const connections = this.byCommunityId.get(communityId);
    if (!connections) {
      return;
    }

    connections.delete(connectionId);
    if (connections.size === 0) {
      this.byCommunityId.delete(communityId);
    }
  }
}

function generateConnectionId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(CONNECTION_ID_BYTES));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Longest window used to warn a client that its session token is about to expire. */
export const MAX_EXPIRY_LEAD_MS = 60_000;

/**
 * Proactive-expiry lead: a quarter of the remaining token lifetime, capped at
 * {@link MAX_EXPIRY_LEAD_MS}. Deriving it from the token keeps short-lived tokens
 * from triggering an immediate reconnect prompt.
 */
export function computeExpiryLead(now: number, expiresAt: number): number {
  const remaining = Math.max(expiresAt - now, 0);
  return Math.min(MAX_EXPIRY_LEAD_MS, Math.floor(remaining / 4));
}

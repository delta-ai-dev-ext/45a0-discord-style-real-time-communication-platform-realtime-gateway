/**
 * Client frame state machine (architecture §3).
 *
 * Controllers hand validated frames here; this service owns every protocol decision
 * (identify, heartbeat, subscribe, unsubscribe, refresh) and pushes resulting frames
 * into the session's outbound queue. Sockets are never touched directly.
 */

import type { Clock } from "../lib/clock";
import type { TokenVerifier, VerifiedSession } from "../lib/jwt";
import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";
import { traceContextFromParent } from "../lib/trace";
import type { AccessClient } from "../gateways/access-client";
import {
  errorFrame,
  eventFrame,
  heartbeatAckFrame,
  readyFrame,
  serializeServerFrame,
  SUBSCRIBED_EVENT_TYPE,
  type ClientFrame,
} from "../models/frames";
import { asUserId } from "../models/ids";
import type { Session } from "./session-registry";
import type { SessionRegistry } from "./session-registry";

export interface SessionServiceOptions {
  readonly registry: SessionRegistry;
  readonly verifier: TokenVerifier;
  readonly accessClient: AccessClient;
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
  readonly clock: Clock;
}

const CLOSE_POLICY_VIOLATION = 1008;

export class SessionService {
  private readonly registry: SessionRegistry;
  private readonly verifier: TokenVerifier;
  private readonly accessClient: AccessClient;
  private readonly metrics: MetricsRegistry;
  private readonly logger: Logger;
  private readonly clock: Clock;

  constructor(options: SessionServiceOptions) {
    this.registry = options.registry;
    this.verifier = options.verifier;
    this.accessClient = options.accessClient;
    this.metrics = options.metrics;
    this.logger = options.logger;
    this.clock = options.clock;
  }

  /** Entry point used by the transport controller for every inbound frame. */
  async handleFrame(connectionId: string, frame: ClientFrame): Promise<void> {
    const session = this.registry.get(connectionId);

    if (!session) {
      return;
    }

    this.registry.touch(connectionId, this.clock.now());

    switch (frame.type) {
      case "identify":
        await this.handleIdentify(session, frame.token);
        return;
      case "heartbeat":
        this.handleHeartbeat(session);
        return;
      case "refresh_token":
        await this.handleRefresh(session, frame.token);
        return;
      case "subscribe_community":
        await this.handleSubscribe(session, frame.community_id);
        return;
      case "unsubscribe_community":
        this.handleUnsubscribe(session, frame.community_id);
        return;
    }
  }

  private async handleIdentify(session: Session, token: string): Promise<void> {
    if (session.userId !== null) {
      this.sendError(session, "already_identified", "session is already identified");
      return;
    }

    const result = await this.verifier.verify(token);

    if (!result.ok) {
      this.metrics.recordIdentification("invalid_token");
      this.logger.info("identify failed", {
        operation: "identify",
        status: result.reason,
        connection_id: session.connectionId,
      });
      this.closeWithError(
        session,
        result.reason,
        result.reason === "token_expired"
          ? "session token has expired"
          : "session token is invalid",
      );
      return;
    }

    this.registerIdentity(session, result.session);
    this.metrics.recordIdentification("ok");

    if (this.registry.markDailyActiveUser(result.session.userId)) {
      this.metrics.recordDailyActiveUserSignal();
    }

    session.outbound.push(
      serializeServerFrame(readyFrame(result.session.userId, result.session.username)),
    );

    this.logger.info("session identified", {
      operation: "identify",
      status: "ok",
      connection_id: session.connectionId,
      user_id: result.session.userId,
    });
  }

  private async handleRefresh(session: Session, token: string): Promise<void> {
    if (session.userId === null) {
      this.closeWithError(session, "not_identified", "identify before refreshing the token");
      return;
    }

    const result = await this.verifier.verify(token);

    if (!result.ok) {
      this.logger.info("token refresh rejected", {
        operation: "refresh_token",
        status: result.reason,
        connection_id: session.connectionId,
      });
      this.closeWithError(session, "invalid_token", "session token is invalid");
      return;
    }

    this.registerIdentity(session, result.session);
    this.logger.debug("session token refreshed", {
      operation: "refresh_token",
      status: "ok",
      connection_id: session.connectionId,
    });
  }

  private handleHeartbeat(session: Session): void {
    if (session.userId === null) {
      this.closeWithError(session, "not_identified", "identify before sending heartbeats");
      return;
    }

    session.outbound.push(serializeServerFrame(heartbeatAckFrame()));
  }

  private async handleSubscribe(session: Session, communityId: string): Promise<void> {
    if (session.userId === null) {
      this.closeWithError(session, "not_identified", "identify before subscribing");
      return;
    }

    const trace = traceContextFromParent(undefined);
    const decision = await this.accessClient.fetchAccess({
      communityId,
      userId: session.userId,
      traceparent: trace.traceparent,
    });

    if (!decision.ok) {
      if (decision.reason === "unavailable") {
        this.sendError(
          session,
          "internal_error",
          "community access could not be verified, retry the subscription",
        );
        return;
      }

      this.sendError(session, "not_authorized", "you are not authorized to view this community");
      this.logger.info("subscribe denied", {
        operation: "subscribe_community",
        status: decision.reason,
        connection_id: session.connectionId,
        user_id: session.userId,
        community_id: communityId,
        trace_id: trace.traceId,
      });
      return;
    }

    this.registry.subscribe(session.connectionId, communityId, decision.visibleChannelIds);

    session.outbound.push(
      serializeServerFrame(
        eventFrame(SUBSCRIBED_EVENT_TYPE, {
          community_id: communityId,
          visible_channel_ids: [...decision.visibleChannelIds],
        }),
      ),
    );

    this.logger.info("community subscribed", {
      operation: "subscribe_community",
      status: "ok",
      connection_id: session.connectionId,
      user_id: session.userId,
      community_id: communityId,
      trace_id: trace.traceId,
    });
  }

  private handleUnsubscribe(session: Session, communityId: string): void {
    if (session.userId === null) {
      this.closeWithError(session, "not_identified", "identify before unsubscribing");
      return;
    }

    if (!this.registry.unsubscribe(session.connectionId, communityId)) {
      this.sendError(session, "not_subscribed", "session is not subscribed to this community");
      return;
    }

    this.logger.debug("community unsubscribed", {
      operation: "unsubscribe_community",
      status: "ok",
      connection_id: session.connectionId,
      user_id: session.userId,
      community_id: communityId,
    });
  }

  private registerIdentity(session: Session, identity: VerifiedSession): void {
    this.registry.identify(session.connectionId, identity);
    session.userId = asUserId(identity.userId);
    session.username = identity.username;
  }

  private sendError(
    session: Session,
    code: Parameters<typeof errorFrame>[0],
    message: string,
  ): void {
    session.outbound.push(serializeServerFrame(errorFrame(code, message)));
  }

  private closeWithError(
    session: Session,
    code: Parameters<typeof errorFrame>[0],
    message: string,
  ): void {
    this.metrics.recordReconnect("error");
    this.logger.info("closing session", {
      operation: "session_close",
      status: code,
      connection_id: session.connectionId,
      user_id: session.userId ?? undefined,
    });
    session.outbound.closeWith(serializeServerFrame(errorFrame(code, message)), CLOSE_POLICY_VIOLATION, code);
  }
}

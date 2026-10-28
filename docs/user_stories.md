# User Stories — realtime_gateway

Behavioral requirements for the real-time WebSocket gateway. Derived from the global
architecture specification (`SYSTEM_DESIGN.md` §4.4, §2, §5, §6, §7), not from implementation
details. Each story is independently testable against the frame protocol contract
(documented in `docs/openapi_spec.yaml`).

Roles: **member** (an authenticated end user with an open WebSocket session),
**moderator** (a member who performs moderation actions in a community — appears here
only as the source of events the gateway relays), **operator** (platform/infrastructure owner).

---

## US-001: Connect and identify with a valid session token

**As a** member,
**I want to** open a WebSocket connection and identify myself with my session token,
**So that** the gateway knows who I am and can start delivering events addressed to me.

### Acceptance Criteria
- Given a valid, unexpired Clerk session JWT, when the client connects to the gateway and sends `identify {token}`, then the gateway verifies the token offline, registers the session as identified, and sends back a `ready` frame containing the user's identity (`user_id`, `username`).
- Given a malformed or syntactically invalid token, when the client sends `identify {token}`, then the gateway sends an `error` frame with code `invalid_token` and closes the connection.
- Given an expired token, when the client sends `identify {token}`, then the gateway sends an `error` frame with code `token_expired` and closes the connection.
- Given an already-identified session, when the client sends another `identify` frame, then the gateway sends an `error` frame with code `already_identified` and does not re-register the session.

### Out of Scope
- Issuing session tokens (owned by Clerk and the client).
- Any per-request HTTP authentication; WebSocket auth is frame-based only.

---

## US-002: Keep the connection alive with heartbeats

**As a** member,
**I want to** keep my idle connection alive with periodic heartbeat frames,
**So that** intermediaries do not drop my session while I am online but not chatting.

### Acceptance Criteria
- Given an identified session, when the client sends a `heartbeat` frame, then the gateway replies with a `heartbeat_ack` frame.
- Given an identified session that has not sent any frame (including `heartbeat`) within the server heartbeat interval, then the gateway closes the connection as dead.
- Given a session that sends frames before any `identify` frame, then the gateway sends an `error` frame with code `not_identified` and closes the connection.

### Out of Scope
- Server-to-client pings beyond protocol frames; transport-level keep-alive tuning.

---

## US-003: Refresh the session token before expiry

**As a** member,
**I want to** submit a fresh session token on the existing connection,
**So that** my long-lived session is not dropped while the underlying Clerk session rotates.

### Acceptance Criteria
- Given an identified session, when the client sends `refresh_token {token}` with a valid new JWT, then the gateway re-verifies it offline and continues delivering events on the same connection.
- Given an identified session, when the client sends `refresh_token {token}` with an invalid or expired JWT, then the gateway sends an `error` frame with code `invalid_token` and closes the connection.
- Given a session whose token reaches expiry without a successful refresh, then the gateway closes the connection with an `error` frame with code `token_expired` (preceded by a `reconnect` frame where the token is near expiry, so the client can refresh proactively).

### Out of Scope
- Token issuance and rotation (Clerk).

---

## US-004: Subscribe to a community and receive its live events

**As a** member,
**I want to** subscribe to a community I belong to,
**So that** messages and community changes in that community reach me in real time.

### Acceptance Criteria
- Given an identified session, when the client sends `subscribe_community {community_id}` for a community where the user is an authorized member, then the gateway authorizes the subscription with the community service and confirms with an `event` frame of type `subscribed` containing the channels the user may see.
- Given a subscription, when the user is not a member (or is banned), then the gateway sends an `error` frame with code `not_authorized` and does not register the subscription.
- Given an identified session, when the client sends `unsubscribe_community {community_id}` for an active subscription, then the gateway stops delivering that community's events to the session.
- Given an `unsubscribe_community` for a community the session is not subscribed to, then the gateway replies with an `error` frame with code `not_subscribed` and no other session state changes.

### Out of Scope
- Deciding *who* is a member or what channels are visible (owned by `community_service`); the gateway only enforces its answers.

---

## US-005: Receive chat messages in real time

**As a** member,
**I want to** see new messages of channels I can view appear instantly without polling,
**So that** the conversation feels live.

### Acceptance Criteria
- Given a session subscribed to community C with visibility of channel X, when a `message_created` event for channel X is published, then the gateway delivers an `event {type: message_created, data}` frame to the session.
- Given a session subscribed to community C **without** visibility of channel X, when a `message_created` event for X is published, then the session receives no frame for that message.
- Given a session subscribed to community C, when `message_edited` and `message_deleted` events for a visible channel are published, then the gateway delivers the corresponding `event` frames in order.
- Given the gateway's community-service access data is unavailable for the delivery decision, then the gateway fails closed (drops delivery for that session rather than delivering to an unauthorized session).

### Out of Scope
- Message persistence, validation, and history serving (owned by `chat_service`); the client backfills missed messages via `chat_service` history.

---

## US-006: Receive community structure changes

**As a** member,
**I want to** see channel, membership, and community changes reflected live,
**So that** my channel sidebar and member list stay current without a reload.

### Acceptance Criteria
- Given a session subscribed to community C, when `channel_created` / `channel_updated` / `channel_deleted` events for C are published, then the gateway delivers the corresponding `event` frames to the session.
- Given a session subscribed to community C, when `member_joined` / `member_left` / `member_roles_changed` / `roles_changed` / `permission_overwrites_changed` events for C are published, then the gateway delivers the corresponding `event` frames to the session.
- Given a permission- or membership-changing event for C, then the gateway re-fetches the affected user's access from the community service and updates its per-session view of visible channels.
- Given a `community_deleted` event for C, then the gateway delivers the event to subscribed sessions and removes the subscription from all sessions.

### Out of Scope
- Rendering UI; computing effective permissions (owned by `community_service`).

---

## US-007: Receive moderation notices and be unsubscribed when removed

**As a** member,
**I want to** be informed, in non-punitive wording, when a moderation action affects me,
**So that** I understand what happened and why I can no longer act in the community.

### Acceptance Criteria
- Given a moderation event (`member_timed_out`, `member_timeout_cleared`, `member_kicked`, `member_banned`, `member_unbanned`) affecting user U, then the gateway delivers an `event {type: moderation_notice, data}` frame — containing the member-facing explanation when present — to every session of user U.
- Given a `member_kicked` or `member_banned` event for user U in community C, then every session of user U subscribed to C is force-unsubscribed immediately after the notice.
- Given a `member_timed_out` event for user U in community C, then the gateway re-fetches U's access and removes visibility of channels the member can no longer see, without force-unsubscribing the community subscription.
- Given a `member_unbanned` event, then no automatic re-subscription happens; the client may subscribe again and the gateway authorizes it normally.

### Out of Scope
- Performing the moderation actions (owned by `community_service`); enforcing them in voice rooms (owned by `voice_service`).

---

## US-008: Receive friend events live

**As a** member,
**I want to** see friend request activity appear live,
**So that** I can act on friend requests without opening the friends page fresh.

### Acceptance Criteria
- Given an identified session of the recipient user, when a `friend_request_sent` event addressing that user is published, then the gateway delivers an `event {type: friend_request_sent, data}` frame to every session of the recipient.
- Given an identified session of the sender user, when `friend_request_accepted` / `friend_request_declined` / `friend_request_cancelled` events involving that user are published, then the gateway delivers the corresponding `event` frames to every session of both the sender and the recipient involved.
- Given a `friendship_removed` event for users A and B, then the gateway delivers the event to every session of A and B.

### Out of Scope
- Friend request state transitions (owned by `identity_service`); email notification (owned by `notification_service`).

---

## US-009: Recover after a lost connection

**As a** member,
**I want to** reconnect to the gateway after a network hiccup and resume where I left off,
**So that** I only miss at most a brief, recoverable window of messages.

### Acceptance Criteria
- Given a dropped connection, when the client reconnects to any gateway pod and re-identifies and re-subscribes, then the gateway accepts the new session independently of the previous one (no pod affinity).
- Given a reconnected session, when the client needs messages it missed while offline, then the gateway delivers only events published after reconnect; the client backfills the gap using `chat_service` message history with an `after=<last_seen_message_id>` cursor.
- Given a connection loss caused by the server (shutdown or error), when the gateway initiates the close, then it sends a `reconnect` frame first so the client reconnects promptly.

### Out of Scope
- Server-side buffering of missed events across reconnects; message history (owned by `chat_service`).

---

## US-010: Graceful handover during server maintenance

**As a** member,
**I want to** be moved to a healthy gateway instance when the pod I am connected to shuts down,
**So that** platform maintenance does not look like an outage.

### Acceptance Criteria
- Given a pod receiving a shutdown signal, then every connected session receives a `reconnect` frame and the connection is closed in an orderly fashion within the drain timeout.
- Given a pod is draining, then new WebSocket upgrade attempts are rejected until the process exits.
- Given the drain timeout elapses, then remaining connections are force-closed so the pod terminates cleanly.

### Out of Scope
- Load-balancer/ingress configuration for connection distribution.

---

## US-011: Health and readiness for traffic

**As an** operator,
**I want to** probe each gateway pod for liveness and readiness,
**So that** traffic and restarts are routed correctly by the platform.

### Acceptance Criteria
- Given a running pod, when `GET /healthz` is called, then it returns `200` while the process event loop is responsive, regardless of Kafka or community-service availability.
- Given a pod whose Kafka consumers have no partition assignments yet, when `GET /readyz` is called, then it returns `503`.
- Given a pod whose Kafka consumers are assigned, when `GET /readyz` is called, then it returns `200`.
- Given a draining pod, when `GET /readyz` is called, then it returns `503` immediately so no new sessions are routed to it.

### Out of Scope
- Orchestrator probe scheduling and thresholds.

---

## US-012: Observability of connection health and delivery

**As an** operator,
**I want to** observe connection counts, identification outcomes, and delivery latency,
**So that** I can verify the real-time delivery SLOs and diagnose fan-out problems.

### Acceptance Criteria
- Given sessions connect, disconnect, and identify, then `gateway_connections_active` reflects the current number of open sessions and `gateway_sessions_identified_total{result=ok|invalid_token}` accumulates per outcome.
- Given a user identifies for the first time in a UTC day on a pod, then `gateway_daily_active_users_signal_total` is incremented once for that user.
- Given an event is consumed and delivered, then `gateway_events_delivered_total{event_type}` is incremented and `gateway_fanout_latency_seconds{source_topic}` samples the time from event publish timestamp to socket write.
- Given clients reconnect, then `gateway_reconnects_total{reason}` accumulates; given consumers fall behind, then `kafka_consumer_lag{topic}` exposes the backlog; given authorization fetches occur, then `gateway_access_fetch_duration_seconds` samples their latency.
- Given any metric emission, then no `user_id`, `community_id`, `channel_id`, or request identifiers appear as metric labels.

### Out of Scope
- Downstream aggregation of daily-return metrics into product dashboards.

---

## Coverage Check
- Every story maps to the frame protocol or the HTTP probe endpoints defined in `docs/openapi_spec.yaml`.
- No story depends on implementation internals; all are verifiable from the client/observer perspective.
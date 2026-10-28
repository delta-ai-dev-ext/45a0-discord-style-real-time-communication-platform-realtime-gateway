# Architecture — realtime_gateway

Clean architectural specification for the real-time WebSocket gateway. This document is the
source of truth for service boundaries, flows, event delivery rules, failure modes, and
observability. Library, framework, and code-layout details live in `docs/engineering_guide.md`.

---

## 1. Service Responsibilities

`realtime_gateway` terminates long-lived client WebSocket connections and delivers
asynchronous platform events to authorized, subscribed sessions in real time.

**Owns / does:**
- Terminates the `GET /v1/gateway` WebSocket upgrade and owns the client frame protocol.
- Authenticates sessions by verifying Clerk session JWTs **offline** (JWKS-based).
- Authorizes community subscriptions and per-channel visibility by querying the
  `community_service` internal member-access endpoint, caching the answer per connection.
- Consumes `chat_message_events`, `community_events`, and `identity_friend_events` from
  Kafka with **pod-unique consumer groups** (broadcast semantics) and fans each event out
  only to locally connected, authorized sessions.
- Delivers moderation notices (with member-facing explanations) and force-unsubscribes
  kicked/banned members.
- Exposes `GET /healthz` (liveness) and `GET /readyz` (readiness).
- Emits business and technical metrics; propagates W3C trace context from Kafka envelopes
  into logs and outgoing authorization calls.

**Explicitly does NOT:**
- Own any persistent state — no database, no object storage, no on-disk session state.
  Connection and subscription registries are ephemeral, in-memory, per pod.
- Store, validate, or serve messages (owned by `chat_service`).
- Decide membership, roles, or effective permissions (owned by `community_service`).
- Handle email or voice (owned by `notification_service` and `voice_service`).

---

## 2. Internal Component Boundaries

| Component | Responsibility | Boundary rules |
|---|---|---|
| **Transport terminator** | Accepts the WS upgrade at `/v1/gateway`; parses/validates client frames; serializes server frames; enforces heartbeat deadlines and token expiry. | Only component that touches sockets. Emits typed frame objects upstream; consumes outbound frame objects. |
| **Session registry** | In-memory map of connection → session (user id, username, token expiry, subscribed communities, visible channel ids, last-activity time). | Pure in-memory state; volatile; cleared on pod restart. Also drives the daily-active-user signal dedupe (per user per UTC day per pod). |
| **Authenticator** | Verifies Clerk session JWTs against the provider's published public key set (offline; no network call in steady state). | Stateless; returns validated claims (`sub` = user_id, `username`) or a typed failure. |
| **Access client** | Calls `GET /internal/v1/communities/{community_id}/members/{user_id}/access` on `community_service` with the internal service-to-service bearer token. | Synchronous, short timeout, one retry, **fails closed**. Response defines `is_member`, `banned`, `visible_channel_ids[]`, `timed_out_until`. |
| **Event router** | Consumes the three Kafka topics; deduplicates by `event_id`; maps each event to target local sessions according to §4 delivery rules. | The only component aware of Kafka and event contracts. Publish-time timestamps (`occurred_at`) feed fan-out latency metrics. |
| **Fan-out dispatcher** | Writes `event` frames to selected sockets with bounded per-connection queues; drops or force-closes sessions whose queues overflow. | Non-blocking; never lets one slow client stall the dispatch loop. |
| **Lifecycle manager** | Runs `/healthz` and `/readyz`; handles `SIGTERM`/`SIGINT`; coordinates reconnect-and-drain. | Readiness fails first on shutdown; drain is bounded. |
| **Metrics & logging** | Registers and emits the metric set in §6; structured JSON logs with `trace_id`. | Cross-cutting; low-cardinality labels only. |

---

## 3. Connection & Session Lifecycle (synchronous flows)

### 3.1 Connect + identify
1. Client opens `GET /v1/gateway`; the pod upgrades the connection (unless draining → reject).
2. Client sends `identify {token}`. The authenticator verifies the JWT offline.
   - Valid → session registered (user_id from `sub`, username claim), `ready {user_id, username}` frame returned. First identify of a UTC day per user increments the daily-active signal.
   - Invalid/expired → `error {code: invalid_token|token_expired}` and connection close.
   - Already identified → `error {code: already_identified}`; session unchanged.
3. Any frame from an unidentified connection (except `identify`) → `error {code: not_identified}` + close.

### 3.2 Heartbeat & expiry
- `heartbeat` → `heartbeat_ack`.
- If no frame arrives within the heartbeat interval → session considered dead → closed
  (counts as a reconnect with reason `timeout`).
- Token expiry tracked per session. Near expiry, the gateway proactively pushes `reconnect`
  so the client can refresh; on actual expiry without a successful `refresh_token`, the
  session is closed with `error {code: token_expired}`.

### 3.3 Subscribe / unsubscribe
1. `subscribe_community {community_id}` → Access client calls the community-service member
   access endpoint for (community, user).
   - `is_member` and not banned → subscription registered with `visible_channel_ids`;
     confirmation `event {type: subscribed, data: {community_id, visible_channel_ids}}`.
   - Not a member or banned → `error {code: not_authorized}`; nothing registered.
   - Access endpoint unavailable/timeout (after one retry) → **fail closed**:
     `error {code: internal_error}`; nothing registered.
2. `unsubscribe_community {community_id}` → removes the subscription; unknown community →
   `error {code: not_subscribed}`.

### 3.4 Refresh token
`refresh_token {token}` → verify offline; success keeps the session and extends expiry;
failure → `error {code: invalid_token}` + close.

---

## 4. Asynchronous Event Flows (Kafka fan-out)

### 4.1 Consumption model — broadcast
Every gateway pod consumes all three topics with a **pod-unique consumer group** whose
starting offset is `latest`. Consequences:
- Every pod sees **every** event (Kafka group semantics become broadcast).
- Each pod delivers only to its **locally connected** sessions; cross-pod routing happens
  implicitly because every pod independently processes the same event.
- No offsets are committed for delivered sessions; at-least-once processing is tolerated
  because delivery is idempotent at the client (message ids, event ids).
- A pod that was down for a long time rejoins at `latest` and delivers only new events;
  historical catch-up is the client's job via `chat_service` history backfill.

Consumers are idempotent by `event_id` (envelope) so reprocessing never double-delivers
within a pod's dispatch window.

### 4.2 Per-topic delivery rules

| Topic (partition key) | Event | Delivered to | Delivery behavior |
|---|---|---|---|
| `chat_message_events` (`channel_id`) | `message_created`, `message_edited`, `message_deleted` | Sessions subscribed to the event's `community_id` whose cached `visible_channel_ids` contains the event's `channel_id` | `event {type, data}` with message payload; ordering within a channel preserved (partition key = channel_id) |
| `community_events` (`community_id`) | `channel_created/updated/deleted`, `member_joined/left`, `member_roles_changed`, `roles_changed`, `permission_overwrites_changed` | All sessions subscribed to that community | Deliver `event {type, data}`; then **re-fetch access** (Access client) for each locally subscribed user and update cached `visible_channel_ids` (rate-limited/deduped per user per burst) |
| `community_events` | `member_timed_out`, `member_timeout_cleared` | All sessions of the affected user | `event {type: moderation_notice, data}` including the member-facing explanation when present; re-fetch affected user's access and adjust channel visibility (timeout can *narrow* visibility; clearing can widen) |
| `community_events` | `member_kicked`, `member_banned` | All sessions of the affected user | `event {type: moderation_notice, data}` (with member-facing explanation); then **force-unsubscribe** the affected user from that community on every local session |
| `community_events` | `member_unbanned` | All sessions of the affected user | `event {type: moderation_notice, data}`; no automatic re-subscription — the client must subscribe again (authorized normally) |
| `community_events` | `community_updated`, `community_deleted` | All sessions subscribed to the community | Deliver event; on `community_deleted`, remove the subscription from all local sessions |
| `identity_friend_events` (`recipient_user_id`) | `friend_request_sent` | All local sessions of the recipient user | `event {type, data}` |
| `identity_friend_events` | `friend_request_accepted/declined/cancelled`, `friendship_removed` | All local sessions of both involved users | `event {type, data}` |

Events with no eligible local session are consumed and dropped (per-pod fan-out is a
no-op elsewhere) — this is expected in the broadcast model.

### 4.3 Ordering guarantees
- Within a Kafka partition (hence per `community_id` and per `channel_id`), events are
  consumed in order and dispatched in order.
- Access re-fetches triggered by permission-changing events are applied before the next
  message event for the same channel is dispatched whenever both are already buffered;
  the router prioritizes access updates over message delivery for the same community to
  avoid leaking to a just-revoked channel.

---

## 5. External Integrations

### 5.1 Identity provider (Clerk) — authentication only
- The gateway never calls the identity provider in the request path. Session JWTs are
  verified **offline** against the provider's published public key set (fetched/rotated
  out of band, cached in-process).
- Trust model: claims `sub` (= platform `user_id`) and `username` are trusted only after
  signature and expiry validation.
- Failure handling: invalid signature/audience/expiry → typed `invalid_token` /
  `token_expired` errors; unavailable key material at startup → readiness fails.

### 5.2 `community_service` — real-time authorization (synchronous)
- Endpoint: `GET /internal/v1/communities/{community_id}/members/{user_id}/access`
  (cluster-internal; internal bearer token).
- Used on: subscribe, and on access-changing `community_events` for locally subscribed users.
- Resilience: short timeout (~300 ms) with a single retry on transient failure;
  **fails closed** — if authorization cannot be established, the gateway never grants
  visibility or delivery. Repeated failure raises the access-fetch error rate metric and
  surfaces as `error {code: internal_error}` to the client.
- The internal bearer token is a shared secret configured per caller via your platform secret store.
- W3C `traceparent` is propagated on every call.

---

## 6. Statelessness, Failure Modes & Resilience

### 6.1 Stateless operational model
- Multiple replicas run concurrently; a client may reconnect to any pod.
- All mutable state is per-pod memory: sessions, subscriptions, visible-channel caches,
  daily-active signal dedupe. Losing a pod loses only its sessions; clients reconnect,
  re-identify, re-subscribe, and backfill via `chat_service` history.

### 6.2 Failure modes & mitigations

| Failure | Mitigation |
|---|---|
| `community_service` unavailable | Fail closed on authorization; short timeout + 1 retry; error metric; clients get `internal_error` and may retry subscribe |
| Slow/socked-buffered client | Bounded per-session outbound queue; overflow → drop that client's frames, then force-close with `reconnect` (client recovers by reconnect + backfill) |
| Kafka consumer rebalance / broker blip | Consumer retries with backoff; readiness reflects assignment state; no local state to repair |
| Consumer falls behind | `kafka_consumer_lag` gauge alarms; broadcast model tolerates lag per pod; SLO lag p95 < 1 s |
| Session token expires mid-session | Proactive `reconnect` frame near expiry; client sends `refresh_token`; expiry without refresh → close |
| Duplicate event delivery (at-least-once) | Router dedupes by `event_id` within the dispatch window; clients treat message/event ids idempotently |
| Malicious/malformed frames | Frames validated at the boundary; unknown or invalid frames → `error {code: invalid_frame}` + close; never propagate raw input |
| Pod shutdown (SIGTERM/SIGINT) | §6.3 drain procedure |
| Heartbeat-dead connections | Idle-deadline sweep closes them; reconnect counter increments with reason `timeout` |

### 6.3 Graceful shutdown (SIGTERM/SIGINT)
1. Readiness immediately fails (`/readyz` → 503); new upgrades rejected.
2. Every connected session receives a `reconnect` frame.
3. Kafka consumption pauses; the dispatch queue is flushed (bounded).
4. Connections close in an orderly fashion; the drain is bounded by the configured
   shutdown timeout, after which remaining sockets are force-closed.
5. Process exits with code 0.

---

## 7. Observability

### 7.1 Business metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `gateway_connections_active` | gauge | — | Open WebSocket sessions on this pod |
| `gateway_sessions_identified_total` | counter | `result=ok\|invalid_token` | Identification outcomes |
| `gateway_daily_active_users_signal_total` | counter | — | First-identify-per-user-per-UTC-day signal (aggregated downstream for daily-return tracking) |
| `gateway_events_delivered_total` | counter | `event_type` | Events successfully written to sockets |

### 7.2 Technical metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `gateway_fanout_latency_seconds` | histogram | `source_topic` | Event `occurred_at` → socket write |
| `gateway_reconnects_total` | counter | `reason` | Reconnect prompts/causes (`shutdown`, `timeout`, `error`, `token_expiry`) |
| `gateway_access_fetch_duration_seconds` | histogram | — | `community_service` access call latency |
| `kafka_consumer_lag` | gauge | `topic` | Consumer lag per topic |

Metric labels are strictly low-cardinality: never `user_id`, `community_id`,
`channel_id`, or request identifiers.

### 7.3 Tracing & logging
- W3C `traceparent` from the Kafka envelope (`trace_parent`) is extracted per consumed
  event and attached to logs of the fan-out decision; the access client propagates
  `traceparent` downstream on HTTP calls.
- Structured JSON logs with at least: timestamp, level, service, operation, status,
  `trace_id`, and session-scoped identifiers where useful (never secrets or token values).

### 7.4 SLOs
- End-to-end message delivery (send accepted by `chat_service` → recipient socket):
  p95 < 500 ms.
- Connection success ≥ 99.5%.
- Consumer lag p95 < 1 s.

---

## 8. Data Ownership

`realtime_gateway` owns **no database tables and no object storage**. It is intentionally
stateless; all durable domain data lives in `identity_service`, `community_service`,
`chat_service`, and `notification_service`. (See `docs/architecture.md` of those services
for their schemas.)
# realtime_gateway

Stateless real-time WebSocket gateway for the communication platform. It terminates
client WebSocket connections, authenticates sessions with Clerk session JWTs **offline**,
authorizes community subscriptions against `community_service`, and fans out Kafka events
to locally connected, authorized sessions.

- TypeScript (strict) on **Bun** only, Elysia HTTP + built-in WebSocket.
- No PostgreSQL, no object storage, no producers: **Kafka consumer only**.
- All state (sessions, subscriptions, cached visibility, daily-active dedupe) is in-memory
  and per pod, so any pod can serve any client and reconnect has no affinity.

Specifications: `docs/architecture.md`, `docs/engineering_guide.md`,
`docs/openapi_spec.yaml`, `docs/user_stories.md`, plus
`docs/decisions/ADR-001-pod-unique-consumer-groups.md` and
`docs/decisions/ADR-002-jose-jwks-auth.md`.

## Endpoints

| Route | Purpose |
|---|---|
| `GET /v1/gateway` | WebSocket upgrade. `503` while draining, `426` for a non-upgrade request. |
| `GET /healthz` | Liveness: event loop only. Never probes Kafka or `community_service`. |
| `GET /readyz` | Readiness: `200` only when all consumers have partition assignments, JWKS key material is loaded, and the pod is not draining. |

## Frame protocol

Client to server: `identify {token}`, `heartbeat`, `subscribe_community {community_id}`,
`unsubscribe_community {community_id}`, `refresh_token {token}`.

Server to client: `ready {user_id, username}`, `heartbeat_ack`, `event {event_type, data}`,
`error {code, message}`, `reconnect {reason}`.

Error codes: `invalid_token`, `token_expired`, `not_identified`, `already_identified`,
`not_authorized`, `not_subscribed`, `invalid_frame`, `internal_error`.

## Delivery rules

- `chat_message_events` — delivered only to subscribed sessions whose cached
  `visible_channel_ids` contains the message's channel (fail closed when unknown).
- `community_events` — structure events broadcast to subscribers, followed by an access
  re-fetch that rewrites cached visibility. `member_timed_out` / `member_timeout_cleared`
  emit `moderation_notice` and adjust visibility without unsubscribing;
  `member_kicked` / `member_banned` emit `moderation_notice` and force-unsubscribe;
  `member_unbanned` only notifies; `community_deleted` also drops the subscription.
- `identity_friend_events` — `friend_request_sent` goes to the recipient; every other
  friend event goes to both involved users.
- Access updates are applied before the next chat event for the same community, so a
  just-revoked channel cannot leak a message.

## Configuration

Validated at startup with `zod` (`src/config.ts`); an invalid environment logs a
structured error and exits non-zero. See `.env.example` for a local template.

| Variable | Required | Default |
|---|---|---|
| `PORT` | no | `3000` |
| `KAFKA_BROKERS` | **yes** | — |
| `KAFKA_CLIENT_ID` | no | `realtime_gateway` |
| `COMMUNITY_SERVICE_URL` | **yes** | — |
| `INTERNAL_SERVICE_TOKEN` | **yes** | — |
| `CLERK_JWKS_URL` | **yes** | — |
| `CLERK_ISSUER` | **yes** | — |
| `CLERK_AUDIENCE` | no | — |
| `POD_NAME` | no | hostname, then random id |
| `WS_HEARTBEAT_INTERVAL_MS` | no | `30000` |
| `WS_MAX_CONNECTIONS_PER_POD` | no | `5000` |
| `ACCESS_TIMEOUT_MS` | no | `300` |
| `SHUTDOWN_TIMEOUT_MS` | no | `20000` |
| `LOG_LEVEL` | no | `info` |

The Kafka consumer group is `realtime_gateway-<POD_NAME|hostname|random>` and starts at
`latest` without relying on committed offsets (ADR-001).

## Development

```bash
bun install
make dev        # bun --watch src/index.ts
make test       # bun test --coverage
make typecheck  # tsc --noEmit
make lint       # eslint src/ tests/
make run        # bun run src/index.ts
make build      # docker build -t realtime_gateway:latest .
```

`make build` requires Docker and is expected to run in CI or locally, not inside the
agent runtime.

## Layout

```
src/
├── index.ts        # wiring, startup, signal handling
├── app.ts          # Elysia app factory (dependency-injected, importable from tests)
├── config.ts       # zod-validated env config
├── routes/         # health + gateway upgrade routers
├── controllers/    # thin frame/probe handlers
├── services/       # session registry, event router, fan-out, sweeper, lifecycle, shutdown
├── gateways/       # adapters: community_service fetch client, kafkajs consumer, ws sender
├── models/         # zod schemas + types for frames, events, access, errors
└── lib/            # logger, metrics, clock, trace context, JWT verification
tests/
├── unit/           # domain behaviour with doubles
├── integration/    # real in-process WebSocket against mocked downstreams
└── mocks/          # fake socket, mock consumer, mock access client, test JWKS
```

## Lifecycle

`SIGTERM`/`SIGINT` marks the pod unready, rejects new upgrades, sends `reconnect
{reason: shutdown}` to every session, pauses Kafka, flushes bounded outbound queues
within `SHUTDOWN_TIMEOUT_MS`, force-closes whatever remains, and exits `0`.

## Metrics

`prom-client` on a per-service registry: `gateway_connections_active`,
`gateway_sessions_identified_total{result}`, `gateway_daily_active_users_signal_total`,
`gateway_events_delivered_total{event_type}`,
`gateway_fanout_latency_seconds{source_topic}`, `gateway_reconnects_total{reason}`,
`gateway_access_fetch_duration_seconds`, `kafka_consumer_lag{topic}`. Labels are
low-cardinality only — never user, community, channel or request ids.

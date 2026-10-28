# Engineering Guide — realtime_gateway

Persistent technical memory for the real-time WebSocket gateway service. This document
governs library choices, runtime lifecycle, testing, conventions, configuration, and
observability for every future change. `Pinned Decisions` at the bottom is append-only.

---

## Runtime & Toolchain

- **Language / runtime:** TypeScript (strict mode), executed exclusively on **Bun** —
  never `node`. The event-loop model suits many concurrent long-lived WebSocket
  connections, and the platform mandates Bun for all TypeScript services.
- **Package manager:** `bun` (built-in). Add dependencies with `bun add` /
  `bun add -d` without hardcoding versions unless constrained in `Pinned Decisions`;
  the package manager resolves the latest stable version.
- **Module system:** ES modules only (`"type": "module"`, `moduleResolution: "bundler"`).
- **Build/test entrypoints (`Makefile` targets):**

```makefile
.PHONY: dev test build run lint

dev:
	bun --watch src/index.ts

test:
	bun test --coverage

build:
	docker build -t realtime_gateway:latest .   # run outside the agent runtime (CI/dev)

run:
	bun run src/index.ts

lint:
	bun x eslint src/ tests/
```

---

## Libraries & External Provider SDKs

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| HTTP framework | `elysia` | language-skill | Bun-native, TypeScript-first. Serves `/healthz`, `/readyz` and the `/v1/gateway` upgrade route. |
| WebSocket | Elysia built-in WebSocket (`Elysia().ws()`) | language-skill | Use the framework's native WS support; no separate `ws` dependency. Handles the upgrade at `GET /v1/gateway`. |
| Frame & env validation | `zod` | language-skill | Validate every inbound frame at the boundary and every env var at startup. Discriminated unions model the frame protocol. |
| Kafka client | `kafkajs` | language-skill | Consumer only (no producer). Configure `clientId`, brokers from `KAFKA_BROKERS`; **pod-unique `groupId`** (`realtime_gateway-<POD_NAME or random pod id>`) so group semantics become broadcast; start from `latest` offsets. See ADR-001. |
| JWT verification (Clerk) | `jose` | tech-lead-research | Offline verification of Clerk session JWTs against Clerk's published JWKS (`createRemoteJWKSet`); validate `exp`, issuer, audience. Bun-compatible (Web Crypto). Avoids the Node-oriented `@clerk/backend` in the hot path. See ADR-002. |
| Logging | `winston` | language-skill | Structured JSON to stdout; levels error/warn/info/debug; never `console.log` in production code. |
| Metrics | `prom-client` | language-skill | Register the metric set defined in `docs/architecture.md` §7; low-cardinality labels only. |
| Testing | `bun:test` | language-skill | Only approved runner; `mock()` from `bun:test` for all doubles; coverage ≥ 80%. |
| HTTP client (internal calls) | `fetch` (Bun native) | language-skill | Calls to `community_service` internal endpoints; never add axios/node-fetch. |
| PostgreSQL driver/ORM | — (none) | system-design | Service owns no persistent state; do not add a database dependency. |
| S3-compatible storage client | — (none) | system-design | No blob storage in scope; do not add `@aws-sdk/client-s3`. |
| External provider SDKs | — (none) | system-design | No outbound third-party provider integrations for this service; Clerk appears only as offline JWT verification via `jose`. |

> Note (assumption, verify during implementation): `kafkajs` is Node-ecosystem but runs
> under Bun; if a blocking incompatibility appears in practice, escalate to the Tech Lead
> before swapping clients — do not silently replace a pinned choice.

---

## Kubernetes Lifecycle & Probes

- **Statelessness rules:** no on-disk state, no local caches that outlive a request's
  usefulness beyond the pod lifetime. Session registry, subscriptions, visible-channel
  caches and the daily-active dedupe set are in-memory per pod and are safe to lose.
  Safe with `replicas > 1` — any pod can serve any client; reconnect has no affinity.
- **`GET /healthz` (liveness):** process/event-loop responsiveness only. MUST NOT probe
  Kafka or `community_service` — a transient downstream blip must never cause a restart.
- **`GET /readyz` (readiness):** `200` only when all three Kafka consumers have partition
  assignments and the pod is not draining; `503` otherwise. Fails immediately when
  shutdown starts so Kubernetes stops routing new sessions.
- **SIGTERM / SIGINT graceful shutdown (ordered, bounded):**
  1. Mark readiness failing; reject new WebSocket upgrades.
  2. Send a `reconnect` frame to every connected session, then begin closing sockets.
  3. Pause Kafka consumption and stop committing dispatch work.
  4. Flush bounded outbound queues within `SHUTDOWN_TIMEOUT_MS` (default 20 s), then
     force-close remaining connections.
  5. Close Kafka consumer connections cleanly; exit with code `0`.
- Kubernetes should pair this with a short `terminationGracePeriodSeconds` that exceeds
  `SHUTDOWN_TIMEOUT_MS`.

---

## Testing Strategy

- **Runner:** `bun test --coverage` (Makefile target `test`). Tests live in
  `tests/unit/` and `tests/integration/`; reusable doubles in `tests/mocks/`.
- **Determinism & parallelism:** no sleeps or wall-clock waits in assertions; inject a
  clock/token-expiry provider where timing matters. Tests share no mutable state and run
  in parallel under Bun's default concurrent test execution.
- **Unit tests (majority):** frame codec (parse/validate/serialize every frame type and
  error code), session registry lifecycle (identify/expire/heartbeat/refresh),
  event router delivery rules per topic (including visibility filtering, moderation
  notice + force-unsubscribe, friend routing), fail-closed behavior when the access
  client fails, metrics emission with correct labels.
- **Integration tests (with mocks):** an Elysia app exercised over an in-process
  WebSocket client with a **mock Kafka consumer** that pushes envelope fixtures, and a
  **mock access client** (in-memory `visible_channel_ids` answers). No real network
  calls, no real Kafka, no Docker.
- **Mocking rules:** use `mock()` from `bun:test`; reset all mocks after each test.
  Kafka (`kafkajs`), the Clerk JWKS/`jose` verification boundary, and the
  `community_service` fetch calls are always replaced by doubles. JWKS keys for
  verification tests are generated test keys (sign fixture tokens locally).
- **Coverage:** ≥ 80% lines/branches; critical paths (identify, subscribe authorization,
  delivery rules, shutdown drain) must be covered.

---

## Coding Conventions & Project Structure

```
src/
├── index.ts              # entrypoint: config load, app factory, server + consumers start
├── config.ts             # zod-validated env config
├── routes/               # Elysia routers: health routes, gateway upgrade route
├── controllers/          # thin request/frame handlers; delegate to services
├── services/             # session registry, event router, fan-out dispatcher, lifecycle
├── gateways/             # outbound adapters: access client (fetch), kafka consumer factory
├── models/               # zod schemas + TS types for frames, events, access responses
└── lib/                  # logger, metrics registry, clock, jwt verifier wiring
tests/
├── unit/                 # *.test.ts next to the domain under test naming
├── integration/
└── mocks/                # kafka consumer stub, access client stub, jwks fixtures
```

- **Layering:** `routes → controllers → services → gateways`. Controllers never contain
  business rules; services never import sockets or fetch directly (adapters injected).
- **Error handling:** typed errors mapped at the transport boundary to the single error
  contract (`error {code, message}` frame for WS; `ErrorResponse` JSON for HTTP).
  Fail closed on authorization failures; never swallow consumer errors silently — log
  and count them.
- **Typing:** `strict: true`; discriminated unions for the frame protocol; no `any`;
  branded ids (`UserId`, `CommunityId`, `ChannelId`) where practical.
- **Comments** explain *why*, not *what*; no dead code, no TODOs.

---

## Configuration & Secrets Management

12-factor: every value below is an environment variable, validated at startup by the
zod schema in `src/config.ts` (fail fast with a structured error log). No `.env` in
production; `.env` allowed locally only and must be gitignored.

| Variable | Required | Description |
|---|---|---|
| `PORT` | no (default `3000`) | HTTP listen port for probes + gateway upgrade |
| `KAFKA_BROKERS` | **yes** | Comma-separated Kafka bootstrap brokers |
| `KAFKA_CLIENT_ID` | no (default `realtime_gateway`) | Kafka `clientId` |
| `COMMUNITY_SERVICE_URL` | **yes** | Base URL of `community_service` (cluster-internal) |
| `INTERNAL_SERVICE_TOKEN` | **yes** | Service-to-service bearer token for `community_service` internal endpoints (from a K8s secret) |
| `CLERK_JWKS_URL` | **yes** | Clerk JWKS endpoint for offline session-JWT verification |
| `CLERK_ISSUER` | **yes** | Expected `iss` claim of Clerk session tokens |
| `CLERK_AUDIENCE` | no | Expected `aud` claim when configured |
| `POD_NAME` | no (fallback: hostname, then random id) | Suffix for the pod-unique Kafka consumer group |
| `WS_HEARTBEAT_INTERVAL_MS` | no (default `30000`) | Idle deadline before a session is closed |
| `WS_MAX_CONNECTIONS_PER_POD` | no (default `5000`) | Backpressure guard on concurrent sessions |
| `ACCESS_TIMEOUT_MS` | no (default `300`) | community_service access call timeout |
| `SHUTDOWN_TIMEOUT_MS` | no (default `20000`) | Bounded drain window on SIGTERM/SIGINT |
| `LOG_LEVEL` | no (default `info`) | `debug\|info\|warn\|error` |

Secrets (`INTERNAL_SERVICE_TOKEN`) come from Kubernetes secrets mounted as env vars —
never committed, never logged.

---

## Observability & Logging Conventions

- **Structured JSON logging** via `winston`: every log line carries `timestamp`,
  `level`, `service: "realtime_gateway"`, `operation`, `status`, and `trace_id`.
  Never log token values, JWKS material, or connection payloads.
- **Trace propagation:** extract `trace_parent` from the Kafka event envelope and use it
  (or generate a `trace_id`) for all logs of that event's fan-out; propagate
  `traceparent` on every `fetch` call to `community_service`. Client frames carry no
  trace headers — the identify/subscribe handlers create a span-scoped `trace_id`.
- **Prometheus registration** at startup via `prom-client` (default registry): the exact
  metric names, types, and labels are defined in `docs/architecture.md` §7. Fan-out
  latency samples use the envelope `occurred_at` → socket-write delta.
- **Metric label discipline:** low-cardinality labels only; never `user_id`,
  `community_id`, `channel_id`, or request ids.

---

## Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| Runtime | Bun-only (`bun` execution, `bun:test`, ES modules) | Platform mandate for TypeScript services; Bun-native ecosystem per language skill | tech-lead | 2026-02-05 |
| Kafka client | `kafkajs` (consumer-only) | Approved messaging client in the language skill; configured with pod-unique consumer groups for broadcast fan-out | tech-lead | 2026-02-05 |
| Broadcast fan-out | Pod-unique consumer group per pod, start offset `latest`, no durable offset commits | Every pod must see every event; stateless reconnect assumes clients backfill via chat history. See ADR-001 | tech-lead | 2026-02-05 |
| Session-JWT verification | `jose` with remote JWKS (offline) | Zero-dependency, Bun/Web-Crypto-compatible offline verification; avoids Node-oriented `@clerk/backend` in the gateway hot path. See ADR-002 | tech-lead | 2026-02-05 |
| Persistence | None — no database, no S3 client | Service owns no durable state by design; do not add DB/storage dependencies | tech-lead | 2026-02-05 |
| WebSocket transport | Elysia built-in WebSocket | Framework-native; no separate `ws` package | tech-lead | 2026-02-05 |
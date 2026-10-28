# ADR-001: Broadcast fan-out via pod-unique Kafka consumer groups

## Status
Accepted

## Date
2026-02-05

## Context
The gateway is horizontally scaled across stateless replicas, and each connected client must
receive platform events (chat messages, community changes, friend events) regardless of
which pod it happens to be connected to. Kafka's consumer-group semantics deliver each
partition to exactly one member of a group — which is the opposite of what a gateway
needs: every pod must see every event so it can check its **local** session registry.

Options considered:
1. **Shared consumer group + an inter-pod routing layer** — one pod consumes and then
   forwards events to the pod holding each session. Requires an extra transport
   (or a stateful coordinator), reintroduces cross-pod state, and adds a hop that
   threatens the < 500 ms end-to-end delivery SLO.
2. **Partition-aware session routing** — the ingress/load balancer pins a session to the
   pod that owns its partition. Ties connection placement to partition count, breaks on
   rebalance, and complicates reconnect-anywhere behavior.
3. **Pod-unique consumer group per pod, starting at `latest`** — each pod is the sole
   member of its own group, so Kafka delivers every event to every pod.

## Decision
Every gateway pod creates a consumer group named `realtime_gateway-<pod id>` (pod-unique
suffix from `POD_NAME`/hostname/random id) and starts from the **latest** offset. Each pod
delivers events only to its locally connected, authorized sessions. No offset commits are
relied on for correctness; delivery is at-least-once and clients treat message/event ids
idempotently.

## Consequences
- Simple, stateless fan-out: no coordinator, no cross-pod messaging.
- Reconnect-anywhere works: a client re-identifies on any pod and backfills missed
  messages from `chat_service` history (`after=<last_seen_message_id>`).
- Every pod pays the cost of consuming every event — acceptable at MVP scale; the design
  explicitly notes an evolution path to partition-aware routing if event volume grows.
- A pod that was down for a while rejoins at `latest` and will not replay history —
  this is intentional; history recovery is the client's job via chat history.
- At-least-once delivery: the router dedupes by `event_id` within its dispatch window;
  clients remain idempotent.

## Alternatives rejected
- Shared group + routing layer: extra stateful hop, latency and complexity.
- Partition-aware routing: couples LB placement to partitions; fragile on rebalance.
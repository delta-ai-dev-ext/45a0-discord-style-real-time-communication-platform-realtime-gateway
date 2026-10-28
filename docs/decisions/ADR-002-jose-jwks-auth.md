# ADR-002: Verify Clerk session JWTs with `jose` + remote JWKS

## Status
Accepted

## Date
2026-02-05

## Context
Every client session is authenticated with a short-lived Clerk session JWT, delivered in
the `identify` (and `refresh_token`) frame. The system design mandates **offline**
verification against Clerk's published public key set (JWKS) — no per-request call to
Clerk or any other service.

Candidates:
1. **`@clerk/backend`** — the official Clerk backend SDK. Full-featured (webhook
   verification, token verification, API client). Heavier, Node-oriented; the gateway
   only needs token verification and must run on Bun.
2. **`jose`** — standards-focused JWT/JWKS library built on Web Crypto, fully
   compatible with Bun. Small surface: `createRemoteJWKSet` + `jwtVerify` covers exactly
   what the gateway needs.

## Decision
Verify Clerk session JWTs with `jose`: a cached `createRemoteJWKSet` over
`CLERK_JWKS_URL`, validating signature, `exp`, `iss` (`CLERK_ISSUER`) and, when
configured, `aud` (`CLERK_AUDIENCE`). Trusted claims: `sub` (platform `user_id`) and
`username` (custom Clerk session claim).

## Consequences
- Zero Node-only dependencies in the gateway hot path; Bun-native via Web Crypto.
- Verification stays fully offline after the initial JWKS fetch (key rotation handled by
  JWKS caching); JWKS unavailability at startup fails readiness.
- Tests replace the verification boundary with locally generated key pairs and fixture
  tokens — deterministic and network-free.
- If richer Clerk functionality (e.g. webhook verification) is ever needed here, revisit
  this decision; the boundary is isolated in a single authenticator component.

## Alternatives rejected
- `@clerk/backend`: correct SDK, but heavier than needed and Node-oriented for this
  Bun-only, verification-only use case.
/**
 * `community_service` access adapter (architecture §5.2).
 *
 * One bearer-authenticated GET with a short timeout and exactly one retry on a
 * transient failure. It never throws and never guesses: any failure that prevents a
 * definitive answer is reported as `unavailable`, which callers treat as FAIL CLOSED
 * (no delivery, no subscription, `error {code: internal_error}` to the client).
 */

import type { Clock } from "../lib/clock";
import type { Logger } from "../lib/logger";
import type { MetricsRegistry } from "../lib/metrics";
import { memberAccessSchema, type AccessDecision, type MemberAccessQuery } from "../models/access";

export interface AccessClient {
  fetchAccess(query: MemberAccessQuery): Promise<AccessDecision>;
}

export type FetchLike = (
  input: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    signal?: AbortSignal;
  },
) => Promise<Response>;

export interface HttpAccessClientOptions {
  readonly baseUrl: string;
  readonly token: string;
  readonly timeoutMs: number;
  readonly clock: Clock;
  readonly metrics: MetricsRegistry;
  readonly logger: Logger;
  readonly fetchImpl?: FetchLike;
  /** Attempts per call: the initial call plus one retry. */
  readonly attempts?: number;
}

type AttemptOutcome =
  | { readonly kind: "decided"; readonly decision: AccessDecision }
  | { readonly kind: "transient"; readonly detail: string };

const DEFAULT_ATTEMPTS = 2;

export class HttpAccessClient implements AccessClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly timeoutMs: number;
  private readonly clock: Clock;
  private readonly metrics: MetricsRegistry;
  private readonly logger: Logger;
  private readonly fetchImpl: FetchLike;
  private readonly attempts: number;

  constructor(options: HttpAccessClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token;
    this.timeoutMs = options.timeoutMs;
    this.clock = options.clock;
    this.metrics = options.metrics;
    this.logger = options.logger;
    this.fetchImpl = options.fetchImpl ?? (fetch as unknown as FetchLike);
    this.attempts = Math.max(options.attempts ?? DEFAULT_ATTEMPTS, 1);
  }

  async fetchAccess(query: MemberAccessQuery): Promise<AccessDecision> {
    let detail = "access check did not run";

    for (let attempt = 1; attempt <= this.attempts; attempt += 1) {
      const outcome = await this.attempt(query, attempt);

      if (outcome.kind === "decided") {
        return outcome.decision;
      }

      detail = outcome.detail;
    }

    this.logger.warn("access check failed, failing closed", {
      operation: "access_fetch",
      status: "unavailable",
      community_id: query.communityId,
      user_id: query.userId,
      detail,
    });

    return { ok: false, reason: "unavailable", detail };
  }

  private async attempt(query: MemberAccessQuery, attempt: number): Promise<AttemptOutcome> {
    const url =
      `${this.baseUrl}/internal/v1/communities/${encodeURIComponent(query.communityId)}` +
      `/members/${encodeURIComponent(query.userId)}/access`;

    const startedAt = this.clock.now();

    try {
      const response = await this.fetchImpl(url, {
        method: "GET",
        headers: {
          authorization: `Bearer ${this.token}`,
          accept: "application/json",
          traceparent: query.traceparent,
        },
        signal: AbortSignal.timeout(this.timeoutMs),
      });

      this.metrics.recordAccessFetchDuration((this.clock.now() - startedAt) / 1_000);

      if (response.status >= 500 || response.status === 429) {
        return {
          kind: "transient",
          detail: `community_service responded ${response.status} (attempt ${attempt})`,
        };
      }

      if (response.status === 401 || response.status === 403) {
        // Our service token was rejected: authorization cannot be established.
        return {
          kind: "transient",
          detail: `community_service rejected the internal token (${response.status})`,
        };
      }

      if (response.status === 404) {
        return { kind: "decided", decision: { ok: false, reason: "not_member" } };
      }

      if (!response.ok) {
        return {
          kind: "transient",
          detail: `community_service responded ${response.status} (attempt ${attempt})`,
        };
      }

      const body: unknown = await response.json();
      const parsed = memberAccessSchema.safeParse(body);

      if (!parsed.success) {
        return {
          kind: "transient",
          detail: `community_service returned an unparseable access payload (attempt ${attempt})`,
        };
      }

      const access = parsed.data;

      if (access.banned) {
        return { kind: "decided", decision: { ok: false, reason: "banned" } };
      }

      if (!access.is_member) {
        return { kind: "decided", decision: { ok: false, reason: "not_member" } };
      }

      return {
        kind: "decided",
        decision: {
          ok: true,
          visibleChannelIds: access.visible_channel_ids,
          timedOutUntil: access.timed_out_until ?? null,
        },
      };
    } catch (error) {
      this.metrics.recordAccessFetchDuration((this.clock.now() - startedAt) / 1_000);
      return {
        kind: "transient",
        detail:
          error instanceof Error
            ? `access request failed (${error.name}, attempt ${attempt})`
            : `access request failed (attempt ${attempt})`,
      };
    }
  }
}

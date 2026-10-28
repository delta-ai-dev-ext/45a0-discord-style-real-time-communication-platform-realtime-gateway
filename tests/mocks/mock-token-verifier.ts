import { asUserId } from "../../src/models/ids";
import type { TokenVerificationResult, TokenVerifier, VerifiedSession } from "../../src/lib/jwt";

export type ScriptedToken = VerifiedSession | "invalid_token" | "token_expired";

export interface MockTokenVerifier extends TokenVerifier {
  readonly tokens: Map<string, ScriptedToken>;
  readonly calls: string[];
  warmupResult: boolean;
}

const DEFAULT_SESSION_TTL_MS = 5 * 60 * 1_000;

/**
 * Stand-in for the jose verification boundary: token strings map to scripted
 * outcomes, so protocol tests never sign real JWTs.
 */
export function createMockTokenVerifier(
  tokens: Record<string, ScriptedToken> = {},
  options: { warmupResult?: boolean } = {},
): MockTokenVerifier {
  const scripted = new Map(Object.entries(tokens));
  const calls: string[] = [];

  return {
    tokens: scripted,
    calls,
    warmupResult: options.warmupResult ?? true,
    async verify(token: string): Promise<TokenVerificationResult> {
      calls.push(token);
      const outcome = scripted.get(token);

      if (!outcome || outcome === "invalid_token") {
        return { ok: false, reason: "invalid_token" };
      }

      if (outcome === "token_expired") {
        return { ok: false, reason: "token_expired" };
      }

      return { ok: true, session: outcome };
    },
    async warmup() {
      return this.warmupResult;
    },
  };
}

export function sessionFor(
  userId: string,
  options: { username?: string; expiresAt?: number; now?: number } = {},
): VerifiedSession {
  const now = options.now ?? Date.now();

  return {
    userId: asUserId(userId),
    username: options.username ?? `${userId}-name`,
    expiresAt: options.expiresAt ?? now + DEFAULT_SESSION_TTL_MS,
  };
}

/**
 * Offline Clerk session-JWT verification (ADR-002).
 *
 * Verification is fully offline after the initial JWKS fetch: `jose` caches the
 * remote key set and refreshes it on unknown `kid`. Claims trusted here are `sub`
 * (platform user id) and `username`; everything else is only checked, never trusted.
 */

import {
  createRemoteJWKSet,
  jwtVerify,
  errors as joseErrors,
  type JWTVerifyGetKey,
  type JWTPayload,
} from "jose";
import { asUserId, type UserId } from "../models/ids";
import type { Logger } from "./logger";

export interface VerifiedSession {
  readonly userId: UserId;
  readonly username: string;
  /** Token expiry, milliseconds since the Unix epoch. */
  readonly expiresAt: number;
}

export type TokenVerificationResult =
  | { readonly ok: true; readonly session: VerifiedSession }
  | { readonly ok: false; readonly reason: "invalid_token" | "token_expired" };

export interface TokenVerifier {
  verify(token: string): Promise<TokenVerificationResult>;
  /** Fail readiness when key material cannot be fetched (architecture §5.1). */
  warmup?(): Promise<boolean>;
}

export interface JoseTokenVerifierOptions {
  readonly jwksUrl: string;
  readonly issuer: string;
  readonly audience?: string;
  readonly logger: Logger;
  /** Test seam: replace the remote key set with a locally generated one. */
  readonly keySet?: JWTVerifyGetKey;
}

const USERNAME_CLAIM = "username";

export class JoseTokenVerifier implements TokenVerifier {
  private readonly keySet: JWTVerifyGetKey;
  private readonly issuer: string;
  private readonly audience?: string;
  private readonly logger: Logger;

  constructor(options: JoseTokenVerifierOptions) {
    this.keySet =
      options.keySet ??
      createRemoteJWKSet(new URL(options.jwksUrl), {
        timeoutDuration: 5_000,
        cooldownDuration: 30_000,
        cacheMaxAge: 600_000,
      });
    this.issuer = options.issuer;
    this.audience = options.audience;
    this.logger = options.logger;
  }

  async verify(token: string): Promise<TokenVerificationResult> {
    let payload: JWTPayload;

    try {
      const verified = await jwtVerify(token, this.keySet, {
        issuer: this.issuer,
        ...(this.audience ? { audience: this.audience } : {}),
        algorithms: ["RS256", "ES256", "EdDSA"],
      });
      payload = verified.payload;
    } catch (error) {
      const reason = isExpiryError(error) ? "token_expired" : "invalid_token";
      this.logger.debug("session token rejected", {
        operation: "authenticate",
        status: reason,
        detail: error instanceof Error ? error.name : "unknown",
      });
      return { ok: false, reason };
    }

    const subject = payload.sub;
    const username = payload[USERNAME_CLAIM];
    const expiresAtSeconds = payload.exp;

    if (typeof subject !== "string" || subject.length === 0) {
      return { ok: false, reason: "invalid_token" };
    }

    if (typeof username !== "string") {
      return { ok: false, reason: "invalid_token" };
    }

    if (typeof expiresAtSeconds !== "number") {
      return { ok: false, reason: "invalid_token" };
    }

    return {
      ok: true,
      session: {
        userId: asUserId(subject),
        username,
        expiresAt: expiresAtSeconds * 1_000,
      },
    };
  }

  /**
   * Prime the key set so startup can detect unavailable JWKS material.
   * A "no matching key" error still proves the document was fetched successfully.
   */
  async warmup(): Promise<boolean> {
    try {
      await this.keySet({ alg: "RS256", kid: "__readiness_probe__" }, undefined as never);
      return true;
    } catch (error) {
      if (error instanceof joseErrors.JWKSNoMatchingKey) {
        return true;
      }
      this.logger.error("clerk jwks unavailable", {
        operation: "jwks_warmup",
        status: "error",
        detail: error instanceof Error ? error.name : "unknown",
      });
      return false;
    }
  }
}

function isExpiryError(error: unknown): boolean {
  return error instanceof joseErrors.JWTExpired || (error instanceof Error && error.name === "JWTExpired");
}

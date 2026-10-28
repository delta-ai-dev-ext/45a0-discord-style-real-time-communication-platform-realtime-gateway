import { describe, expect, it } from "bun:test";
import { asUserId } from "../../src/models/ids";
import { SignJWT, exportJWK, generateKeyPair, createLocalJWKSet } from "jose";
import { JoseTokenVerifier } from "../../src/lib/jwt";
import { silentLogger } from "../../src/lib/logger";
import { TEST_ISSUER, createTestSigner } from "../mocks/test-jwks";

describe("jose session-token verification", () => {
  it("trusts sub as user_id and the username claim", async () => {
    const signer = await createTestSigner();
    const token = await signer.sign({ sub: "user_123", username: "alice" });
    const result = await signer.verifier().verify(token);

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.session.userId).toBe(asUserId("user_123"));
      expect(result.session.username).toBe("alice");
      expect(result.session.expiresAt).toBeGreaterThan(Date.now());
    }
  });

  it("maps an expired token to token_expired", async () => {
    const signer = await createTestSigner();
    const token = await signer.sign({ sub: "user_123" }, { expiresIn: -10 });
    const result = await signer.verifier().verify(token);

    expect(result).toEqual({ ok: false, reason: "token_expired" });
  });

  it("rejects a wrong issuer, missing claims and malformed tokens", async () => {
    const signer = await createTestSigner();
    const otherIssuer = await signer.sign({ sub: "user_123" }, { issuer: "https://attacker.test" });
    const missingUsername = await signer.sign({ sub: "user_123", username: undefined });
    const missingSubject = await signer.sign({ username: "alice" });

    expect(await signer.verifier().verify(otherIssuer)).toEqual({
      ok: false,
      reason: "invalid_token",
    });
    expect((await signer.verifier().verify(missingUsername)).ok).toBe(false);
    expect((await signer.verifier().verify(missingSubject)).ok).toBe(false);
    expect((await signer.verifier().verify("not-a-jwt")).ok).toBe(false);
  });

  it("enforces the audience claim when configured", async () => {
    const signer = await createTestSigner();
    const { privateKey } = await generateKeyPair("RS256");
    void privateKey;

    const token = await signer.sign({ sub: "user_123", aud: "other-service" });
    const verifier = signer.verifier({ audience: "realtime_gateway" });

    expect(await verifier.verify(token)).toEqual({ ok: false, reason: "invalid_token" });
  });

  it("warmup reports success when keys are reachable and failure otherwise", async () => {
    const signer = await createTestSigner();

    expect(await signer.verifier().warmup()).toBe(true);

    const { publicKey } = await generateKeyPair("RS256");
    const publicJwk = await exportJWK(publicKey);
    const unreachable = new JoseTokenVerifier({
      jwksUrl: "https://clerk.test/.well-known/jwks.json",
      issuer: TEST_ISSUER,
      logger: silentLogger,
      keySet: () => {
        throw new Error("jwks unreachable");
      },
    });

    expect(publicJwk).toBeDefined();
    expect(await unreachable.warmup()).toBe(false);
    expect(await unreachable.verify(await signer.sign({ sub: "u" }))).toEqual({
      ok: false,
      reason: "invalid_token",
    });
    expect(createLocalJWKSet({ keys: [publicJwk] })).toBeDefined();
  });

  it("rejects tokens signed by an unknown key", async () => {
    const trusted = await createTestSigner();
    const rogue = await createTestSigner();
    const rogueToken = await rogue.sign({ sub: "user_123", username: "mallory" });
    const token = await new SignJWT({ sub: "user_123", username: "alice" })
      .setProtectedHeader({ alg: "RS256" })
      .setIssuer(TEST_ISSUER)
      .sign(await (await generateKeyPair("RS256")).privateKey);

    expect((await trusted.verifier().verify(rogueToken)).ok).toBe(false);
    expect((await trusted.verifier().verify(token)).ok).toBe(false);
  });
});

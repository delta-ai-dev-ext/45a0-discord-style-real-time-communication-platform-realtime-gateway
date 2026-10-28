import { SignJWT, createLocalJWKSet, exportJWK, generateKeyPair, type JWTPayload } from "jose";
import { JoseTokenVerifier } from "../../src/lib/jwt";
import { silentLogger } from "../../src/lib/logger";

export interface TestSigner {
  sign(claims?: JWTPayload, options?: { expiresIn?: string | number; issuer?: string }): Promise<string>;
  verifier(options?: { issuer?: string; audience?: string }): JoseTokenVerifier;
  keySet: ReturnType<typeof createLocalJWKSet>;
  kid: string;
}

const TEST_ISSUER = "https://clerk.test";
const TEST_KID = "test-key-1";

/**
 * Locally generated key pair + `createLocalJWKSet`, so JWKS-based verification runs
 * offline and deterministically (docs/engineering_guide.md testing strategy).
 */
export async function createTestSigner(issuer: string = TEST_ISSUER): Promise<TestSigner> {
  const { privateKey, publicKey } = await generateKeyPair("RS256");
  const publicJwk = await exportJWK(publicKey);
  const keySet = createLocalJWKSet({
    keys: [{ ...publicJwk, alg: "RS256", kid: TEST_KID, use: "sig" }],
  });

  return {
    keySet,
    kid: TEST_KID,
    async sign(claims: JWTPayload = {}, options: { expiresIn?: string | number; issuer?: string } = {}) {
      const payload: JWTPayload = { username: "alice", ...claims };
      const token = new SignJWT(payload)
        .setProtectedHeader({ alg: "RS256", kid: TEST_KID })
        .setIssuedAt();

      if (payload.sub) {
        token.setSubject(payload.sub);
      }

      token.setIssuer(options.issuer ?? issuer);
      token.setExpirationTime(options.expiresIn ?? "5m");

      return token.sign(privateKey);
    },
    verifier(options: { issuer?: string; audience?: string } = {}) {
      return new JoseTokenVerifier({
        jwksUrl: "https://unused.test/.well-known/jwks.json",
        issuer: options.issuer ?? issuer,
        ...(options.audience ? { audience: options.audience } : {}),
        logger: silentLogger,
        keySet,
      });
    },
  };
}

export { TEST_ISSUER };

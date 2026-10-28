import { describe, expect, it } from "bun:test";
import { buildKafkaGroupId, parseConfig, resolvePodName } from "../../src/config";
import { ConfigError } from "../../src/models/errors";

const baseEnv = {
  KAFKA_BROKERS: "broker-a:9092, broker-b:9092",
  COMMUNITY_SERVICE_URL: "http://community_service:3000/",
  INTERNAL_SERVICE_TOKEN: "token",
  CLERK_JWKS_URL: "https://clerk.test/.well-known/jwks.json",
  CLERK_ISSUER: "https://clerk.test",
  POD_NAME: "pod-7",
};

describe("config", () => {
  it("applies every documented default", () => {
    const config = parseConfig({ ...baseEnv, POD_NAME: undefined, HOSTNAME: "host-9" });

    expect(config.port).toBe(3000);
    expect(config.kafkaClientId).toBe("realtime_gateway");
    expect(config.wsHeartbeatIntervalMs).toBe(30_000);
    expect(config.wsMaxConnectionsPerPod).toBe(5_000);
    expect(config.accessTimeoutMs).toBe(300);
    expect(config.shutdownTimeoutMs).toBe(20_000);
    expect(config.logLevel).toBe("info");
    expect(config.clerkAudience).toBeUndefined();
    expect(config.podName).toBe("host-9");
  });

  it("parses brokers, trims the service URL and derives the pod-unique group", () => {
    const config = parseConfig({ ...baseEnv, CLERK_AUDIENCE: "realtime_gateway", LOG_LEVEL: "debug" });

    expect(config.kafkaBrokers).toEqual(["broker-a:9092", "broker-b:9092"]);
    expect(config.communityServiceUrl).toBe("http://community_service:3000");
    expect(config.kafkaGroupId).toBe("realtime_gateway-pod-7");
    expect(config.clerkAudience).toBe("realtime_gateway");
    expect(config.logLevel).toBe("debug");
  });

  it("fails fast with every missing required variable", () => {
    let caught: unknown;

    try {
      parseConfig({});
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ConfigError);
    const issues = (caught as ConfigError).issues.join(" ");
    expect(issues).toContain("KAFKA_BROKERS");
    expect(issues).toContain("COMMUNITY_SERVICE_URL");
    expect(issues).toContain("INTERNAL_SERVICE_TOKEN");
    expect(issues).toContain("CLERK_JWKS_URL");
    expect(issues).toContain("CLERK_ISSUER");
  });

  it("rejects non-url service endpoints and unknown log levels", () => {
    expect(() => parseConfig({ ...baseEnv, COMMUNITY_SERVICE_URL: "not-a-url" })).toThrow(ConfigError);
    expect(() => parseConfig({ ...baseEnv, LOG_LEVEL: "trace" })).toThrow(ConfigError);
  });

  it("resolves pod identity from env then hostname then a random fallback", () => {
    expect(resolvePodName({ POD_NAME: "explicit", HOSTNAME: "host" })).toBe("explicit");
    expect(resolvePodName({ HOSTNAME: "host" })).toBe("host");
    expect(resolvePodName({}, () => "random-id")).toBe("random-id");
  });

  it("composes the group id required by ADR-001", () => {
    expect(buildKafkaGroupId("abc")).toBe("realtime_gateway-abc");
  });
});

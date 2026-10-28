/**
 * Runtime configuration, validated with zod at startup (12-factor, fail fast).
 * Mirrors the configuration table in docs/engineering_guide.md.
 */

import { z } from "zod";
import { ConfigError } from "./models/errors";

const LOG_LEVELS = ["debug", "info", "warn", "error"] as const;

const positiveInt = (fallback: number) =>
  z.coerce.number().int().positive().default(fallback);

const envSchema = z.object({
  PORT: positiveInt(3000),
  KAFKA_BROKERS: z.string().min(1),
  KAFKA_CLIENT_ID: z.string().min(1).default("realtime_gateway"),
  COMMUNITY_SERVICE_URL: z.string().url(),
  INTERNAL_SERVICE_TOKEN: z.string().min(1),
  CLERK_JWKS_URL: z.string().url(),
  CLERK_ISSUER: z.string().min(1),
  CLERK_AUDIENCE: z.string().min(1).optional(),
  POD_NAME: z.string().min(1).optional(),
  WS_HEARTBEAT_INTERVAL_MS: positiveInt(30_000),
  WS_MAX_CONNECTIONS_PER_POD: positiveInt(5_000),
  ACCESS_TIMEOUT_MS: positiveInt(300),
  SHUTDOWN_TIMEOUT_MS: positiveInt(20_000),
  LOG_LEVEL: z.enum(LOG_LEVELS).default("info"),
});

export type LogLevel = (typeof LOG_LEVELS)[number];

export interface AppConfig {
  readonly port: number;
  readonly kafkaBrokers: readonly string[];
  readonly kafkaClientId: string;
  readonly kafkaGroupId: string;
  readonly communityServiceUrl: string;
  readonly internalServiceToken: string;
  readonly clerkJwksUrl: string;
  readonly clerkIssuer: string;
  readonly clerkAudience?: string;
  readonly podName: string;
  readonly wsHeartbeatIntervalMs: number;
  readonly wsMaxConnectionsPerPod: number;
  readonly accessTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
  readonly logLevel: LogLevel;
}

const SERVICE_GROUP_PREFIX = "realtime_gateway";

const RANDOM_ID_BYTES = 6;

/** Random pod id fallback; uses Bun's Web Crypto API per project conventions. */
export function randomPodId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(RANDOM_ID_BYTES));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/**
 * Pod identity drives the pod-unique Kafka group (ADR-001): `POD_NAME`, then the
 * hostname Kubernetes injects, then a random id.
 */
export function resolvePodName(
  env: Record<string, string | undefined>,
  fallbackId: () => string = randomPodId,
): string {
  return env.POD_NAME ?? env.HOSTNAME ?? fallbackId();
}

export function buildKafkaGroupId(podName: string): string {
  return `${SERVICE_GROUP_PREFIX}-${podName}`;
}

export function parseConfig(env: Record<string, string | undefined>): AppConfig {
  const result = envSchema.safeParse(env);

  if (!result.success) {
    const issues = result.error.issues.map(
      (issue) => `${issue.path.join(".") || "<root>"}: ${issue.message}`,
    );
    throw new ConfigError(issues);
  }

  const parsed = result.data;
  const podName = resolvePodName(env);

  return {
    port: parsed.PORT,
    kafkaBrokers: parsed.KAFKA_BROKERS.split(",")
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    kafkaClientId: parsed.KAFKA_CLIENT_ID,
    kafkaGroupId: buildKafkaGroupId(podName),
    communityServiceUrl: parsed.COMMUNITY_SERVICE_URL.replace(/\/+$/, ""),
    internalServiceToken: parsed.INTERNAL_SERVICE_TOKEN,
    clerkJwksUrl: parsed.CLERK_JWKS_URL,
    clerkIssuer: parsed.CLERK_ISSUER,
    clerkAudience: parsed.CLERK_AUDIENCE,
    podName,
    wsHeartbeatIntervalMs: parsed.WS_HEARTBEAT_INTERVAL_MS,
    wsMaxConnectionsPerPod: parsed.WS_MAX_CONNECTIONS_PER_POD,
    accessTimeoutMs: parsed.ACCESS_TIMEOUT_MS,
    shutdownTimeoutMs: parsed.SHUTDOWN_TIMEOUT_MS,
    logLevel: parsed.LOG_LEVEL,
  };
}

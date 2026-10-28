/**
 * Service entrypoint: config load, dependency wiring, server + consumers start,
 * signal handling. Everything here is side-effectful; `src/app.ts` holds the
 * importable factory so tests never start a server.
 */

import { parseConfig, type AppConfig } from "./config";
import { createApp } from "./app";
import { systemClock } from "./lib/clock";
import { createLogger, type Logger } from "./lib/logger";
import { MetricsRegistry } from "./lib/metrics";
import { JoseTokenVerifier } from "./lib/jwt";
import { ConfigError } from "./models/errors";
import { CONSUMED_TOPICS } from "./models/events";
import { createFrameController } from "./controllers/frames-controller";
import { createHealthController } from "./controllers/health-controller";
import { HttpAccessClient } from "./gateways/access-client";
import {
  createKafkaClient,
  createKafkajsConsumer,
  type KafkaConsumerRunner,
  type KafkaLike,
} from "./gateways/kafka-consumer";
import { createKafkaLagSource } from "./gateways/kafka-lag-source";
import { EventRouter } from "./services/event-router";
import { FanoutDispatcher } from "./services/fanout-dispatcher";
import { LagReporter } from "./services/lag-reporter";
import { LifecycleState } from "./services/lifecycle";
import { SessionRegistry } from "./services/session-registry";
import { SessionService } from "./services/session-service";
import { SessionSweeper, startSweeper } from "./services/session-sweeper";
import { ShutdownService } from "./services/shutdown-service";

const OUTBOUND_QUEUE_CAPACITY = 64;
const LAG_SAMPLE_INTERVAL_MS = 5_000;
const JWKS_WARMUP_RETRY_MS = 5_000;

async function main(): Promise<void> {
  let config: AppConfig;

  try {
    config = parseConfig(process.env);
  } catch (error) {
    const issues = error instanceof ConfigError ? error.issues : [String(error)];
    createLogger("error").error("invalid configuration", {
      operation: "startup",
      status: "failed",
      detail: issues.join("; "),
    });
    process.exit(1);
  }

  const logger = createLogger(config.logLevel);
  const metrics = new MetricsRegistry();
  const lifecycle = new LifecycleState();

  const registry = new SessionRegistry({
    clock: systemClock,
    metrics,
    logger,
    maxConnections: config.wsMaxConnectionsPerPod,
    outboundQueueCapacity: OUTBOUND_QUEUE_CAPACITY,
  });

  const verifier = new JoseTokenVerifier({
    jwksUrl: config.clerkJwksUrl,
    issuer: config.clerkIssuer,
    ...(config.clerkAudience ? { audience: config.clerkAudience } : {}),
    logger,
  });

  const accessClient = new HttpAccessClient({
    baseUrl: config.communityServiceUrl,
    token: config.internalServiceToken,
    timeoutMs: config.accessTimeoutMs,
    clock: systemClock,
    metrics,
    logger,
  });

  const sessionService = new SessionService({
    registry,
    verifier,
    accessClient,
    metrics,
    logger,
    clock: systemClock,
  });

  const dispatcher = new FanoutDispatcher({ metrics, logger, clock: systemClock });

  const router = new EventRouter({
    registry,
    dispatcher,
    accessClient,
    logger,
    clock: systemClock,
  });

  const kafka = createKafkaClient({
    brokers: config.kafkaBrokers,
    clientId: config.kafkaClientId,
  });

  const consumers = createKafkajsConsumer({
    brokers: config.kafkaBrokers,
    clientId: config.kafkaClientId,
    groupId: config.kafkaGroupId,
    topics: CONSUMED_TOPICS,
    logger,
    kafka,
    onAssignment: (topic, assigned) => lifecycle.markConsumerAssigned(topic, assigned),
  });

  const shutdownService = new ShutdownService({
    lifecycle,
    registry,
    consumers,
    metrics,
    logger,
    clock: systemClock,
    timeoutMs: config.shutdownTimeoutMs,
  });

  const app = createApp({
    lifecycle,
    healthController: createHealthController({ lifecycle, clock: systemClock }),
    frameController: createFrameController({ registry, sessionService, metrics, logger }),
  });

  app.listen(config.port);
  logger.info("http server listening", {
    operation: "startup",
    status: "ok",
    detail: `port=${config.port} group=${config.kafkaGroupId}`,
  });

  const sweeper = new SessionSweeper({
    registry,
    metrics,
    logger,
    clock: systemClock,
    idleTimeoutMs: config.wsHeartbeatIntervalMs,
  });
  const sweeperHandle = startSweeper(sweeper, config.wsHeartbeatIntervalMs);
  void startConsumers({ consumers, router, lifecycle, logger });
  void warmupAuthKeys({ verifier, lifecycle, logger });
  const lagTimer = startLagReporter({ kafka, consumers, metrics, logger });

  registerSignalHandlers({
    shutdownService,
    logger,
    onStopped: () => {
      sweeperHandle.stop();
      clearInterval(lagTimer);
    },
  });
}

async function startConsumers(dependencies: {
  consumers: KafkaConsumerRunner;
  router: EventRouter;
  lifecycle: LifecycleState;
  logger: Logger;
}): Promise<void> {
  const { consumers, router, lifecycle, logger } = dependencies;

  try {
    await consumers.start((topic, raw) => router.handle(topic, raw));
  } catch (error) {
    // Kafka unavailability must not crash the pod: `/readyz` stays 503 until the
    // consumer reconnects and receives assignments.
    logger.error("kafka consumer failed to start", {
      operation: "startup",
      status: "error",
      detail: error instanceof Error ? error.message : "unknown",
    });

    for (const topic of CONSUMED_TOPICS) {
      lifecycle.markConsumerAssigned(topic, false);
    }
  }
}

async function warmupAuthKeys(dependencies: {
  verifier: JoseTokenVerifier;
  lifecycle: LifecycleState;
  logger: Logger;
}): Promise<void> {
  const { verifier, lifecycle, logger } = dependencies;

  while (!lifecycle.isDraining) {
    if (await verifier.warmup()) {
      lifecycle.markAuthKeysReady(true);
      logger.info("clerk jwks loaded", { operation: "startup", status: "ok" });
      return;
    }

    logger.warn("clerk jwks unavailable, readiness withheld", {
      operation: "startup",
      status: "retrying",
    });
    await new Promise((resolve) => setTimeout(resolve, JWKS_WARMUP_RETRY_MS));
  }
}

function startLagReporter(dependencies: {
  kafka: KafkaLike;
  consumers: KafkaConsumerRunner;
  metrics: MetricsRegistry;
  logger: Logger;
}): Timer {
  const { kafka, consumers, metrics, logger } = dependencies;
  let reporter: LagReporter | undefined;

  return setInterval(() => {
    void (async () => {
      try {
        reporter ??= new LagReporter({
          metrics,
          logger,
          topics: CONSUMED_TOPICS,
          source: await createKafkaLagSource({ kafka, positions: consumers }),
        });

        await reporter.sampleAll();
      } catch (error) {
        logger.debug("consumer lag sample unavailable", {
          operation: "lag_report",
          status: "error",
          detail: error instanceof Error ? error.message : "unknown",
        });
      }
    })();
  }, LAG_SAMPLE_INTERVAL_MS);
}

function registerSignalHandlers(dependencies: {
  shutdownService: ShutdownService;
  logger: Logger;
  onStopped: () => void;
}): void {
  const { shutdownService, logger, onStopped } = dependencies;

  const handle = (signal: string): void => {
    logger.info("received shutdown signal", { operation: "shutdown", status: signal });

    void shutdownService
      .shutdown()
      .catch((error: unknown) => {
        logger.error("shutdown failed", {
          operation: "shutdown",
          status: "error",
          detail: error instanceof Error ? error.message : "unknown",
        });
      })
      .finally(() => {
        onStopped();
        process.exit(0);
      });
  };

  process.on("SIGTERM", () => handle("SIGTERM"));
  process.on("SIGINT", () => handle("SIGINT"));
}

if (import.meta.main) {
  await main();
}

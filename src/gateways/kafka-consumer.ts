/**
 * Kafka consumer adapter (ADR-001: pod-unique group, `latest` offset, broadcast).
 *
 * One consumer subscribes to all three topics under a pod-unique group id, so every pod
 * sees every event. Offsets are never committed (`autoCommit: false`) — correctness does
 * not depend on them, and a long-down pod intentionally rejoins at `latest`.
 *
 * `kafkajs` is reached through `KafkaLike`/`ConsumerLike` so unit tests can drive
 * assignment changes and rebalances without a broker.
 */

import {
  Kafka,
  logLevel as kafkaLogLevel,
  type Admin,
  type Consumer,
  type KafkaConfig,
  type LogEntry,
} from "kafkajs";
import type { Logger } from "../lib/logger";
import type { ConsumerController } from "../services/lifecycle";

export interface EachMessageLike {
  readonly topic: string;
  readonly partition: number;
  readonly message: { readonly offset: string; readonly value: Buffer | null };
}

export interface ConsumerLike {
  connect(): Promise<void>;
  subscribe(subscription: { topics: string[]; fromBeginning: boolean }): Promise<void>;
  run(config: {
    autoCommit?: boolean;
    eachMessage: (payload: EachMessageLike) => Promise<void>;
  }): Promise<void>;
  pause(topics: Array<{ topic: string }>): void;
  resume(topics: Array<{ topic: string }>): void;
  disconnect(): Promise<void>;
  on(eventName: string, listener: (event: unknown) => void): void;
  readonly events: { groupJoin: string; crash: string; disconnect: string };
}

export interface AdminLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  fetchTopicOffsets(
    topic: string,
  ): Promise<Array<{ partition: number; offset: string; high: string; low: string }>>;
}

export interface KafkaLike {
  consumer(config: { groupId: string }): ConsumerLike;
  admin(): AdminLike;
}

export interface KafkaConsumerOptions {
  readonly brokers: readonly string[];
  readonly clientId: string;
  readonly groupId: string;
  readonly topics: readonly string[];
  readonly logger: Logger;
  /** Reports per-topic assignment state; drives `/readyz`. */
  readonly onAssignment?: (topic: string, assigned: boolean) => void;
  /** Test seam: replace the real client. */
  readonly kafka?: KafkaLike;
}

export interface KafkaConsumerRunner extends ConsumerController {
  /** Last consumed offset per partition, used by the lag reporter. */
  lastOffsets(topic: string): Record<string, number> | undefined;
}

export interface GroupJoinPayloadLike {
  readonly memberAssignment: Record<string, number[] | undefined>;
}

export class KafkajsConsumerRunner implements KafkaConsumerRunner {
  private readonly consumer: ConsumerLike;
  private readonly topics: readonly string[];
  private readonly logger: Logger;
  private readonly onAssignment?: (topic: string, assigned: boolean) => void;
  private readonly assigned = new Map<string, boolean>();
  private readonly offsets = new Map<string, Record<string, number>>();
  private paused = false;

  constructor(options: KafkaConsumerOptions & { readonly kafka: KafkaLike }) {
    const { kafka } = options;

    this.consumer = kafka.consumer({ groupId: options.groupId });
    this.topics = options.topics;
    this.logger = options.logger;
    this.onAssignment = options.onAssignment;

    for (const topic of this.topics) {
      this.assigned.set(topic, false);
    }

    this.consumer.on(this.consumer.events.groupJoin, (event) => this.handleGroupJoin(event));
    this.consumer.on(this.consumer.events.disconnect, () => this.clearAssignments("disconnect"));
    this.consumer.on(this.consumer.events.crash, () => this.clearAssignments("crash"));
  }

  async start(handler: (topic: string, raw: string) => Promise<void>): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [...this.topics], fromBeginning: false });

    this.logger.info("kafka consumer started", {
      operation: "kafka_consumer",
      status: "started",
      detail: this.topics.join(","),
    });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        if (this.paused || payload.message.value === null) {
          return;
        }

        this.recordOffset(payload.topic, payload.partition, payload.message.offset);

        try {
          await handler(payload.topic, payload.message.value.toString());
        } catch (error) {
          this.logger.error("kafka handler failed", {
            operation: "kafka_consumer",
            status: "error",
            topic: payload.topic,
            detail: error instanceof Error ? error.message : "unknown",
          });
        }
      },
    });
  }

  async pause(): Promise<void> {
    this.paused = true;

    try {
      this.consumer.pause(this.topics.map((topic) => ({ topic })));
    } catch (error) {
      this.logger.warn("kafka pause failed", {
        operation: "kafka_consumer",
        status: "error",
        detail: error instanceof Error ? error.message : "unknown",
      });
    }

    this.clearAssignments("pause");
  }

  async stop(): Promise<void> {
    try {
      await this.consumer.disconnect();
    } catch (error) {
      this.logger.warn("kafka disconnect failed", {
        operation: "kafka_consumer",
        status: "error",
        detail: error instanceof Error ? error.message : "unknown",
      });
    }

    this.clearAssignments("stop");
  }

  hasAssignments(): boolean {
    for (const topic of this.topics) {
      if (!this.assigned.get(topic)) {
        return false;
      }
    }

    return this.topics.length > 0;
  }

  lastOffsets(topic: string): Record<string, number> | undefined {
    return this.offsets.get(topic);
  }

  /** Exposed for tests and for rebalances delivered out of band. */
  handleGroupJoin(event: unknown): void {
    const payload = (event as { payload?: GroupJoinPayloadLike } | undefined)?.payload;
    const assignment = payload?.memberAssignment;

    if (!assignment) {
      return;
    }

    for (const topic of this.topics) {
      const partitions = assignment[topic];
      this.setAssigned(topic, Array.isArray(partitions) && partitions.length > 0);
    }
  }

  private recordOffset(topic: string, partition: number, offset: string): void {
    const parsed = Number(offset);

    if (Number.isNaN(parsed)) {
      return;
    }

    let byPartition = this.offsets.get(topic);

    if (!byPartition) {
      byPartition = {};
      this.offsets.set(topic, byPartition);
    }

    byPartition[String(partition)] = parsed;
  }

  private clearAssignments(reason: string): void {
    for (const topic of this.topics) {
      this.setAssigned(topic, false);
    }

    this.logger.debug("kafka assignments cleared", {
      operation: "kafka_consumer",
      status: reason,
    });
  }

  private setAssigned(topic: string, assigned: boolean): void {
    if (this.assigned.get(topic) === assigned) {
      return;
    }

    this.assigned.set(topic, assigned);
    this.onAssignment?.(topic, assigned);
  }
}

function adaptConsumer(consumer: Consumer): ConsumerLike {
  return {
    connect: () => consumer.connect(),
    subscribe: async (subscription) => {
      await consumer.subscribe({ topics: subscription.topics, fromBeginning: subscription.fromBeginning });
    },
    run: async (config) => {
      await consumer.run({
        autoCommit: config.autoCommit ?? false,
        eachMessage: async (payload) => {
          await config.eachMessage({
            topic: payload.topic,
            partition: payload.partition,
            message: {
              offset: payload.message.offset,
              value: payload.message.value,
            },
          });
        },
      });
    },
    pause: (topics) => {
      consumer.pause(topics.map(({ topic }) => ({ topic })));
    },
    resume: (topics) => {
      consumer.resume(topics.map(({ topic }) => ({ topic })));
    },
    disconnect: () => consumer.disconnect(),
    on: (eventName, listener) => {
      consumer.on(eventName as never, listener as never);
    },
    events: {
      groupJoin: consumer.events.GROUP_JOIN,
      crash: consumer.events.CRASH,
      disconnect: consumer.events.DISCONNECT,
    },
  };
}

function adaptAdmin(admin: Admin): AdminLike {
  return {
    connect: () => admin.connect(),
    disconnect: () => admin.disconnect(),
    fetchTopicOffsets: async (topic) => admin.fetchTopicOffsets(topic),
  };
}

/**
 * Route kafkajs logs through the service logger so every line stays structured JSON
 * with the platform fields; console output from the client is never mixed in.
 */
function createKafkaLogCreator(logger: Logger) {
  return (level: kafkaLogLevel) => (entry: LogEntry) => {
    const context = {
      operation: "kafka_client",
      status: entry.label,
      topic: entry.log.message,
      detail: entry.namespace,
    };

    switch (level) {
      case kafkaLogLevel.ERROR:
        logger.error("kafka client error", context);
        return;
      case kafkaLogLevel.WARN:
        logger.warn("kafka client warning", context);
        return;
      case kafkaLogLevel.INFO:
        logger.info("kafka client info", context);
        return;
      default:
        logger.debug("kafka client debug", context);
    }
  };
}

/** Kafka client with the pod-unique group id from ADR-001. */
export function createKafkaClient(options: {
  readonly brokers: readonly string[];
  readonly clientId: string;
  readonly logger?: Logger;
}): KafkaLike {
  const config: KafkaConfig = {
    clientId: options.clientId,
    brokers: [...options.brokers],
    logLevel: kafkaLogLevel.WARN,
    retry: { retries: 8, initialRetryTime: 300, maxRetryTime: 30_000 },
    ...(options.logger ? { logCreator: createKafkaLogCreator(options.logger) } : {}),
  };

  const kafka = new Kafka(config);

  return {
    consumer: (consumerConfig) => {
      const consumer = kafka.consumer({ groupId: consumerConfig.groupId });
      return adaptConsumer(consumer);
    },
    admin: () => adaptAdmin(kafka.admin()),
  };
}

export function createKafkajsConsumer(options: KafkaConsumerOptions): KafkajsConsumerRunner {
  const kafka =
    options.kafka ??
    createKafkaClient({
      brokers: options.brokers,
      clientId: options.clientId,
      logger: options.logger,
    });

  return new KafkajsConsumerRunner({ ...options, kafka });
}

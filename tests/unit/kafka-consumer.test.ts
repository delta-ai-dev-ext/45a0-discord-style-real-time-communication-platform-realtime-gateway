import { describe, expect, it } from "bun:test";
import { silentLogger } from "../../src/lib/logger";
import {
  KafkajsConsumerRunner,
  createKafkaClient,
  createKafkajsConsumer,
  type AdminLike,
  type ConsumerLike,
  type EachMessageLike,
  type KafkaLike,
} from "../../src/gateways/kafka-consumer";
import { createKafkaLagSource } from "../../src/gateways/kafka-lag-source";
import { CONSUMED_TOPICS } from "../../src/models/events";

interface FakeConsumer extends ConsumerLike {
  readonly listeners: Map<string, (event: unknown) => void>;
  readonly subscribed: Array<{ topics: string[]; fromBeginning: boolean }>;
  readonly runConfigs: Array<{ autoCommit?: boolean }>;
  paused: boolean;
  disconnected: boolean;
  connected: boolean;
  emitMessage(payload: EachMessageLike): Promise<void>;
}

function createFakeConsumer(): FakeConsumer {
  const listeners = new Map<string, (event: unknown) => void>();
  const subscribed: Array<{ topics: string[]; fromBeginning: boolean }> = [];
  const runConfigs: Array<{ autoCommit?: boolean }> = [];
  let eachMessage: ((payload: EachMessageLike) => Promise<void>) | undefined;
  let paused = false;

  const consumer: FakeConsumer = {
    listeners,
    subscribed,
    runConfigs,
    get paused() {
      return paused;
    },
    set paused(next: boolean) {
      paused = next;
    },
    connected: false,
    disconnected: false,
    async connect() {
      consumer.connected = true;
    },
    async subscribe(subscription) {
      subscribed.push(subscription);
    },
    async run(config) {
      runConfigs.push({ autoCommit: config.autoCommit });
      eachMessage = config.eachMessage;
    },
    pause() {
      paused = true;
    },
    resume() {
      paused = false;
    },
    async disconnect() {
      consumer.disconnected = true;
    },
    on(eventName, listener) {
      listeners.set(eventName, listener);
    },
    events: {
      groupJoin: "consumer.group_join",
      crash: "consumer.crash",
      disconnect: "consumer.disconnect",
    },
    async emitMessage(payload) {
      if (!eachMessage) {
        throw new Error("consumer has not been run");
      }
      await eachMessage(payload);
    },
  };

  return consumer;
}

function setup() {
  const consumer = createFakeConsumer();
  const admin: AdminLike = {
    async connect() {},
    async disconnect() {},
    async fetchTopicOffsets(topic) {
      return [
        { partition: 0, offset: "0", high: topic === "community_events" ? "8" : "5", low: "0" },
      ];
    },
  };
  const kafka: KafkaLike = {
    consumer: () => consumer,
    admin: () => admin,
  };
  const assignments: Array<{ topic: string; assigned: boolean }> = [];

  const runner = new KafkajsConsumerRunner({
    brokers: ["broker:9092"],
    clientId: "realtime_gateway",
    groupId: "realtime_gateway-pod-1",
    topics: CONSUMED_TOPICS,
    logger: silentLogger,
    kafka,
    onAssignment: (topic, assigned) => assignments.push({ topic, assigned }),
  });

  return { consumer, kafka, runner, assignments };
}

const message = (offset: string, value: string | null): EachMessageLike => ({
  topic: "chat_message_events",
  partition: 0,
  message: { offset, value: value === null ? null : Buffer.from(value) },
});

describe("kafkajs consumer runner", () => {
  it("subscribes to every topic at latest and never commits offsets", async () => {
    const { consumer, runner } = setup();
    const handled: string[] = [];

    await runner.start(async (topic, raw) => {
      handled.push(`${topic}:${raw}`);
    });

    expect(consumer.connected).toBe(true);
    expect(consumer.subscribed).toEqual([{ topics: [...CONSUMED_TOPICS], fromBeginning: false }]);
    expect(consumer.runConfigs[0]?.autoCommit).toBe(false);

    await consumer.emitMessage(message("7", '{"event_id":"e1"}'));
    expect(handled).toEqual(['chat_message_events:{"event_id":"e1"}']);
  });

  it("tracks readiness from group-join assignments", async () => {
    const { consumer, runner, assignments } = setup();
    await runner.start(async () => {});

    expect(runner.hasAssignments()).toBe(false);

    consumer.listeners.get("consumer.group_join")?.({
      payload: { memberAssignment: { chat_message_events: [0] } },
    });
    expect(runner.hasAssignments()).toBe(false);
    expect(assignments).toEqual([{ topic: "chat_message_events", assigned: true }]);

    consumer.listeners.get("consumer.group_join")?.({
      payload: {
        memberAssignment: {
          chat_message_events: [0],
          community_events: [0],
          identity_friend_events: [0],
        },
      },
    });
    expect(runner.hasAssignments()).toBe(true);

    consumer.listeners.get("consumer.group_join")?.({});
    expect(runner.hasAssignments()).toBe(true);
  });

  it("clears assignments on crash and disconnect", async () => {
    const { consumer, runner } = setup();
    await runner.start(async () => {});

    consumer.listeners.get("consumer.group_join")?.({
      payload: {
        memberAssignment: {
          chat_message_events: [0],
          community_events: [0],
          identity_friend_events: [0],
        },
      },
    });
    expect(runner.hasAssignments()).toBe(true);

    consumer.listeners.get("consumer.crash")?.({});
    expect(runner.hasAssignments()).toBe(false);

    consumer.listeners.get("consumer.disconnect")?.({});
    expect(runner.hasAssignments()).toBe(false);
  });

  it("tracks per-partition offsets and skips tombstones and non-numeric offsets", async () => {
    const { consumer, runner } = setup();
    await runner.start(async () => {});

    await consumer.emitMessage(message("42", "payload"));
    await consumer.emitMessage(message("43", null));
    await consumer.emitMessage(message("not-a-number", "payload"));

    expect(runner.lastOffsets("chat_message_events")).toEqual({ "0": 42 });
    expect(runner.lastOffsets("community_events")).toBeUndefined();
  });

  it("stops consuming after pause and reports lost assignments", async () => {
    const { consumer, runner, assignments } = setup();
    const handled: string[] = [];
    await runner.start(async (_topic, raw) => {
      handled.push(raw);
    });

    consumer.listeners.get("consumer.group_join")?.({
      payload: {
        memberAssignment: {
          chat_message_events: [0],
          community_events: [0],
          identity_friend_events: [0],
        },
      },
    });
    expect(runner.hasAssignments()).toBe(true);

    await runner.pause();
    expect(consumer.paused).toBe(true);
    expect(runner.hasAssignments()).toBe(false);
    // Every topic is reported unassigned once, so `/readyz` flips to 503.
    expect(assignments.filter((entry) => !entry.assigned)).toHaveLength(CONSUMED_TOPICS.length);

    await consumer.emitMessage(message("1", "ignored"));
    expect(handled).toHaveLength(0);
  });

  it("survives handler errors and disconnect failures", async () => {
    const { consumer, runner } = setup();
    await runner.start(async () => {
      throw new Error("handler exploded");
    });

    await consumer.emitMessage(message("1", "payload"));
    expect(runner.lastOffsets("chat_message_events")).toEqual({ "0": 1 });

    consumer.disconnect = async () => {
      throw new Error("already gone");
    };
    consumer.pause = () => {
      throw new Error("pause not allowed");
    };

    await runner.pause();
    await runner.stop();
    expect(runner.hasAssignments()).toBe(false);
  });

  it("builds a runner through the factory without a real broker", () => {
    const runner = createKafkajsConsumer({
      brokers: ["broker:9092"],
      clientId: "realtime_gateway",
      groupId: "realtime_gateway-pod-2",
      topics: CONSUMED_TOPICS,
      logger: silentLogger,
      kafka: { consumer: () => createFakeConsumer(), admin: () => ({}) as AdminLike },
    });

    expect(runner.hasAssignments()).toBe(false);
    expect(runner.lastOffsets("chat_message_events")).toBeUndefined();
  });
});

describe("kafka client factory", () => {
  it("builds adapters that expose the consumer and admin surface", () => {
    const client = createKafkaClient({
      brokers: ["broker:9092"],
      clientId: "realtime_gateway",
      logger: silentLogger,
    });

    const consumer = client.consumer({ groupId: "realtime_gateway-pod-3" });
    expect(consumer.events.groupJoin).toBe("consumer.group_join");
    expect(typeof consumer.connect).toBe("function");
    expect(typeof consumer.run).toBe("function");
    expect(typeof consumer.pause).toBe("function");
    expect(typeof consumer.resume).toBe("function");
    expect(typeof consumer.on).toBe("function");

    const admin = client.admin();
    expect(typeof admin.fetchTopicOffsets).toBe("function");
  });
});

describe("kafka lag source", () => {
  it("reads high watermarks from the admin client and positions from the consumer", async () => {
    const admin: AdminLike = {
      async connect() {},
      async disconnect() {},
      async fetchTopicOffsets() {
        return [
          { partition: 0, offset: "10", high: "12", low: "0" },
          { partition: 1, offset: "bad", high: "nope", low: "0" },
        ];
      },
    };
    const source = await createKafkaLagSource({
      kafka: { consumer: () => createFakeConsumer(), admin: () => admin },
      positions: { lastOffsets: () => ({ "0": 7 }) },
    });

    expect(await source.highWatermarks("chat_message_events")).toEqual({ "0": 12 });
    expect(source.lastOffsets("chat_message_events")).toEqual({ "0": 7 });
  });
});

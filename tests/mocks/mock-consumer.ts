import { mock } from "bun:test";
import type { ConsumerController } from "../../src/services/lifecycle";

export interface MockConsumer extends ConsumerController {
  readonly startCalls: number;
  readonly paused: boolean;
  readonly stopped: boolean;
  /** Route a record through the handler registered by `start()`. */
  emit(topic: string, payload: unknown): Promise<void>;
  /** Simulate a completed rebalance assignment for `/readyz` tests. */
  setAssignments(assigned: boolean): void;
}

/** Kafka consumer double: pushes envelope fixtures without a broker. */
export function createMockConsumer(options: { assigned?: boolean } = {}): MockConsumer {
  let handler: ((topic: string, raw: string) => Promise<void>) | undefined;
  let assigned = options.assigned ?? true;
  let startCalls = 0;
  let paused = false;
  let stopped = false;

  const start = mock(
    async (nextHandler: (topic: string, raw: string) => Promise<void>): Promise<void> => {
      handler = nextHandler;
      startCalls += 1;
    },
  );

  return {
    get startCalls() {
      return startCalls;
    },
    get paused() {
      return paused;
    },
    get stopped() {
      return stopped;
    },
    start,
    async pause() {
      paused = true;
    },
    async stop() {
      stopped = true;
      assigned = false;
    },
    hasAssignments() {
      return assigned;
    },
    async emit(topic: string, payload: unknown) {
      if (!handler) {
        throw new Error("consumer has not been started");
      }
      await handler(topic, JSON.stringify(payload));
    },
    setAssignments(next: boolean) {
      assigned = next;
    },
  };
}

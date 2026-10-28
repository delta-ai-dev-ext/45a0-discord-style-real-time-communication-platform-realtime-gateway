/**
 * Readiness/draining state (architecture §6.3, docs/user_stories.md US-011).
 *
 * `/healthz` never consults this; `/readyz` does. A pod is ready only when every
 * consumed topic has an assignment, the Clerk key material was fetched, and shutdown
 * has not started. Draining flips the flag first so new sessions stop being routed here.
 */

import { CONSUMED_TOPICS } from "../models/events";

export interface ReadinessSnapshot {
  readonly ready: boolean;
  readonly draining: boolean;
  readonly authKeysReady: boolean;
  readonly consumers: Readonly<Record<string, boolean>>;
}

export class LifecycleState {
  private draining = false;
  private authKeysReady = false;
  private readonly assignments = new Map<string, boolean>();

  constructor(topics: readonly string[] = CONSUMED_TOPICS) {
    for (const topic of topics) {
      this.assignments.set(topic, false);
    }
  }

  get isDraining(): boolean {
    return this.draining;
  }

  markConsumerAssigned(topic: string, assigned: boolean): void {
    this.assignments.set(topic, assigned);
  }

  markAuthKeysReady(ready: boolean): void {
    this.authKeysReady = ready;
  }

  beginDraining(): void {
    this.draining = true;
  }

  isReady(): boolean {
    if (this.draining || !this.authKeysReady) {
      return false;
    }

    for (const assigned of this.assignments.values()) {
      if (!assigned) {
        return false;
      }
    }

    return this.assignments.size > 0;
  }

  snapshot(): ReadinessSnapshot {
    return {
      ready: this.isReady(),
      draining: this.draining,
      authKeysReady: this.authKeysReady,
      consumers: Object.fromEntries(this.assignments),
    };
  }
}

export interface ConsumerController {
  start(handler: (topic: string, raw: string) => Promise<void>): Promise<void>;
  pause(): Promise<void>;
  stop(): Promise<void>;
  hasAssignments(): boolean;
}

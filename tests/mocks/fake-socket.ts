import type { ServerFrame } from "../../src/models/frames";
import type { BunSocketLike } from "../../src/gateways/ws-sender";

export interface FakeSocket extends BunSocketLike {
  readonly sent: string[];
  readonly closed: { code?: number; reason?: string } | null;
  readonly terminated: boolean;
  /** Parsed frames received by the client. */
  frames(): ServerFrame[];
  /** Remaining scripted send statuses, for backpressure simulation. */
  statuses(): number[];
}

/**
 * Bun-like socket double. `statuses` scripts the value returned by `send()`:
 * a positive byte count (normal), `-1` (backpressure) or `0` (dropped).
 */
export function createFakeSocket(options: { statuses?: number[] } = {}): FakeSocket {
  const sent: string[] = [];
  const statuses = [...(options.statuses ?? [])];
  let closed: { code?: number; reason?: string } | null = null;
  let terminated = false;

  return {
    sent,
    get closed() {
      return closed;
    },
    get terminated() {
      return terminated;
    },
    send(payload: string) {
      sent.push(payload);
      const scripted = statuses.shift();
      return scripted ?? payload.length;
    },
    close(code?: number, reason?: string) {
      closed = { code, reason };
    },
    terminate() {
      terminated = true;
    },
    frames() {
      return sent.map((payload) => JSON.parse(payload) as ServerFrame);
    },
    statuses() {
      return [...statuses];
    },
  };
}

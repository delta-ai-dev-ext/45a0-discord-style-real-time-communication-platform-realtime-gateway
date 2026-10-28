/**
 * Transport adapter: Elysia/Bun server WebSocket → `OutboundSender`.
 *
 * Services only ever see the `OutboundSender` interface, so they stay socket-free and
 * unit-testable. Bun's `send()` reports `0` for a dropped frame and `-1` for
 * backpressure, which the outbound queue interprets as overflow vs. buffer-and-wait.
 */

import type { OutboundSender } from "../services/outbound-queue";

export interface BunSocketLike {
  send(data: string): number;
  close(code?: number, reason?: string): void;
  terminate(): void;
}

export function createWsSender(socket: BunSocketLike): OutboundSender {
  return {
    send: (payload: string) => socket.send(payload),
    close: (code?: number, reason?: string) => socket.close(code, reason),
    terminate: () => socket.terminate(),
  };
}

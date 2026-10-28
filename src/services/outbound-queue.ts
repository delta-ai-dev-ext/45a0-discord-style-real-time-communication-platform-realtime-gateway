/**
 * Bounded per-session outbound queue (architecture §6.2 "slow/socked-buffered client").
 *
 * A slow client must never stall the fan-out loop: frames are buffered only while the
 * transport reports backpressure, the buffer is bounded, and an overflow drops the
 * session (the caller sends a `reconnect` frame and closes) instead of growing memory.
 *
 * The queue talks to an `OutboundSender` — implemented over Elysia/Bun sockets in
 * `src/gateways/ws-sender.ts` and over a fake in tests — so no service touches a socket.
 */

export interface OutboundSender {
  /** Returns bytes written, `0` when dropped, `-1` when the socket is backpressured. */
  send(payload: string): number | void;
  close(code?: number, reason?: string): void;
  terminate(): void;
}

export interface OutboundQueueOptions {
  readonly capacity: number;
  readonly sender: OutboundSender;
  /** Invoked once when the queue overflows (buffer full or transport drop). */
  readonly onOverflow?: (queue: OutboundQueue) => void;
  readonly onWriteError?: (error: unknown) => void;
}

interface QueuedFrame {
  readonly payload: string;
  readonly onWritten?: () => void;
}

/** Transport status values returned by Bun's `ServerWebSocket.send`. */
const SEND_DROPPED = 0;
const SEND_BACKPRESSURE = -1;

export class OutboundQueue {
  private readonly capacity: number;
  private readonly sender: OutboundSender;
  private readonly onOverflow?: (queue: OutboundQueue) => void;
  private readonly onWriteError?: (error: unknown) => void;

  private pending: QueuedFrame[] = [];
  private backpressured = false;
  private closed = false;
  private overflowed = false;
  private drainWaiters: Array<() => void> = [];

  constructor(options: OutboundQueueOptions) {
    this.capacity = Math.max(options.capacity, 1);
    this.sender = options.sender;
    this.onOverflow = options.onOverflow;
    this.onWriteError = options.onWriteError;
  }

  get size(): number {
    return this.pending.length;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get isBackpressured(): boolean {
    return this.backpressured;
  }

  /**
   * Enqueue one serialized frame. Returns `false` when the frame was dropped
   * because the session overflowed (the caller must not retry).
   */
  push(payload: string, onWritten?: () => void): boolean {
    if (this.closed || this.overflowed) {
      return false;
    }

    if (this.backpressured) {
      return this.buffer({ payload, onWritten });
    }

    const status = this.trySend({ payload, onWritten });

    if (status === SEND_BACKPRESSURE) {
      // The transport accepted and queued the frame; only later frames are buffered here.
      this.backpressured = true;
      return true;
    }

    if (status === SEND_DROPPED) {
      this.triggerOverflow();
      return false;
    }

    return true;
  }

  /**
   * Flush buffered frames; stops at the first backpressure signal.
   * `force` is used when the session is closing: pending frames are written out
   * even if the transport previously reported backpressure, because a closing
   * socket has no later drain callback.
   */
  flush(force = false): void {
    if (this.closed) {
      return;
    }

    if (force) {
      this.backpressured = false;
    }

    while (this.pending.length > 0 && !this.backpressured) {
      const frame = this.pending[0];

      if (!frame) {
        break;
      }

      this.pending.shift();
      const status = this.trySend(frame);

      if (status === SEND_BACKPRESSURE) {
        this.backpressured = true;
        break;
      }

      if (status === SEND_DROPPED) {
        this.pending = [];
        this.triggerOverflow();
        return;
      }
    }

    if (this.pending.length === 0) {
      this.releaseDrainWaiters();
    }
  }

  /** Transport reported `drain`: resume writing buffered frames in order. */
  handleDrain(): void {
    this.backpressured = false;
    this.flush();
    this.releaseDrainWaiters();
  }

  /** Resolves once the queue holds no buffered frames (or the session is closed). */
  whenDrained(): Promise<void> {
    if (this.pending.length === 0 || this.closed) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.drainWaiters.push(resolve);
    });
  }

  /** Flush buffered frames, then close the socket gracefully. */
  closeNow(code?: number, reason?: string): void {
    if (this.closed) {
      return;
    }

    this.flush(true);
    this.closed = true;
    this.pending = [];
    this.releaseDrainWaiters();
    this.sender.close(code, reason);
  }

  /**
   * Drop buffered frames, write one final frame (the `reconnect` notice) and close.
   * Used for overflow and shutdown, where the pending backlog is unsalvageable.
   */
  closeWith(payload: string, code?: number, reason?: string): void {
    if (this.closed) {
      return;
    }

    this.pending = [];
    this.closed = true;
    this.releaseDrainWaiters();

    try {
      this.sender.send(payload);
    } catch (error) {
      this.onWriteError?.(error);
    }

    this.sender.close(code, reason);
  }

  /** Abrupt teardown for sessions that did not drain inside the shutdown window. */
  terminate(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.pending = [];
    this.releaseDrainWaiters();
    this.sender.terminate();
  }

  private buffer(frame: QueuedFrame): boolean {
    if (this.pending.length >= this.capacity) {
      this.triggerOverflow();
      return false;
    }

    this.pending.push(frame);
    return true;
  }

  private trySend(frame: QueuedFrame): number | void {
    try {
      const status = this.sender.send(frame.payload);
      frame.onWritten?.();
      return status;
    } catch (error) {
      this.onWriteError?.(error);
      this.pending = [];
      this.closed = true;
      this.releaseDrainWaiters();
      this.sender.terminate();
      return SEND_DROPPED;
    }
  }

  private triggerOverflow(): void {
    if (this.overflowed) {
      return;
    }

    this.overflowed = true;
    this.pending = [];
    this.releaseDrainWaiters();
    this.onOverflow?.(this);
  }

  private releaseDrainWaiters(): void {
    const waiters = this.drainWaiters;
    this.drainWaiters = [];

    for (const resolve of waiters) {
      resolve();
    }
  }
}

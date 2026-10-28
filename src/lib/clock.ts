/**
 * Injectable wall clock. Timing-sensitive logic (token expiry, heartbeat deadlines,
 * fan-out latency, access-call timeouts) reads time through this interface so tests
 * never sleep.
 */

export interface Clock {
  /** Milliseconds since the Unix epoch. */
  now(): number;
}

export const systemClock: Clock = {
  now: () => Date.now(),
};

export interface ManualClock extends Clock {
  advance(ms: number): void;
  set(epochMs: number): void;
}

export function createManualClock(startEpochMs = 1_700_000_000_000): ManualClock {
  let current = startEpochMs;
  return {
    now: () => current,
    advance: (ms) => {
      current += ms;
    },
    set: (epochMs) => {
      current = epochMs;
    },
  };
}

/** UTC calendar day (`YYYY-MM-DD`) — the bucket for the daily-active-user signal. */
export function utcDayKey(epochMs: number): string {
  return new Date(epochMs).toISOString().slice(0, 10);
}

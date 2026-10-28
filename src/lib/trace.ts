/**
 * W3C trace context helpers.
 *
 * Kafka envelopes carry `trace_parent`; it is propagated verbatim to
 * `community_service` on the `traceparent` header and its trace id is attached to
 * every log line of that event's fan-out. When an envelope has no trace context
 * (or a client frame creates a new span) a fresh trace id is generated.
 */

const TRACEPARENT_PATTERN = /^[\da-f]{2}-([\da-f]{32})-([\da-f]{16})-([\da-f]{2})$/;

export interface TraceContext {
  readonly traceId: string;
  readonly traceparent: string;
}

function hex(bytes: number): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

export function newTraceId(): string {
  return hex(16);
}

/** Build a sampled (flags `01`) traceparent with a fresh trace and span id. */
export function newTraceContext(): TraceContext {
  const traceId = newTraceId();
  const traceparent = `00-${traceId}-${hex(8)}-01`;
  return { traceId, traceparent };
}

/**
 * Accept an upstream `traceparent` when it is structurally valid, otherwise
 * generate a new one. Invalid input is never forwarded downstream.
 */
export function traceContextFromParent(parent: string | undefined): TraceContext {
  if (parent) {
    const match = TRACEPARENT_PATTERN.exec(parent);

    if (match?.[1]) {
      return { traceId: match[1], traceparent: parent };
    }
  }

  return newTraceContext();
}

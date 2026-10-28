/**
 * The single error contract shared by the WebSocket frame protocol
 * (`error {code, message}`) and the HTTP probe responses (`ErrorResponse`).
 *
 * Code values are part of the public API (docs/openapi_spec.yaml) and must not
 * be renamed without a spec change.
 */

export const GATEWAY_ERROR_CODES = [
  "invalid_token",
  "token_expired",
  "not_identified",
  "already_identified",
  "not_authorized",
  "not_subscribed",
  "invalid_frame",
  "internal_error",
] as const;

export type GatewayErrorCode = (typeof GATEWAY_ERROR_CODES)[number];

/** Reasons the gateway asks a client to reconnect; mirrors `ReconnectFrame.reason`. */
export const RECONNECT_REASONS = ["shutdown", "token_expiry", "error"] as const;

export type ReconnectReason = (typeof RECONNECT_REASONS)[number];

/**
 * Reasons recorded on `gateway_reconnects_total`. Superset of the reconnect
 * frame reasons: `timeout` is counted when an idle session is dropped, which is
 * also a reconnect cause from the client's perspective.
 */
export const RECONNECT_METRIC_REASONS = ["shutdown", "token_expiry", "error", "timeout"] as const;

export type ReconnectMetricReason = (typeof RECONNECT_METRIC_REASONS)[number];

export class ConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`invalid configuration: ${issues.join("; ")}`);
    this.name = "ConfigError";
    this.issues = issues;
  }
}

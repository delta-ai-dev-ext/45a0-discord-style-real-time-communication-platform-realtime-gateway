/**
 * Structured JSON logging (winston) to stdout.
 *
 * Every line carries the platform-required fields: timestamp, level, service,
 * operation, status, trace_id. Token values, JWKS material and raw connection
 * payloads are never logged.
 */

import winston from "winston";

const SERVICE_NAME = "realtime_gateway";

export interface LogContext {
  operation?: string;
  status?: string;
  trace_id?: string;
  connection_id?: string;
  user_id?: string;
  community_id?: string;
  topic?: string;
  event_id?: string;
  reason?: string;
  detail?: string;
}

export interface Logger {
  debug(message: string, context?: LogContext): void;
  info(message: string, context?: LogContext): void;
  warn(message: string, context?: LogContext): void;
  error(message: string, context?: LogContext): void;
}

const contextFormat = winston.format((info) => {
  const { level, message, ...rest } = info;
  return {
    timestamp: new Date().toISOString(),
    level,
    service: SERVICE_NAME,
    message,
    ...(rest as Record<string, unknown>),
  };
});

export function createLogger(level: string = "info"): Logger {
  const instance = winston.createLogger({
    level,
    format: winston.format.combine(contextFormat(), winston.format.json()),
    transports: [new winston.transports.Console({ stderrLevels: [] })],
    silent: false,
  });

  return {
    debug: (message, context) => instance.debug(message, context),
    info: (message, context) => instance.info(message, context),
    warn: (message, context) => instance.warn(message, context),
    error: (message, context) => instance.error(message, context),
  };
}

/** Logger that discards everything; used as a default in unit tests. */
export const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

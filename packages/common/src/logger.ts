export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void;
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
  child(fields: Record<string, unknown>): Logger;
}

/**
 * Minimal structured (JSON lines) logger. Every service logs with its name;
 * event handlers add correlationId/orderId/eventId so a whole saga can be
 * grepped out of the combined logs with one correlationId.
 */
export function createLogger(
  service: string,
  base: Record<string, unknown> = {},
  minLevel: LogLevel = (process.env.LOG_LEVEL as LogLevel) || 'info',
): Logger {
  const write = (level: LogLevel, msg: string, fields?: Record<string, unknown>) => {
    if (LEVEL_ORDER[level] < LEVEL_ORDER[minLevel]) return;
    const line = JSON.stringify({
      ts: new Date().toISOString(),
      level,
      service,
      msg,
      ...base,
      ...fields,
    });
    process.stdout.write(line + '\n');
  };
  return {
    debug: (m, f) => write('debug', m, f),
    info: (m, f) => write('info', m, f),
    warn: (m, f) => write('warn', m, f),
    error: (m, f) => write('error', m, f),
    child: (fields) => createLogger(service, { ...base, ...fields }, minLevel),
  };
}

/** A logger that swallows everything (handy default for tests). */
export const nullLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  child: () => nullLogger,
};

import path from 'node:path';
import { createBrokerFromEnv } from './broker/factory';
import type { MessageBroker } from './broker/types';
import { createLogger, type Logger } from './logger';
import { initTracing } from './otel';

/**
 * Shared `main()` skeleton for all five services: init tracing, connect the
 * env-selected broker, hand control to the service factory, expose a
 * cross-platform graceful shutdown (signals + "shutdown" on stdin, because
 * Windows children cannot catch signals from a parent).
 */

export interface ServiceHandle {
  stop(): Promise<void>;
}

export interface ServiceBootDeps {
  broker: MessageBroker;
  dbPath: string;
  logger: Logger;
}

export async function runService(
  name: string,
  create: (deps: ServiceBootDeps) => Promise<ServiceHandle>,
): Promise<void> {
  const logger = createLogger(name);
  const tracing = initTracing(name);
  const broker = await createBrokerFromEnv(logger);
  const dbPath = process.env.DB_PATH ?? path.join('data', `${name}.db`);

  const service = await create({ broker, dbPath, logger });
  logger.info('service started', { dbPath });
  // Machine-readable readiness marker (the demo orchestrator waits for it).
  console.log(`SERVICE_READY ${name}`);

  let shuttingDown = false;
  const shutdown = async (reason: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info('shutting down', { reason });
    try {
      await service.stop();
      await broker.close();
      await tracing.shutdown();
    } catch (err) {
      logger.error('error during shutdown', { error: (err as Error).message });
    }
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.stdin.on('data', (chunk) => {
    if (chunk.toString().includes('shutdown')) void shutdown('stdin');
  });
  process.stdin.on('error', () => undefined);
}

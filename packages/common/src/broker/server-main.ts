import { createLogger } from '../logger';
import { injectionOptionsFromEnv } from './memory';
import { BrokerTcpServer } from './tcp-server';

/**
 * Standalone entry point for the local broker server:
 *   node packages/common/dist/broker/server-main.js
 * Configured entirely via env (BROKER_PORT, BROKER_DUPLICATE_TOPICS,
 * BROKER_JITTER_MS, BROKER_ACK_TIMEOUT_MS, ...).
 */
async function main(): Promise<void> {
  const logger = createLogger('broker');
  const port = Number(process.env.BROKER_PORT ?? 47100);
  const server = new BrokerTcpServer(injectionOptionsFromEnv(), logger);
  await server.listen(port);
  // Machine-readable readiness marker for the demo orchestrator.
  console.log(`BROKER_READY ${port}`);

  let shuttingDown = false;
  const shutdown = async (reason: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info('broker shutting down', { reason });
    await server.close();
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  // Cross-platform graceful stop: parent writes "shutdown\n" to stdin
  // (Windows cannot deliver catchable signals to child processes).
  process.stdin.on('data', (chunk) => {
    if (chunk.toString().includes('shutdown')) void shutdown('stdin');
  });
  process.stdin.on('error', () => undefined);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

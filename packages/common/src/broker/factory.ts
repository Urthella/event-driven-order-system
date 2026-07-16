import type { Logger } from '../logger';
import { nullLogger } from '../logger';
import { InMemoryBroker, injectionOptionsFromEnv } from './memory';
import { RabbitMqBroker } from './rabbitmq';
import { TcpBrokerClient } from './tcp-client';
import type { MessageBroker } from './types';

/**
 * Environment-driven broker selection:
 *   RABBITMQ_URL -> RabbitMQ adapter (real infra path, docker-compose)
 *   BROKER_URL   -> local TCP broker client (default zero-infra path)
 *   neither      -> in-process broker (single-process / test usage)
 */
export async function createBrokerFromEnv(
  logger: Logger = nullLogger,
  env: NodeJS.ProcessEnv = process.env,
): Promise<MessageBroker> {
  if (env.RABBITMQ_URL) {
    logger.info('broker: RabbitMQ', { url: env.RABBITMQ_URL.replace(/\/\/.*@/, '//***@') });
    return RabbitMqBroker.connect(env.RABBITMQ_URL, logger);
  }
  if (env.BROKER_URL) {
    logger.info('broker: local TCP', { url: env.BROKER_URL });
    return TcpBrokerClient.connect(env.BROKER_URL, logger);
  }
  logger.warn('broker: in-process (no BROKER_URL/RABBITMQ_URL set); only visible within this process');
  return new InMemoryBroker(injectionOptionsFromEnv(env));
}

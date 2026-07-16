import type { Channel, ChannelModel, ConsumeMessage } from 'amqplib';
import amqp from 'amqplib';
import type { EventEnvelope } from '../events';
import type { Logger } from '../logger';
import { nullLogger } from '../logger';
import type { Delivery, DeliveryHandler, MessageBroker, Subscription } from './types';

/**
 * RabbitMQ adapter (selected via RABBITMQ_URL). Topology:
 *   - topic exchange `os.events` for all traffic
 *   - one durable queue per (consumer group, pattern), bound with the pattern;
 *     consumers on the same queue share deliveries -> consumer-group semantics
 *   - manual ack; nack(requeue=false) or exceeding max delivery attempts
 *     dead-letters into the `os.events.dlx` exchange -> `os.dead-letters` queue
 *
 * Delivery count is derived from the `x-death` header plus the redelivered
 * flag, mirroring the local broker's deliveryCount so consumer code is
 * transport-agnostic.
 */

const EXCHANGE = 'os.events';
const DLX = 'os.events.dlx';
const DLQ = 'os.dead-letters';

export interface RabbitMqBrokerOptions {
  prefetch?: number;
  maxDeliveries?: number;
}

function queueNameFor(group: string, pattern: string): string {
  const sanitized = pattern.replace(/\*/g, 'star').replace(/#/g, 'hash').replace(/\./g, '-');
  return `os.${group}.${sanitized}`;
}

export class RabbitMqBroker implements MessageBroker {
  private constructor(
    private readonly connection: ChannelModel,
    private readonly channel: Channel,
    private readonly maxDeliveries: number,
    private readonly logger: Logger,
  ) {}

  static async connect(url: string, logger: Logger = nullLogger, options: RabbitMqBrokerOptions = {}): Promise<RabbitMqBroker> {
    const connection = await amqp.connect(url);
    const channel = await connection.createChannel();
    await channel.prefetch(options.prefetch ?? 10);
    await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
    await channel.assertExchange(DLX, 'topic', { durable: true });
    await channel.assertQueue(DLQ, { durable: true });
    await channel.bindQueue(DLQ, DLX, '#');
    logger.info('connected to RabbitMQ', { exchange: EXCHANGE });
    return new RabbitMqBroker(connection, channel, options.maxDeliveries ?? 5, logger);
  }

  async publish(topic: string, event: EventEnvelope, headers: Record<string, string> = {}): Promise<void> {
    const ok = this.channel.publish(EXCHANGE, topic, Buffer.from(JSON.stringify(event)), {
      persistent: true,
      contentType: 'application/json',
      messageId: event.eventId,
      correlationId: event.correlationId,
      headers,
    });
    if (!ok) {
      // Write buffer full: wait for drain to preserve backpressure.
      await new Promise<void>((resolve) => this.channel.once('drain', () => resolve()));
    }
  }

  async subscribe(pattern: string, group: string, handler: DeliveryHandler): Promise<Subscription> {
    const queue = queueNameFor(group, pattern);
    await this.channel.assertQueue(queue, {
      durable: true,
      arguments: { 'x-dead-letter-exchange': DLX },
    });
    await this.channel.bindQueue(queue, EXCHANGE, pattern);

    const { consumerTag } = await this.channel.consume(
      queue,
      (msg: ConsumeMessage | null) => {
        if (!msg) return;
        let event: EventEnvelope;
        try {
          event = JSON.parse(msg.content.toString('utf8')) as EventEnvelope;
        } catch (err) {
          this.logger.error('unparseable message, dead-lettering', { error: (err as Error).message });
          this.channel.nack(msg, false, false);
          return;
        }
        const rawHeaders = (msg.properties.headers ?? {}) as Record<string, unknown>;
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(rawHeaders)) {
          if (typeof v === 'string') headers[k] = v;
        }
        const deaths = rawHeaders['x-death'] as Array<{ count?: number }> | undefined;
        const deathCount = deaths?.[0]?.count ? Number(deaths[0].count) : 0;
        const deliveryCount = deathCount + (msg.fields.redelivered ? 1 : 0) + 1;

        const delivery: Delivery = {
          message: { topic: msg.fields.routingKey, event, headers },
          deliveryTag: String(msg.fields.deliveryTag),
          redelivered: msg.fields.redelivered || deathCount > 0,
          deliveryCount,
          ack: () => this.channel.ack(msg),
          nack: (requeue = true) => {
            if (!requeue || deliveryCount >= this.maxDeliveries) {
              this.channel.nack(msg, false, false); // -> DLX
            } else {
              this.channel.nack(msg, false, true);
            }
          },
        };

        Promise.resolve()
          .then(() => handler(delivery))
          .catch((err) => {
            this.logger.error('delivery handler threw', { error: (err as Error).message, topic: msg.fields.routingKey });
            delivery.nack(true);
          });
      },
      { noAck: false },
    );

    return {
      unsubscribe: async () => {
        await this.channel.cancel(consumerTag);
      },
    };
  }

  async close(): Promise<void> {
    try {
      await this.channel.close();
    } catch {
      /* channel may already be closed */
    }
    await this.connection.close();
  }
}

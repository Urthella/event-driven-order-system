import type { BrokerMessage, EventEnvelope } from '../events';

/**
 * Transport-agnostic broker contract with explicit ack/nack, consumer groups
 * and at-least-once semantics. Implementations:
 *   - InMemoryBroker      (in-process; tests and the TCP server core)
 *   - TcpBrokerClient     (cross-process on one machine, zero external infra)
 *   - RabbitMqBroker      (amqplib topic exchange, manual ack, DLQ)
 */
export interface Delivery {
  message: BrokerMessage;
  deliveryTag: string;
  /** true when this is not the first delivery attempt of this message copy. */
  redelivered: boolean;
  deliveryCount: number;
  ack(): void;
  /** requeue=true -> redeliver later; requeue=false -> dead-letter. */
  nack(requeue?: boolean): void;
}

export type DeliveryHandler = (delivery: Delivery) => void | Promise<void>;

export interface Subscription {
  unsubscribe(): Promise<void>;
}

export interface MessageBroker {
  /**
   * Publish an event to a topic. `headers` carries transport metadata such as
   * the W3C traceparent. Resolves once the broker has accepted the message.
   */
  publish(topic: string, event: EventEnvelope, headers?: Record<string, string>): Promise<void>;

  /**
   * Subscribe a consumer group to a topic pattern (RabbitMQ-style: `*` = one
   * segment, `#` = zero or more trailing segments). Within a group each
   * message is delivered to exactly one consumer; distinct groups each get
   * their own copy. Deliveries must be acked; unacked messages are redelivered.
   */
  subscribe(pattern: string, group: string, handler: DeliveryHandler): Promise<Subscription>;

  close(): Promise<void>;
}

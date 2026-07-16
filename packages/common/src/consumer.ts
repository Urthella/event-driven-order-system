import { context, propagation, trace, SpanKind, SpanStatusCode } from '@opentelemetry/api';
import type { MessageBroker, Subscription } from './broker/types';
import type { DbHandle } from './db';
import type { EventEnvelope } from './events';
import {
  markOutboxPublished,
  processEventIdempotently,
  loadUnpublishedOutbox,
  type EmitContext,
  type OutboxEntry,
} from './idempotency';
import type { Logger } from './logger';
import { extractTraceContext, injectTraceHeaders } from './otel';

/**
 * The one place that wires together:
 *   at-least-once delivery -> transactional inbox (dedup) -> business handler
 *   -> transactional outbox -> traced publish -> ack.
 *
 * Handler map is keyed by exact topic; '*' is a catch-all fallback.
 */

export interface ConsumerDeps {
  serviceName: string;
  group: string;
  broker: MessageBroker;
  db: DbHandle;
  logger: Logger;
}

export type EventHandler = (event: EventEnvelope, ctx: EmitContext) => void;

/** Publish an event wrapped in a PRODUCER span; trace context rides in headers. */
export async function publishEvent(
  deps: Pick<ConsumerDeps, 'serviceName' | 'broker'>,
  entry: OutboxEntry,
): Promise<void> {
  const tracer = trace.getTracer(deps.serviceName);
  const span = tracer.startSpan(
    `publish ${entry.topic}`,
    {
      kind: SpanKind.PRODUCER,
      attributes: {
        'messaging.destination.name': entry.topic,
        'messaging.message.id': entry.event.eventId,
        'order.id': entry.event.orderId,
      },
    },
    context.active(),
  );
  const headers: Record<string, string> = {};
  propagation.inject(trace.setSpan(context.active(), span), headers);
  try {
    await deps.broker.publish(entry.topic, entry.event, headers);
  } catch (err) {
    span.recordException(err as Error);
    span.setStatus({ code: SpanStatusCode.ERROR });
    throw err;
  } finally {
    span.end();
  }
}

async function publishAndSettle(deps: ConsumerDeps, entries: OutboxEntry[]): Promise<void> {
  for (const entry of entries) {
    await publishEvent(deps, entry);
  }
  markOutboxPublished(deps.db, entries.map((e) => e.id));
}

/** Crash recovery: publish outbox rows staged before a previous shutdown/crash. */
export async function flushOutbox(deps: ConsumerDeps): Promise<number> {
  const entries = loadUnpublishedOutbox(deps.db);
  if (entries.length > 0) {
    deps.logger.warn('recovering unpublished outbox rows', { count: entries.length });
    await publishAndSettle(deps, entries);
  }
  return entries.length;
}

export async function subscribeIdempotent(
  deps: ConsumerDeps,
  pattern: string,
  handlers: Record<string, EventHandler>,
): Promise<Subscription> {
  const tracer = trace.getTracer(deps.serviceName);

  return deps.broker.subscribe(pattern, deps.group, async (delivery) => {
    const { topic, event, headers } = delivery.message;
    const log = deps.logger.child({
      topic,
      eventId: event.eventId,
      orderId: event.orderId,
      correlationId: event.correlationId,
    });

    const parentCtx = extractTraceContext(headers);
    const span = tracer.startSpan(
      `consume ${topic}`,
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          'messaging.destination.name': topic,
          'messaging.message.id': event.eventId,
          'messaging.redelivered': delivery.redelivered,
          'order.id': event.orderId,
        },
      },
      parentCtx,
    );

    await context.with(trace.setSpan(parentCtx, span), async () => {
      try {
        const handler = handlers[topic] ?? handlers['*'];
        if (!handler) {
          log.debug('no handler for topic, acking');
          delivery.ack();
          return;
        }

        const result = processEventIdempotently(deps.db, deps.serviceName, event, (ctx) => handler(event, ctx));

        if (result.duplicate) {
          span.setAttribute('idempotency.duplicate', true);
          log.info('duplicate delivery detected - state change skipped (no-op)', {
            deliveryCount: delivery.deliveryCount,
          });
        }
        // Fresh outbox rows, or rows recovered on the duplicate path.
        await publishAndSettle(deps, result.toPublish);
        delivery.ack();
      } catch (err) {
        span.recordException(err as Error);
        span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
        log.error('handler failed, nacking for redelivery', { error: (err as Error).message });
        delivery.nack(true);
      } finally {
        span.end();
      }
    });
  });
}

/** Convenience: inject the active trace context into a fresh headers object. */
export function tracedHeaders(): Record<string, string> {
  return injectTraceHeaders({});
}

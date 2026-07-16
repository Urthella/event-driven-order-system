import fs from 'node:fs';
import path from 'node:path';
import {
  context,
  propagation,
  trace,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  type Context,
  type Span,
  type Tracer,
} from '@opentelemetry/api';
import { ExportResultCode, W3CTraceContextPropagator, type ExportResult } from '@opentelemetry/core';
import { resourceFromAttributes } from '@opentelemetry/resources';
import {
  BatchSpanProcessor,
  ConsoleSpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
  type SpanExporter,
  type SpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

/**
 * Tracing bootstrap shared by every service.
 *
 * Exporters (combinable):
 *  - OTEL_TRACES_FILE:             JSON-lines span file - the offline default the
 *                                  demo uses; needs zero infrastructure.
 *  - OTEL_EXPORTER_OTLP_ENDPOINT:  OTLP/HTTP (e.g. Jaeger all-in-one on :4318)
 *                                  for the optional docker-compose path.
 *  - OTEL_TRACES_STDOUT=1:         ConsoleSpanExporter.
 *
 * Context propagation is W3C `traceparent`, injected into and extracted from
 * broker message headers, so one order = one distributed trace across all
 * five services regardless of transport.
 */

/** Writes finished spans as JSON lines - a poor man's Jaeger that works offline. */
export class FileSpanExporter implements SpanExporter {
  constructor(private readonly file: string) {
    fs.mkdirSync(path.dirname(path.resolve(file)), { recursive: true });
  }

  export(spans: ReadableSpan[], resultCallback: (result: ExportResult) => void): void {
    try {
      const lines = spans.map((s) => JSON.stringify(serializeSpan(s))).join('\n') + '\n';
      fs.appendFileSync(this.file, lines);
      resultCallback({ code: ExportResultCode.SUCCESS });
    } catch (err) {
      resultCallback({ code: ExportResultCode.FAILED, error: err as Error });
    }
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

function hrToMs(hr: [number, number]): number {
  return hr[0] * 1000 + hr[1] / 1e6;
}

function serializeSpan(span: ReadableSpan): Record<string, unknown> {
  const ctx = span.spanContext();
  const anySpan = span as unknown as {
    parentSpanId?: string;
    parentSpanContext?: { spanId?: string };
  };
  return {
    traceId: ctx.traceId,
    spanId: ctx.spanId,
    parentSpanId: anySpan.parentSpanContext?.spanId ?? anySpan.parentSpanId ?? null,
    name: span.name,
    kind: SpanKind[span.kind],
    service: String(span.resource.attributes[ATTR_SERVICE_NAME] ?? 'unknown'),
    startMs: hrToMs(span.startTime),
    endMs: hrToMs(span.endTime),
    durationMs: Number((hrToMs(span.endTime) - hrToMs(span.startTime)).toFixed(3)),
    status: span.status.code === SpanStatusCode.ERROR ? 'ERROR' : 'OK',
    attributes: span.attributes,
  };
}

export interface TracingHandle {
  tracer: Tracer;
  shutdown(): Promise<void>;
}

export function initTracing(serviceName: string, env: NodeJS.ProcessEnv = process.env): TracingHandle {
  const processors: SpanProcessor[] = [];

  const file = env.OTEL_TRACES_FILE;
  if (file) {
    processors.push(new SimpleSpanProcessor(new FileSpanExporter(file)));
  }
  if (env.OTEL_TRACES_STDOUT === '1') {
    processors.push(new SimpleSpanProcessor(new ConsoleSpanExporter()));
  }
  if (env.OTEL_EXPORTER_OTLP_ENDPOINT) {
    // Lazy import keeps the OTLP dependency out of the hot path when unused.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
    processors.push(
      new BatchSpanProcessor(
        new OTLPTraceExporter({ url: `${env.OTEL_EXPORTER_OTLP_ENDPOINT.replace(/\/$/, '')}/v1/traces` }),
      ),
    );
  }
  if (processors.length === 0) {
    // Default offline exporter: per-service JSON-lines file under data/traces.
    processors.push(new SimpleSpanProcessor(new FileSpanExporter(path.join('data', 'traces', `${serviceName}.spans.jsonl`))));
  }

  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: serviceName }),
    spanProcessors: processors,
  });
  provider.register({ propagator: new W3CTraceContextPropagator() });

  return {
    tracer: trace.getTracer(serviceName),
    shutdown: () => provider.shutdown(),
  };
}

/** Extract a parent context from broker message headers (W3C traceparent). */
export function extractTraceContext(headers: Record<string, string>): Context {
  return propagation.extract(ROOT_CONTEXT, headers);
}

/** Inject the given (or active) context into headers for the next hop. */
export function injectTraceHeaders(headers: Record<string, string>, ctx: Context = context.active()): Record<string, string> {
  propagation.inject(ctx, headers);
  return headers;
}

export function currentTraceId(): string | null {
  return trace.getActiveSpan()?.spanContext().traceId ?? null;
}

/** Run fn inside a new span, recording exceptions and setting error status. */
export async function withSpan<T>(
  tracer: Tracer,
  name: string,
  options: { kind?: SpanKind; parent?: Context; attributes?: Record<string, string | number | boolean> },
  fn: (span: Span) => Promise<T>,
): Promise<T> {
  const parent = options.parent ?? context.active();
  const span = tracer.startSpan(name, { kind: options.kind, attributes: options.attributes }, parent);
  return context.with(trace.setSpan(parent, span), async () => {
    try {
      return await fn(span);
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      throw err;
    } finally {
      span.end();
    }
  });
}

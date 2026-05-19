import { APIRequestContext, expect, request as playwrightRequest } from '@playwright/test';

const JAEGER_URL = process.env.JAEGER_URL ?? 'http://localhost:16686';

export interface JaegerSpanRef {
  refType: 'CHILD_OF' | 'FOLLOWS_FROM';
  traceID: string;
  spanID: string;
}

export interface JaegerTag {
  key: string;
  type: string;
  value: string | number | boolean;
}

export interface JaegerSpan {
  traceID: string;
  spanID: string;
  operationName: string;
  references: JaegerSpanRef[];
  tags: JaegerTag[];
  processID: string;
  startTime: number;
  duration: number;
}

export interface JaegerProcess {
  serviceName: string;
  tags: JaegerTag[];
}

export interface JaegerTrace {
  traceID: string;
  spans: JaegerSpan[];
  processes: Record<string, JaegerProcess>;
}

export interface JaegerTraceResponse {
  data: JaegerTrace[];
  total: number;
  limit: number;
  offset: number;
  errors: unknown;
}

export function tag(span: JaegerSpan, key: string): JaegerTag['value'] | undefined {
  return span.tags.find(t => t.key === key)?.value;
}

export function parentOf(span: JaegerSpan): string | null {
  const ref = span.references.find(r => r.refType === 'CHILD_OF');
  return ref?.spanID ?? null;
}

export function spanByOperation(trace: JaegerTrace, operationName: string): JaegerSpan {
  const found = trace.spans.find(s => s.operationName === operationName);
  if (!found) {
    const available = trace.spans.map(s => s.operationName).join(', ');
    throw new Error(`Operation "${operationName}" not in trace ${trace.traceID}. Got: [${available}]`);
  }
  return found;
}

export function spansByKind(trace: JaegerTrace, kind: 'producer' | 'consumer' | 'internal' | 'server' | 'client'): JaegerSpan[] {
  return trace.spans.filter(s => tag(s, 'span.kind') === kind);
}

export function serviceOf(trace: JaegerTrace, span: JaegerSpan): string {
  return trace.processes[span.processID]?.serviceName ?? '<unknown>';
}

/**
 * Poll Jaeger's /api/traces/<id> until the trace has at least `expectedSpans`
 * spans. Jaeger query is eventually-consistent against its in-memory store and
 * the sample's worker writes asynchronously, so we have to wait.
 */
export async function fetchTraceWhenReady(
  traceId: string,
  expectedSpans: number,
  options: { timeoutMs?: number; pollIntervalMs?: number } = {},
): Promise<JaegerTrace> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  const pollIntervalMs = options.pollIntervalMs ?? 500;
  const api = await playwrightRequest.newContext({ baseURL: JAEGER_URL });
  try {
    const deadline = Date.now() + timeoutMs;
    let last: JaegerTrace | null = null;
    let lastError = '';
    while (Date.now() < deadline) {
      const res = await api.get(`/api/traces/${traceId}`);
      if (res.ok()) {
        const body = (await res.json()) as JaegerTraceResponse;
        const trace = body.data?.[0];
        if (trace) {
          last = trace;
          if (trace.spans.length >= expectedSpans) return trace;
        }
      } else {
        lastError = `Jaeger HTTP ${res.status()}`;
      }
      await sleep(pollIntervalMs);
    }
    const got = last ? `${last.spans.length} spans` : `no trace (${lastError || 'not found'})`;
    throw new Error(
      `Trace ${traceId} did not reach ${expectedSpans} spans within ${timeoutMs}ms (got ${got}).`,
    );
  } finally {
    await api.dispose();
  }
}

export async function findTraceByOperation(
  serviceName: string,
  operationName: string,
  options: { timeoutMs?: number; pollIntervalMs?: number; lookbackHours?: number } = {},
): Promise<JaegerTrace> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  const pollIntervalMs = options.pollIntervalMs ?? 500;
  const lookbackHours = options.lookbackHours ?? 1;
  const api = await playwrightRequest.newContext({ baseURL: JAEGER_URL });
  try {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const res = await api.get(`/api/traces`, {
        params: {
          service: serviceName,
          operation: operationName,
          lookback: `${lookbackHours}h`,
          limit: '1',
        },
      });
      if (res.ok()) {
        const body = (await res.json()) as JaegerTraceResponse;
        if (body.data?.length) return body.data[0];
      }
      await sleep(pollIntervalMs);
    }
    throw new Error(
      `No trace for service=${serviceName} operation=${operationName} within ${timeoutMs}ms.`,
    );
  } finally {
    await api.dispose();
  }
}

/**
 * Build a parent → children adjacency map and a quick spanID → span lookup.
 * Useful for tree-shape assertions without writing the same boilerplate per test.
 */
export function indexTrace(trace: JaegerTrace) {
  const byId = new Map<string, JaegerSpan>();
  const childrenOf = new Map<string, JaegerSpan[]>();
  for (const span of trace.spans) {
    byId.set(span.spanID, span);
  }
  for (const span of trace.spans) {
    const parent = parentOf(span);
    if (parent) {
      const list = childrenOf.get(parent) ?? [];
      list.push(span);
      childrenOf.set(parent, list);
    }
  }
  return { byId, childrenOf };
}

export async function enqueueTask(
  request: APIRequestContext,
  body: { task: string; trigger_span: string; args?: unknown[]; kwargs?: Record<string, unknown> },
): Promise<{ task_id: string; task: string; trace_id: string }> {
  const res = await request.post('/api/enqueue/', { data: body });
  expect(res.ok(), `enqueue failed: ${res.status()} ${await res.text()}`).toBeTruthy();
  return (await res.json()) as { task_id: string; task: string; trace_id: string };
}

export interface ChainEntry {
  task: string;
  args?: unknown[];
  kwargs?: Record<string, unknown>;
}

export async function enqueueChain(
  request: APIRequestContext,
  body: { trigger_span: string; chain: ChainEntry[] },
): Promise<{ group_id: string; trace_id: string; chain_length: number }> {
  const res = await request.post('/api/enqueue-chain/', { data: body });
  expect(res.ok(), `enqueue-chain failed: ${res.status()} ${await res.text()}`).toBeTruthy();
  return (await res.json()) as { group_id: string; trace_id: string; chain_length: number };
}

export async function enqueueIter(
  request: APIRequestContext,
  body: { task: string; trigger_span: string; args_iter: unknown[][] },
): Promise<{ task_id: string; trace_id: string; iter_count: number }> {
  const res = await request.post('/api/enqueue-iter/', { data: body });
  expect(res.ok(), `enqueue-iter failed: ${res.status()} ${await res.text()}`).toBeTruthy();
  return (await res.json()) as { task_id: string; trace_id: string; iter_count: number };
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

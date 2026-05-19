import { randomUUID } from 'node:crypto';

/**
 * Generate a unique correlation id for a test scenario. The Playwright suite
 * passes this as `trigger_span` to /api/enqueue/, which makes it the operation
 * name of the HTTP root span — Jaeger then lets us look up exactly that trace.
 */
export function unique(prefix: string): string {
  return `${prefix}-${randomUUID()}`;
}

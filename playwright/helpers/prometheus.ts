import { request as playwrightRequest } from '@playwright/test';

const COLLECTOR_PROMETHEUS_URL = process.env.COLLECTOR_PROMETHEUS_URL ?? 'http://localhost:8889';

export interface PromSample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

/**
 * Minimal Prometheus exposition parser — handles the line shapes we care about:
 *   metric_name{label="v",...} 42
 *   metric_name 42
 * Comment lines (# HELP / # TYPE) and blanks are skipped.
 * Returns one sample per matched line; the histogram comes out as a stream of
 * `_count`, `_sum`, and `_bucket` samples just as the wire format dictates.
 */
export function parsePrometheus(body: string): PromSample[] {
  const out: PromSample[] = [];
  for (const rawLine of body.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const match = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9eE.naNif]+)/.exec(line);
    if (!match) continue;
    const [, name, labelBlock, valueStr] = match;
    const value = Number(valueStr);
    if (Number.isNaN(value)) continue;
    const labels: Record<string, string> = {};
    if (labelBlock) {
      for (const part of splitLabels(labelBlock)) {
        const eq = part.indexOf('=');
        if (eq < 0) continue;
        const k = part.slice(0, eq).trim();
        const v = part.slice(eq + 1).trim().replace(/^"|"$/g, '').replace(/\\"/g, '"').replace(/\\\\/g, '\\');
        labels[k] = v;
      }
    }
    out.push({ name, labels, value });
  }
  return out;
}

// Labels are comma-separated, but commas inside quoted values are legal. Walk
// character by character so we don't split on a comma sitting inside a label
// value like `{foo="a,b"}`.
function splitLabels(block: string): string[] {
  const parts: string[] = [];
  let buf = '';
  let inQuotes = false;
  for (const ch of block) {
    if (ch === '"') inQuotes = !inQuotes;
    if (ch === ',' && !inQuotes) {
      parts.push(buf);
      buf = '';
      continue;
    }
    buf += ch;
  }
  if (buf) parts.push(buf);
  return parts;
}

/**
 * Poll the collector's /metrics endpoint until `predicate` returns true.
 * Returns the parsed sample list for the matching scrape.
 */
export async function fetchPrometheusUntil(
  predicate: (samples: PromSample[]) => boolean,
  options: { timeoutMs?: number; pollIntervalMs?: number } = {},
): Promise<PromSample[]> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  const pollIntervalMs = options.pollIntervalMs ?? 500;
  const api = await playwrightRequest.newContext({ baseURL: COLLECTOR_PROMETHEUS_URL });
  try {
    const deadline = Date.now() + timeoutMs;
    let lastSamples: PromSample[] = [];
    while (Date.now() < deadline) {
      const res = await api.get('/metrics');
      if (res.ok()) {
        lastSamples = parsePrometheus(await res.text());
        if (predicate(lastSamples)) return lastSamples;
      }
      await sleep(pollIntervalMs);
    }
    const taskSamples = lastSamples.filter(s => s.name.startsWith('django_q2_'));
    throw new Error(
      `Predicate did not match within ${timeoutMs}ms. ` +
      `Last django_q2_* samples (${taskSamples.length}): ${JSON.stringify(taskSamples.slice(0, 20))}`,
    );
  } finally {
    await api.dispose();
  }
}

export function countMatching(
  samples: PromSample[],
  metricName: string,
  labelMatch: Record<string, string>,
): number {
  for (const s of samples) {
    if (s.name !== metricName) continue;
    let ok = true;
    for (const [k, v] of Object.entries(labelMatch)) {
      if (s.labels[k] !== v) {
        ok = false;
        break;
      }
    }
    if (ok) return s.value;
  }
  return 0;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

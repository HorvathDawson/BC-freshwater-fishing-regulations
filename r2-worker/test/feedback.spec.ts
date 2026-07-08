/**
 * Integration tests for the POST /api/feedback endpoint in the R2 worker.
 *
 * Runs inside Miniflare (via @cloudflare/vitest-pool-workers) with a local R2
 * bucket. Email delivery is not exercised here — the SEND_EMAIL binding is
 * unconfigured in tests, so only the durable R2-storage path is verified.
 */

import { env, createExecutionContext, waitOnExecutionContext } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import worker from '../src/index';

type TestEnv = { BUCKET: R2Bucket; SHARD_VERSION: string };

async function postFeedback(body: unknown): Promise<Response> {
  const request = new Request('https://test.example.com/api/feedback', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const ctx = createExecutionContext();
  const response = await worker.fetch(request, env as unknown as TestEnv, ctx);
  await waitOnExecutionContext(ctx);
  return response;
}

async function listFeedback(): Promise<string[]> {
  const list = await (env as unknown as TestEnv).BUCKET.list({ prefix: 'feedback/' });
  return list.objects.map(o => o.key);
}

describe('/api/feedback', () => {
  it('stores a valid report in R2 and returns ok', async () => {
    const resp = await postFeedback({ title: '[Report] Bug', body: 'The lake boundary is wrong.' });
    expect(resp.status).toBe(200);
    const json = (await resp.json()) as { ok: boolean };
    expect(json.ok).toBe(true);

    const keys = await listFeedback();
    expect(keys.length).toBeGreaterThan(0);
    const stored = await (env as unknown as TestEnv).BUCKET.get(keys[keys.length - 1]);
    const record = JSON.parse(await stored!.text());
    expect(record.body).toBe('The lake boundary is wrong.');
    expect(record.title).toBe('[Report] Bug');
    expect(typeof record.receivedAt).toBe('string');
  });

  it('rejects an empty body', async () => {
    const resp = await postFeedback({ title: 'x', body: '   ' });
    expect(resp.status).toBe(400);
  });

  it('rejects invalid JSON', async () => {
    const request = new Request('https://test.example.com/api/feedback', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{ not json',
    });
    const ctx = createExecutionContext();
    const resp = await worker.fetch(request, env as unknown as TestEnv, ctx);
    await waitOnExecutionContext(ctx);
    expect(resp.status).toBe(400);
  });

  it('silently drops honeypot submissions without storing', async () => {
    const before = await listFeedback();
    const resp = await postFeedback({ title: 'spam', body: 'buy now', hp: 'i am a bot' });
    expect(resp.status).toBe(200);
    const after = await listFeedback();
    expect(after.length).toBe(before.length);
  });

  it('does not serve stored feedback over GET', async () => {
    await postFeedback({ body: 'a private note' });
    const keys = await listFeedback();
    const request = new Request(`https://test.example.com/${keys[0]}`);
    const ctx = createExecutionContext();
    const resp = await worker.fetch(request, env as unknown as TestEnv, ctx);
    await waitOnExecutionContext(ctx);
    expect(resp.status).toBe(404);
  });

  it('includes CORS headers on the response', async () => {
    const resp = await postFeedback({ body: 'cors check' });
    expect(resp.headers.get('Access-Control-Allow-Origin')).toBe('*');
  });

  it('rejects a GET to /api/feedback (POST only)', async () => {
    const request = new Request('https://test.example.com/api/feedback', { method: 'GET' });
    const ctx = createExecutionContext();
    const resp = await worker.fetch(request, env as unknown as TestEnv, ctx);
    await waitOnExecutionContext(ctx);
    // Falls through to R2 file lookup for key "api/feedback" → 404.
    expect(resp.status).toBe(404);
  });
});

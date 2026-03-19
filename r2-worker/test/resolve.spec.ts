/**
 * Integration tests for the /api/resolve endpoint in the R2 worker.
 *
 * Uses @cloudflare/vitest-pool-workers to run tests inside Miniflare
 * with a local R2 bucket. Shard files are seeded into R2 before each test.
 */

import {
  env,
} from 'cloudflare:test';
import { describe, it, expect, beforeAll } from 'vitest';
import worker from '../src/index';

// ── Test fixtures ────────────────────────────────────────────────────

/**
 * Compute SHA-256 shard prefix (3 hex chars) matching the Python r2_sharder.
 */
async function shardPrefix(id: string): Promise<string> {
  const data = new TextEncoder().encode(id);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = new Uint8Array(hashBuffer);
  const hex = Array.from(hashArray.slice(0, 2))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
  return hex.slice(0, 3);
}

const SAMPLE_FID = '707231';
const SAMPLE_FID_2 = '707232';
const SAMPLE_REACH_ID = 'a1b2c3d4e5f6';
const SAMPLE_REACH_ID_2 = 'f9e8d7c6b5a4';
const SAMPLE_WBK = '351';
const VERSION = env.SHARD_VERSION;

type ReachFixture = {
  display_name: string;
  name_variants: { name: string; source: 'direct' | 'tributary' | 'admin' }[];
  feature_type: string;
  reg_set_index: number;
  watershed_code: string;
  min_zoom: number;
  regions: string[];
  bbox: [number, number, number, number];
  length_km: number;
  fids: string[];
};

const SAMPLE_REACH_DATA: ReachFixture = {
  display_name: 'Test Creek',
  name_variants: [{ name: 'Test Ck', source: 'direct' }],
  feature_type: 'stream',
  reg_set_index: 42,
  watershed_code: '930-508366',
  min_zoom: 11,
  regions: ['REGION 3 - Thompson Nicola'],
  bbox: [-120.5, 50.1, -120.4, 50.2] as [number, number, number, number],
  length_km: 1.23,
  fids: [SAMPLE_FID, SAMPLE_FID_2, '707233'],
};

const SAMPLE_REACH_DATA_2: ReachFixture = {
  display_name: 'Big Lake',
  name_variants: [],
  feature_type: 'lake',
  reg_set_index: 88,
  watershed_code: '351',
  min_zoom: 7,
  regions: ['REGION 3 - Thompson Nicola'],
  bbox: [-119.8, 50.7, -119.0, 51.1] as [number, number, number, number],
  length_km: 0,
  fids: [],
};

/**
 * Seed R2 with shard files for the test fixtures.
 */
async function seedR2(r2: R2Bucket): Promise<void> {
  const fidPrefix = await shardPrefix(SAMPLE_FID);
  const fid2Prefix = await shardPrefix(SAMPLE_FID_2);

  const fidShards: Record<string, Record<string, string>> = {};
  fidShards[fidPrefix] = { ...(fidShards[fidPrefix] || {}), [SAMPLE_FID]: SAMPLE_REACH_ID };
  fidShards[fid2Prefix] = { ...(fidShards[fid2Prefix] || {}), [SAMPLE_FID_2]: SAMPLE_REACH_ID };

  for (const [prefix, data] of Object.entries(fidShards)) {
    await r2.put(`shards/${VERSION}/fids/${prefix}.json`, JSON.stringify(data));
  }

  const reachPrefix = await shardPrefix(SAMPLE_REACH_ID);
  const reachPrefix2 = await shardPrefix(SAMPLE_REACH_ID_2);

  const reachShards: Record<string, Record<string, ReachFixture>> = {};
  reachShards[reachPrefix] = { ...(reachShards[reachPrefix] || {}), [SAMPLE_REACH_ID]: SAMPLE_REACH_DATA };
  reachShards[reachPrefix2] = { ...(reachShards[reachPrefix2] || {}), [SAMPLE_REACH_ID_2]: SAMPLE_REACH_DATA_2 };

  for (const [prefix, data] of Object.entries(reachShards)) {
    await r2.put(`shards/${VERSION}/reaches/${prefix}.json`, JSON.stringify(data));
  }

  const polyPrefix = await shardPrefix(SAMPLE_WBK);
  await r2.put(
    `shards/${VERSION}/polys/${polyPrefix}.json`,
    JSON.stringify({ [SAMPLE_WBK]: SAMPLE_REACH_ID_2 }),
  );

  await r2.put(
    `shards/${VERSION}/MANIFEST.json`,
    JSON.stringify({ version: VERSION, status: 'complete', shard_counts: { fids: 1, reaches: 1, polys: 1 } }),
  );
}

// ── Helper to call the worker ────────────────────────────────────────

async function callWorker(
  path: string,
  method = 'GET',
): Promise<Response> {
  const request = new Request(`https://test.example.com${path}`, { method });
  const response = await worker.fetch(request, env as unknown as { BUCKET: R2Bucket; SHARD_VERSION: string });
  return response;
}

// ── Tests ─────────────────────────────────────────────────────────────

describe('/api/resolve', () => {
  beforeAll(async () => {
    await seedR2((env as unknown as { BUCKET: R2Bucket }).BUCKET);
  });

  it('resolves a single stream fid', async () => {
    const response = await callWorker(`/api/resolve?fids=${SAMPLE_FID}`);
    expect(response.status).toBe(200);

    const body = await response.json() as { results: any[] };
    expect(body.results).toHaveLength(1);
    expect(body.results[0].reach_id).toBe(SAMPLE_REACH_ID);
    expect(body.results[0].reach.display_name).toBe('Test Creek');
    expect(body.results[0].fids).toEqual([SAMPLE_FID, SAMPLE_FID_2, '707233']);
    expect(body.results[0].matched_fids).toEqual([SAMPLE_FID]);
    expect(body.results[0].matched_wbks).toEqual([]);
  });

  it('resolves multiple fids — deduplicates same reach', async () => {
    const response = await callWorker(`/api/resolve?fids=${SAMPLE_FID},${SAMPLE_FID_2}`);
    expect(response.status).toBe(200);

    const body = await response.json() as { results: any[] };
    expect(body.results).toHaveLength(1);
    expect(body.results[0].reach_id).toBe(SAMPLE_REACH_ID);
    expect(body.results[0].matched_fids).toEqual(expect.arrayContaining([SAMPLE_FID, SAMPLE_FID_2]));
  });

  it('resolves a polygon waterbody_key', async () => {
    const response = await callWorker(`/api/resolve?wbks=${SAMPLE_WBK}`);
    expect(response.status).toBe(200);

    const body = await response.json() as { results: any[] };
    expect(body.results).toHaveLength(1);
    expect(body.results[0].reach_id).toBe(SAMPLE_REACH_ID_2);
    expect(body.results[0].reach.feature_type).toBe('lake');
    expect(body.results[0].matched_wbks).toEqual([SAMPLE_WBK]);
  });

  it('resolves fids + wbks in a single request', async () => {
    const response = await callWorker(`/api/resolve?fids=${SAMPLE_FID}&wbks=${SAMPLE_WBK}`);
    expect(response.status).toBe(200);

    const body = await response.json() as { results: any[] };
    expect(body.results).toHaveLength(2);
    const reachIds = body.results.map((r: any) => r.reach_id).sort();
    expect(reachIds).toEqual([SAMPLE_REACH_ID, SAMPLE_REACH_ID_2].sort());
  });

  it('returns 404 for unknown fid', async () => {
    const response = await callWorker('/api/resolve?fids=999999999');
    expect(response.status).toBe(404);
    const body = await response.json() as { results: any[] };
    expect(body.results).toEqual([]);
  });

  it('returns 400 with no params', async () => {
    const response = await callWorker('/api/resolve');
    expect(response.status).toBe(400);
  });

  it('rejects non-numeric fid', async () => {
    const response = await callWorker('/api/resolve?fids=abc');
    expect(response.status).toBe(400);
  });

  it('rejects fids exceeding batch limit', async () => {
    const tooMany = Array.from({ length: 25 }, (_, i) => String(i)).join(',');
    const response = await callWorker(`/api/resolve?fids=${tooMany}`);
    expect(response.status).toBe(400);
  });

  it('includes CORS headers', async () => {
    const response = await callWorker(`/api/resolve?fids=${SAMPLE_FID}`);
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
  });

  it('handles OPTIONS preflight', async () => {
    const response = await callWorker('/api/resolve', 'OPTIONS');
    expect(response.status).toBe(204);
    expect(response.headers.get('Access-Control-Allow-Methods')).toContain('GET');
  });
});

describe('/api/version', () => {
  it('returns the current shard version', async () => {
    const response = await callWorker('/api/version');
    expect(response.status).toBe(200);
    const body = await response.json() as { version: string };
    expect(body.version).toBe(VERSION);
  });
});

describe('R2 file serving', () => {
  beforeAll(async () => {
    const bucket = (env as unknown as { BUCKET: R2Bucket }).BUCKET;
    await bucket.put('test_data.json', JSON.stringify({ hello: 'world' }));
  });

  it('serves R2 files for non-API paths', async () => {
    const response = await callWorker('/test_data.json');
    expect(response.status).toBe(200);
    const body = await response.json() as { hello: string };
    expect(body.hello).toBe('world');
  });

  it('returns 404 for missing R2 files', async () => {
    const response = await callWorker('/nonexistent.json');
    expect(response.status).toBe(404);
  });
});

describe('shard prefix consistency', () => {
  it('produces valid 3-char hex prefixes', async () => {
    const testIds = ['707231', '351', 'abc123def456', '1166294466'];
    for (const id of testIds) {
      const prefix = await shardPrefix(id);
      expect(prefix).toHaveLength(3);
      expect(prefix).toMatch(/^[0-9a-f]{3}$/);
    }
  });
});

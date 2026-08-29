#!/usr/bin/env node
/**
 * Seed local R2 bucket for wrangler dev.
 *
 * Walks output/pipeline/deploy/ and inserts every file into
 * the local Miniflare R2 storage so wrangler dev can serve them — giving
 * true dev/prod parity (same worker code path handles all requests).
 *
 * A hash-based stamp file skips re-seeding when deploy/ hasn't changed,
 * making restart-after-code-change near-instant.
 *
 * Usage:
 *   node scripts/seed.mjs           # seed from default deploy dir
 *   node scripts/seed.mjs --force   # always re-seed
 *   node scripts/seed.mjs --clean   # wipe local R2 state, then re-seed
 */

import { resolve, dirname, relative } from 'path';
import { fileURLToPath } from 'url';
import {
  readdirSync, readFileSync, statSync, openSync, readSync, closeSync,
  existsSync, writeFileSync, mkdirSync, rmSync,
} from 'fs';
import { createHash } from 'crypto';
import { createRequire } from 'module';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, '..');
const WORKER_DIR = resolve(ROOT, 'r2-worker');
const DEPLOY_DIR = resolve(ROOT, 'output', 'pipeline', 'deploy');
const PERSIST_DIR = resolve(WORKER_DIR, '.wrangler', 'state', 'v3');
const STAMP_FILE = resolve(WORKER_DIR, '.seed-stamp');
const BUCKET_NAME = 'bc-fishing-regulations';

const force = process.argv.includes('--force');
const clean = process.argv.includes('--clean');

// ── Clean local R2 state ────────────────────────────────────────────

if (clean) {
  const wranglerState = resolve(WORKER_DIR, '.wrangler', 'state');
  for (const target of [wranglerState, STAMP_FILE]) {
    if (existsSync(target)) {
      rmSync(target, { recursive: true, force: true });
      console.log(`\x1b[33m⟳\x1b[0m Removed ${relative(ROOT, target)}`);
    }
  }
}

// ── Preflight ───────────────────────────────────────────────────────

if (!existsSync(DEPLOY_DIR)) {
  console.error(
    '\x1b[31m✗\x1b[0m Pipeline output not found at:\n' +
    `  ${DEPLOY_DIR}\n\n` +
    '  Run the pipeline first:\n' +
    '    python -m pipeline --step all\n'
  );
  process.exit(1);
}

// ── Walk deploy/ ────────────────────────────────────────────────────

function walkDir(dir) {
  const results = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === '_tile_temp') continue; // skip tippecanoe work dir
    const full = resolve(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...walkDir(full));
    } else if (entry.isFile()) {
      results.push(full);
    }
  }
  return results;
}

const files = walkDir(DEPLOY_DIR);

// ── Hash-based skip ─────────────────────────────────────────────────

function deployHash(fileList) {
  const h = createHash('sha256');
  for (const f of fileList.sort()) {
    h.update(relative(DEPLOY_DIR, f));
    h.update(String(statSync(f).mtimeMs));
  }
  return h.digest('hex');
}

const currentHash = deployHash(files);

if (!force && !clean && existsSync(STAMP_FILE)) {
  const savedHash = readFileSync(STAMP_FILE, 'utf-8').trim();
  if (savedHash === currentHash) {
    console.log('\x1b[32m✓\x1b[0m Local R2 up to date — skipping seed');
    process.exit(0);
  }
}

// ── Seed via Miniflare ──────────────────────────────────────────────

console.log(`Seeding ${files.length} files from deploy/ into local R2...`);

const require = createRequire(resolve(WORKER_DIR, 'package.json'));
const { Miniflare } = require('miniflare');

const mf = new Miniflare({
  modules: true,
  script: 'export default { async fetch() { return new Response("seed"); } }',
  r2Buckets: { BUCKET: BUCKET_NAME },
  // Miniflare 4 uses per-plugin persist options, NOT `persistTo`.
  // `defaultPersistRoot` makes r2Persist default to path.join(root, 'r2'),
  // matching the path wrangler dev uses internally.
  defaultPersistRoot: PERSIST_DIR,
});

const bucket = await mf.getR2Bucket('BUCKET');

// Multipart upload for large files to avoid devalue's V8 string-length
// limit (~384 MB effective for binary data). Reads the file in chunks
// and uploads each as a multipart part.
const PART_SIZE = 10 * 1024 * 1024; // 10 MB per part

async function putLargeFile(key, filePath, size) {
  const upload = await bucket.createMultipartUpload(key);
  const parts = [];
  const fd = openSync(filePath, 'r');
  const buf = Buffer.alloc(PART_SIZE);
  let partNumber = 1;
  let offset = 0;

  try {
    while (offset < size) {
      const toRead = Math.min(PART_SIZE, size - offset);
      const bytesRead = readSync(fd, buf, 0, toRead, offset);
      const data = new Uint8Array(buf.buffer, buf.byteOffset, bytesRead);
      const part = await upload.uploadPart(partNumber, data);
      parts.push(part);
      partNumber++;
      offset += bytesRead;
    }
    await upload.complete(parts);
  } catch (err) {
    await upload.abort();
    throw err;
  } finally {
    closeSync(fd);
  }
}

// Files >100 MB use multipart upload; smaller files use direct put.
const LARGE_THRESHOLD = 100 * 1024 * 1024;

let count = 0;
for (const file of files) {
  const key = relative(DEPLOY_DIR, file).replace(/\\/g, '/');
  const size = statSync(file).size;

  if (size >= LARGE_THRESHOLD) {
    const sizeMB = (size / 1048576).toFixed(1);
    process.stdout.write(`\n  ${key} (${sizeMB} MB, multipart)...`);
    await putLargeFile(key, file, size);
  } else {
    const buf = readFileSync(file);
    const body = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
    await bucket.put(key, body);
  }

  count++;
  if (count % 1000 === 0) {
    process.stdout.write(`\r  ${count}/${files.length} files`);
  }
}
process.stdout.write(`\r  ${count}/${files.length} files`);

// Write a dev data_version.json so the frontend can fetch it
const devVersion = new Date().toISOString();
await bucket.put('data_version.json', JSON.stringify({ v: devVersion }));

await mf.dispose();

// ── Write stamp ─────────────────────────────────────────────────────

mkdirSync(dirname(STAMP_FILE), { recursive: true });
writeFileSync(STAMP_FILE, currentHash);

console.log(`\n\x1b[32m✓\x1b[0m Seeded ${files.length} files into local R2 (data_version: ${devVersion})`);

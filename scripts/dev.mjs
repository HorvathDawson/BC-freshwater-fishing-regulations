#!/usr/bin/env node
/**
 * Dev server launcher — seeds local R2, starts wrangler dev, then Vite.
 *
 * Usage:
 *   node scripts/dev.mjs
 *
 * Starts:
 *   1. Seeds local R2 from pipeline output (deploy/)
 *   2. R2 Worker (wrangler dev) at http://localhost:8787
 *   3. Vite dev server at http://localhost:5173
 *
 * Vite proxies /data/* and /api/* to the R2 Worker — same code path as prod.
 * Press Ctrl+C to stop both servers.
 */

import { spawn, execFileSync } from 'child_process';
import { existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, '..');

// ── Preflight checks ────────────────────────────────────────────────

const checks = [
  {
    path: resolve(ROOT, 'r2-worker/node_modules'),
    fix: 'cd r2-worker && npm install',
    label: 'R2 Worker dependencies',
  },
  {
    path: resolve(ROOT, 'webapp/node_modules'),
    fix: 'cd webapp && npm install',
    label: 'Webapp dependencies',
  },
  {
    path: resolve(ROOT, 'output/pipeline/deploy'),
    fix: 'python -m pipeline --step all',
    label: 'Pipeline output (deploy/)',
  },
];

let failed = false;
for (const check of checks) {
  if (!existsSync(check.path)) {
    console.error(`\x1b[31m✗\x1b[0m ${check.label} not found.`);
    console.error(`  Run: ${check.fix}\n`);
    failed = true;
  }
}
if (failed) {
  process.exit(1);
}

// ── Seed local R2 ──────────────────────────────────────────────────

console.log('\x1b[36m── Seeding local R2 ──\x1b[0m\n');

try {
  execFileSync(
    process.execPath,
    [resolve(ROOT, 'scripts/seed.mjs')],
    { cwd: ROOT, stdio: 'inherit' },
  );
} catch (err) {
  console.error('\x1b[31m✗\x1b[0m Seed failed — aborting.');
  process.exit(1);
}

// ── Launch servers ──────────────────────────────────────────────────

const IS_WIN = process.platform === 'win32';

function startProcess(name, cwd, command, args, color) {
  const proc = spawn(command, args, {
    cwd,
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: IS_WIN,
  });

  const prefix = `\x1b[${color}m[${name}]\x1b[0m`;

  proc.stdout.on('data', (data) => {
    for (const line of data.toString().split('\n').filter(Boolean)) {
      console.log(`${prefix} ${line}`);
    }
  });

  proc.stderr.on('data', (data) => {
    for (const line of data.toString().split('\n').filter(Boolean)) {
      console.error(`${prefix} ${line}`);
    }
  });

  proc.on('error', (err) => {
    console.error(`${prefix} Failed to start: ${err.message}`);
  });

  proc.on('exit', (code) => {
    if (code !== null && code !== 0) {
      console.error(`${prefix} Exited with code ${code}`);
    }
  });

  return proc;
}

console.log('\n\x1b[36m── Starting dev servers ──\x1b[0m\n');

const workerProc = startProcess(
  'r2-worker',
  resolve(ROOT, 'r2-worker'),
  'npx',
  ['wrangler', 'dev', '--port', '8787'],
  '33', // cyan
);

// Wait for wrangler to be ready before starting Vite (proxy target must be up)
async function waitForWorker(url, maxWaitMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    try {
      const resp = await fetch(url);
      if (resp.ok) return true;
    } catch { /* not ready yet */ }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}

const workerReady = await waitForWorker('http://localhost:8787/api/version');
if (!workerReady) {
  console.error('\x1b[31m✗\x1b[0m Wrangler did not become ready within 30s — aborting.');
  workerProc.kill();
  process.exit(1);
}
console.log('\x1b[32m✓\x1b[0m R2 Worker ready\n');

const viteProc = startProcess(
  'webapp',
  resolve(ROOT, 'webapp'),
  'npx',
  ['vite'],
  '35', // magenta
);

console.log(`
\x1b[32m── Dev servers ready ──\x1b[0m
  Webapp:    http://localhost:5173
  R2 Worker: http://localhost:8787
  Data:      /data/* and /api/* proxied → :8787 (local R2)

  Press \x1b[1mCtrl+C\x1b[0m to stop both servers.
`);

// ── Graceful shutdown ───────────────────────────────────────────────

function shutdown() {
  console.log('\n\x1b[33mShutting down...\x1b[0m');
  workerProc.kill();
  viteProc.kill();
  // Give processes time to clean up
  setTimeout(() => process.exit(0), 1000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Keep alive
await new Promise(() => {});

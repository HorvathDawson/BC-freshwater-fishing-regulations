#!/usr/bin/env node
/**
 * sync-assets — stage the pipeline's mobile artifacts into `mobile/assets/` so
 * they can be bundled into the app (or used by the simulator).
 *
 * Copies:
 *   output/pipeline/deploy/mobile/<version>/regulations.sqlite → assets/db/
 *   output/pipeline/deploy/bathymetry/*.pdf                    → assets/bathymetry/
 *   output/pipeline/deploy/source/*.png (synopsis pages)       → assets/source/
 *
 * The SQLite DB is large (~1.2 GB) — bundling it is optional and intended for
 * offline dev/simulator use. In production the app downloads it once from R2
 * (see src/data/database.ts). PDFs/PNGs are small enough to bundle.
 *
 * Usage: node scripts/sync-assets.mjs [--no-db]
 */
import { existsSync, mkdirSync, readdirSync, copyFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const mobileRoot = resolve(__dirname, '..');
const repoRoot = resolve(mobileRoot, '..');
const deploy = join(repoRoot, 'output', 'pipeline', 'deploy');

const SHARD_VERSION = 'v2'; // keep in sync with app.json extra.shardVersion
const includeDb = !process.argv.includes('--no-db');

function ensureDir(dir) {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

function copyOne(from, to) {
  ensureDir(dirname(to));
  copyFileSync(from, to);
  const mb = (statSync(to).size / 1024 / 1024).toFixed(1);
  console.log(`  ✓ ${to.replace(mobileRoot + '/', '')} (${mb} MB)`);
}

function copyGlob(fromDir, toDir, ext) {
  if (!existsSync(fromDir)) {
    console.warn(`  ⚠ skip (missing): ${fromDir.replace(repoRoot + '/', '')}`);
    return 0;
  }
  ensureDir(toDir);
  let n = 0;
  for (const name of readdirSync(fromDir)) {
    if (!name.toLowerCase().endsWith(ext)) continue;
    copyFileSync(join(fromDir, name), join(toDir, name));
    n += 1;
  }
  console.log(`  ✓ ${n} ${ext} file(s) → ${toDir.replace(mobileRoot + '/', '')}`);
  return n;
}

function main() {
  console.log('── Syncing mobile assets from pipeline deploy output ──');

  if (includeDb) {
    const db = join(deploy, 'mobile', SHARD_VERSION, 'regulations.sqlite');
    if (existsSync(db)) {
      copyOne(db, join(mobileRoot, 'assets', 'db', 'regulations.sqlite'));
    } else {
      console.warn(
        `  ⚠ DB not found: ${db.replace(repoRoot + '/', '')} — build it with the ` +
          `pipeline mobile step, or run with --no-db to skip.`,
      );
    }
  } else {
    console.log('  • skipping DB (--no-db)');
  }

  copyGlob(join(deploy, 'bathymetry'), join(mobileRoot, 'assets', 'bathymetry'), '.pdf');
  copyGlob(join(deploy, 'source'), join(mobileRoot, 'assets', 'source'), '.png');

  console.log('Done.');
}

main();

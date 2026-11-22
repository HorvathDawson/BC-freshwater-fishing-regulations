#!/usr/bin/env node
/**
 * (T866 S1) EVERY APP TEST FILE RUNS IN EXACTLY ONE OF CI'S VITEST SHARDS.
 *
 * app-ci splits `pnpm run test` over N runners (`--shard=i/N`, the matrix in .github/workflows/app-ci.yml). A shard
 * count typed twice, a matrix leg left out, or a vitest upgrade that slices differently would leave files that no leg
 * runs, and every leg would still be green. Two checks, both through vitest's OWN sharding (its sequencer over its own
 * file list, grouped as its pool groups them), never a copy of the arithmetic:
 *
 *   node tools/vitest-shards.mjs N                    the N shards are a partition of the unsharded file list: each
 *                                                     file in exactly one, none empty (each CI leg runs it first)
 *   node tools/vitest-shards.mjs N --ran I report.json  the files leg I actually ran (vitest's json reporter) are the
 *                                                     ones this file says shard I holds (each CI leg runs it last)
 *
 * Exit 1 names every file that is missing, doubled or unexpected.
 */
import { readFileSync } from "node:fs";
import { relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));

/** Problems with `shards` (lists of `project:file` keys) as a partition of `all`; empty when each key is in one. */
export function partitionProblems(all, shards) {
  const out = [];
  const seen = new Map();
  shards.forEach((keys, i) => {
    if (keys.length === 0) out.push(`shard ${i + 1}/${shards.length} is empty`);
    for (const k of keys) seen.set(k, [...(seen.get(k) ?? []), i + 1]);
  });
  const want = new Set(all);
  for (const k of [...want].sort()) {
    const legs = seen.get(k) ?? [];
    if (legs.length === 0) out.push(`runs in no shard: ${k}`);
    else if (legs.length > 1) out.push(`runs in shards ${legs.join(", ")}: ${k}`);
  }
  for (const k of [...seen.keys()].sort()) if (!want.has(k)) out.push(`in a shard but not in the unsharded run: ${k}`);
  return out;
}

/** Problems with the files a leg ran (`ran`, repo-relative) against the files its shard holds (`want`). */
export function ranProblems(want, ran) {
  const w = new Set(want), r = new Set(ran);
  return [...[...w].filter((f) => !r.has(f)).sort().map((f) => `the shard holds it but the run did not report it: ${f}`),
          ...[...r].filter((f) => !w.has(f)).sort().map((f) => `the run reported it but the shard does not hold it: ${f}`)];
}

const key = (spec) => `${spec.project.name}:${relative(APP, spec.moduleId)}`;

/** The unsharded file list and the N shards, as vitest itself slices them (`project:file` keys). */
async function shardKeys(n) {
  const { createVitest } = await import("vitest/node");
  const ctx = await createVitest("test", { watch: false, run: true, root: APP });
  try {
    const specs = await ctx.globTestSpecifications();
    // vitest shards each (group order, pool) group on its own (its pool's executeTests): mirror that grouping
    const groups = new Map();
    for (const s of specs) {
      const g = `${s.project.config.sequence.groupOrder ?? 0}:${s.pool}`;
      groups.set(g, [...(groups.get(g) ?? []), s]);
    }
    const Sequencer = ctx.config.sequence.sequencer;
    const shards = [];
    for (let i = 1; i <= n; i++) {
      ctx.config.shard = { index: i, count: n };
      const seq = new Sequencer(ctx);
      const keys = [];
      for (const g of groups.values()) keys.push(...(await seq.shard(g)).map(key));
      shards.push(keys);
    }
    ctx.config.shard = undefined;
    return { all: specs.map(key), shards };
  } finally {
    await ctx.close();
  }
}

async function main(argv) {
  const n = Number(argv[0]);
  if (!Number.isInteger(n) || n < 1 || (argv.length !== 1 && !(argv.length === 4 && argv[1] === "--ran"))) {
    console.error("usage: node tools/vitest-shards.mjs N [--ran I report.json]");
    return 2;
  }
  const { all, shards } = await shardKeys(n);
  if (argv.length === 1) {
    const bad = partitionProblems(all, shards);
    for (const b of bad) console.error(`vitest-shards: ${b}`);
    console.log(bad.length ? `vitest-shards: FAILED (${bad.length})`
      : `vitest-shards: ${all.length} files, each in exactly one of ${n} shards (${shards.map((s) => s.length).join(" + ")})`);
    return bad.length ? 1 : 0;
  }
  const i = Number(argv[2]);
  if (!Number.isInteger(i) || i < 1 || i > n) { console.error(`vitest-shards: shard ${argv[2]} is not 1..${n}`); return 2; }
  const report = JSON.parse(readFileSync(argv[3], "utf8"));
  const want = shards[i - 1].map((k) => k.slice(k.indexOf(":") + 1));
  const ran = report.testResults.map((r) => relative(APP, r.name));
  const bad = ranProblems(want, ran);
  for (const b of bad) console.error(`vitest-shards: ${b}`);
  console.log(bad.length ? `vitest-shards: shard ${i}/${n} FAILED (${bad.length})`
    : `vitest-shards: shard ${i}/${n} ran the ${ran.length} files it holds`);
  return bad.length ? 1 : 0;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  process.exitCode = await main(process.argv.slice(2));
}

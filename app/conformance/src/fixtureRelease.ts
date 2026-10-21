/**
 * THE FIXTURE RELEASE, for tests (P11): `python -m pipeline.tools.fixture_release` over a copy of the dev fixture bundle
 * (the release builder's own bundle half), and the node-side pieces a test needs to read it the way the app does — a
 * `fetch` that serves the release folder as R2 / `pnpm tiles` would, and a node:sqlite opener.
 */
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdtempSync, readdirSync, readFileSync, statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { DEV_BUNDLE } from "@app/data/bundle/node";

const REPO = resolve(__dirname, "../../..");

export interface FixtureRelease {
  /** `<root>/<release id>`: manifest.json + the gzipped files. */
  dir: string;
  /** The release root every manifest url is relative to. */
  root: string;
  /** The bundle the release was cut from (the dev fixture + any `extra` rows). */
  bundle: string;
}

/** Build a fixture release; `extra(db)` may add rows to the bundle copy first. */
export function buildFixtureRelease(validUntil: string | null, extra?: (db: DatabaseSync) => void): FixtureRelease {
  const root = mkdtempSync(join(tmpdir(), "fixture-release-"));
  const bundle = join(root, "bundle.sqlite");
  copyFileSync(DEV_BUNDLE, bundle);
  if (extra) {
    const w = new DatabaseSync(bundle);
    extra(w);
    w.close();
  }
  const args = ["-m", "pipeline.tools.fixture_release", "--bundle", bundle, "--out", root];
  if (validUntil) args.push("--valid-until", validUntil);
  const dir = execFileSync(join(REPO, ".venv/bin/python"), args, { cwd: REPO, encoding: "utf8" }).trim();
  return { dir, root, bundle };
}

/**
 * A `fetch` over a release root, as R2 serves it: `<base>manifest.json` is the release's manifest, every other url a
 * file under the root, sent gzipped (`Content-Encoding` unset: the bytes are the .gz, which `fetchVerified` decodes).
 * `log` records every url asked, in order — what a cold start fetched is a test's question.
 */
export function releaseFetch(base: string, dir: string, log: string[]): typeof fetch {
  return (async (input: string | URL) => {
    const url = String(input);
    log.push(url);
    const rel = url.startsWith(base) ? decodeURIComponent(url.slice(base.length)) : null;
    const path = rel === "manifest.json" ? join(dir, "manifest.json") : rel ? join(dir, "..", rel) : null;
    if (!path || !existsSync(path)) return new Response("no", { status: 404 });
    const body = readFileSync(path);
    return new Response(rel === "manifest.json" ? body : new Uint8Array(path.endsWith(".gz") ? body : gzipSync(body)),
                        { status: 200 });
  }) as unknown as typeof fetch;
}

/**
 * THE LIVE RELEASE on this machine (`python -m pipeline.deliver release` → data/release/r-*, not in git): $RELEASE_DIR
 * (relative to the repo root, or absolute), else the newest release folder in this checkout, else — in a
 * `.claude/worktrees/<name>` checkout — the main checkout's (read only: its files are read, never written). null: none.
 */
export function liveRelease(): string | null {
  if (process.env.RELEASE_DIR) {
    const d = resolve(REPO, process.env.RELEASE_DIR);
    return existsSync(join(d, "manifest.json")) ? d : null;
  }
  for (const root of ROOTS()) {
    const rel = join(root, "data/release");
    if (!existsSync(rel)) continue;
    const dirs = readdirSync(rel).filter((n) => /^r-/.test(n) && existsSync(join(rel, n, "manifest.json")))
      .map((n) => join(rel, n)).sort((a, b) => statSync(join(b, "manifest.json")).mtimeMs - statSync(join(a, "manifest.json")).mtimeMs);
    if (dirs.length) return dirs[0]!;
  }
  return null;
}

/** This checkout's root, and — in a `.claude/worktrees/<name>` checkout — the main checkout's (read only). */
const ROOTS = (): string[] => {
  const main = /\/\.claude\/worktrees\/[^/]+\/?$/.test(REPO) ? REPO.replace(/\/\.claude\/worktrees\/[^/]+\/?$/, "") : null;
  return main ? [REPO, main] : [REPO];
};

/**
 * The regs store `python -m pipeline.deliver store` writes (data/generated/bundle/regs.sqlite, not in git): the PORT
 * TOOLING's file (taps.py, hydrate.mjs) until P20, and the Python reference decoder's input — never the app's data, which
 * is the release. $REGS_STORE, else this checkout's, else the main checkout's. null: none.
 */
export function liveRegsStore(): string | null {
  if (process.env.REGS_STORE) return existsSync(process.env.REGS_STORE) ? process.env.REGS_STORE : null;
  for (const root of ROOTS()) {
    const p = join(root, "data/generated/bundle/regs.sqlite");
    if (existsSync(p)) return p;
  }
  return null;
}

/** A release folder opened as the app opens one, and the node:sqlite opener: the data drivers' (one copy, T10). */
export { nodeOpener, openReleaseDir } from "@app/data/release/node";

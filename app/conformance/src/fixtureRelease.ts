/**
 * THE FIXTURE RELEASE, for tests (P11): `python -m pipeline.tools.fixture_release` over a copy of the dev fixture bundle
 * (the release builder's own bundle half), and the node-side pieces a test needs to read it the way the app does — a
 * `fetch` that serves the release folder as R2 / `pnpm tiles` would, and a node:sqlite opener.
 */
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import type { Opener } from "@app/data/release";
import { DEV_BUNDLE, openBundle } from "@app/data/bundle/node";

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

/** node:sqlite over a verified file: the bytes written to a temp file and opened read-only. */
export function nodeOpener(): Opener {
  const tmp = mkdtempSync(join(tmpdir(), "release-open-"));
  return async (name, bytes) => {
    const p = join(tmp, `${name}.sqlite`);
    writeFileSync(p, await bytes());
    return openBundle(p);
  };
}

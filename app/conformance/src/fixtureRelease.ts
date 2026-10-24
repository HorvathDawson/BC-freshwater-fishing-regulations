/**
 * THE FIXTURE RELEASES, for tests (P11, T61): the release's BUNDLE half — paint, flow-area, access, core, near,
 * conditions — committed under `packages/data/dev/fixture-release/`, and the node-side pieces a test needs to read one
 * the way the app does: a `fetch` that serves the release folder as R2 / `pnpm tiles` would, and a node:sqlite opener.
 *
 * They were cut once (2026-10-23) by the release builder's own code (`python -m pipeline.tools.fixture_release`) from
 * the retired development bundle — the Chilliwack / Harrison valley slice of build 54ea0bb4 — and are frozen: the dev
 * bundle, its generator and the riffle prototype it was extracted from are gone (T61), so no test needs Python or the dev data path to read a
 * release. Three, so an update can be tested between two real releases:
 *   plain        the slice as it was
 *   lakeGauged   + one `lake_gauge` row (wbk:329177889 08MH103): changes core (item.gauged) and conditions, not paint
 *   conformance  + three `lake_gauge` rows (two stations in one lake pin "the lowest") and the fixture source's
 *                edition (valid_until 2027-03-31), so the two fixtures state one
 * The regulations half is the regs fixture release (`@app/data/release/node` `openRegsFixture`), cut from a live one.
 */
import { cpSync, existsSync, mkdtempSync, readdirSync, readFileSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { KNOWN_VERSIONS, SECTION_NAMES } from "@app/core";

const REPO = resolve(__dirname, "../../..");

/** Where the committed fixture releases live. */
export const FIXTURE_RELEASE_ROOT = resolve(REPO, "app/packages/data/dev/fixture-release");

/** The committed fixture releases by what they are (see the header). */
export const FIXTURE_RELEASES = {
  plain: "r-6d85601a9b99",
  lakeGauged: "r-6dfd7c1cf86a",
  conformance: "r-cc200b480be5",
} as const;

export interface FixtureRelease {
  /** `<root>/<release id>`: manifest.json + the gzipped files. */
  dir: string;
  /** The release root every manifest url is relative to: a temp folder of its own, so a test may write beside it. */
  root: string;
}

/** A committed fixture release, copied into a temp root of its own (a test writes tile folders and manifests there). */
export function fixtureRelease(which: keyof typeof FIXTURE_RELEASES): FixtureRelease {
  const id = FIXTURE_RELEASES[which];
  const root = mkdtempSync(join(tmpdir(), "fixture-release-"));
  cpSync(join(FIXTURE_RELEASE_ROOT, id), join(root, id), { recursive: true });
  return { dir: join(root, id), root };
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
 * `.claude/worktrees/<name>` checkout — the main checkout's (read only: its files are read, never written). null: none,
 * or one this app refuses by its section versions (`liveReleaseAt` says why).
 */
export function liveRelease(): string | null {
  return liveReleaseAt().dir;
}

/**
 * The live release, or why there is none to test against. A release whose manifest says other section versions than
 * core `KNOWN_VERSIONS` was built for another port — the app refuses it (`openRegsStore`), so every live suite over it
 * would only test that refusal: it is a SKIP with the reason (a store-format bump lands before `deliver all` rebuilds the
 * release), never passed over for an older folder — unless REGS_STORE_REQUIRED=1 (a machine meant to prove parity),
 * where it throws.
 */
export function liveReleaseAt(env: Readonly<Record<string, string | undefined>> = process.env):
  { dir: string; reason: null } | { dir: null; reason: string } {
  const dir = findRelease(env);
  if (!dir) return { dir: null, reason: "no release on this machine (data/release/r-*)" };
  const said = (JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as
    { regulations?: { versions?: Record<string, number> } }).regulations?.versions ?? {};
  const off = SECTION_NAMES.filter((n) => said[n] !== KNOWN_VERSIONS[n])
    .map((n) => `${n} ${said[n] ?? "absent"} (this app reads ${KNOWN_VERSIONS[n]})`);
  if (!off.length) return { dir, reason: null };
  const reason = `release ${dir} is ${off.join(", ")}: rebuild it (deliver all), then re-cut the goldens (sync.mjs)`;
  if (env.REGS_STORE_REQUIRED && env.REGS_STORE_REQUIRED !== "0") throw new Error(`REGS_STORE_REQUIRED is set: ${reason}`);
  return { dir: null, reason };
}

function findRelease(env: Readonly<Record<string, string | undefined>>): string | null {
  if (env.RELEASE_DIR) {
    const d = resolve(REPO, env.RELEASE_DIR);
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

/** This checkout's `.venv/bin/python`, else the main checkout's (a worktree has none of its own); null: neither. */
export function venvPython(): string | null {
  return ROOTS().map((root) => join(root, ".venv/bin/python")).find((p) => existsSync(p)) ?? null;
}

const PROVINCE = new Map<string, string>();
/**
 * THE PROVINCE REGS STORE of the release folder `dir`, as one SQLite file (P20, T61 2.4: no regs.sqlite is kept). The
 * release ships the store's tables split by reader — water's common tables, the nine regs-<region> shards, regs-sid —
 * and `pipeline.deliver.store.release.province_store` puts them back together row for row (held to the store's own
 * store_digest when the release carries it); that is written once per process to a temp file, so a test reads the
 * same rows with node:sqlite as the Python reference decoder (`Store`) does. Made from the release and nothing else.
 * null: no `dir`, or no .venv to run the reassembly.
 */
export function provinceStoreOf(dir: string | null): string | null {
  if (!dir) return null;
  const py = venvPython();
  if (!py) return null;
  let p = PROVINCE.get(dir);
  if (!p) {
    p = join(mkdtempSync(join(tmpdir(), "province-store-")), "regs.sqlite");
    execFileSync(py, ["-c", [
      "import sys", "from pathlib import Path", "from pipeline.deliver.store.release import province_store",
      "province_store(Path(sys.argv[1])).con.execute('VACUUM INTO ?', (sys.argv[2],))"].join("\n"), dir, p],
      { cwd: REPO, stdio: ["ignore", "ignore", "inherit"] });
    PROVINCE.set(dir, p);
  }
  return p;
}

/** A release folder opened as the app opens one, and the node:sqlite opener: the data drivers' (one copy, T10). */
export { nodeOpener, openReleaseDir, type ReleaseTamper } from "@app/data/release/node";

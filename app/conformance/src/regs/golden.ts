/**
 * The golden oracles, read from app/design/regs-v11/port/golden/ (tests.md §1.2). T05 starts this file with `golden()`,
 * `taps()`, `provinceStore()` and their types; T12 adds `requestOf`, `inputsOf`, `cases`, `writeVmFixtures`.
 *
 * `provinceStore()` is the built regs store the oracles were cut from (`taps.json` `meta.store.store_digest`), read here
 * over node:sqlite with T02's blob codec until T03/T04's store reads land and replace it. Looked for at $REGS_STORE, then
 * this checkout's data/generated/bundle/regs.sqlite, then (in a `.claude/worktrees/<name>` checkout) the main checkout's —
 * read-only. Absent, or a store whose digest is not the oracles' (a stale pairing), is a SKIP with its reason, never a fail —
 * unless REGS_STORE_REQUIRED=1 (`paritySkip`). Not yet tests.md §1.2's `Promise<RegsStore | null>` over the release: T03/T04
 * are not on this base, so this reads the bundle store with hand-written SQL; replace it with T04's reads when they land.
 */
import { existsSync, readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import type { Day, Moment, RegsInputs, RegsTarget, Weekday } from "@app/core";
import { loadRegsInputs, openRegsStore, unpackBlob, type RegsStore } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
const GOLDEN_DIR = `${REPO}app/design/regs-v11/port/golden/`;

export interface GoldenSheet { title: string | null; sub: string | null; folds: { summary: string; open: boolean }[];
  pages: { text: string; pdf: number | null }[]; lines: string[]; nested: string[] }
export interface GoldenState { id: string; w: string; d: number; p: number; water: string; header: unknown; keep: unknown;
  gear: unknown; methods: unknown; licence: Record<`${"adult" | "u16"}|${string}`, unknown>; sheets: Record<string, GoldenSheet | "@">; pins: unknown }
export interface Cell { ladder: number; rows: number; gear: number; licence: number; display: number }
export interface Tap { item: string; partIx: number; md: number; date: string /* "2026-05-15" */; weekday: Weekday;
  item_ord: number; akey: number; seg: number; mom: number | null; nseg: number; starts: number[]; moments: Moment[] | null;
  s: number; cell: Cell; displayByDay: number[] }
/** A Feb 29 case (P06/D31): the date reads `readingDay` (59) on its own `weekday`, as `encode.tap_key` taps it. */
export interface LeapTap { id: string; item: string; partIx: number; date: string; weekday: Weekday; readingDay: number; akey: number;
  starts: number[]; moments: Moment[] | null; s: number; cell: Cell }
export interface TapsMeta { generator: string; store: { format: string; section_handles: string; reach_digest: string;
  rule_ids_sha256: string; store_digest: string; layout: string }; golden: string; version: string; year: number }

let G: ReturnType<typeof golden> | undefined;
/** golden.json (8.6 MB), read once per test file. */
export function golden(): { meta: { sha: string; ref: string; version: string }; states: Record<string, GoldenState>; sheets: Record<string, GoldenSheet> } {
  return (G ??= JSON.parse(readFileSync(`${GOLDEN_DIR}golden.json`, "utf8")));
}

type Taps = { meta: TapsMeta; states: Record<string, Tap>; unnamed: { handle: number; akey: number; place: number; name: string }[];
  leap: LeapTap[] };
export function taps(): Taps {
  return JSON.parse(readFileSync(`${GOLDEN_DIR}taps.json`, "utf8"));
}
let TP: Taps | undefined;
const tapsOnce = (): Taps => (TP ??= taps());

/** The store reads the tap parity needs (T04's `akey`/`segStarts`/`segMoments`/`cell`, by the same names). */
export interface TapStore {
  readonly path: string;
  /** The part's akey (`part.akey` of the item's `item_ord`), or null for a part with no rule set. */
  partAkey(item: string, partIx: number): number | null;
  akey(akey: number): { seg: number; mom: number | null; nseg: number };
  segStarts(seg: number): number[];
  /** The key's moments (`segmom.ix` → `static answers.moments`), null when `mom` is null. */
  segMoments(mom: number | null): Moment[] | null;
  cell(akey: number, s: number): Cell;
  statics(k: string): unknown;
}

/** The places a regs store is looked for, in order: $REGS_STORE, this checkout's bundle, the main checkout's (from a worktree). */
export function storeCandidates(): (string | undefined)[] {
  const here = `${REPO}data/generated/bundle/regs.sqlite`;
  const main = /\/\.claude\/worktrees\/[^/]+\/$/.test(REPO) ? `${REPO.replace(/\.claude\/worktrees\/[^/]+\/$/, "")}data/generated/bundle/regs.sqlite` : undefined;
  return [process.env.REGS_STORE, here, main];
}

/** Where the province store is, or why there is none to test against. The FIRST candidate on disk is judged: a stale one is a
 *  skip with its reason, never passed over for a later one (REGS_STORE pointing at a stale store must say so). */
export function provinceStoreAt(candidates: readonly (string | undefined)[] = storeCandidates()):
  { path: string; reason: null } | { path: null; reason: string } {
  const path = candidates.find((p): p is string => !!p && existsSync(p));
  if (!path) return { path: null, reason: `no regs store on this machine (looked at ${candidates.filter(Boolean).join(", ") || "nothing"})` };
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    const got = (db.prepare("SELECT v FROM meta WHERE k = 'store_digest'").get() as { v: string } | undefined)?.v;
    const want = taps().meta.store.store_digest;
    return got === want ? { path, reason: null }
      : { path: null, reason: `${path} is store ${got ?? "(no store_digest)"}, the oracles were cut from ${want}: re-run app/design/regs-v11/port/taps.py` };
  } finally { db.close(); }
}

/** How a store-backed parity suite treats the pairing: run it, or skip it with the reason. REGS_STORE_REQUIRED=1 (a machine
 *  that is meant to prove parity) turns the skip into a failure, so a rebuilt store cannot make `pnpm check` pass silently. */
export function paritySkip(at: ReturnType<typeof provinceStoreAt>, env: Readonly<Record<string, string | undefined>> = process.env):
  { skip: false; fail: null } | { skip: true; fail: null; reason: string } | { skip: true; fail: string } {
  if (at.path !== null) return { skip: false, fail: null };
  const required = !!env.REGS_STORE_REQUIRED && env.REGS_STORE_REQUIRED !== "0";
  return required ? { skip: true, fail: `REGS_STORE_REQUIRED is set and the parity cannot run: ${at.reason}` }
    : { skip: true, fail: null, reason: at.reason };
}
export const hasProvinceRegs = (): boolean => provinceStoreAt().path !== null;

/** The paired regs store, or null (skip; `provinceStoreAt().reason` says why). */
export async function provinceStore(): Promise<TapStore | null> {
  const at = provinceStoreAt();
  if (!at.path) return null;
  const db = new DatabaseSync(at.path, { readOnly: true });
  const dicts = new Map<string, Uint8Array>();
  for (const r of db.prepare("SELECT tbl, d FROM zdict").all() as { tbl: string; d: Uint8Array }[]) dicts.set(r.tbl, r.d);
  const statics = (k: string): unknown => {
    const r = db.prepare("SELECT j FROM static WHERE k = ?").get(k) as { j: Uint8Array } | undefined;
    if (!r) throw new Error(`regs store: no static ${k}`);
    return unpackBlob(r.j, dicts.get("static") ?? null);
  };
  const moments = statics("answers.moments") as Moment[];
  const one = <T>(sql: string, ...args: (number | string)[]): T => {
    const r = db.prepare(sql).get(...args) as T | undefined;
    if (r === undefined) throw new Error(`regs store: nothing for ${sql} ${JSON.stringify(args)}`);
    return r;
  };
  const partQ = db.prepare("SELECT p.akey AS akey FROM item i JOIN str s ON s.id = i.item_id JOIN part p ON p.item_ord = i.item_ord " +
    "WHERE s.text = ? AND p.part_ix = ?");
  return {
    path: at.path,
    partAkey(item, partIx) {
      const r = partQ.get(item, partIx) as { akey: number | null } | undefined;
      if (!r) throw new Error(`regs store: no part ${item}#${partIx}`);
      return r.akey;
    },
    akey: (k) => one("SELECT seg, mom, nseg FROM akey WHERE akey = ?", k),
    segStarts: (seg) => JSON.parse(one<{ starts: string }>("SELECT starts FROM seglist WHERE seg = ?", seg).starts) as number[],
    segMoments: (mom) => mom === null ? null
      : (JSON.parse(one<{ ix: string }>("SELECT ix FROM segmom WHERE mom = ?", mom).ix) as number[]).map((i) => moments[i]!),
    cell: (k, s) => ({ ...one<Cell>("SELECT ladder, rows, gear, licence, display FROM cell WHERE akey = ? AND s = ?", k, s) }),
    statics,
  };
}

/** golden id → the app's request (tests.md §1.2): the tap's item, part, date and weekday as taps.json gives them (never
 *  recomputed from another year); `today` is Oct 10 2026 for every state (the prototype's "today" is d=1010). */
export function requestOf(id: string): { target: RegsTarget; day: Day; weekday: Weekday; today: Day } {
  const t = tapsOnce().states[id];
  if (!t) throw new Error(`taps.json has no state ${id}`);
  const [, m, d] = t.date.split("-").map(Number);
  return { target: { kind: "part", item: t.item, partIx: t.partIx }, day: { month: m!, day: d! }, weekday: t.weekday,
           today: { month: 10, day: 10 } };
}

/**
 * The regs store the app reads — the LIVE RELEASE (`openRegsStore` over its water / regs-<region> / regs-sid files, D22) —
 * paired with the oracles: the province store the goldens were cut from (`provinceStoreAt`, taps.json `store_digest`) must
 * exist and carry the same `answers_sha256` as the release's water file. Else a skip with the reason: a release delivered
 * after the goldens (a pipeline answers change) would make every parity a stale comparison. REGS_STORE_REQUIRED=1 turns
 * the skip into a failure (`paritySkip`).
 */
export async function releasePairing(): Promise<{ path: string; reason: null } | { path: null; reason: string }> {
  const at = provinceStoreAt();
  if (at.path === null) return { path: null, reason: at.reason };
  const dir = liveRelease();
  if (!dir) return { path: null, reason: "no release on this machine (data/release/r-*)" };
  const db = new DatabaseSync(at.path, { readOnly: true });
  let want: string | undefined;
  try { want = (db.prepare("SELECT v FROM meta WHERE k = 'answers_sha256'").get() as { v: string } | undefined)?.v; }
  finally { db.close(); }
  const got = (await regsStoreAt(dir)).meta.answersSha256;
  return got === want ? { path: dir, reason: null }
    : { path: null, reason: `release ${dir} carries answers ${got.slice(0, 12)}, the oracles' store ${String(want).slice(0, 12)}: re-run app/design/regs-v11/port/sync.mjs` };
}

const STORES = new Map<string, Promise<RegsStore>>();
const regsStoreAt = (dir: string): Promise<RegsStore> => {
  let p = STORES.get(dir);
  if (!p) STORES.set(dir, p = openRegsStore(openReleaseDir(dir)));
  return p;
};

/** The paired release's store, or null (skip; `releasePairing()` says why). One store per test file. */
export async function regsStore(): Promise<RegsStore | null> {
  const at = await releasePairing();
  return at.path === null ? null : regsStoreAt(at.path);
}

const INPUTS = new Map<string, Promise<RegsInputs | null>>();
/** The memoised `RegsInputs` of one golden state (T09 `loadRegsInputs` over the paired release; calendar year = taps.json
 *  meta.year, 2026), or null when there is no paired release (skip). */
export function inputsOf(id: string): Promise<RegsInputs | null> {
  let p = INPUTS.get(id);
  if (!p) {
    const r = requestOf(id);
    INPUTS.set(id, p = regsStore().then((st) => st && loadRegsInputs(st, r.target, r.day, r.weekday, tapsOnce().meta.year)));
  }
  return p;
}

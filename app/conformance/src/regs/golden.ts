/**
 * The golden parity harness (tests.md §1.2; T05 → T09 → T12): the oracles (oracles.ts: golden.json, taps.json,
 * hydrate.json), the store a parity suite reads, the per-state `RegsInputs` cache, and `cases(block)`.
 *
 * WHICH STORE (`paritySource`, REGS_PARITY=province|fixture|auto, default auto):
 * - province: the LIVE RELEASE on this machine (`releasePairing`): the province regs.sqlite the oracles were cut from
 *   (taps.json `meta.store.store_digest`) must be here and carry the release's `answers_sha256`.
 * - fixture: the COMMITTED regs fixture release (T10, packages/data/dev/regs-release; all 134 golden states), held to
 *   taps.json's section_handles / reach_digest / rule_ids_sha256 — and, when the province store is on this machine, to its
 *   answers_sha256 too. This is the CI path.
 * - auto: province when paired, else the fixture when paired, else none.
 * None is a SKIP with its reason, never a wall of false failures — unless REGS_STORE_REQUIRED=1 (`paritySkip`).
 *
 * `provinceStore()` is still T05's hand-SQL reader of the province regs.sqlite (`TapStore`, tap.parity.test.ts reads it);
 * the harness's store is `parityStore()` — the ticket's `provinceStore(): Promise<RegsStore | null>` under another name.
 */
import { existsSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { buildRegsPage, type Day, type Moment, type RegsInputs, type RegsPage, type RegsTarget, type Weekday, type Who } from "@app/core";
import type { Block, Cell, GoldenState, Tap } from "@app/core/regs/testing";
import { loadRegsInputs, openRegsStore, unpackBlob, type RegsStore } from "@app/data/regs";
import { regsFixtureDir } from "@app/data/release/node";
import { liveRelease, liveReleaseAt, openReleaseDir } from "../fixtureRelease";
import { EXCLUDE, excluded } from "./exclusions";
import { golden, REPO, taps, tapsOnce } from "./oracles";

export { golden, hydrate, taps } from "./oracles";
export type { Block, Cell, GoldenFile, GoldenSheet, GoldenState, LeapTap, Tap, TapsFile, TapsMeta } from "@app/core/regs/testing";

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
  const live = liveReleaseAt(), dir = live.dir;
  if (!dir) return { path: null, reason: live.reason! };
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

// ── T12: the parity store (province, else the committed fixture) ─────────────────────────────────────────────────────

export type ParityMode = "province" | "fixture" | "auto";
type Env = Readonly<Record<string, string | undefined>>;

/** REGS_PARITY, default auto; anything else is a typo that must not silently mean auto. */
export function parityMode(env: Env = process.env): ParityMode {
  const v = env.REGS_PARITY ?? "auto";
  if (v === "province" || v === "fixture" || v === "auto") return v;
  throw new Error(`REGS_PARITY=${v}: expected province, fixture or auto`);
}

/** What the fixture release's water meta must agree with: taps.json's store digests, and the province store's answers. */
export interface FixtureFacts { readonly sectionHandles: string; readonly reachDigest: string; readonly ruleIdsSha256: string; readonly answersSha256: string }

/**
 * Why the fixture is NOT the oracles' store, or null when it is: its section_handles / reach_digest / rule_ids_sha256 must be
 * taps.json's, and — when the paired province store is on this machine (`provinceAnswers` non-null) — its answers_sha256 the
 * province store's (the release meta carries no store_digest; answers are what a pipeline change moves).
 */
export function fixtureVerdict(fx: FixtureFacts, store: { section_handles: string; reach_digest: string; rule_ids_sha256: string },
                               provinceAnswers: string | null): string | null {
  const bad = ([["section_handles", fx.sectionHandles, store.section_handles], ["reach_digest", fx.reachDigest, store.reach_digest],
                ["rule_ids_sha256", fx.ruleIdsSha256, store.rule_ids_sha256]] as const).find(([, a, b]) => a !== b);
  if (bad) return `the regs fixture release has ${bad[0]} ${bad[1]}, the oracles' store ${bad[2]}: run pnpm -C app regs-fixture (after port/sync.mjs)`;
  if (provinceAnswers !== null && provinceAnswers !== fx.answersSha256)
    return `the regs fixture release carries answers ${fx.answersSha256.slice(0, 12)}, the oracles' store ${provinceAnswers.slice(0, 12)}: run pnpm -C app regs-fixture`;
  return null;
}

/** The mode's choice between the two pairings (each a reason or null = paired). */
export function chooseSource(mode: ParityMode, province: string | null, fixture: string | null):
  { label: "province" | "fixture"; reason: null } | { label: null; reason: string } {
  if (mode === "province") return province === null ? { label: "province", reason: null } : { label: null, reason: `REGS_PARITY=province: ${province}` };
  if (mode === "fixture") return fixture === null ? { label: "fixture", reason: null } : { label: null, reason: `REGS_PARITY=fixture: ${fixture}` };
  if (province === null) return { label: "province", reason: null };
  if (fixture === null) return { label: "fixture", reason: null };
  return { label: null, reason: `no paired regs store: province — ${province}; fixture — ${fixture}` };
}

const provinceAnswers = (): string | null => {
  const at = provinceStoreAt();
  if (at.path === null) return null;
  const db = new DatabaseSync(at.path, { readOnly: true });
  try { return (db.prepare("SELECT v FROM meta WHERE k = 'answers_sha256'").get() as { v: string } | undefined)?.v ?? null; }
  finally { db.close(); }
};

type Source = { label: "province" | "fixture"; dir: string; reason: null } | { label: null; dir: null; reason: string };
/** What the fixture is held to: taps.json's store digests, and the province store's answers (null when it is not here). */
export interface SourceFacts {
  readonly store: { section_handles: string; reach_digest: string; rule_ids_sha256: string };
  readonly provinceAnswers: () => string | null;
}

/** The store the harness reads (REGS_PARITY), unmemoised: a fixture is paired only when `fixtureVerdict` says so. */
export async function resolveSource(env: Env = process.env,
                                    facts: SourceFacts = { store: tapsOnce().meta.store, provinceAnswers }): Promise<Source> {
  const mode = parityMode(env);
  const province = mode === "fixture" ? "not asked" : (await releasePairing()).reason;
  let fixture: string | null = "not asked";
  if (mode !== "province" && province !== null) {
    const dir = regsFixtureDir();
    fixture = existsSync(`${dir}/manifest.json`)
      ? fixtureVerdict((await regsStoreAt(dir)).meta, facts.store, facts.provinceAnswers())
      : `no regs fixture release at ${dir}: run pnpm -C app regs-fixture`;
  }
  const c = chooseSource(mode, province, fixture);
  if (c.label === null) return { label: null, dir: null, reason: c.reason };
  return { label: c.label, dir: c.label === "province" ? liveRelease()! : regsFixtureDir(), reason: null };
}

let SOURCE: Promise<Source> | undefined;
/** The store the harness reads (REGS_PARITY), decided once per test file (`resolveSource`). */
export function paritySource(env: Env = process.env): Promise<Source> {
  return (SOURCE ??= resolveSource(env));
}

/** The harness's regs store (one per test file), or null — `paritySource()` says why. */
export async function parityStore(): Promise<RegsStore | null> {
  const s = await paritySource();
  return s.dir === null ? null : regsStoreAt(s.dir);
}
export async function storeLabel(): Promise<"province" | "fixture" | null> {
  return (await paritySource()).label;
}
/** `paritySkip` over the harness's store: describe.skipIf(gate.skip); gate.fail under REGS_STORE_REQUIRED=1. */
export async function parityGate(env: Env = process.env): Promise<ReturnType<typeof paritySkip>> {
  const s = await paritySource(env);
  return paritySkip(s.dir === null ? { path: null, reason: s.reason } : { path: s.dir, reason: null }, env);
}

const INPUTS = new Map<string, Promise<RegsInputs | null>>();
/** The memoised `RegsInputs` of one golden state (T09 `loadRegsInputs` over `parityStore()`; calendar year = taps.json
 *  meta.year, 2026), or null when there is no paired store (skip). The same promise for the same id, per test file. */
export function inputsOf(id: string): Promise<RegsInputs | null> {
  let p = INPUTS.get(id);
  if (!p) {
    const r = requestOf(id);
    INPUTS.set(id, p = parityStore().then((st) => st && loadRegsInputs(st, r.target, r.day, r.weekday, tapsOnce().meta.year)));
  }
  return p;
}

// ── T12: cases ─────────────────────────────────────────────────────────────────────────────────────────────────────

export const ADULT_RESIDENT: Who = { age: "adult", residency: "resident" };

export interface Case {
  readonly id: string; readonly g: GoldenState; readonly tap: Tap;
  /** = inputsOf(id), never null inside cases */
  inputs(): Promise<RegsInputs>;
  /** buildRegsPage(inputs, {today: requestOf(id).today, who, tot, pid: id}) — throws "buildRegsPage: T21" until T21 */
  page(who?: Who, tot?: "a" | "b"): Promise<RegsPage>;
}

/** The state ids for it.each (sync): every taps.json state minus the block's whole-state exclusions (keyed ones stay). */
export function caseIds(block: Block): readonly string[] {
  return Object.keys(tapsOnce().states).filter((id) => excluded(block, id) === null);
}

const caseOf = (id: string, g: GoldenState): Case => ({
  id, g, tap: tapsOnce().states[id]!,
  async inputs() {
    const inp = await inputsOf(id);
    if (!inp) throw new Error(`inputsOf(${id}): no paired regs store`);
    return inp;
  },
  async page(who = ADULT_RESIDENT, tot) {
    return buildRegsPage(await this.inputs(), { today: requestOf(id).today, who, tot, pid: id });
  },
});

const warned = new Set<string>();
/**
 * Every case for a block (`caseIds`), with its golden state. Yields nothing when there is no paired store — the reason
 * printed once; under REGS_STORE_REQUIRED=1 that throws instead (a parity machine must not pass on zero cases).
 */
export async function* cases(block: Block): AsyncIterable<Case> {
  const gate = await parityGate();
  if (gate.fail) throw new Error(gate.fail);
  if (gate.skip) {
    const msg = `${block} parity skipped: ${"reason" in gate ? gate.reason : ""}`;
    if (!warned.has(msg)) { warned.add(msg); console.warn(msg); }
    return;
  }
  const G = golden();
  for (const id of caseIds(block)) yield caseOf(id, G.states[id]!);
}

/**
 * The afterAll line: "<block> parity: P/T states equal", then " · N excluded (ids)" for whole-state exclusions and
 * " · <key> not compared (<until>)" for each keyed one.
 */
export function summary(block: Block, passed: number, total: number): string {
  const ids = Object.keys(tapsOnce().states).filter((id) => excluded(block, id) !== null);
  const keyed = EXCLUDE.filter((e) => e.block === block && e.key);
  return `${block} parity: ${passed}/${total} states equal`
    + (ids.length ? ` · ${ids.length} excluded (${ids.join(", ")})` : "")
    + keyed.map((e) => ` · ${e.key!.source} not compared (${e.until ?? "permanent"})`).join("");
}

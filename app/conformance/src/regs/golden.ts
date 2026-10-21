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
 * Either store is a release opened the way the app opens one (`openPaired`: `@app/data/regs` `openRegsStore(loader)`,
 * every region file the oracle states read opened up front), so a release the app refuses — wrong tiles, wrong handles,
 * a region file missing or from another build — is a skip reason in the app's own words, not 134 failures.
 * `parityStore()` is tests.md §1.2's `provinceStore(): Promise<RegsStore | null>`; every parity suite reads through it.
 */
import { existsSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { buildRegsPage, type Day, type RegsInputs, type RegsPage, type RegsTarget, type Weekday, type Who } from "@app/core";
import type { Block, GoldenState, Tap, TapsFile } from "@app/core/regs/testing";
import { loadRegsInputs, openRegsStore, RegsStoreError, type RegsLoader, type RegsStore } from "@app/data/regs";
import { regsFixtureDir } from "@app/data/release/node";
import { liveRelease, liveReleaseAt, openReleaseDir, regsStoreCandidates } from "../fixtureRelease";
import { EXCLUDE, excluded } from "./exclusions";
import { golden, taps, tapsOnce } from "./oracles";

export { golden, hydrate, HYDRATE, REPO, taps, TAPS } from "./oracles";
export type { Block, Cell, GoldenFile, GoldenSheet, GoldenState, LeapTap, Tap, TapsFile, TapsMeta } from "@app/core/regs/testing";

/** Where the port tooling's province store (regs.sqlite) is, or why there is none to pair with. The FIRST candidate on
 *  disk is judged: a stale one is a skip with its reason, never passed over for a later one (REGS_STORE pointing at a
 *  stale store must say so). */
export function provinceStoreAt(candidates: readonly (string | undefined)[] = regsStoreCandidates()):
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

/** golden id → the app's request (tests.md §1.2): the tap's item, part, date and weekday as taps.json gives them (never
 *  recomputed from another year); `today` is Oct 10 2026 for every state (the prototype's "today" is d=1010). */
export function requestOf(id: string): { target: RegsTarget; day: Day; weekday: Weekday; today: Day } {
  const t = tapsOnce().states[id];
  if (!t) throw new Error(`taps.json has no state ${id}`);
  const [, m, d] = t.date.split("-").map(Number);
  return { target: { kind: "part", item: t.item, partIx: t.partIx }, day: { month: m!, day: d! }, weekday: t.weekday,
           today: { month: 10, day: 10 } };
}

/** A release opened as the app opens it, or why it cannot be. */
export type Opened = { store: RegsStore; reason: null } | { store: null; reason: string };

/**
 * `loader`'s release opened as the app opens it (`openRegsStore`), with every region file the oracle states read
 * (`oracle`'s states and Feb 29 cases) opened — and so held to water's build — now. A refusal (`RegsStoreError`) is the
 * reason, in the app's own words; anything else (a damaged read, a bug) is thrown, never a skip.
 */
export async function openPaired(loader: RegsLoader, oracle: TapsFile = tapsOnce()): Promise<Opened> {
  const refused = (e: unknown): Opened => {
    if (e instanceof RegsStoreError) return { store: null, reason: `release ${loader.release} refused: ${e.message}` };
    throw e;
  };
  let st: RegsStore;
  try { st = await openRegsStore(loader); } catch (e) { return refused(e); }
  try { for (const a of new Set([...Object.values(oracle.states), ...oracle.leap].map((t) => t.akey))) await st.region(a); }
  catch (e) { st.close(); return refused(e); }
  return { store: st, reason: null };
}

const STORES = new Map<string, Promise<Opened>>();
/** The release folder `dir`, opened and paired once per test file. */
const storeAt = (dir: string): Promise<Opened> => {
  let p = STORES.get(dir);
  if (!p) STORES.set(dir, p = openPaired(openReleaseDir(dir)));
  return p;
};

/**
 * The LIVE RELEASE paired with the oracles: the province store the goldens were cut from (`provinceStoreAt`, taps.json
 * `store_digest`) must exist, the app must accept the release (`openPaired`), and its water file must carry the province
 * store's `answers_sha256`. Else a skip with the reason: a release delivered after the goldens (a pipeline answers change)
 * would make every parity a stale comparison. REGS_STORE_REQUIRED=1 turns the skip into a failure (`paritySkip`).
 */
export async function releasePairing(): Promise<{ path: string; reason: null } | { path: null; reason: string }> {
  const at = provinceStoreAt();
  if (at.path === null) return { path: null, reason: at.reason };
  const live = liveReleaseAt(), dir = live.dir;
  if (!dir) return { path: null, reason: live.reason! };
  const opened = await storeAt(dir);
  if (opened.store === null) return { path: null, reason: opened.reason };
  const want = provinceAnswers(), got = opened.store.meta.answersSha256;
  return got === want ? { path: dir, reason: null }
    : { path: null, reason: `release ${dir} carries answers ${got.slice(0, 12)}, the oracles' store ${String(want).slice(0, 12)}: re-run app/design/regs-v11/port/sync.mjs` };
}

/** The paired LIVE release's store, or null (skip; `releasePairing()` says why). One store per test file. */
export async function regsStore(): Promise<RegsStore | null> {
  const at = await releasePairing();
  return at.path === null ? null : (await storeAt(at.path)).store;
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
    const opened = existsSync(`${dir}/manifest.json`) ? await storeAt(dir) : null;
    fixture = opened === null ? `no regs fixture release at ${dir}: run pnpm -C app regs-fixture`
      : opened.store === null ? opened.reason
      : fixtureVerdict(opened.store.meta, facts.store, facts.provinceAnswers());
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
  return s.dir === null ? null : (await storeAt(s.dir)).store;
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
 * " · <key> not compared (<until>)" for each keyed one, and " · lines /<re>/ dropped from <key> (<until>)" for each entry that
 * drops golden lines from a key it still compares.
 */
export function summary(block: Block, passed: number, total: number): string {
  const ids = Object.keys(tapsOnce().states).filter((id) => excluded(block, id) !== null);
  const keyed = EXCLUDE.filter((e) => e.block === block && e.key);
  return `${block} parity: ${passed}/${total} states equal`
    + (ids.length ? ` · ${ids.length} excluded (${ids.join(", ")})` : "")
    + keyed.map((e) => e.lines
      ? ` · lines /${e.lines.source}/ dropped from ${e.key!.source} in ${e.ids === "*" ? "every state" : `${e.ids.length} states`} (${e.until ?? "permanent"})`
      : ` · ${e.key!.source} not compared (${e.until ?? "permanent"})`).join("");
}

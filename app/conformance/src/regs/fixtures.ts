/**
 * The frozen pages the RN render tests mount (jsdom cannot open a store): one `RegsPage` per tab-worthy state, its
 * lazy `sheet` function replaced by every sheet the committed file already holds (a new state: every sheet key its page
 * names, `sheetKeysOf`). Written by `UPDATE_REGS_FIXTURES=1`, checked otherwise (harness.parity.test.ts). The page is
 * `buildRegsPage` over the parity store's inputs — never hand-edited. (T439: the sheet keys no longer come from golden.json.)
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { buildRegsPage, parseSheetKey, type RegsPage, type SheetKey, type SheetVM, type Who } from "@app/core";
import { loadRegsInputs } from "@app/data/regs";
import { ADULT_RESIDENT, baseUnnamedStream, inputsOf, parityStore, requestOf } from "./golden";
import { REPO } from "./oracles";

export const FIXTURE_STATES = ["vedder_1010", "vedder_615", "vedder_515", "klwa_1010", "bc1_501", "glenn_501"] as const;
export const FIXTURE_DIR = `${REPO}app/packages/ui-native/src/regs/fixtures`;
export const UPDATE_HINT = "run UPDATE_REGS_FIXTURES=1 pnpm -C app exec vitest run conformance/src/regs/harness";

export type FrozenRegsPage = Omit<RegsPage, "sheet"> & { readonly sheets: Readonly<Record<string, SheetVM | null>> };

/** The page minus `sheet`, plus `sheets`: each key (deduplicated, sorted) through `page.sheet(key)`. */
export function freezePage(page: RegsPage, sheetKeys: readonly string[]): FrozenRegsPage {
  const { sheet, ...rest } = page;
  return { ...rest, sheets: Object.fromEntries([...new Set(sheetKeys)].sort().map((k) => [k, sheet(k as SheetKey)])) };
}

/** JSON with every object's keys sorted, 1-space indent; a non-finite number or a function is refused (a VM prints words). */
export function stableJson(v: unknown): string {
  const sort = (o: unknown, path: string): unknown => {
    if (typeof o === "number" && !Number.isFinite(o)) throw new Error(`stableJson: ${o} at ${path || "(root)"}`);
    if (typeof o === "function") throw new Error(`stableJson: a function at ${path || "(root)"}`);
    if (Array.isArray(o)) return o.map((x, i) => sort(x, `${path}[${i}]`));
    if (o && typeof o === "object")
      return Object.fromEntries(Object.keys(o).sort().map((k) => [k, sort((o as Record<string, unknown>)[k], path ? `${path}.${k}` : k)]));
    return o;
  };
  return `${JSON.stringify(sort(v, ""), null, 1)}\n`;
}

/** Every sheet key a page names anywhere in its view model (the keys its presses open). */
export function sheetKeysOf(page: RegsPage): string[] {
  const out = new Set<string>(["hatch", "pos:pos"]);
  const walk = (v: unknown): void => {
    if (typeof v === "string") { if (parseSheetKey(v)) out.add(v); return; }
    if (Array.isArray(v)) { v.forEach(walk); return; }
    if (v && typeof v === "object") for (const [k, x] of Object.entries(v)) if (k !== "sheet") walk(x);
  };
  walk(page);
  return [...out];
}

/** The sheet keys of a state's committed fixture file `file` (the set it was frozen with), else the page's own. */
function frozenKeys(file: string, page: RegsPage): string[] {
  return existsSync(file) ? Object.keys((JSON.parse(readFileSync(file, "utf8")) as { sheets: Record<string, unknown> }).sheets) : sheetKeysOf(page);
}

/** The frozen page of one fixture state, as JSON text. */
export async function frozenJson(id: string, file = `${FIXTURE_DIR}/${id}.json`): Promise<string> {
  const inp = await inputsOf(id);
  if (!inp) throw new Error(`frozen page ${id}: no paired regs store`);
  const page = buildRegsPage(inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
  return stableJson(freezePage(page, frozenKeys(file, page)));
}

/**
 * (T35) The states whose Licence tab the render test mounts for all six anglers (`licence["<age>|<res>"]`):
 * vedder_1010, x5p1_1010 (a classified water — the class box, D17) and klmb_1010 (the not-yet-mapped box).
 * Between them they draw every licence block class.
 */
export const LICENCE_FIXTURE_STATES = ["vedder_1010", "x5p1_1010", "klmb_1010"] as const;
export const LICENCE_FIXTURE_PROFILES = ["adult|resident", "adult|non_resident", "adult|non_resident_alien", "u16|resident", "u16|non_resident",
  "u16|non_resident_alien"] as const;

/** (T35) `licence_<id>.json`: "<age>|<res>" → that angler's `LicenceVM` (buildRegsPage with that `who`), as JSON text. */
export async function licenceJson(id: string): Promise<string> {
  const inp = await inputsOf(id);
  if (!inp) throw new Error(`frozen licence ${id}: no paired regs store`);
  return stableJson(Object.fromEntries(LICENCE_FIXTURE_PROFILES.map((k) => {
    const [age, residency] = k.split("|") as [Who["age"], Who["residency"]];
    return [k, buildRegsPage(inp, { today: requestOf(id).today, who: { age, residency }, pid: id }).licence];
  })));
}

/**
 * (T42) The edge states the render test mounts (Edges.test.tsx): tidal water (x31p_1010) and the hours window's timed
 * caveat (x15p2_1010) — each `edge_<id>.json`, a frozen page as above — and an unnamed section
 * (`edge_unnamed.json`, golden.ts `baseUnnamedStream`: taps.json's first sampled unnamed stream on its region's base —
 * chosen by that property, since the sample's handles move with every build; no sheets). Outside B.C. needs no store (core
 * `buildRegsPage` over no key).
 */
export const EDGE_FIXTURE_STATES = ["x31p_1010", "x15p2_1010"] as const;

/**
 * (T40) The style-token gate's states (check.mjs `r20c-style-tokens`: vedder_1010, x35p_1010, klwa_1012, glenn_615 — the
 * Keep tab of each): `<id>.json`, a frozen page as above, beside the fixture states it does not already share.
 */
export const STYLE_FIXTURE_STATES = ["x35p_1010", "klwa_1012", "glenn_615"] as const;
export async function unnamedJson(): Promise<string> {
  const st = await parityStore();
  if (!st) throw new Error("frozen unnamed page: no paired regs store");
  const { handle } = await baseUnnamedStream(st);
  const inp = await loadRegsInputs(st, { kind: "section", handle }, { month: 10, day: 10 }, "Saturday", 2026);
  return stableJson(freezePage(buildRegsPage(inp, { today: { month: 10, day: 10 }, who: ADULT_RESIDENT }), []));
}

/** UPDATE_REGS_FIXTURES=1: write <FIXTURE_DIR>/<id>.json for each state; otherwise every committed file must equal a fresh one. */
export async function syncFixtures(env: Readonly<Record<string, string | undefined>> = process.env): Promise<void> {
  const write = env.UPDATE_REGS_FIXTURES === "1";
  if (write) mkdirSync(FIXTURE_DIR, { recursive: true });
  const stale: string[] = [];
  const files: [string, string, () => Promise<string>][] = [
    ...FIXTURE_STATES.map((id): [string, string, () => Promise<string>] => [id, `${FIXTURE_DIR}/${id}.json`, () => frozenJson(id)]),
    ...LICENCE_FIXTURE_STATES.map((id): [string, string, () => Promise<string>] => [`licence_${id}`, `${FIXTURE_DIR}/licence_${id}.json`, () => licenceJson(id)]),
    ...EDGE_FIXTURE_STATES.map((id): [string, string, () => Promise<string>] => [`edge_${id}`, `${FIXTURE_DIR}/edge_${id}.json`, () => frozenJson(id, `${FIXTURE_DIR}/edge_${id}.json`)]),
    ["edge_unnamed", `${FIXTURE_DIR}/edge_unnamed.json`, () => unnamedJson()],
    ...STYLE_FIXTURE_STATES.map((id): [string, string, () => Promise<string>] => [id, `${FIXTURE_DIR}/${id}.json`, () => frozenJson(id)]),
  ];
  for (const [id, path, make] of files) {
    const fresh = await make();
    if (write) writeFileSync(path, fresh);
    else if (!existsSync(path) || readFileSync(path, "utf8") !== fresh) stale.push(id);
  }
  if (stale.length) throw new Error(`stale regs VM fixtures: ${stale.join(", ")} — ${UPDATE_HINT}`);
}

/**
 * The frozen golden pages the RN render tests mount (jsdom cannot open a store): one `RegsPage` per tab-worthy state, its
 * lazy `sheet` function replaced by every sheet the golden state can open. Written by `UPDATE_REGS_FIXTURES=1`, checked
 * otherwise (harness.parity.test.ts). The page is `buildRegsPage` over the parity store's inputs — never hand-edited.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { buildRegsPage, type RegsPage, type SheetKey, type SheetVM } from "@app/core";
import { ADULT_RESIDENT, inputsOf, requestOf } from "./golden";
import { golden, REPO } from "./oracles";

export const FIXTURE_STATES = ["vedder_1010", "vedder_615", "vedder_515", "klwa_1010", "bc1_501", "glenn_501"] as const;
export const FIXTURE_DIR = `${REPO}app/packages/ui-native/src/regs/fixtures`;
export const UPDATE_HINT = "run UPDATE_REGS_FIXTURES=1 pnpm -C app exec vitest run conformance/src/regs/harness";

export type FrozenRegsPage = Omit<RegsPage, "sheet"> & { readonly sheets: Readonly<Record<string, SheetVM | null>> };

/**
 * A golden sheet key → the app's (contracts §6 #1: the app's keys carry no state id). `licsrc:lic|<id>|adult` →
 * `licsrc:adult`; `other:<id>|o0` → `other:o0`; `srcs:gl|<id>|hook` → `srcs:gl|hook`; `srcs:bait|<id>` → `srcs:bait`.
 * Key plumbing only, until T36's `appKeyOf` (core sheetKey.ts) replaces it.
 */
export function appSheetKey(goldenKey: string, id: string): string {
  const lic = new RegExp(`^licsrc:lic\\|${id}\\|(.+)$`).exec(goldenKey);
  if (lic) return `licsrc:${lic[1]}`;
  return goldenKey.replace(`${id}|`, "").replace(`|${id}`, "");
}

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

/** The frozen page of one fixture state, as JSON text. */
export async function frozenJson(id: string): Promise<string> {
  const inp = await inputsOf(id);
  if (!inp) throw new Error(`frozen page ${id}: no paired regs store`);
  const page = buildRegsPage(inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
  return stableJson(freezePage(page, Object.keys(golden().states[id]!.sheets).map((k) => appSheetKey(k, id))));
}

/** UPDATE_REGS_FIXTURES=1: write <FIXTURE_DIR>/<id>.json for each state; otherwise every committed file must equal a fresh one. */
export async function syncFixtures(env: Readonly<Record<string, string | undefined>> = process.env): Promise<void> {
  const write = env.UPDATE_REGS_FIXTURES === "1";
  if (write) mkdirSync(FIXTURE_DIR, { recursive: true });
  const stale: string[] = [];
  for (const id of FIXTURE_STATES) {
    const path = `${FIXTURE_DIR}/${id}.json`, fresh = await frozenJson(id);
    if (write) writeFileSync(path, fresh);
    else if (!existsSync(path) || readFileSync(path, "utf8") !== fresh) stale.push(id);
  }
  if (stale.length) throw new Error(`stale regs VM fixtures: ${stale.join(", ")} — ${UPDATE_HINT}`);
}

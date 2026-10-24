/**
 * The decoded rule inputs T06's parity runs on, from two places:
 *
 * - `pageSource()` — ALWAYS present: page_v36.html's own `#data` (export slice: rules, rid, species, waters) and
 *   `#answers` (`sections.display.rules`), i.e. exactly what port/hydrate.mjs fed page v36 when it wrote the oracle
 *   (the data blocks are unchanged since the oracle's ref; only page code changed). 726 rules; a part's via is its
 *   `rules [[ix, via]]` list.
 * - `storeSource()` — the live RELEASE when this machine has one (data/release/r-*, every rule), read through
 *   `@app/data/regs` `openRegsStore(loader)` exactly as the app reads it (T03/T04 replaced T06's stopgap that opened
 *   regs.sqlite with node:sqlite): `rules()`, `statics().exportSpecies`, and a state's via from its `akey`'s ruleset.
 *   Absent: null, and the store-backed tests report themselves skipped.
 *
 * And one oracle for inputs hydrate.json does not cover: `pageDecode(src)` runs page v36's OWN decode layer
 * (page_v36.js up to `function segBar`, as port/hydrate.mjs does) over a source's rules, so a store-backed case is
 * checked against the page's code, not against a re-reading of it.
 */
import { readFileSync } from "node:fs";
import type { ExportRule, RuleFacts, SpeciesBook } from "@app/core";
import { openRegsStore, type RegsLoader } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";
import { REPO } from "./oracles";

const REF = `${REPO}app/design/regs-v11/reference/`;
const PAGE = `${REF}page_v36.html`;

export interface RuleSource {
  readonly name: string;
  readonly stored: ReadonlyMap<number, { readonly export: ExportRule; readonly facts: RuleFacts | null }>;
  readonly book: SpeciesBook;
  /** The part's ruleset for a taps.json state (its reach and trib lists). */
  ruleset(state: TapState): { readonly reach: readonly number[]; readonly trib: readonly number[] };
}
/** `goldenPartIx`: the prototype's index where the release renumbered the part (taps.py PART_MOVED; T115) */
export interface TapState { readonly item: string; readonly partIx: number; readonly akey: number; readonly goldenPartIx?: number }

interface PageData {
  readonly rules: Readonly<Record<string, ExportRule>>; readonly rid: Readonly<Record<string, string>>; readonly species: SpeciesBook;
  readonly waters: readonly { readonly id: string; readonly parts: readonly ({ readonly rules: readonly (readonly [number, "reach" | "trib"])[] } | null)[] }[];
}

export function pageSource(): RuleSource {
  const html = readFileSync(PAGE, "utf8");
  const block = (id: string): unknown => {
    const m = html.match(new RegExp(`<script[^>]*id="${id}"[^>]*>([\\s\\S]*?)</script>`));
    if (!m) throw new Error(`page_v36.html has no #${id}`);
    return JSON.parse(m[1]!);
  };
  const D = block("data") as PageData;
  const A = block("answers") as { sections: { display: { rules: Readonly<Record<string, RuleFacts>> } } };
  const stored = new Map<number, { export: ExportRule; facts: RuleFacts | null }>();
  for (const [ix, k] of Object.entries(D.rid)) stored.set(+ix, { export: D.rules[k]!, facts: A.sections.display.rules[ix] ?? null });
  return {
    name: "page_v36.html", stored, book: D.species,
    ruleset(t) {
      const w = D.waters.find((x) => x.id === t.item);
      if (!w) throw new Error(`page_v36.html has no water ${t.item}`);
      // the page's parts are the PROTOTYPE's: a part the release renumbered (taps.py PART_MOVED) is read at its golden index
      const rules = w.parts[t.goldenPartIx ?? t.partIx]?.rules ?? [];
      return { reach: rules.filter(([, v]) => v !== "trib").map(([ix]) => ix), trib: rules.filter(([, v]) => v === "trib").map(([ix]) => ix) };
    },
  };
}

/**
 * The rules of a RELEASE, read the way the app reads them (T03/T04): `openRegsStore(loader)` → `rules()` (rule +
 * display_rule, every blob through `unpackBlob` with its table's dictionary), `statics().exportSpecies`, and a state's
 * via from the ruleset of its `akey`. Default: the live release on this machine (`liveRelease()`; none: null, and the
 * store-backed tests report themselves skipped). Rejects with RegsStoreError on a release this app refuses and
 * RegsBlobError on a bad blob.
 */
export async function storeSource(loader: RegsLoader | null = defaultLoader()): Promise<RuleSource | null> {
  if (!loader) return null;
  const st = await openRegsStore(loader);
  const [rules, statics, water] = await Promise.all([st.rules(), st.statics(), st.water()]);
  const stored = new Map<number, { export: ExportRule; facts: RuleFacts | null }>();
  for (const [rix, r] of rules) stored.set(rix, { export: r.export as ExportRule, facts: r.facts });
  // RuleSource.ruleset is synchronous (the parity loops call it per state): every key's ruleset, read once
  const sets = new Map<number, { reach: readonly number[]; trib: readonly number[] }>();
  for (const r of await water.all("SELECT akey, ruleset FROM akey")) sets.set(Number(r.akey), await st.ruleset(Number(r.ruleset)));
  return {
    name: `release ${st.meta.release}`, stored, book: statics.exportSpecies,
    ruleset(t) {
      const r = sets.get(t.akey);
      if (!r) throw new Error(`release ${st.meta.release}: no ruleset for akey ${t.akey}`);
      return { reach: r.reach, trib: r.trib };
    },
  };
}

const defaultLoader = (): RegsLoader | null => {
  const dir = liveRelease();
  return dir ? openReleaseDir(dir) : null;
};

/** What page v36's RULES decode leaves on a rule (the fields describe.parity compares). */
export interface PageRule {
  readonly ix: number; readonly key: string; readonly k: string | null; readonly baseRank: number;
  readonly species: readonly string[]; readonly speciesExcept: readonly string[];
  readonly wins: readonly (readonly [number, number])[]; readonly tcond: string; readonly part: string | null; readonly notes: readonly string[];
}

/**
 * page v36's own decode over `src`'s rules: page_v36.js (this checkout's copy) up to `function segBar`, with
 * calendar.generated.js ahead of it, fed a `#data`/`#answers` built from `src` (rule key = page_data.py's
 * `entry_id::rule_id`; no waters). Returns the page's decoded rules by rix and its describe().
 */
export function pageDecode(src: RuleSource): { readonly rules: ReadonlyMap<number, PageRule>; describe(rix: number): string } {
  const rules: Record<string, ExportRule> = {}, rid: Record<string, string> = {}, dr: Record<string, RuleFacts> = {};
  for (const [ix, s] of src.stored) {
    const k = `${s.export.entry_id}::${s.export.rule_id}`;
    rules[k] = structuredClone(s.export) as ExportRule; rid[ix] = k;   // the page writes onto its rules: never onto src's
    if (s.facts) dr[ix] = s.facts;
  }
  const blocks: Record<string, unknown> = {
    data: { about: {}, rules, rid, species: src.book, waters: [] },
    answers: { about: { format: "answers/2" }, sections: { display: { rules: dr, waters: {} } } },
    cases: { about: {} },
  };
  const js = readFileSync(`${REF}page_v36.js`, "utf8").split("\n");
  const end = js.findIndex((l) => /^function segBar\(/.test(l));
  if (end < 0) throw new Error("page_v36.js: no `function segBar` — the decode layer moved; update pageDecode (and port/hydrate.mjs)");
  const body = `${readFileSync(`${REF}calendar.generated.js`, "utf8")}\n${js.slice(0, end).join("\n")}\nreturn { RULES, describe };`;
  const P = new Function("document", body)({ getElementById: (id: string) => ({ textContent: JSON.stringify(blocks[id]) }) }) as
    { RULES: Record<string, PageRule>; describe(r: PageRule): string };
  const byIx = new Map<number, PageRule>();
  for (const r of Object.values(P.RULES)) byIx.set(r.ix, r);
  return {
    rules: byIx,
    describe(rix) {
      const r = byIx.get(rix);
      if (!r) throw new Error(`pageDecode: no rule ${rix}`);
      return P.describe(r);
    },
  };
}

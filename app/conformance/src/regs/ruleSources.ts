/**
 * The decoded rule inputs T06's parity runs on, from two places (T04's store reads and T05's `provinceStore()` replace
 * the second when they land):
 *
 * - `pageSource()` — ALWAYS present: page_v36.html's own `#data` (export slice: rules, rid, species, waters) and
 *   `#answers` (`sections.display.rules`), i.e. exactly what port/hydrate.mjs fed page v36 when it wrote the oracle
 *   (the data blocks are unchanged since the oracle's ref; only page code changed). 726 rules; a part's via is its
 *   `rules [[ix, via]]` list.
 * - `storeSource()` — the built regs store (`data/generated/bundle/regs.sqlite`, all 3,406 rules) when it is on this
 *   machine: `rule.j`, `display_rule.j`, `static export.species`, and a state's via from `ruleset.j` of its `akey`.
 *   Looked for at $REGS_STORE, then this checkout's data/, then (in a `.claude/worktrees/<name>` checkout) the main
 *   checkout's data/ — read-only. Absent: null, and the store-backed tests report themselves skipped.
 *   Blobs go through `@app/data/regs` `unpackBlob` (T02's codec, `meta.blob_codec` "zdict") with the table's `zdict.d`
 *   or null — so a 0x02 blob in a table with no dictionary, an empty blob or non-JSON bytes throw RegsBlobError.
 *
 * And one oracle for inputs hydrate.json does not cover: `pageDecode(src)` runs page v36's OWN decode layer
 * (page_v36.js up to `function segBar`, as port/hydrate.mjs does) over a source's rules, so a store-backed case is
 * checked against the page's code, not against a re-reading of it.
 */
import { existsSync, readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import type { ExportRule, RuleFacts, SpeciesBook } from "@app/core";
import { unpackBlob } from "@app/data/regs";

const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const HYDRATE = `${REPO}app/design/regs-v11/port/golden/hydrate.json`;
export const TAPS = `${REPO}app/design/regs-v11/port/golden/taps.json`;
const REF = `${REPO}pipeline/deliver/answers/reference/`;
const PAGE = `${REF}page_v36.html`;

export interface RuleSource {
  readonly name: string;
  readonly stored: ReadonlyMap<number, { readonly export: ExportRule; readonly facts: RuleFacts | null }>;
  readonly book: SpeciesBook;
  /** The part's ruleset for a taps.json state (its reach and trib lists). */
  ruleset(state: TapState): { readonly reach: readonly number[]; readonly trib: readonly number[] };
}
export interface TapState { readonly item: string; readonly partIx: number; readonly akey: number }

export interface OracleMeta { readonly generator: string; readonly ref: string; readonly sha: string; readonly golden: string; readonly version: string }
export interface Hydrate {
  readonly meta: OracleMeta;
  readonly describe: Readonly<Record<string, string>>;
  readonly rules: Readonly<Record<string, Readonly<Record<string, unknown>>>>;
  readonly states: Readonly<Record<string, { readonly via: Readonly<Record<string, "trib">>; readonly ranks: Readonly<Record<string, number>> }>>;
}
export const hydrate = (): Hydrate => JSON.parse(readFileSync(HYDRATE, "utf8")) as Hydrate;
export const taps = (): { readonly meta: OracleMeta; readonly states: Readonly<Record<string, TapState>> } => JSON.parse(readFileSync(TAPS, "utf8"));

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
      const rules = w.parts[t.partIx]?.rules ?? [];
      return { reach: rules.filter(([, v]) => v !== "trib").map(([ix]) => ix), trib: rules.filter(([, v]) => v === "trib").map(([ix]) => ix) };
    },
  };
}

function storePath(): string | null {
  const here = `${REPO}data/generated/bundle/regs.sqlite`;
  const main = /\/\.claude\/worktrees\/[^/]+\/$/.test(REPO) ? `${REPO.replace(/\.claude\/worktrees\/[^/]+\/$/, "")}data/generated/bundle/regs.sqlite` : null;
  for (const p of [process.env.REGS_STORE, here, main]) if (p && existsSync(p)) return p;
  return null;
}

/** The regs store at `path` (default: the one found on this machine; none: null). Throws RegsBlobError on a bad blob. */
export function storeSource(path: string | null = storePath()): RuleSource | null {
  if (!path) return null;
  const db = new DatabaseSync(path, { readOnly: true });
  const dicts = new Map<string, Uint8Array>();
  for (const r of db.prepare("SELECT tbl, d FROM zdict").all() as { tbl: string; d: Uint8Array }[]) dicts.set(r.tbl, r.d);
  const unpack = (tbl: string, b: Uint8Array): unknown => unpackBlob(b, dicts.get(tbl) ?? null);
  const facts = new Map<number, RuleFacts>();
  for (const r of db.prepare("SELECT id, j FROM display_rule").all() as { id: number; j: Uint8Array }[]) facts.set(r.id, unpack("display_rule", r.j) as RuleFacts);
  const stored = new Map<number, { export: ExportRule; facts: RuleFacts | null }>();
  for (const r of db.prepare("SELECT rix, j FROM rule").all() as { rix: number; j: Uint8Array }[])
    stored.set(r.rix, { export: unpack("rule", r.j) as ExportRule, facts: facts.get(r.rix) ?? null });
  const sp = db.prepare("SELECT j FROM static WHERE k = 'export.species'").get() as { j: Uint8Array };
  const book = unpack("static", sp.j) as SpeciesBook;
  const rsOf = db.prepare("SELECT r.j AS j FROM akey a JOIN ruleset r ON r.set_id = a.ruleset WHERE a.akey = ?");
  return {
    name: path, stored, book,
    ruleset(t) {
      const r = rsOf.get(t.akey) as { j: Uint8Array } | undefined;
      if (!r) throw new Error(`regs store: no ruleset for akey ${t.akey}`);
      const j = unpack("ruleset", r.j) as { reach: number[]; trib?: number[] };
      return { reach: j.reach, trib: j.trib ?? [] };   // the store leaves out an empty trib list (T04: `trib ?? []`)
    },
  };
}

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

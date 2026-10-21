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
 *
 * Blobs are inflated with node:zlib (raw DEFLATE with the table's preset dictionary = T02's codec, `meta.blob_codec`
 * "zdict"); `@app/data` does not export its codec yet (T03/T04).
 */
import { existsSync, readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { inflateRawSync } from "node:zlib";
import type { ExportRule, RuleFacts, SpeciesBook } from "@app/core";

const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const HYDRATE = `${REPO}app/design/regs-v11/port/golden/hydrate.json`;
export const TAPS = `${REPO}app/design/regs-v11/port/golden/taps.json`;
const PAGE = `${REPO}pipeline/deliver/answers/reference/page_v36.html`;

export interface RuleSource {
  readonly name: string;
  readonly stored: ReadonlyMap<number, { readonly export: ExportRule; readonly facts: RuleFacts | null }>;
  readonly book: SpeciesBook;
  /** The part's ruleset for a taps.json state (its reach and trib lists). */
  ruleset(state: TapState): { readonly reach: readonly number[]; readonly trib: readonly number[] };
}
export interface TapState { readonly item: string; readonly partIx: number; readonly akey: number }

export interface Hydrate {
  readonly meta: { readonly ref: string; readonly version: string };
  readonly describe: Readonly<Record<string, string>>;
  readonly rules: Readonly<Record<string, Readonly<Record<string, unknown>>>>;
  readonly states: Readonly<Record<string, { readonly via: Readonly<Record<string, "trib">>; readonly ranks: Readonly<Record<string, number>> }>>;
}
export const hydrate = (): Hydrate => JSON.parse(readFileSync(HYDRATE, "utf8")) as Hydrate;
export const taps = (): { readonly states: Readonly<Record<string, TapState>> } => JSON.parse(readFileSync(TAPS, "utf8"));

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

export function storeSource(): RuleSource | null {
  const path = storePath();
  if (!path) return null;
  const db = new DatabaseSync(path, { readOnly: true });
  const dicts = new Map<string, Buffer>();
  for (const r of db.prepare("SELECT tbl, d FROM zdict").all() as { tbl: string; d: Uint8Array }[]) dicts.set(r.tbl, Buffer.from(r.d));
  const unpack = (tbl: string, b: Uint8Array): unknown => {
    const buf = Buffer.from(b);
    const head = buf[0];
    const bytes = head === 0x01 ? inflateRawSync(buf.subarray(1))
      : head === 0x02 ? inflateRawSync(buf.subarray(1), { dictionary: dicts.get(tbl)! })
      : buf;
    return JSON.parse(bytes.toString("utf8"));
  };
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

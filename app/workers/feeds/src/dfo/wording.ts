// T321 port of pipeline/regs/dfo_salmon/wording.py: DFO's limits wording as a CLOSED REGISTRY. Every clause of a limits
// cell must be one DFO has printed before (data/curated/dfo_salmon/wording.json — the ONE file both readers load; this
// one bundles it at build). A clause the registry does not hold is NEW WORDING: the row's rules carry review_reason
// "new wording: '…'" (no number in the app), the card shows the row in DFO's words, and the owner is emailed URGENTLY,
// once per new template (live.ts `wordingNotices`, email.ts). Parity with the Python: test/dfo.wording.test.ts.
import doc from "../../../../../data/curated/dfo_salmon/wording.json";
import { repr, pyRe, split, strip, sub } from "./py";
import { unreadReason } from "./typed";

export const FORMAT = "dfo-wording/1";
export const NEW_WORDING = "new wording: ";
export const REGISTRY_PATH = "data/curated/dfo_salmon/wording.json";

export interface WordingEntry {
  template: string;
  kind: "structure" | "note" | "provenance";
  reads: string[];
  status: "proposed" | "accepted" | "rejected";
  rows?: number;
  pages?: number;
  example?: string;
  cell?: string;
}
export type Registry = Map<string, WordingEntry>;

const RE_SPLIT = pyRe("[;,]|\\s[–—-]\\s|\\.\\s");
const RE_FN = pyRe("\\bFN\\s*\\d+", "i");
const RE_NUM = pyRe("\\d+(?:[.:]\\d+)?");
const RE_WS = pyRe("\\s+");

/** wording.template: numbers → N, "FN 1281" → FN#, spaces squashed, lower case, " .;" trimmed */
export function template(clause: string): string {
  let c = sub(RE_FN, "FN#", clause);
  c = sub(RE_NUM, "N", c);
  return strip(sub(RE_WS, " ", c), " .;").toLowerCase();
}

/** wording.clauses: the cell split at ";", ",", a spaced dash and ". " */
export const clauses = (cell: string): string[] => split(RE_SPLIT, cell || "").map((c) => strip(c)).filter((c) => c);

export function loadRegistry(d: { format?: string; entries: WordingEntry[] }): Registry {
  if (!d || d.format !== FORMAT || !Array.isArray(d.entries)) throw new Error(`not a ${FORMAT} registry`);
  return new Map(d.entries.map((e) => [e.template, e]));
}
/** the bundled registry */
export const REGISTRY: Registry = loadRegistry(doc as { format: string; entries: WordingEntry[] });

/** The registry from the R2 copy (priv `wording.json`, put there by deploy/staging/sync-inputs.sh), or the compiled-in
 *  one with the reason it fell back. Read once per run: a missing / invalid copy never stops the run. */
export function readRegistry(text: string | null): { reg: Registry; warning?: string } {
  if (text === null) return { reg: REGISTRY, warning: "wording.json missing in R2: using the compiled-in registry" };
  try {
    return { reg: loadRegistry(JSON.parse(text)) };
  } catch (e) {
    return { reg: REGISTRY, warning: `wording.json in R2 invalid (${e instanceof Error ? e.message : String(e)}): using the compiled-in registry` };
  }
}

const known = (t: string, reg: Registry): WordingEntry | null => {
  const e = reg.get(t);
  return e && e.status !== "rejected" ? e : null;
};

/** wording.unknown: the clauses DFO has not used before, as printed, once each */
export const unknown = (cell: string, reg: Registry = REGISTRY): string[] =>
  [...new Set(clauses(cell).filter((c) => !known(template(c), reg)))];

/** wording.notes: the clauses whose meaning is a note in DFO's words */
export const notes = (cell: string, reg: Registry = REGISTRY): string[] =>
  clauses(cell).filter((c) => known(template(c), reg)?.kind === "note");

/** wording.reason: "" or the rule's review_reason, "new wording: '…'; '…'" */
export function reason(cell: string, reg: Registry = REGISTRY): string {
  const n = unknown(cell, reg);
  return n.length ? NEW_WORDING + n.map((c) => repr(c)).join("; ") : "";
}

/** A row the card shows in DFO's words (wording.row_words): k new | unread | note; species, dates, limits as printed. */
export interface RowWords {
  k: "new" | "unread" | "note";
  sp: string;
  d: string;
  t: string;
  new?: string[];
}
export function rowWords(rec: { species?: string; dates?: string; limits_gear?: string }, reg: Registry = REGISTRY): RowWords | null {
  const cell = strip(rec.limits_gear || "");
  const out = { sp: strip(rec.species || ""), d: strip(rec.dates || ""), t: cell };
  const n = unknown(cell, reg);
  if (n.length) return { k: "new", ...out, new: n };
  if (unreadReason(cell)) return { k: "unread", ...out };
  if (notes(cell, reg).length) return { k: "note", ...out };
  return null;
}


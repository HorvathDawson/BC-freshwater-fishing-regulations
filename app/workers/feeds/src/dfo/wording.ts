// T321 port of pipeline/regs/dfo_salmon/wording.py: DFO's limits wording as a CLOSED REGISTRY. Every clause of a limits
// cell must be one DFO has printed before (data/curated/dfo_salmon/wording.json — the ONE file both readers load; the
// Worker reads the copy `pipeline.deliver.sync_inputs` puts at R2 private/feeds/dfo/wording.json, and nothing else:
// T324, owner 2026-11-05 "one copy, fail loud" — no compiled-in copy, a missing / invalid one FAILS the DFO job). A clause the registry does not hold is NEW WORDING: the row's rules carry review_reason
// "new wording: '…'" (no number in the app), the card shows the row in DFO's words, and the owner is emailed URGENTLY,
// once per new template (live.ts `wordingNotices`, email.ts). Parity with the Python: test/dfo.wording.test.ts.
import { finditer, lstrip, repr, pyRe, strip, sub } from "./py";
import { unreadReason } from "./typed";

// T332: /2 — the clause split changed; a registry of another format in R2 is refused (T324: the DFO job fails and
// reports, the app keeps the last good feed) — never read with the wrong split, which would flag every clause as new
export const FORMAT = "dfo-wording/2";
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

const RE_SPLIT = pyRe("[;,]|\\s[–—-]\\s|\\.\\s(?![a-z])");
/** (T332) wording._TITLE: a lettered section title ("F", "F. Fraser River Watershed") — its dot and dash are no break */
const RE_TITLE = pyRe("[A-Z](?:\\.\\s+[A-Z][A-Za-z ]*)?");
/** (T332) wording._GLUED: DFO's comma in "Single, barbless hook …" */
const RE_GLUED = pyRe("single", "i");
/** (T332) wording._SPECIES_LIST: the rest of a species list stays with the clause before it */
const RE_SPECIES_LIST = pyRe("(?:chinook|coho|sockeye|pink|chum)(?:\\s+(?:or|and)\\s+(?:chinook|coho|sockeye|pink|chum))*(?:\\s+salmon)?", "i");
const DASHES = "–—-";
const RE_FN = pyRe("\\bFN\\s*\\d+", "i");
const RE_NUM = pyRe("\\d+(?:[.:]\\d+)?");
const RE_WS = pyRe("\\s+");

/** wording.template: numbers → N, "FN 1281" → FN#, spaces squashed, lower case, " .;" trimmed */
export function template(clause: string): string {
  let c = sub(RE_FN, "FN#", clause);
  c = sub(RE_NUM, "N", c);
  return strip(sub(RE_WS, " ", c), " .;").toLowerCase();
}

const clean = (c: string) => strip(lstrip(strip(c), DASHES));

/** wording.clauses: the cell split at ";", ",", a spaced dash and ". " — not inside a lettered section title, not after
 *  "Single,", not before a lower-case word; a species list stays with its clause; leading dashes dropped (T332) */
export function clauses(cell: string): string[] {
  const s = cell || "";
  const spans: [number, number][] = [];
  let start = 0;
  for (const m of finditer(RE_SPLIT, s)) {
    const head = strip(s.slice(start, m.index));
    if (RE_TITLE.full.test(head) || (m[0] === "," && RE_GLUED.full.test(head))) continue;
    spans.push([start, m.index]);
    start = m.index + m[0].length;
  }
  spans.push([start, s.length]);
  const out: [number, number][] = [];
  for (const [a, b] of spans) {
    const c = clean(s.slice(a, b));
    if (!c) continue;
    if (out.length && RE_SPECIES_LIST.full.test(strip(c, " ."))) { out[out.length - 1]![1] = b; continue; }
    out.push([a, b]);
  }
  return out.map(([a, b]) => clean(s.slice(a, b)));
}

export function loadRegistry(d: { format?: string; entries: WordingEntry[] }): Registry {
  if (!d || d.format !== FORMAT || !Array.isArray(d.entries)) throw new Error(`not a ${FORMAT} registry`);
  return new Map(d.entries.map((e) => [e.template, e]));
}
/** The registry from THE copy, R2 private/feeds/dfo/wording.json (put there by `pipeline.deliver.sync_inputs` through
 *  deploy/staging/sync-inputs.sh: the Mac, by hand or the review app after a save — T520). Read once per run. Missing or invalid THROWS
 *  (T324): the DFO job fails and is reported; nothing is typed with a registry nobody reviewed. */
export function readRegistry(text: string | null): Registry {
  if (text === null)
    throw new Error(`wording registry missing: no R2 private/feeds/dfo/wording.json — sync it (deploy/staging/sync-inputs.sh, pipeline.deliver.sync_inputs) from ${REGISTRY_PATH}`);
  try {
    return loadRegistry(JSON.parse(text));
  } catch (e) {
    throw new Error(`wording registry invalid: R2 private/feeds/dfo/wording.json (${e instanceof Error ? e.message : String(e)}) — re-sync it from ${REGISTRY_PATH}`);
  }
}

/** (T549 item 6, wording._known) known = the owner ACCEPTED it: a `proposed` entry is the builder's guess at what a clause
 *  reads as, and reading it would clear the "new wording" guard before anyone had read the clause */
const known = (t: string, reg: Registry): WordingEntry | null => {
  const e = reg.get(t);
  return e && e.status === "accepted" ? e : null;
};

/** wording.unknown: the clauses DFO has not used before, as printed, once each */
export const unknown = (cell: string, reg: Registry): string[] =>
  [...new Set(clauses(cell).filter((c) => !known(template(c), reg)))];

/** wording.notes: the clauses whose meaning is a note in DFO's words */
export const notes = (cell: string, reg: Registry): string[] =>
  clauses(cell).filter((c) => known(template(c), reg)?.kind === "note");

/** wording.reason: "" or the rule's review_reason, "new wording: '…'; '…'" */
export function reason(cell: string, reg: Registry): string {
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
export function rowWords(rec: { species?: string; dates?: string; limits_gear?: string }, reg: Registry): RowWords | null {
  const cell = strip(rec.limits_gear || "");
  const out = { sp: strip(rec.species || ""), d: strip(rec.dates || ""), t: cell };
  const n = unknown(cell, reg);
  if (n.length) return { k: "new", ...out, new: n };
  if (unreadReason(cell)) return { k: "unread", ...out };
  if (notes(cell, reg).length) return { k: "note", ...out };
  return null;
}


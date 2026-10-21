/**
 * What the golden parity does NOT compare, each with its reason and what removes it (tests.md §1.2). An entry with no
 * `key` drops the whole state from `cases(block)`; a keyed entry drops only that key (a sheet key, or a dotted VM path
 * under the block) and the parity test asks `excluded(block, id, key)` before comparing it.
 */
import type { Block } from "@app/core/regs/testing";
import { golden } from "./oracles";

/** The states whose golden keep.strip.line carries a "Trout …" line — the strip part that words a row's STORED year strip
 *  (rows 30 `rows[].year`; compared since T13, no longer excluded). */
export const TROUT_LINE_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => ((s.keep as { strip?: { line?: { text: string }[] } } | null)?.strip?.line ?? []).some((l) => /^Trout /.test(l.text)))
  .map(([id]) => id);

/** The states whose golden header has no part label because the PROTOTYPE had no water map of the water (place null,
 *  action not "map"): the prototype's no-picker label is its water map's part name. The app (D1) frames every named part
 *  from the tiles, so T16's rule applies — the slot is empty only when the label is the water's own name. */
export const NO_MAP_LABEL_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => { const h = s.header as { place: unknown; action: { kind: string } | null } | null;
    return !!h && h.place === null && h.action?.kind !== "map"; })
  .map(([id]) => id);
/** The three Kitimat states the owner ruling 2026-10-21 (a) moves away from page v36 (card.parity.test.ts KITIMAT_RULED). */
export const KITIMAT_RULED_IDS: readonly string[] = ["x14p0_1010", "x14p1_1010", "x14p2_1010"];

/** Every golden Keep group line (`groups.N.lines`, and each tree kind's `lines`), state by state. */
const groupLines = (s: { keep?: unknown }): string[] => {
  const out: string[] = [];
  const walk = (ns: unknown): void => { for (const x of (ns as { lines?: string[]; children?: unknown }[] | null) ?? []) { out.push(...(x.lines ?? [])); walk(x.children); } };
  for (const g of ((s.keep as { groups?: { lines?: string[]; tree?: unknown }[] } | null)?.groups ?? [])) { out.push(...(g.lines ?? [])); walk(g.tree); }
  return out;
};
/** The licence-document line the prototype's round UR7 `stampsFor` adds under a fish ("You need Steelhead Conservation
 *  Surcharge Stamp › only if you fish for steelhead, …") — COMPARED since rows 31 (the store's `stamps`, no longer
 *  excluded). */
export const STAMP_LINE = /^You need /;
/** The group's year-window line the prototype lifts from page v36's strip (`.gyear`: "Through the year You can keep them
 *  until Oct 31. …"). */
export const YEAR_WINDOW_LINE = /^Through the year /;
const idsWithLine = (re: RegExp): string[] => Object.entries(golden().states).filter(([, s]) => groupLines(s).some((l) => re.test(l))).map(([id]) => id);
/** The states whose golden Keep groups carry a stamp line (in a kind's lines or the group's) — computed at load; the
 *  parity compares them whole (rows 31). */
export const STAMP_LINE_IDS: readonly string[] = idsWithLine(STAMP_LINE);
/** The states whose golden Keep groups carry a `.gyear` line — computed at load. Compared, not excluded (T21 review): the
 *  group draws it (GroupVM.year, plainrow.ts `yearWindowOf`: page v36 runTxt over the STORED HydRow.year). */
export const YEAR_WINDOW_IDS: readonly string[] = idsWithLine(YEAR_WINDOW_LINE);

/**
 * `lines`: the entry does not exclude its key — the key IS compared, with only the golden lines matching `lines` removed
 * (`droppedLines`); `excluded` never returns such an entry.
 */
export interface Exclusion { block: Block; ids: readonly string[] | "*"; key?: RegExp; lines?: RegExp; reason: string; until?: string }

/** The states whose golden Keep tab (`keep.lines`, any block — a group's kind, Other fish) carries a stamp line. */
/** The prototype's stamp lines under OTHER FISH rows (no group draws them): the pipeline ships `stamps` on a group's kinds
 *  and not-kept fish since rows 31, not on Other fish rows yet. Measured on the golden: only these two documents. */
export const OTHER_FISH_STAMP_LINE = /^You need (White Sturgeon Conservation Licence|Conservation Surcharge Stamp for Kootenay Lake rainbow trout) ›/;
/** The states whose golden Keep tab (`keep.lines`) carries an Other fish stamp line. */
export const KEEP_STAMP_LINE_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => ((s.keep as { lines?: string[] } | null)?.lines ?? []).some((l) => OTHER_FISH_STAMP_LINE.test(l))).map(([id]) => id);
/** decisions D19: the one pooled row with no stored group headline, drawn by page v36's legacy card path. */
export const D19_POOLED_IDS: readonly string[] = ["klmb_1010"];

/** (T33) the states whose golden licence list lacks the Steelhead Conservation Surcharge Stamp because the PROTOTYPE removed
 *  it at render time (round 20i O3: a card whose not_kept says steelhead are closed here). licence.parity holds this list to
 *  the oracle condition (stored rows not_kept closed ST × a stored steelhead_stamp document), so it cannot drift. */
export const STEELHEAD_DROP_IDS: readonly string[] = ["bc1_501", "bc0_501", "bc1_1010", "bc0_1010", "ape_501", "ape_1010", "x15p2_1010"];
export const STEELHEAD_STAMP_ITEM = /^Steelhead Conservation Surcharge Stamp\b/;
/** (T33) tidal water: the prototype draws an empty Licence section (as on a closed day); page v36 licParts draws its
 *  "Tidal water" box with the stored tidal licence words, which the app keeps (D33 (b): tidal water is not closed). */
export const TIDAL_LICENCE_IDS: readonly string[] = ["x31p_1010"];
/** (T33) the under-16 youth box where the 16+ angler's base document has an alternative ("Basic angling licence or Yukon
 *  angling licence"): the stored `licence_frame.under_16[].buy[]` names each document but not its `or` alternatives. */
export const YOUTH_ALT_IDS: readonly string[] = ["x12p_1010", "x30p_1010", "x35p_1010"];
export const YOUTH_ALT_LINES = /^Under 16: no licence needed if you fish with a licensed adult\b| Or, for your own limit: /;
export const LICENCE_EXCLUDE: readonly Exclusion[] = [
  { block: "licence", ids: YOUTH_ALT_IDS, key: /^u16\|non_resident(_alien)?\.blocks$/, lines: YOUTH_ALT_LINES,
    reason: "PIPELINE NEED (owner rule 2026-10-21): the youth box and its 'Or, for your own limit' item name the 16+ base document with its alternatives ('Basic angling licence or Yukon angling licence'); licence_frame.under_16[].buy[] carries {doc, name, prices} but not the DocumentNeed's `or` alternatives, and the app must not look them up in the 16+ answer. Both lines are dropped from both sides until the pipeline ships them",
    until: "the pipeline's under_16[].buy[] carries `or` (each alternative's need/names) — licence frame, next version" },
  { block: "licence", ids: TIDAL_LICENCE_IDS,
    reason: "decisions D33 (b): the prototype's empty Licence section on tidal water is the closed-day path over a key that is not closed; the app draws page v36 licParts' tidal box (\"Tidal water\" + the stored tidal licence words) — licence.parity asserts it",
    until: "never — a prototype artefact, not a regulation" },
  { block: "licence", ids: STEELHEAD_DROP_IDS, key: /^adult\|[a-z_]+\.blocks$/, lines: STEELHEAD_STAMP_ITEM,
    reason: "PIPELINE NEED (owner rule 2026-10-21; decisions D21 (b)): the prototype's render() drops the steelhead stamp from the list where the card says steelhead are closed — a page-side regulation decision the app must not make. The stored licence answer still lists the stamp; the line is dropped from both sides until the pipeline ships the per-water answer",
    until: "the pipeline's licence answer omits (or qualifies) the stamp where steelhead are closed (licence 7)" },
];
/** (T21) the whole Keep tab: the Other fish rows' stamp lines (pending in the pipeline), and the D19 row. */
export const KEEP_TAB_EXCLUDE: readonly Exclusion[] = [
  { block: "keep", ids: D19_POOLED_IDS, key: /^plainRows\.0$/,
    reason: "decisions D19: klmb_1010's pooled row has no stored group headline; page v36 draws it on its legacy card path (groupBlock / speciesBlock), which is not ported. Its article is dropped from the golden order and its lines from keep.lines; every other block of the state is compared",
    until: "D19 (a): the pipeline ships a group for that row" },
  ...(KEEP_STAMP_LINE_IDS.length ? [{
    block: "keep" as const, ids: KEEP_STAMP_LINE_IDS, key: /^lines$/, lines: OTHER_FISH_STAMP_LINE,
    reason: "PENDING (owner rule 2026-10-21): the prototype's round UR7 stampsFor line under an OTHER FISH row ('You need White Sturgeon Conservation Licence › only if …') — rows 31 ships `stamps` on a group's kinds and not-kept fish (drawn and compared), not on Other fish rows yet. Every other line of the tab is compared",
    until: "the pipeline ships stamps on Other fish rows" }] : []),
];

/**
 * (T31) The Bella Coola's own parts, where the golden's Bait source reads "Bella Coola River ›": the prototype names a
 * downstream rule by its water from the rule SHEET's sub (`levelOf`/`downWater` over `SNAP.rules[k].sub`), and
 * regs-v11/tools/snap.mjs keeps ONE sheet per rule key for the whole snapshot (`takeRules`: first taken wins) — taken on
 * a tributary part (ape_*), where the Bella Coola's bait-ban row is reached from downstream ("Downstream rule · Bella
 * Coola River"). On the river's own parts the same rule is the water's own row (rank 0, via reach): the app names it per
 * part ("This water’s ›", the level every other own-row source line in the golden says). The parity asserts exactly that
 * word for these states and compares everything else.
 */
export const SNAPSHOT_SUB_IDS: readonly string[] = ["bc0_501", "bc1_501", "bc0_1010", "bc1_1010"];
export const SNAPSHOT_SUB_EXCLUDE: readonly Exclusion[] = [{
  block: "gear", ids: SNAPSHOT_SUB_IDS, key: /^bait\.src\.who$/,
  reason: "prototype snapshot artifact: snap.mjs keeps one rule sheet per key for every state (first taken: a tributary part, 'Downstream rule · Bella Coola River'); on the Bella Coola's own parts the bait-ban row is the water's own (rank 0), so the app's per-part word is 'This water’s ›' — asserted instead",
  until: "never — the prototype's snapshot is global; the app's level is per part",
}];

/**
 * (T32) The allowed "Set lines" way's conditions. Page v36 draws the way as `<button class="wrow">…<b>Set lines</b> ⓘ<small>…</small>`
 * where the ⓘ (set_line) is itself a `<button>`: a button inside a button is not valid HTML, so the parser closes the way's
 * button at the ⓘ and the `<small>` conditions fall outside it — the prototype's `w.querySelector('small')` finds none and
 * the golden note is null. The app shows the pipeline's conditions for the way (its clauses, conduct, while-rules) as it
 * does for every other allowed way; the parity asserts the golden note is null and the app's is not, and compares every
 * other field.
 */
export const SET_LINE_NOTE_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => ((s.methods as { ways?: { list: { col: string; items: { name: string }[] }[] } | null } | null)?.ways?.list ?? [])
    .some((c) => c.col === "ok" && c.items.some((i) => i.name === "Set lines")))
  .map(([id]) => id);

export const EXCLUDE: readonly Exclusion[] = [
  // (T28) no pins entry: the prototype's pinUpdate throw on a group-less article (25 states at v33, facts A#25) is fixed
  // since UI-NOTES v35 (a group-less row pins nothing, IMPLEMENT-HANDOFF C5) — pins.parity replays every state whole
  { block: "sheets", ids: "*", key: /^parts$/,
    reason: "the prototype's parts sheet carries a prototype-only line ('This prototype has no map of this water…'); T36 compares its rows only" },
  { block: "sheets", ids: "*", key: /^rule:/,
    reason: "'Technical detail' fold body (key + fields JSON) is compared by structure, not string: JSON.stringify spacing",
    until: "never — T36 test asserts the fold exists, closed, and that its first line starts with the key" },
  // (T36) owner rule 2026-10-21: the app reads the page map, it never computes one
  { block: "sheets", ids: "*", key: /^(gl:|hatch$)/,
    reason: "PIPELINE NEED: the release's statics carry no printed → PDF page map (contracts §3 Statics.pdfPages, source pipeline.common.synopsis_pages / the export guide's pdf_pages), so a glossary page is drawn unlinked ('p.4', no ↗) where the prototype links it. sheets.parity compares the printed page numbers and every other line; it fails once the map ships (then drop this entry)",
    until: "the pipeline ships static pdf_pages in the release" },
  { block: "header", ids: NO_MAP_LABEL_IDS, key: /^place$/,
    reason: "the prototype shows no part label where it had no water map of the water; the app frames every named part (D1) and labels it unless the label is the water's name (T16) — the parity asserts that instead",
    until: "never — prototype map coverage, not a regulation" },
  { block: "header", ids: "*", key: /^place\.shown$/,
    reason: "which of full/short is shown is a measurement (fitLabel); the VM carries both candidates — T41 checks the choice on Expo web" },
  // (T18/T19) the Kitimat owner ruling 2026-10-21 (a): the store's group tree differs from page v36's by design
  { block: "keep", ids: KITIMAT_RULED_IDS, key: /^groups\.\d+\.(tree|worked|lines)$/,
    reason: "OWNER RULING 2026-10-21 (a) (pipeline/docs/GOTCHAS.md): on the Kitimat, r5's hatchery-steelhead quota does not lift Region 6's Nov 1–June 30 stream release, so the stored group tree (the river's own steelhead tier, the size box) and its worked over-50 cm line are not page v36's. tree.parity holds these trees to the STORE's TreeTier.says / TreeKind.says, tail.parity these worked lines to the store's group.worked; the group's drawn lines (which contain both) are not compared; never 'fix' the data to match the page",
    until: "never — a re-synced prototype on the ruled data removes it" },
  { block: "pins", ids: KITIMAT_RULED_IDS, key: /^anchors\.\d+\.tiers$/,
    reason: "OWNER RULING 2026-10-21 (a): the Kitimat's stored size tier says 'Only 1 … over 50 cm — 2 if one is a hatchery steelhead' where page v36 dropped the tail, so TierVM.short (shortOf over the STORE's tier) carries '(2 if one is a hatchery steelhead)' and the app's bar is not the golden's on these states. The group line is compared; the golden-string replay (pins.parity B) still covers the geometry; never 'fix' the data to match the page",
    until: "never — a re-synced prototype on the ruled data removes it" },
  // (T18/T19 review, rows 31 + T21 review) the stamp lines (the store's `stamps`) and the .gyear line are drawn and compared
  ...KEEP_TAB_EXCLUDE,
  // (T31) a source line's who-word read from the prototype's GLOBAL rule-sheet snapshot
  ...SNAPSHOT_SUB_EXCLUDE,
  // (T32) a way's conditions the prototype's DOM lost (a button nested in a button)
  ...(SET_LINE_NOTE_IDS.length ? [{
    block: "methods" as const, ids: SET_LINE_NOTE_IDS, key: /^ways\.list\.0\.items\.\d+\.note$/,
    reason: "prototype DOM artifact: page v36 nests the set_line ⓘ <button> inside the Set lines way's <button>, so the parser ends the way at the ⓘ and its <small> conditions are not read (golden note null); the app shows the pipeline's conditions — the parity asserts golden null and an app note, and compares everything else",
    until: "never — a prototype markup bug, not a regulation" }] : []),
  // (T33) the Licence tab
  ...LICENCE_EXCLUDE,
];

/** The entry that excludes this state (and key), or null. A keyed entry never excludes a whole state. */
export function excluded(block: Block, id: string, key?: string): { reason: string } | null {
  const e = EXCLUDE.find((x) => !x.lines && x.block === block && (x.ids === "*" || x.ids.includes(id))
    && (x.key === undefined || (key !== undefined && x.key.test(key))));
  return e ? { reason: e.reason } : null;
}

/** The golden-line patterns a `lines` entry drops from this state's key (empty when the key is compared whole). */
export function droppedLines(block: Block, id: string, key: string): readonly RegExp[] {
  return EXCLUDE.filter((x) => !!x.lines && x.block === block && (x.ids === "*" || x.ids.includes(id)) && !!x.key?.test(key)).map((x) => x.lines!);
}
/** golden lines with the dropped ones removed */
export const keepLines = (lines: readonly string[], drop: readonly RegExp[]): string[] => lines.filter((l) => !drop.some((re) => re.test(l)));

// ── (T33) the Licence tab ──────────────────────────────────────────────────────────────────────────────────────────
/** The six anglers the golden snapshots per state, in the prototype's order (golden `licence` keys). */
export const LICENCE_PROFILES = ["adult|resident", "adult|non_resident", "adult|non_resident_alien", "u16|resident", "u16|non_resident", "u16|non_resident_alien"] as const;
/** The golden licence lines a `lines` entry names, dropped from BOTH sides of the state's blocks: the golden LACKS a line
 *  the stored answer carries (the opposite of the Keep entries, which drop golden lines the app does not draw). */
export function licenceDropped(id: string, profile: string): readonly RegExp[] {
  return droppedLines("licence", id, `${profile}.blocks`);
}

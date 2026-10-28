/**
 * What the golden parity does NOT compare, each with its reason and what removes it (tests.md §1.2). An entry with no
 * `key` drops the whole state from `cases(block)`; a keyed entry drops only that key (a sheet key, or a dotted VM path
 * under the block) and the parity test asks `excluded(block, id, key)` before comparing it.
 */
import type { Block } from "@app/core/regs/testing";
import { golden, LOIS_FOLD, tapsOnce } from "./oracles";

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
/** (T71) The states whose golden header offers the prototype's Map button. Owner call 3 (decisions.md, 2026-10-22):
 *  "Keep only one Map entry (the view strip; drop the header's Map button)" — the app's header offers Parts when the
 *  picker has a choice to make, else nothing; header.parity asserts that rule on these states instead of the golden. */
export const GOLDEN_MAP_ACTION_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => (s.header as { action: { kind: string } | null } | null)?.action?.kind === "map")
  .map(([id]) => id);
/** (T150) The states whose golden header offers the prototype's Parts button. Owner call 3, 2026-10-23 ("dont we want to
 *  remove 'parts' from reg page since map is now at the top tabs?", Q122): the regs header has no action at all; the
 *  water's Map view lists its parts by name. header.parity asserts the app's header has no action on these states. */
export const GOLDEN_PARTS_ACTION_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => (s.header as { action: { kind: string } | null } | null)?.action?.kind === "parts")
  .map(([id]) => id);
/** The three Kitimat states the owner ruling 2026-10-21 (a) moves away from page v36 (card.parity.test.ts KITIMAT_RULED). */
export const KITIMAT_RULED_IDS: readonly string[] = ["x14p0_1010", "x14p1_1010", "x14p2_1010"];
/** (T203, owner 2026-10-28, the Dala River) the Region 6 states whose shared "1 over 50 cm" is now ONE size tier (the stream's
 *  count tier split, a rainbow of 50 cm or less beside it) where page v36 said it on each kind. */
export const T203_SIZE_BOX_IDS: readonly string[] = ["x17p0_1010", "x17p1_1010", "x17p2_1010", "x17p3_1010", "x17p4_1010", "x17p7_1010", "x19p0_1010", "x19p1_1010", "x19p2_1010", "x19p3_1010", "x19p4_1010", "x19p6_1010", "x19p7_1010"];
const T203_REASON = "T203 (owner 2026-10-28, pipeline/deliver/answers/ANSWERS-SPEC.md T203): a size cap shared by several kinds is ONE size tier holding exactly them; the count tier it crosses is split (the rainbow of 50 cm or less leaves it and says the count on its line). Page v36 said Region 6's 1 over 50 cm on each kind, so the stored tree, its kind lines and the pin bar's tiers are not the golden's; never 'fix' the data to match the page";
const T203_UNTIL = "a re-synced prototype on the T203 tree";

/** (T206, oracles.ts LOIS_FOLD) why Lois Lake's Keep tab, pin bar and second-row source sheet are not page v36's. */
const LOIS_REASON = "T206 ORACLE_RETIRED (overlapping-rules.md, owner 2026-10-26): lois_lake.r3 (hatchery rainbow trout, 6 a day) is retired, so hatchery rainbow trout answer to r4 and the card folds them into r4's steelhead row: the group is row r1, not page v36's r2 (oracles.ts LOIS_FOLD, loisRuled), and the app's r2 is the next row (Kokanee). The sheet must differ; the folded group's own source sheet (src:r1) has no golden";
const LOIS_UNTIL = "the page blocks and golden re-cut on the retired rules";
/** (T206, ORACLE_ON_PARTS_DROPPED) the states whose Methods tab gains a Boats block: their vessel rule held 'on parts'. */
export const ON_PARTS_BOAT_IDS: readonly string[] = ["cow1_1010", "cow2_1010", "cow3_1010", "x37p0_1010", "x37p1_1010", "x37p2_1010"];

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

/** The prototype's stamp lines under OTHER FISH rows (and klmb's page-v36 keep line, D19) — COMPARED since rows 32 (the
 *  store's `Row.stamps`, the pipeline's `stamps.row_stamps`; no longer excluded). Measured on the golden: only these two
 *  documents. */
export const OTHER_FISH_STAMP_LINE = /^You need (White Sturgeon Conservation Licence|Conservation Surcharge Stamp for Kootenay Lake rainbow trout) ›/;
/** The states whose golden Keep tab (`keep.lines`) carries an Other fish stamp line — computed at load; compared whole. */
export const KEEP_STAMP_LINE_IDS: readonly string[] = Object.entries(golden().states)
  .filter(([, s]) => ((s.keep as { lines?: string[] } | null)?.lines ?? []).some((l) => OTHER_FISH_STAMP_LINE.test(l))).map(([id]) => id);
/** decisions D19: the one pooled row with no stored group headline, drawn by page v36's legacy card path. */
export const D19_POOLED_IDS: readonly string[] = ["klmb_1010"];

/**
 * (T80 item 3 = decisions D19 (a), AS BUILT — 210d0996 + 3a6d20c3, shipped in r-36fbc5326506) the pipeline now ships a
 * one-kind trout/char group for that row (`rows.A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP`: Kootenay Main Body's own
 * 10 rainbow trout a day), so the app draws it as a GROUP. The golden (the regs v38 prototype over page v36) still draws it
 * on page v36's legacy card path as a plain article (KEEP_TAB_EXCLUDE), so the two cannot be compared line for line: the
 * golden's article is dropped (KEEP_TAB_EXCLUDE) and the app's group is set aside here — by row, asserted present, so the
 * entry fails the day the group is gone (`comparableGroups`). The group's own words are pinned in keep.parity
 * ("klmb_1010 (D19 (a))"). Not "removed after the re-cut": T80's review predicted exactly this — the golden's legacy path
 * differs from the group path, so parity is not the right test; removing the exclusion would fail by construction.
 */
export const D19_GROUPS: readonly { readonly id: string; readonly row: number; readonly title: string }[] = [
  { id: "klmb_1010", row: 2, title: "Rainbow trout" },
];
// Why: decisions D19 (a) as built (T80 item 3): the pipeline ships a one-kind group for the row page v36 draws on its
// legacy card path; the golden has no group to compare it with.
/** `groups` without the D19 (a) group(s) of state `id` — each asserted present exactly once. */
export function comparableGroups<G extends { readonly ctx: { readonly rowIx: number } }>(id: string, groups: readonly G[]): G[] {
  for (const d of D19_GROUPS.filter((x) => x.id === id)) {
    const n = groups.filter((g) => g.ctx.rowIx === d.row).length;
    if (n !== 1) throw new Error(`D19_GROUPS: ${id} draws ${n} groups on row r${d.row}, the entry expects 1 — update exclusions.ts`);
  }
  return groups.filter((g) => !D19_GROUPS.some((d) => d.id === id && d.row === g.ctx.rowIx));
}
/** Lines the app's sheets draw for the D19 (a) group that the golden's sheets lack (its legacy path drew no group). Each is
 *  removed from the APP's lines and asserted present exactly once. */
export const D19_SHEET_LINES: readonly { readonly id: string; readonly key: RegExp; readonly line: string }[] = [
  { id: "klmb_1010", key: /^pos:/, line: "With 10 rainbow trout a day you may have 20: 2 days’ fish." },
];

/** (T33) the states whose golden licence list lacks the Steelhead Conservation Surcharge Stamp because the PROTOTYPE removed
 *  it at render time (round 20i O3: a card whose not_kept says steelhead are closed here) — COMPARED since licence 7: the
 *  pipeline's stored documents leave it out there (`stamps.drop_closed_steelhead_stamp`). licence.parity holds this list to
 *  the stored condition (rows not_kept closed ST, and no profile lists the stamp), so it cannot drift. */
export const STEELHEAD_DROP_IDS: readonly string[] = ["bc1_501", "bc0_501", "bc1_1010", "bc0_1010", "ape_501", "ape_1010", "x15p2_1010"];
/** (T33) tidal water: the prototype draws an empty Licence section (as on a closed day); page v36 licParts draws its
 *  "Tidal water" box with the stored tidal licence words, which the app keeps (D33 (b): tidal water is not closed). */
export const TIDAL_LICENCE_IDS: readonly string[] = ["x31p_1010"];
/** (T33) the under-16 youth box where the 16+ angler's base document has an alternative ("Basic angling licence or Yukon
 *  angling licence") — COMPARED since licence 7: the stored `under_16[].buy[].or` carries the alternatives' documents and
 *  names. */
export const YOUTH_ALT_IDS: readonly string[] = ["x12p_1010", "x30p_1010", "x35p_1010"];
export const LICENCE_EXCLUDE: readonly Exclusion[] = [
  { block: "licence", ids: TIDAL_LICENCE_IDS,
    reason: "decisions D33 (b): the prototype's empty Licence section on tidal water is the closed-day path over a key that is not closed; the app draws page v36 licParts' tidal box (\"Tidal water\" + the stored tidal licence words) — licence.parity asserts it",
    until: "never — a prototype artefact, not a regulation" },
];
/** (T21) the whole Keep tab: the D19 row (the Other fish rows' stamp lines are compared since rows 32). */
export const KEEP_TAB_EXCLUDE: readonly Exclusion[] = [
  { block: "keep", ids: D19_POOLED_IDS, key: /^plainRows\.0$/,
    reason: "decisions D19: klmb_1010's pooled row is drawn by page v36 on its legacy card path (groupBlock / speciesBlock), which is not ported; since T80 (D19 (a), r-36fbc5326506) the pipeline ships a group for it, which the app draws as a group (D19_GROUPS) and the golden cannot. Its article is dropped from the golden order and its lines from keep.lines; every other block of the state is compared",
    until: "never on this golden — D19 (a) shipped: a prototype re-synced to draw the row as a group" },
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

/**
 * (T36 part 2) Golden sheet lines the app says differently BY DESIGN, each a whole-line substitution the parity applies to
 * the golden before comparing (and asserts it applied, so an entry cannot go stale silently).
 */
/** `ids: "*"`: every state holding a sheet the key matches. */
export interface SheetSubst { readonly ids: readonly string[] | "*"; readonly key: RegExp; readonly from: string; readonly to: string; readonly reason: string }
export const substIn = (s: SheetSubst, id: string): boolean => s.ids === "*" || s.ids.includes(id);
export const SHEET_SUBST: readonly SheetSubst[] = [
  { ids: SNAPSHOT_SUB_IDS, key: /^srcs:bait\|/,
    from: "Bait ban, Sep 1-May 15 — downstream of Tweedsmuir Park eastern boundary Bella Coola River This water’s own row",
    to: "Bait ban, Sep 1-May 15 — downstream of Tweedsmuir Park eastern boundary This water’s This water’s own row",
    reason: "the T31 snapshot artifact (SNAPSHOT_SUB_EXCLUDE) in the Sources sheet: the prototype's who-word comes from its GLOBAL rule-sheet snapshot ('Downstream rule · Bella Coola River', taken on a tributary part); on the Bella Coola's own parts the bait-ban row is the water's own (rank 0) — 'This water’s'" },
  { ids: ["klmb_1010"], key: /^src:klmb_1010\|r2$/, from: "Size limit Rule book p.7 ↗", to: "Something you must do Rule book p.7 ↗",
    reason: "owner rule 2026-10-21: the record-retention rule's role is the STORE's (rows 31 `stored_role`: duty — the pipeline took over page v36's ladderHtml record_retention → duty move); page v36's openRowSources read the pre-move role (floor). The app reads the stored role" },
  { ids: "*", key: /^parts$/, from: "This prototype has no map of this water, so its parts are listed here. ", to: "",
    reason: "decisions D1: the prototype's own note about its missing water map is never ported (the app frames every named part on the map); the rows after it are compared whole" },
];

/** (T36 part 2) The Other fish rows page v36 draws on its legacy pooled card path (a pool with stored conditions:
 *  rowConds → groupBlock / speciesBlock — "Your own limit, counted across …", "Each kind · every fish counts toward the 15",
 *  "This river’s rule: 15 a day here …"), not ported (decisions D19 (b), ~250 lines); measured on the golden. */
export const LEGACY_POOL_SHEETS: readonly string[] = ["x18p5_1010 other:x18p5_1010|o0", "x22p1_1010 other:x22p1_1010|o1", "x22p3_1010 other:x22p3_1010|o1"];

/**
 * (T115, r-941dcc149b17) The golden states whose PART the release merged away: owner decision AGENTS 54 /
 * tickets/T115-one-region-membership.md — a stream piece straddling a region line is its home region's only. The four
 * Fraser pieces that carried two regions' rows were parts of their own in the prototype (x15p14/15/17/18); they now carry
 * their home region's rows and are part of the neighbouring part, so the golden's two-region page has no store part to
 * be compared with. taps.py declares them (PART_GONE, checked against the store) and writes them to taps.json `gone`, not
 * `states`; every block excludes exactly those, each with its taps.json reason. Not the Fraser's other states: x15p16 is
 * tapped at its renumbered part (taps.py PART_MOVED) and x15p0/1/4/6 at their grown parts (PART_GROWN), compared whole
 * except the extent words (PART_GROWN_EXCLUDE).
 * (T183, r-98c43091c440: a straddling piece carries both regions' rules again, reversing T115) the four stay gone: the
 * straddling pieces are parts of their own again, but their rows add Region 5's zone and r5's Fraser rows r1 / r6, which
 * the prototype's page did not draw. The West Road's x27p1 (its Region 6 stretch now shares part 0's rules) joins them.
 */
export const PART_GONE_IDS: readonly string[] = Object.keys(tapsOnce().gone ?? {});
const BLOCKS: readonly Block[] = ["header", "keep", "gear", "methods", "licence", "sheets", "pins"];
export const PART_GONE_EXCLUDE: readonly Exclusion[] = PART_GONE_IDS.flatMap((id) => BLOCKS.map((block): Exclusion => ({
  block, ids: [id], reason: `part gone — ${tapsOnce().gone[id]!.reason}`,
  until: "never on this golden — a prototype re-synced on T115's data (it drew the old two-region rows)" })));

/** (T115) The tapped states at a part that took in a merged piece (taps.py PART_GROWN): same rows, a longer extent. */
export const PART_GROWN_IDS: readonly string[] = Object.entries(tapsOnce().states).filter(([, t]) => t.grown).map(([id]) => id);
/** (T67) The tapped states whose part kept its label and rows while the atlas re-cut the sections under it (taps.py
 *  PART_RECOUNTED: braid cuts, a ditch leaving its river, moved length cuts): {id: the store's count, why}. header.parity
 *  holds the subtitle to the golden's with that count (`recountedSub`); every block is compared whole. */
export const PART_RECOUNTED: Readonly<Record<string, { reason: string; sections: number }>> = Object.fromEntries(
  Object.entries(tapsOnce().states).filter(([, t]) => t.recounted).map(([id, t]) => [id, t.recounted!]));
/** The golden subtitle `kind · N section(s)[ · label]` with the section count `n`. Throws on a subtitle of another shape. */
export function recountedSub(sub: string, n: number): string {
  const m = /^(.*? · )\d+ sections?((?: · .*)?)$/.exec(sub);
  if (!m) throw new Error(`recountedSub: ${JSON.stringify(sub)} is not 'kind · N sections[ · label]'`);
  return `${m[1]}${n} ${n === 1 ? "section" : "sections"}${m[2]}`;
}
/** (T220) Re-cut waters whose Parts sheet is still the golden's: the release renumbered or recounted their parts (a
 *  closed part is a PART_GONE state) but the sheet lists the same stretches in the same words. Their Parts sheet is
 *  compared whole; sheets.parity would otherwise demand it differ. */
export const RECUT_SAME_PARTS: Readonly<Record<string, string>> = {
  "gnis:16075": "T220 (owner 2026-10-28): restored braid pieces renumbered the Dean's parts (x13p2's closed part is 3 now); the stretches and their words are unchanged",
};
/** (T115) Every tapped state of a water whose parts the release re-cut (taps.json `recut`). */
export const RECUT_IDS: readonly string[] = Object.entries(tapsOnce().states)
  .filter(([, t]) => t.item in (tapsOnce().recut ?? {}) && !(t.item in RECUT_SAME_PARTS)).map(([id]) => id);
export const RECUT_EXCLUDE: readonly Exclusion[] = [
  ...(PART_GROWN_IDS.length ? [{ block: "header" as const, ids: PART_GROWN_IDS, key: /^place\.(full|short)$/,
    reason: "T115 (owner decision AGENTS 54, tickets/T115-one-region-membership.md), then T183 (owner 2026-10-24) and T67: the part's extent changed — the Skeena's section 2 starts at the Exchamsiks, the West Road's straddling pieces are parts of their own (taps.json `grown`) — so its extent words (the header's full and short label, and the subtitle's label and section count) are not the prototype's; its rows are, and are compared. header.parity asserts the label and subtitle DIFFER (so the entry cannot go stale). (The Parts button whose aria quoted the label is gone: T150, GOLDEN_PARTS_ACTION_IDS)",
    until: "never on this golden — a prototype re-synced on T115's data" }] : []),
  ...(RECUT_IDS.length ? [{ block: "sheets" as const, ids: RECUT_IDS, key: /^parts$/,
    reason: "T115 (owner decision AGENTS 54, tickets/T115-one-region-membership.md): the Fraser's parts were re-cut (19 → 15: each straddling piece joined its neighbour; T183 made them parts again, 15 → 17), T183 the West Road's (3 → 5), and T67 (owner 2026-10-24) the Skeena's (8 → 9: a channel piece of the Ecological Reserve is a part of its own — since T183 the Exchamsiks host channel above the cut, still section 2); T67/T187 the Peace's (the Site C closure takes two braid pieces and is renumbered); taps.json `recut` gives each water's reason. So the Parts sheet lists parts the prototype did not have; every other sheet of these states is compared",
    until: "never on this golden — a prototype re-synced on T115's data" }] : []),
];

export const EXCLUDE: readonly Exclusion[] = [
  // (T115) golden states whose part the release merged away, and the extent words of the re-cut Fraser
  ...PART_GONE_EXCLUDE,
  ...RECUT_EXCLUDE,
  // (T28) no pins entry: the prototype's pinUpdate throw on a group-less article (25 states at v33, facts A#25) is fixed
  // since UI-NOTES v35 (a group-less row pins nothing, IMPLEMENT-HANDOFF C5) — pins.parity replays every state whole
  ...[...new Set(LEGACY_POOL_SHEETS.map((x) => x.split(" ")[0]!))].map((id): Exclusion => ({
    block: "sheets", ids: [id], key: new RegExp(`^(${LEGACY_POOL_SHEETS.filter((x) => x.startsWith(id + " ")).map((x) => x.split(" ")[1]!.replace(/[|]/g, "\\|")).join("|")})$`),
    reason: "decisions D19: an Other fish row page v36 draws on its legacy pooled card path (a pool with stored conditions: groupBlock / speciesBlock), not ported; the sheet's pictures, value and ladder are built, the legacy block lines are not",
    until: "D19 (a): the pipeline ships a group (headline) for these rows, or D19 (b) ports the legacy path" })),
  { block: "sheets", ids: "*", key: /^rule:/,
    reason: "'Technical detail' fold body (key + fields JSON) is compared by structure, not string: JSON.stringify spacing",
    until: "never — T36 test asserts the fold exists, closed, and that its first line starts with the key" },
  // (T36) owner rule 2026-10-21: the app reads the page map, it never computes one — the release ships it (static
  // export.pdf_pages, Statics.pdfPages), so the gl: and hatch sheets' pages are compared whole, links and all
  { block: "header", ids: NO_MAP_LABEL_IDS, key: /^place$/,
    reason: "the prototype shows no part label where it had no water map of the water; the app frames every named part (D1) and labels it unless the label is the water's name (T16) — the parity asserts that instead",
    until: "never — prototype map coverage, not a regulation" },
  { block: "header", ids: GOLDEN_MAP_ACTION_IDS, key: /^action$/,
    reason: "owner call 3 (2026-10-22, T71): one Map entry — the view strip's; the header's Map button is dropped, so where the prototype offers Map the app offers nothing (T150 dropped Parts too) — the parity asserts that instead",
    until: "never — an owner decision, not a regulation" },
  { block: "header", ids: GOLDEN_PARTS_ACTION_IDS, key: /^action$/,
    reason: "owner call 3 (2026-10-23, T150, Q122): the regs header's Parts button is dropped — the water's Map view (the view strip's) lists the parts by name as a collapsible list under the map, and the header keeps the part's name — so where the prototype offers Parts the app offers nothing; the parity asserts that instead",
    until: "never — an owner decision, not a regulation" },
  { block: "header", ids: "*", key: /^place\.shown$/,
    reason: "which of full/short is shown is a measurement (fitLabel); the VM carries both candidates — T41 checks the choice on Expo web" },
  // (T18/T19) the Kitimat owner ruling 2026-10-21 (a): the store's group tree differs from page v36's by design
  { block: "keep", ids: KITIMAT_RULED_IDS, key: /^groups\.\d+\.(tree|worked|lines)$/,
    reason: "OWNER RULING 2026-10-21 (a) (pipeline/docs/GOTCHAS.md): on the Kitimat, r5's hatchery-steelhead quota does not lift Region 6's Nov 1–June 30 stream release, so the stored group tree (the river's own steelhead tier, the size box) and its worked over-50 cm line are not page v36's. tree.parity holds these trees to the STORE's TreeTier.says / TreeKind.says, tail.parity these worked lines to the store's group.worked; the group's drawn lines (which contain both) are not compared; never 'fix' the data to match the page",
    until: "never — a re-synced prototype on the ruled data removes it" },
  { block: "keep", ids: T203_SIZE_BOX_IDS, key: /^groups\.\d+\.(tree|lines)$/, reason: T203_REASON, until: T203_UNTIL },
  { block: "pins", ids: T203_SIZE_BOX_IDS, key: /^anchors\.\d+\.tiers$/, reason: T203_REASON, until: T203_UNTIL },
  { block: "pins", ids: KITIMAT_RULED_IDS, key: /^anchors\.\d+\.tiers$/,
    reason: "OWNER RULING 2026-10-21 (a): the Kitimat's stored size tier says 'Only 1 … over 50 cm — 2 if one is a hatchery steelhead' where page v36 dropped the tail, so TierVM.short (shortOf over the STORE's tier) carries '(2 if one is a hatchery steelhead)' and the app's bar is not the golden's on these states. The group line is compared; the golden-string replay (pins.parity B) still covers the geometry; never 'fix' the data to match the page",
    until: "never — a re-synced prototype on the ruled data removes it" },
  // (T206) page v36 predates the retired Lois Lake r3 (LOIS_FOLD) and the owner's on-parts ruling (ORACLE_ON_PARTS_DROPPED)
  { block: "sheets", ids: [LOIS_FOLD.id], key: /^src:x25p_1010\|r2$/, reason: LOIS_REASON, until: LOIS_UNTIL },
  { block: "methods", ids: ON_PARTS_BOAT_IDS, key: /^boats$/,
    reason: "T206 (owner 2026-10-26, curation-review/on-parts.md; c16b1c5f drop_on_parts): 'on parts' holds on the whole water, so the Cowichan's (r8/r9) and the Coquitlam's (r2) vessel rules now draw a Boats block where page v36 drew none (its rule held 'on parts', undrawn). methods.parity compares the tab without the block and pins the block's items (ON_PARTS_BOATS)",
    until: "the page blocks and golden re-cut on drop_on_parts" },
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

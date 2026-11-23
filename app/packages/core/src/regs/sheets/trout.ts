/**
 * (T810 P) THE TROUT FISH SHEET (`tf:<row key>`, `tf:ST/hatchery`) and the table's
 * every-rule sheet (`src:trout`, the table's "Sources ›" and the fish sheet's "All sources ›"). Build spec 3.4; contract
 * 2.3 `FishSheet`, `DateSet.sheet`, `TroutTable.src`.
 *
 * Owner rule 2026-11-16 16:50: Python decides every word; the app draws only. So every rule word here is the payload's,
 * drawn unchanged, in the payload's order:
 * - the header: the row's name (title), its "Hatchery only" label or origin word as on the row (sub), `FishSheet.head`;
 * - "Here, today": `FishSheet.here` (bold where `bold`), then the coming-change sentences `DateSet.sheet[key]`;
 * - (T1031) the Identification tab (`tf:<row>|id`, `troutIdSheet`), app content, not a rule: the fish manifest's pictures
 *   (both for Dolly Varden / bull trout), the tips (`FISH_TIPS`), and the hatchery link on a row that carries an origin, a
 *   label or a hatchery/wild split;
 * - the Sources tab (`tf:<row>`): the above, "How this was decided", then `FishSheet.src` exactly as given (Python cuts it
 *   to 4) by authority (species.ts `breakdown`), then "All sources ›" = `TroutTable.src`.
 * THE APP NEVER SORTS, UNIONS OR CUTS A RULE LIST (Q-D6): each list is mapped one index to one row, in order.
 *
 * A fish with no picture or no tips is REPORTED (`reportFailure`), never drawn as a blank section; the vectors' test holds
 * every fish code to both.
 */
import { ruleBook } from "../book";
import { FISH, FISH_TIPS, type FishManifest } from "../fish.generated";
import { reportFailure } from "../../failure";
import type { RuleIndex, RuleView } from "../inputs";
import { plain, text, type Run } from "../line";
import type { FlowVM, SheetBlock, SheetVM, SourceRowVM, TroutBoxVM, TroutRowVM, TroutVM } from "../model";
import type { Decided, FishAnswer } from "../../answers.generated";
import type { LadderItem } from "../decided";
import { downLine, flowOf } from "../flow";
import { clean } from "../names";
import { imageBlocks } from "./fish";
import type { SheetKey } from "../sheetKey";
import { SPECIES_SHEET, TROUT_NAV, TROUT_SHEET, WORDS } from "../words";
import { breakdown } from "./breakdown";

/** The key of the table's every-rule sheet. */
export const TROUT_SOURCES_KEY = "src:trout" as const satisfies SheetKey;

/** A fish of the vectors that the app cannot show how to tell: reported, the sheet drawn without the missing part. */
export class TroutLooksMissing extends Error {
  constructor(readonly fish: string, readonly what: "picture" | "tips") { super(`trout sheet: no ${what} for ${fish}`); this.name = "TroutLooksMissing"; }
}

/** One rule as a Sources row: its title, the book's words, its Rule book pages; it opens the rule's own sheet. */
function sourceRow(r: RuleView): SourceRowVM {
  return { key: r.missing ? null : r.key, title: r.facts?.title || null, quote: clean(r.export.verbatim), role: "", book: ruleBook(r) };
}
/** A rule list as Sources rows: one row per index, in the list's order (never sorted, merged or cut). */
const sourceRows = (src: readonly number[], rules: Pick<RuleIndex, "get">): SourceRowVM[] => src.map((ix) => sourceRow(rules.get(ix)));

/** A navigation link of the sheet ("How to tell a hatchery fish ›"): a link block, not a source. */
const link = (s: string, open: SheetKey): SheetBlock => ({ k: "link", line: [{ t: "link", s, open } satisfies Run] });
/** (T1031 item 3) the sheet's ONE Source line, its last block: "All sources ›" (the table's every-rule sheet). */
const sourceLink = (s: string, open: SheetKey): SheetBlock => ({ k: "source", line: [{ t: "link", s, open } satisfies Run] });

/** The row a key names in today's table, with its box. */
function rowAt(t: TroutVM, rowKey: string): { box: TroutBoxVM; row: TroutRowVM } | null {
  for (const box of t.boxes) for (const row of box.rows) if (row.key === rowKey) return { box, row };
  return null;
}

/**
 * The fish sheet of row `rowKey` of today's table, or null when the table holds no such row. `rules` resolves the rule
 * indexes (N's check has held each to the store); `fish` is the picture manifest (the page's, else the app's).
 */
export function troutFishSheet(key: SheetKey, t: TroutVM, rowKey: string, rules: Pick<RuleIndex, "get">, fish: FishManifest = FISH,
  answers?: TroutAnswers, water = ""): SheetVM | null {
  const at = rowAt(t, rowKey);
  if (!at) return null;
  const { box, row } = at, s = row.sheet;
  const blocks: SheetBlock[] = [{ k: "value", text: [text(s.head)] }];
  // Here, today
  blocks.push({ k: "head", text: TROUT_SHEET.HERE });
  for (const l of s.here) blocks.push(l.bold ? { k: "value", text: [text(l.t)] } : { k: "para", text: [text(l.t)] });
  for (const c of row.coming) blocks.push({ k: "para", text: [text(c)], tone: "small" });
  // (T883) How this was decided, where every rule of the row's whole list (`row.src`, uncut) has its stored role for this
  // fish. (T1031 item 4) Then this fish's rules as Python gave them (cut to 4) by the authority each is from — a list that
  // claims nothing, each rule its pages (highlighted). Then the table's every rule
  const flow = answers ? troutFlow(row, answers(row.fish), rules, water) : null;
  if (flow) blocks.push({ k: "head", text: WORDS.DECIDED }, { k: "flow", flow });
  if (s.src.length) blocks.push({ k: "head", text: SPECIES_SHEET.BREAKDOWN }, breakdown(s.src.map((ix) => ({ r: rules.get(ix), role: "" }))));
  blocks.push(sourceLink(TROUT_SHEET.ALL_SOURCES, TROUT_SOURCES_KEY));
  return { key, title: row.name, sub: row.label ?? box.label ?? row.origin, blocks };
}

/**
 * (T1031 item 5) The Identification tab of row `rowKey` (`tf:<row>|id`): the fish's pictures, its tips and, on a row that
 * carries an origin, a label or a hatchery/wild split, the hatchery link — app content, not a rule. A fish with no picture
 * or no tips is reported. Null when today's table holds no such row.
 */
export function troutIdSheet(key: SheetKey, t: TroutVM, rowKey: string, fish: FishManifest = FISH): SheetVM | null {
  const at = rowAt(t, rowKey);
  if (!at) return null;
  const { box, row } = at;
  const pics = fish[row.fish] ?? [], tips = FISH_TIPS[row.fish] ?? [];
  if (!pics.length) reportFailure(new TroutLooksMissing(row.fish, "picture"));
  if (!tips.length) reportFailure(new TroutLooksMissing(row.fish, "tips"));
  const blocks: SheetBlock[] = [...imageBlocks(row.fish, pics)];
  if (tips.length) blocks.push({ k: "head", text: WORDS.SHEET_LOOK_FOR }, { k: "list", items: tips.map((x) => ({ line: [text(x)], strong: null })) });
  if (row.origin !== null || row.label !== null || row.split || box.label !== null) blocks.push(link(TROUT_NAV.HATCH, "hatch"));
  return { key, title: row.name, sub: row.label ?? box.label ?? row.origin, blocks };
}

/** (T1031) the trout table's row whose fish is `code` (the first, a twin's hatchery row before its wild one): its key. */
export function troutRowOf(t: TroutVM, code: string): string | null {
  for (const box of t.boxes) for (const row of box.rows) if (row.fish === code) return row.key;
  return null;
}

/** (T883) The rows frame's stored answer for a fish (the same one the other fish's sheets read): its hatchery and wild
 *  answers, each with its roles, and which leads. */
export type TroutAnswers = (fish: string) => FishAnswer | null;

/**
 * (T883) A trout row's rail: each rule of `row.src` (uncut, Python's list) with its stored role in this fish's answer.
 * Never one origin's role for both (re-review a): where the sheet covers both origins, a rule both answers give the same
 * role is one step; a rule they give different roles, or only one of them holds, is a step per origin with its own
 * role, tagged "Hatchery ones" / "Wild ones". A rule neither answer holds: null, and the sheet keeps its Sources list.
 * The answer under the rail is the sheet's own head, whole — and only where every number it names is carried by a step
 * (re-review b: "counts toward the 4 and the 6" with no step saying 6 keeps the list).
 */
export function troutFlow(row: TroutRowVM, a: FishAnswer | null, rules: Pick<RuleIndex, "get">, water: string): FlowVM | null {
  const items = troutItems(row, a, rules);
  if (!items) return null;
  // (T892) the answers the row covers: its origin's, else both (a "Hatchery only" row's wild ones go back by a rule too)
  const cells = (row.origin === "hatchery" ? [a!.hatchery] : row.origin === "wild" ? [a!.wild] : [a!.hatchery, a!.wild])
    .flatMap((d) => (d ? [d] : []));
  const f = flowOf(items, (it) => [text(it.r.facts?.title || clean(it.r.export.verbatim))], water, row.sheet.head, { cells, rules });
  const said = f.steps.map((x) => plain(x.says)).join(" ");
  const carried = (n: string) => new RegExp(`(^|\\D)${n}(\\D|$)`).test(said);
  if (!(row.sheet.head.match(/\d+/g) ?? []).every(carried)) return null;
  return f;
}

/** (T883) A trout row's rail items: each rule of `row.src` with its stored role in this fish's answer (`troutFlow`); null
 *  where a rule has none (the sheet keeps its Sources list). */
function troutItems(row: TroutRowVM, a: FishAnswer | null, rules: Pick<RuleIndex, "get">): LadderItem[] | null {
  if (!a || !row.src.length) return null;
  const rolesOf = (d: Decided | null) => new Map((d?.roles ?? []).map(([ix, role, by]) => [ix, { role, by: by ?? null }] as const));
  const h = rolesOf(a.hatchery), w = rolesOf(a.wild), both = !!a.hatchery && !!a.wild;
  const items: LadderItem[] = [];
  const add = (ix: number, p: { role: string; by: number | null }, origin?: "hatchery" | "wild") =>
    items.push({ r: rules.get(ix), role: p.role, by: p.by == null ? null : rules.get(p.by), spp: new Set(), ...(origin ? { origin } : {}) });
  for (const ix of row.src) {
    const ph = h.get(ix), pw = w.get(ix);
    if (!ph && !pw) return null;
    if (ph && pw && ph.role === pw.role && ph.by === pw.by) add(ix, ph);
    else {
      if (ph) add(ix, ph, both ? "hatchery" : undefined);
      if (pw) add(ix, pw, both ? "wild" : undefined);
    }
  }
  return items;
}

/**
 * (T892) The trout fold's tributary line: where EVERY row of today's table draws its rail (`troutFlow`) and every rail's
 * deciding steps name the same waters "<that water> · its tributaries" (`FlowVM.down`), the fold row of the one table
 * says so once ("Bella Coola River’s rule for its tributaries"); else null. A table only some of whose rows rest on such
 * a rule says it on those rows' own sheets only: the table's rows are Python's words (T810), the app adds none to them.
 */
export function troutFrom(t: TroutVM, answers: TroutAnswers, rules: Pick<RuleIndex, "get">, water: string): string | null {
  const lines = t.boxes.flatMap((b) => b.rows).map((row) => downLine(troutFlow(row, answers(row.fish), rules, water)?.down ?? []));
  return lines.length && lines.every((x) => x !== null && x === lines[0]) ? lines[0]! : null;
}

/** The table's every-rule sheet: `TroutTable.src`, first-cited order as Python gave it. `sub`: the water and the day. */
export function troutSourcesSheet(key: SheetKey, t: TroutVM, rules: Pick<RuleIndex, "get">, sub: string | null): SheetVM {
  return { key, title: TROUT_SHEET.SOURCES, sub, blocks: [{ k: "sources", groups: [{ where: "", rows: sourceRows(t.src, rules) }] }] };
}

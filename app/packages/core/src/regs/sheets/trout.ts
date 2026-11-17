/**
 * (T810 P, behind the dev switch `troutTable`) THE TROUT FISH SHEET (`tf:<row key>`, `tf:ST/hatchery`) and the table's
 * every-rule sheet (`src:trout`, the table's "Sources ›" and the fish sheet's "All sources ›"). Build spec 3.4; contract
 * 2.3 `FishSheet`, `DateSet.sheet`, `TroutTable.src`.
 *
 * Owner rule 2026-11-16 16:50: Python decides every word; the app draws only. So every rule word here is the payload's,
 * drawn unchanged, in the payload's order:
 * - the header: the row's name (title), its "Hatchery only" label or origin word as on the row (sub), `FishSheet.head`;
 * - "Here, today": `FishSheet.here` (bold where `bold`), then the coming-change sentences `DateSet.sheet[key]`;
 * - "How to tell it": app content, not a rule: the fish manifest's pictures (both for Dolly Varden / bull trout), the
 *   tips (`FISH_TIPS`), and the hatchery link on a row that carries an origin or a label;
 * - "Sources": `FishSheet.src` exactly as given (Python cuts it to 4), then "All sources ›" = `TroutTable.src`.
 * THE APP NEVER SORTS, UNIONS OR CUTS A RULE LIST (Q-D6): each list is mapped one index to one row, in order.
 *
 * A fish with no picture or no tips is REPORTED (`reportFailure`), never drawn as a blank section; the vectors' test holds
 * every fish code to both.
 */
import { ruleBook } from "../book";
import { FISH, FISH_TIPS, type FishManifest } from "../fish.generated";
import { reportFailure } from "../../failure";
import type { RuleIndex, RuleView } from "../inputs";
import { text, type Run } from "../line";
import type { SheetBlock, SheetVM, SourceRowVM, TroutBoxVM, TroutRowVM, TroutVM } from "../model";
import { clean } from "../names";
import { imageBlocks } from "./fish";
import type { SheetKey } from "../sheetKey";
import { TROUT_NAV, TROUT_SHEET } from "../words";

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

const link = (s: string, open: SheetKey): SheetBlock => ({ k: "source", line: [{ t: "link", s, open } satisfies Run] });

/** The row a key names in today's table, with its box. */
function rowAt(t: TroutVM, rowKey: string): { box: TroutBoxVM; row: TroutRowVM } | null {
  for (const box of t.boxes) for (const row of box.rows) if (row.key === rowKey) return { box, row };
  return null;
}

/**
 * The fish sheet of row `rowKey` of today's table, or null when the table holds no such row. `rules` resolves the rule
 * indexes (N's check has held each to the store); `fish` is the picture manifest (the page's, else the app's).
 */
export function troutFishSheet(key: SheetKey, t: TroutVM, rowKey: string, rules: Pick<RuleIndex, "get">, fish: FishManifest = FISH): SheetVM | null {
  const at = rowAt(t, rowKey);
  if (!at) return null;
  const { box, row } = at, s = row.sheet;
  const blocks: SheetBlock[] = [{ k: "value", text: [text(s.head)] }];
  // Here, today
  blocks.push({ k: "head", text: TROUT_SHEET.HERE });
  for (const l of s.here) blocks.push(l.bold ? { k: "value", text: [text(l.t)] } : { k: "para", text: [text(l.t)] });
  for (const c of row.coming) blocks.push({ k: "para", text: [text(c)], tone: "small" });
  // How to tell it (app content)
  blocks.push({ k: "head", text: TROUT_SHEET.TELL });
  const pics = fish[row.fish] ?? [], tips = FISH_TIPS[row.fish] ?? [];
  if (!pics.length) reportFailure(new TroutLooksMissing(row.fish, "picture"));
  if (!tips.length) reportFailure(new TroutLooksMissing(row.fish, "tips"));
  blocks.push(...imageBlocks(row.fish, pics));
  if (tips.length) blocks.push({ k: "list", items: tips.map((x) => ({ line: [text(x)], strong: null })) });
  if (row.origin !== null || row.label !== null || box.label !== null) blocks.push(link(TROUT_NAV.HATCH, "hatch"));
  // Sources: this fish's rules as Python gave them, then the table's every rule
  if (s.src.length) blocks.push({ k: "head", text: TROUT_SHEET.SOURCES }, { k: "sources", groups: [{ where: "", rows: sourceRows(s.src, rules) }] });
  blocks.push(link(TROUT_SHEET.ALL_SOURCES, TROUT_SOURCES_KEY));
  return { key, title: row.name, sub: row.label ?? box.label ?? row.origin, blocks };
}

/** The table's every-rule sheet: `TroutTable.src`, first-cited order as Python gave it. `sub`: the water and the day. */
export function troutSourcesSheet(key: SheetKey, t: TroutVM, rules: Pick<RuleIndex, "get">, sub: string | null): SheetVM {
  return { key, title: TROUT_SHEET.SOURCES, sub, blocks: [{ k: "sources", groups: [{ where: "", rows: sourceRows(t.src, rules) }] }] };
}

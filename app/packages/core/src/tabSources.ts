/**
 * (T991 round 3, owner 2026-11-22 ~23:55) ONE SOURCE ROW AT THE FOOT OF EACH TAB, naming only that tab's authorities:
 * "Yes but map sources don't need to be there already has attribution.. also in the synopsis page for skeena at bottom
 * source. Page 9 is not the water body page the source there should be maybe one for each page referenced in the card?
 * Also make sure highlighting is still there for each?"
 *
 * - Regulations: "Source  B.C. Synopsis p.8 ↗  p.9 ↗ …  B.C. in-season changes ↗  DFO notices ↗  <local authority> ↗
 *   All sources ›" — ONE LINK PER SYNOPSIS PAGE the page cites (every "Rule book p.N" run its lines, its section source
 *   lines and its every-source sheet carry, and the water's own rule-book row), never one fixed page; each page link
 *   carries every highlight box the citing rules hold on that PDF page (T215), so the opened copy marks each passage.
 *   The in-season and DFO links appear only where the page shows them; "All sources ›" opens the breakdown sheet (T990).
 * - Conditions: "Source  Environment Canada ↗  BC River Forecast Centre ↗  Open-Meteo ↗" over its "Sources and notes" fold
 *   (the legally exact credits stay verbatim inside it).
 * - Map: none — the map draws its own attribution.
 *
 * The authorities' names are typed once, here (`SOURCE_AUTHORITIES`); their pages are the Sources and licences list's
 * (`LICENCE_ROWS`), so a publisher's address is not typed twice. "↗" leaves the app; "›" opens a sheet in it.
 */
import type { BookBox } from "./answers.generated";
import { LICENCE_ROWS } from "./menu/licences";
import { bookText, type Line } from "./regs/line";
import type { RegsPage } from "./regs/model";
import type { SheetKey } from "./regs/sheetKey";
import { ALL_SOURCES_KEY } from "./regs/sheets/allsources";
import { ALL_SOURCES, WORDS } from "./regs/words";

/** Each authority a tab's Source row can name: its one short name, and the Sources-and-licences row holding its page. */
export const SOURCE_AUTHORITIES = Object.freeze({
  synopsis: { name: "B.C. Synopsis", row: "synopsis" },
  inseason: { name: "B.C. in-season changes", row: "inseason" },
  dfo: { name: "DFO notices", row: "dfo" },
  eccc: { name: "Environment Canada", row: "hydat" },
  rfc: { name: "BC River Forecast Centre", row: "forecast" },
  openMeteo: { name: "Open-Meteo", row: "weather" },
} as const);
export type AuthorityId = keyof typeof SOURCE_AUTHORITIES;

/** The row's label. */
export const SOURCE_ROW_LABEL = "Source";
/** A Source row page link's spoken name. */
export const sourcePageA11y = (printed: number, marked: boolean): string =>
  `${SOURCE_AUTHORITIES.synopsis.name} page ${printed}${marked ? ", every passage this page cites marked" : ""}, opens the PDF`;

/** An authority's page: its Sources-and-licences row's. A row without one is a programming error, thrown (never a blank link). */
export function authorityUrl(id: AuthorityId): string {
  const row = LICENCE_ROWS.find((r) => r.k === SOURCE_AUTHORITIES[id].row);
  if (!row?.url) throw new Error(`tabSources: no page for authority ${id} (LICENCE_ROWS "${SOURCE_AUTHORITIES[id].row}")`);
  return row.url;
}

/** One link of a Source row: a synopsis page (opened highlighted), a web page, or a sheet of the page. `label` is drawn as is. */
export type SourceLinkVM =
  | { readonly k: "book"; readonly label: string; readonly printed: number; readonly pdf: number | null; readonly box?: readonly BookBox[];
      /** what a screen reader says: the page, and that every passage the page cites there is marked — a different action from
       *  a rule's own "Rule book p.N ↗", which marks that rule's passage only (T60: one action, one name) */
      readonly aria: string }
  | { readonly k: "web"; readonly label: string; readonly url: string }
  | { readonly k: "sheet"; readonly label: string; readonly open: SheetKey; readonly aria: string };

export interface TabSourcesVM {
  readonly label: string;
  readonly links: readonly SourceLinkVM[];
}

/** The whole row as one string (the a11y summary and the test's pin, AGENTS 33): the label, then each link's words. */
export const sourcesText = (vm: TabSourcesVM): string => [vm.label, ...vm.links.map((l) => l.label)].join(" ");

const web = (id: AuthorityId): SourceLinkVM => ({ k: "web", label: `${SOURCE_AUTHORITIES[id].name} ${WORDS.LINK_OUT}`, url: authorityUrl(id) });

type Page = { pdf: number | null; boxes: BookBox[] };
const boxKey = (b: BookBox) => `${b.pdf}:${b.box.join(",")}`;

/**
 * Every synopsis page `v` links, by printed page, with the highlight boxes of every run citing it (deduplicated): a
 * `{t:"book"}` run anywhere (a page's lines, a sheet's rows), and a section source line's `pages` (`SourceLineVM`) —
 * except a per-row gear source (`srcs:gl|…`), which is not drawn since T900. Functions (the page's lazy `sheet`) are
 * not walked; their sheets are asked for by name.
 */
function citedPages(v: unknown, out: Map<number, Page>, seen = new Set<unknown>()): Map<number, Page> {
  if (!v || typeof v !== "object" || seen.has(v)) return out;
  seen.add(v);
  if (Array.isArray(v)) { for (const x of v) citedPages(x, out, seen); return out; }
  const o = v as Record<string, unknown>;
  const add = (printed: number, pdf: number | null, box: unknown) => {
    const p = out.get(printed) ?? { pdf: null, boxes: [] };
    if (p.pdf === null) p.pdf = pdf;
    for (const b of Array.isArray(box) ? (box as BookBox[]) : []) if (!p.boxes.some((x) => boxKey(x) === boxKey(b))) p.boxes.push(b);
    out.set(printed, p);
  };
  if (o.t === "book" && typeof o.printed === "number") add(o.printed, typeof o.pdf === "number" ? o.pdf : null, o.box);
  if (typeof o.open === "string" && o.open.startsWith("srcs:") && !o.open.startsWith("srcs:gl|") && Array.isArray(o.pages))
    for (const p of o.pages as readonly { printed?: unknown; pdf?: unknown; box?: unknown }[])
      if (typeof p?.printed === "number") add(p.printed, typeof p.pdf === "number" ? p.pdf : null, p.box);
  for (const k in o) citedPages(o[k], out, seen);
  return out;
}

/** The local authorities the page's rules cite (T981, a rule's `facts.cited`): each "<site> ↗" once, as the rule names it. */
function citedWeb(v: unknown, out: Map<string, string>, seen = new Set<unknown>()): Map<string, string> {
  if (!v || typeof v !== "object" || seen.has(v)) return out;
  seen.add(v);
  if (Array.isArray(v)) { for (const x of v) citedWeb(x, out, seen); return out; }
  const o = v as Record<string, unknown>;
  if (o.t === "web" && typeof o.s === "string" && typeof o.url === "string" && !out.has(o.url)) out.set(o.url, o.s);
  for (const k in o) citedWeb(o[k], out, seen);
  return out;
}

/**
 * The Regulations tab's Source row. `waterPages`: the water's own rule-book row (core `waterPagesOf`); `inSeason`: the
 * page shows a Province in-season change; `dfo`: the page shows DFO's salmon (`salmonShowsDfo`) — or a rule on it is
 * DFO's (the every-source sheet's DFO group).
 */
export function regsSourcesOf(page: RegsPage, o: { waterPages?: Line; inSeason: boolean; dfo: boolean }): TabSourcesVM {
  const all = page.sheet(ALL_SOURCES_KEY);
  const pages = citedPages(o.waterPages ?? [], citedPages(all, citedPages(page, new Map())));
  const links: SourceLinkVM[] = [...pages].sort((a, b) => a[0] - b[0]).map(([printed, p], i): SourceLinkVM => {
    const words = bookText({ printed, pdf: p.pdf, bare: true });
    return { k: "book", label: i ? words : `${SOURCE_AUTHORITIES.synopsis.name} ${words}`, printed, pdf: p.pdf,
             ...(p.boxes.length ? { box: p.boxes } : {}), aria: sourcePageA11y(printed, p.boxes.length > 0) };
  });
  if (o.inSeason) links.push(web("inseason"));
  const dfoRule = !!all && all.blocks.some((b) => b.k === "sources" && b.groups.some((g) => g.where === ALL_SOURCES.DFO));
  if (o.dfo || dfoRule) links.push(web("dfo"));
  for (const [url, s] of citedWeb(all, new Map())) links.push({ k: "web", label: `${s} ${WORDS.LINK_OUT}`, url });
  return { label: SOURCE_ROW_LABEL, links: [...links, ...regsSourcesNone().links] };
}

/** The Regulations row of a page whose host computed no sources (a frozen test page, RegsScreen's default): "All sources ›" only. */
export const regsSourcesNone = (): TabSourcesVM =>
  ({ label: SOURCE_ROW_LABEL, links: [{ k: "sheet", label: ALL_SOURCES.LINK, open: ALL_SOURCES_KEY, aria: ALL_SOURCES.ARIA }] });

/**
 * Whether the page shows DFO's salmon: a salmon year bar some of whose days are DFO's (`by` "dfo" / "both"), or, with no
 * bar, the at-a-glance table's Salmon row (its block is tagged DFO, KeepTab).
 */
export function salmonShowsDfo(strip: { readonly by: "dfo" | "both" | "book" } | null, glanceSalmon: boolean): boolean {
  return strip ? strip.by !== "book" : glanceSalmon;
}

/** The Conditions tab's Source row: the gauges' readings, the River Forecast Centre's forecasts, the weather. */
export function conditionsSourcesOf(): TabSourcesVM {
  return { label: SOURCE_ROW_LABEL, links: [web("eccc"), web("rfc"), web("openMeteo")] };
}

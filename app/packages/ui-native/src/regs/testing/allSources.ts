/**
 * (T1036) THE EVERY-SOURCE SHEET'S PROBE, for a water screen drawn by a test: open "All sources ›" on the foot row and read
 * every synopsis page its rows link ("B.C. Synopsis page N, …") and every rule it lists (`rule-row:<key>`), then the trout
 * table's every-rule sheet (its Source line's "Who says so ›", where the table is drawn) the same way. Each sheet is
 * closed again (Done), so the screen is as it was. The test hands in its testing library (`act`, `fireEvent`, `screen`):
 * only a test file may import it (check-boundaries).
 * (T1036, owner 2026-11-24) The sheet has two groups: what the drawn lines cite, then under its head (`ALL_SOURCES.OTHER`)
 * what the sheets one tap away cite. Each is read apart, by where its rows are drawn (before or after that head).
 */
import { ALL_SOURCES } from "@app/core";
export interface ProbeLib {
  act(f: () => void): unknown;
  fireEvent: { click(e: Element): unknown };
  screen: { getByTestId(id: string): HTMLElement; queryByTestId(id: string): HTMLElement | null; getByLabelText(label: string): HTMLElement;
            queryAllByLabelText?(label: string): HTMLElement[] };
}

/**
 * (T1036, review of PR 126) THE SHEETS THE DRAWN SCREEN REALLY OPENS, independent of core's walk: every press target of the
 * screen (`[role=button]`, `[role=link]`; the foot Source row's and the folds' own presses aside, a "B.C. Synopsis page"
 * link aside: it opens the book) is pressed in turn, and the sheet key the screen asks its sheets for (`opened`, recorded
 * by the test where the screen reads its sheet) is kept; every sheet that opened is closed again (Done). The test opens
 * every fold first.
 */
export function pressedSheets({ act, fireEvent, screen }: ProbeLib, root: () => Element, opened: { keys: string[] }): string[] {
  const out = new Set<string>();
  for (let i = 0; ; i++) {
    const els = [...root().querySelectorAll("[role=button],[role=link]")]
      .filter((e) => !e.closest('[data-testid="regs-sources"]') && !e.closest('[data-testid="sheet-body"]') && e.getAttribute("aria-expanded") === null);
    if (i >= els.length) break;
    const el = els[i]!;
    if (PAGE.test(el.getAttribute("aria-label") ?? "")) continue;
    opened.keys = [];
    act(() => { fireEvent.click(el); });
    if (opened.keys.length) out.add(opened.keys[0]!);
    for (let n = 0; n < 5 && (screen.queryAllByLabelText?.("Done") ?? []).length; n++)
      act(() => { for (const d of screen.queryAllByLabelText!("Done")) fireEvent.click(d); });
  }
  return sorted(out);
}

const PAGE = /^B\.C\. Synopsis page (\d+),/;
const sorted = <T,>(xs: Iterable<T>): T[] => [...new Set(xs)].sort();

function opened({ act, fireEvent, screen }: ProbeLib, open: Element): { pages: number[]; keys: string[]; otherPages: number[]; other: string[]; heads: number } {
  act(() => { fireEvent.click(open); });
  const body = screen.getByTestId("sheet-body"), links = [...body.querySelectorAll("[role=link]")];
  const heads = [...body.querySelectorAll("[role=heading]")].filter((e) => e.textContent === ALL_SOURCES.OTHER);
  /** drawn after the second group's head */
  const second = (e: Element) => !!heads[0] && !!(heads[0].compareDocumentPosition(e) & 4 /* Node.DOCUMENT_POSITION_FOLLOWING */);
  const pagesOf = (els: Element[]) => [...new Set(els.flatMap((e) => { const m = PAGE.exec(e.getAttribute("aria-label") ?? ""); return m ? [Number(m[1])] : []; }))].sort((a, b) => a - b);
  const rows = [...body.querySelectorAll('[data-testid^="rule-row:"]')];
  const keyOf = (e: Element) => e.getAttribute("data-testid")!.slice("rule-row:".length);
  const out = { pages: pagesOf(links.filter((e) => !second(e))), keys: sorted(rows.filter((e) => !second(e)).map(keyOf)),
                otherPages: pagesOf(links.filter(second)), other: sorted(rows.filter(second).map(keyOf)), heads: heads.length };
  act(() => { fireEvent.click(screen.getByLabelText("Done")); });
  return out;
}

/** `all` / `keys`: the every-source sheet's first group's pages and rules (what the drawn lines cite); (T1036) `other` /
 *  `otherPages`: its second group's (what the sheets one tap away cite), `heads` how many times that group's head is drawn;
 *  `trout`: the trout table's sheet's pages ([] where no table). */
export function sheetPages(lib: ProbeLib): { all: number[]; keys: string[]; other: string[]; otherPages: number[]; heads: number; trout: number[] } {
  const all = opened(lib, lib.screen.getByTestId("regs-all-sources-button"));
  const table = lib.screen.queryByTestId("trout-sources");
  return { all: all.pages, keys: all.keys, other: all.other, otherPages: all.otherPages, heads: all.heads,
           trout: table ? opened(lib, table).pages : [] };
}

/** The pages the every-source sheet must list: those the drawn tabs link, and the drawn trout table's (its line opens them). */
export const sheetExpected = (drawn: Iterable<number>, trout: readonly number[]): number[] =>
  [...new Set([...drawn, ...trout])].sort((a, b) => a - b);

/** A part's rule index, as the screen hands it to the Source row. */
export interface ProbeIndex { has(i: number): boolean; get(i: number): { key: string }; byKey?(k: string): { key: string; missing?: boolean } | null }

/**
 * The rules the sheet must list, read off the view models the screen DRAWS (captured where it hands them to the Source row)
 * by the ticket's words, not by core's walk: each key on a drawn section Source line (a per-row gear line, `srcs:gl|…`, is not
 * drawn), each `rule` of a drawn line that shows a "Rule book p.N" or a cited page beside it, each rule index of the drawn
 * trout table. A rule the index lacks is not listed (it is reported).
 */
export function expectedKeys(drawn: unknown, index: ProbeIndex): string[] {
  const out: string[] = [], seen = new Set<unknown>();
  const byKey = (k: string) => { const r = index.byKey?.(k); if (r && !r.missing) out.push(r.key); };
  const go = (v: unknown, field: string): void => {
    if (typeof v === "number" && field === "src") { if (index.has(v)) out.push(index.get(v).key); return; }
    if (!v || typeof v !== "object" || seen.has(v)) return;
    seen.add(v);
    if (Array.isArray(v)) { v.forEach((x) => go(x, field)); return; }
    const o = v as Record<string, unknown>;
    const open = typeof o.open === "string" ? o.open : "";
    if (open.startsWith("srcs:") && !open.startsWith("srcs:gl|")) (o.keys as string[] | undefined)?.forEach(byKey);
    const runs = Object.values(o).filter(Array.isArray).flat() as { t?: string }[];
    if (typeof o.rule === "string" && runs.some((r) => r?.t === "book" || r?.t === "web")) byKey(o.rule);
    for (const [k, x] of Object.entries(o)) go(x, k);
  };
  go(drawn, "");
  return sorted(out);
}

/** A sheet as the probe reads it: its blocks and tabs (`SheetVM`, read by shape). */
type ProbeSheet = { blocks: readonly unknown[]; tabs?: readonly { open: string }[] } | null;

/**
 * (T1036) The second group's rules, read by the ticket's words, not by core's walk: open every sheet the drawn screen's
 * presses opened (`pressed`, `pressedSheets`: not the every-source sheet, not a per-row gear line's — none is drawn), and
 * each of its tabs; take each rule it lists in a Sources list (not an "Overruled" card), each rule whose sheet a line opens
 * beside a "B.C. Synopsis p.N" / cited page, and a rule's own sheet's rule; minus `first` (the first group); the index's
 * key for each it holds.
 */
export function expectedOtherKeys(pressed: readonly string[], sheet: (k: string) => ProbeSheet, index: ProbeIndex, first: readonly string[]): string[] {
  const keys: string[] = [], seen = new Set<unknown>();
  const go = (v: unknown, into: (o: Record<string, unknown>) => void): void => {
    if (!v || typeof v !== "object" || seen.has(v)) return;
    seen.add(v);
    if (Array.isArray(v)) { v.forEach((x) => go(x, into)); return; }
    into(v as Record<string, unknown>);
    for (const x of Object.values(v as Record<string, unknown>)) go(x, into);
  };
  const done = new Set<string>();
  const read = (k: string) => {
    if (done.has(k) || k === "src:all") return;
    done.add(k);
    const vm = sheet(k);
    if (!vm) return;
    if (k.startsWith("rule:")) keys.push(k.slice(5));
    go(vm.blocks, (o) => {
      if (o.k === "sources" && o.overruled !== true)
        for (const g of o.groups as { rows: { key: string | null }[] }[]) for (const r of g.rows) if (r.key) keys.push(r.key);
      const runs = Object.values(o).filter(Array.isArray).flat() as { t?: string; open?: string }[];
      if (typeof o.open === "string" && o.open.startsWith("rule:") && runs.some((r) => r?.t === "book" || r?.t === "web")) keys.push(o.open.slice(5));
      for (const line of Object.values(o).filter(Array.isArray) as { t?: string; open?: string }[][])
        if (line.some((r) => r?.t === "book" || r?.t === "web"))
          for (const r of line) if (r?.t === "link" && r.open?.startsWith("rule:")) keys.push(r.open.slice(5));
    });
    for (const t of vm.tabs ?? []) read(t.open);
  };
  pressed.forEach(read);
  const out = new Set<string>();
  for (const k of keys) { const r = index.byKey?.(k); if (r && !r.missing && !first.includes(r.key)) out.add(r.key); }
  return sorted(out);
}

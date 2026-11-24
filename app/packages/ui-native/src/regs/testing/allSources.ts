/**
 * (T1036) THE EVERY-SOURCE SHEET'S PROBE, for a water screen drawn by a test: open "All sources ›" on the foot row and read
 * every synopsis page its rows link ("B.C. Synopsis page N, …") and every rule it lists (`rule-row:<key>`), then the trout
 * table's every-rule sheet (its Source line's "Who says so ›", where the table is drawn) the same way. Each sheet is
 * closed again (Done), so the screen is as it was. The test hands in its testing library (`act`, `fireEvent`, `screen`):
 * only a test file may import it (check-boundaries).
 */
export interface ProbeLib {
  act(f: () => void): unknown;
  fireEvent: { click(e: Element): unknown };
  screen: { getByTestId(id: string): HTMLElement; queryByTestId(id: string): HTMLElement | null; getByLabelText(label: string): HTMLElement };
}

const PAGE = /^B\.C\. Synopsis page (\d+),/;
const sorted = <T,>(xs: Iterable<T>): T[] => [...new Set(xs)].sort();

function opened({ act, fireEvent, screen }: ProbeLib, open: Element): { pages: number[]; keys: string[] } {
  act(() => { fireEvent.click(open); });
  const body = screen.getByTestId("sheet-body"), links = [...body.querySelectorAll("[role=link]")];
  const pages = [...new Set(links.flatMap((e) => { const m = PAGE.exec(e.getAttribute("aria-label") ?? ""); return m ? [Number(m[1])] : []; }))];
  const keys = [...body.querySelectorAll('[data-testid^="rule-row:"]')].map((e) => e.getAttribute("data-testid")!.slice("rule-row:".length));
  act(() => { fireEvent.click(screen.getByLabelText("Done")); });
  return { pages: pages.sort((a, b) => a - b), keys: sorted(keys) };
}

/** `all` / `keys`: the every-source sheet's pages and rules; `trout`: the trout table's sheet's pages ([] where no table). */
export function sheetPages(lib: ProbeLib): { all: number[]; keys: string[]; trout: number[] } {
  const all = opened(lib, lib.screen.getByTestId("regs-all-sources-button"));
  const table = lib.screen.queryByTestId("trout-sources");
  return { all: all.pages, keys: all.keys, trout: table ? opened(lib, table).pages : [] };
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

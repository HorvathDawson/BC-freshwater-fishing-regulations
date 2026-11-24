/**
 * (T1036) THE EVERY-SOURCE SHEET'S PROBE, for a water screen drawn by a test: open "All sources ›" on the foot row and read
 * every synopsis page its rows link ("B.C. Synopsis page N, …"), then the trout table's every-rule sheet (its Source line's
 * "Who says so ›", where the table is drawn) the same way. Each sheet is closed again (Done), so the screen is as it was.
 * The test hands in its testing library (`act`, `fireEvent`, `screen`): only a test file may import it (check-boundaries).
 */
export interface ProbeLib {
  act(f: () => void): unknown;
  fireEvent: { click(e: Element): unknown };
  screen: { getByTestId(id: string): HTMLElement; queryByTestId(id: string): HTMLElement | null; getByLabelText(label: string): HTMLElement };
}

const PAGE = /^B\.C\. Synopsis page (\d+),/;

function openedPages({ act, fireEvent, screen }: ProbeLib, open: Element): number[] {
  act(() => { fireEvent.click(open); });
  const links = [...screen.getByTestId("sheet-body").querySelectorAll("[role=link]")];
  const pages = [...new Set(links.flatMap((e) => { const m = PAGE.exec(e.getAttribute("aria-label") ?? ""); return m ? [Number(m[1])] : []; }))];
  act(() => { fireEvent.click(screen.getByLabelText("Done")); });
  return pages.sort((a, b) => a - b);
}

/** `all`: the every-source sheet's pages; `trout`: the trout table's sheet's pages ([] where no table is drawn). */
export function sheetPages(lib: ProbeLib): { all: number[]; trout: number[] } {
  const all = openedPages(lib, lib.screen.getByTestId("regs-all-sources-button"));
  const table = lib.screen.queryByTestId("trout-sources");
  return { all, trout: table ? openedPages(lib, table) : [] };
}

/** The pages the every-source sheet must list: those the drawn tabs link, and the drawn trout table's (its line opens them). */
export const sheetExpected = (drawn: Iterable<number>, trout: readonly number[]): number[] =>
  [...new Set([...drawn, ...trout])].sort((a, b) => a - b);

/**
 * T37 render: the one sheet over the Regulations view, mounted over the FROZEN pages (fixtures/*.json — buildRegsPage over
 * the parity store, each golden sheet key through `page.sheet`; `UPDATE_REGS_FIXTURES=1` writes them, never by hand).
 * Then the golden mount: every sheet vedder_1010 can open draws every line the golden holds for it, in order.
 *
 * react-native-web's Modal renders into a portal: queries go to `screen` (the document), not the container.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { appKeyOf, type RegsPage, type SheetKey, type SheetVM } from "@app/core";
import { BackLayerContext, type BackLayers } from "@app/ui";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { SheetHost } from "./SheetHost";
import { RegsView } from "./RegsView";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";
import klwa1010 from "./fixtures/klwa_1010.json";
import golden from "../../../../design/regs-v11/port/golden/golden.json";

afterEach(cleanup);

type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
/** a RegsPage whose sheet() is the frozen one (fixtures.ts freezePage's inverse) */
const sheetPage = (f: unknown): RegsPage => { const p = f as Frozen; return { ...p, sheet: (k: SheetKey) => p.sheets[k] ?? null } as RegsPage; };
const VEDDER = sheetPage(vedder1010), KLWA = sheetPage(klwa1010);

function host(open: SheetKey | null, page = VEDDER) {
  const opened: SheetKey[] = [], onClose = vi.fn(), onBook = vi.fn();
  const el = (k: SheetKey | null) => <SheetHost page={page} open={k} onOpen={(x) => opened.push(x)} onClose={onClose} onBook={onBook} palette={LIGHT} />;
  const r = render(el(open));
  return { r, opened, onClose, onBook, rerender: (k: SheetKey | null) => r.rerender(el(k)) };
}

describe("<SheetHost> over the frozen vedder_1010 page", () => {
  it("1. opens one sheet by key: its title is the heading, one Done", () => {
    host("gl:catch_and_release");
    expect(screen.getByRole("heading", { name: "Catch and release (release)" })).toBeTruthy();
    expect(screen.getAllByLabelText("Done")).toHaveLength(1);
  });

  it("2. a fold starts closed; pressing it shows the book's words (sheet-book-words)", () => {
    host("gl:catch_and_release");
    const fold = screen.getByRole("button", { name: "In the book’s words" });
    const quote = "Catch and Release: You may fish for the named species, but you must release any that you catch.";
    expect([fold.getAttribute("aria-expanded"), screen.queryByText(quote)]).toEqual(["false", null]);
    fireEvent.click(fold);
    expect([fold.getAttribute("aria-expanded"), !!screen.getByText(quote)]).toEqual(["true", true]);
  });

  it("3. a press inside opens the next sheet in this one's place (one sheet at a time)", () => {
    const h = host("pos:pos");
    fireEvent.click(screen.getByText("Source ›"));
    expect(h.opened).toEqual(["rule:z2:possession_quota::possession_quota.r1"]);
    h.rerender("rule:zp:conduct::conduct.r1");
    expect(screen.getByRole("heading", { name: "Do not waste the fish you catch" })).toBeTruthy();
    expect(screen.queryByRole("heading", { name: "Possession" })).toBeNull();
    expect(screen.getAllByLabelText("Done")).toHaveLength(1);
  });

  it("4. Done and the scrim each close it once", () => {
    const h = host("hatch");
    act(() => { screen.getByLabelText("Done").click(); screen.getByLabelText("Close").click(); });
    expect(h.onClose).toHaveBeenCalledTimes(2);
  });

  it("G1: a downward flick on the handle closes it (the one Sheet's drag); a slow drag springs back", () => {
    const flick = (vy: number) => { const h = host("pos:pos");
      act(() => { fireGestureHandler(getByGestureTestId("sheet-drag"), [{ translationY: 0 }, { translationY: 20 }, { translationY: 40, velocityY: vy }]); });
      const n = h.onClose.mock.calls.length; cleanup(); return n; };
    expect([flick(900), flick(200)]).toEqual([1, 0]);
  });

  it("G1: back closes it first — the sheet is a back layer while open, and lets go when closed", () => {
    const log: string[] = [];
    const layers: BackLayers = { register: () => { log.push("register"); return () => { log.push("unregister"); }; }, back: () => true };
    const el = (k: SheetKey | null) => (
      <BackLayerContext.Provider value={layers}>
        <SheetHost page={VEDDER} open={k} onOpen={() => {}} onClose={() => {}} palette={LIGHT} />
      </BackLayerContext.Provider>);
    const r = render(el("hatch"));
    r.rerender(el("pos:pos"));                                      // a nested open replaces: still ONE back layer
    r.rerender(el(null));
    expect(log).toEqual(["register", "unregister"]);
  });

  it("G1: the regs view's sheet is a back layer of the provider above it (Shell wraps the water body in it): one back closes it, not the water", () => {
    const log: string[] = [];
    let close: (() => void) | null = null;
    const layers: BackLayers = { register: (c) => { log.push("register"); close = c; return () => { log.push("unregister"); }; },
      back: () => { log.push("back"); close?.(); return true; } };
    render(<BackLayerContext.Provider value={layers}><RegsView page={VEDDER} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} }} /></BackLayerContext.Provider>);
    fireEvent.click(screen.getByText("Kokanee"));
    act(() => { close!(); });
    expect([log, screen.queryByLabelText("Done"), !!screen.getByTestId("regs-view")]).toEqual([["register", "unregister"], null, true]);
  });

  it("5. a stale key (the page has no such sheet) draws nothing and does not throw", () => {
    host("other:o99");
    expect(screen.queryByLabelText("Done")).toBeNull();
    host(null);
    expect(screen.queryByLabelText("Done")).toBeNull();
  });

  it("6. the sheet sits over the regs view: the header's place and date stay mounted (howtell-current-water)", () => {
    render(<RegsView page={VEDDER} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} }} />);
    fireEvent.click(screen.getByText("Kokanee"));
    expect(screen.getByRole("heading", { name: "Kokanee" })).toBeTruthy();
    expect(screen.getByText(VEDDER.header.date)).toBeTruthy();
    expect(screen.getByTestId("regs-view")).toBeTruthy();
  });

  it("7. book links: a rule sheet's read 'Rule book p.N ↗' and open their PDF page; a glossary page is bare after 'Rule book'", () => {
    const h = host("rule:zp:conduct::conduct.r1");
    const links = screen.getAllByRole("link").map((e) => e.textContent);
    expect(links).toEqual(["Rule book p.8 ↗", "Rule book p.10 ↗"]);
    fireEvent.click(screen.getByText("Rule book p.8 ↗"));
    expect(h.onBook).toHaveBeenCalledWith(10);
    cleanup();
    host("gl:catch_and_release");
    // the release ships no printed → PDF map yet (T36 PIPELINE NEED): the glossary page is drawn unlinked
    expect(screen.getByTestId("sheet-body").textContent).toContain("Rule book p.4");
  });

  it("8. a picture in an other: sheet opens the fish sheet", () => {
    const h = host("other:o0");
    fireEvent.click(screen.getByRole("button", { name: "What a Kokanee looks like" }));
    expect(h.opened).toEqual(["sp:KO"]);
  });

  it("the weekday sheet (klwa_1010 other:o0): today's part is marked with the visible word Today", () => {
    host("other:o0", KLWA);
    expect(screen.getByTestId("sheet-body").textContent).toContain("Sat–Sun Keep up to 5 Today");
  });

  it("the sub line: a row's sources sheet names the water and the day", () => {
    host("src:r0");
    expect(screen.getByTestId("sheet-sub").textContent).toBe(
      "Vedder River · Oct 10. The more specific rule speaks first: this water, then from downstream, a named area, the region, then the province.");
  });
});

/** whitespace dropped: the golden joins separate elements with one space, the components draw them as separate Text */
const bare = (s: string) => s.replace(/\s+/g, "");

describe("C. golden mount: every vedder_1010 sheet draws every golden line, in order", () => {
  type G = { title: string; lines: string[] };
  const G = golden as unknown as { states: Record<string, { sheets: Record<string, G | "@"> }>; sheets: Record<string, G> };
  const keys = Object.entries(G.states.vedder_1010!.sheets);
  it(`all ${keys.length} sheet keys`, () => {
    const missing: string[] = [];
    for (const [gk, v] of keys) {
      if (gk === "parts") continue;                                 // T36: a pipeline need (no parts sheet)
      const want = v === "@" ? G.sheets[gk]! : v;
      const r = host(appKeyOf(gk, "vedder_1010"));
      // open every fold, nested ones too (a closed fold draws no body)
      for (let k = 0; k < 20; k++) {
        const closed = screen.queryAllByRole("button").filter((b) => b.getAttribute("aria-expanded") === "false");
        if (!closed.length) break;
        closed.forEach((b) => fireEvent.click(b));
      }
      const text = bare(screen.getByRole("heading", { name: want.title }).textContent! + screen.getByTestId("sheet-body").textContent!);
      let at = 0;
      for (const line of want.lines) {
        // the release has no printed → PDF map (T36 PIPELINE NEED): a glossary page is drawn without its ↗
        const l = bare(/^(gl:|hatch$)/.test(gk) ? line.replace(/ ↗/g, "") : line);
        const i = text.indexOf(l, at);
        if (i < 0) { missing.push(`${gk}: ${line}`); continue; }
        at = i + l.length;
      }
      r.r.unmount();
    }
    expect(missing).toEqual([]);
  });
});

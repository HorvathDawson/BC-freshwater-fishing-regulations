/**
 * T37 render: the one sheet over the Regulations view, mounted over the FROZEN pages (fixtures/*.json — buildRegsPage over
 * the parity store, each sheet key through `page.sheet`; `UPDATE_REGS_FIXTURES=1` writes them, never by hand).
 * (T439: the golden mount, every vedder_1010 sheet against golden.json's lines, is gone.)
 *
 * react-native-web's Modal renders into a portal: queries go to `screen` (the document), not the container.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import {
  TROUT_NAV, TROUT_SHEET, TROUT_SOURCES_KEY, troutFishSheet, troutOf, troutSourcesSheet,
  type RegsInputs, type RegsPage, type RuleIndex, type SheetKey, type SheetVM, type TroutValue, type TroutVM,
} from "@app/core";
import vectors from "../../../../fixtures/trout/trout-vectors.generated.json";
import { BackLayerContext, type BackLayers } from "@app/ui";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { SheetHost } from "./SheetHost";
import { RegsScreen } from "./RegsScreen";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";
import klwa1010 from "./fixtures/klwa_1010.json";
import klwa1012 from "./fixtures/klwa_1012.json";

afterEach(cleanup);

type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
/** a RegsPage whose sheet() is the frozen one (fixtures.ts freezePage's inverse) */
const sheetPage = (f: unknown): RegsPage => { const p = f as Frozen; return { ...p, sheet: (k: SheetKey) => p.sheets[k] ?? null } as RegsPage; };
const VEDDER = sheetPage(vedder1010), KLWA = sheetPage(klwa1010), KLWA12 = sheetPage(klwa1012);

// (T810 P) the trout sheets over the shared vectors: N's view model, P's builders, a rule index of stand-ins ("Rule <ix>")
interface TroutCase { readonly name: string; readonly day: string; readonly frame: TroutValue | null }
const TROUT_CASES = (vectors as unknown as { cases: readonly TroutCase[] }).cases.filter((c) => c.frame !== null);
function troutVmOf(c: TroutCase): TroutVM {
  const [month, day] = c.day.split("-").map(Number) as [number, number];
  const inp = { trout: c.frame, rules: { has: () => true }, frames: { rows: { rows: { length: 1e6 } } } } as unknown as RegsInputs;
  const vm = troutOf(inp, { month, day }, { month, day });
  if (vm?.kind !== "table") throw new Error(`${c.name}: no table`);
  return vm;
}
const RULES = { get: (ix: number) => ({ rix: ix, key: `k${ix}`, missing: false, export: { verbatim: `words of ${ix}` },
  facts: { title: `Rule ${ix}`, book: [[40, 42]] } }) } as unknown as RuleIndex;
const troutPage = (t: TroutVM): RegsPage => ({ ...VEDDER, sheet: (k: SheetKey) =>
  (k === TROUT_SOURCES_KEY ? troutSourcesSheet(k, t, RULES, null) : k.startsWith("tf:") ? troutFishSheet(k, t, k.slice(3), RULES) : null) });

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
    render(<BackLayerContext.Provider value={layers}><RegsScreen page={VEDDER} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} }} onDate={() => {}} /></BackLayerContext.Provider>);
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
    render(<RegsScreen page={VEDDER} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} }} onDate={() => {}} />);
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
    // the release ships the printed → PDF map (export.pdf_pages): the glossary page is a link
    expect(screen.getByTestId("sheet-body").textContent).toContain("Rule book p.4 ↗");
  });

  it("8. a picture in an other: sheet opens the fish sheet", () => {
    const h = host("other:o0");
    fireEvent.click(screen.getByRole("button", { name: "Kokanee — what it looks like" }));
    expect(h.opened).toEqual(["sp:KO"]);
  });

  it("the weekday sheet (klwa_1010 other:o0): today's part is marked with the visible word Today", () => {
    host("other:o0", KLWA);
    // (T883) the part is a step of the flow: its words, then its tag "Today" on the line under them
    expect(screen.getByTestId("sheet-body").textContent).toContain("Sat–Sun: Keep up to 5p.37 ↗Today");
  });

  it("(T883 review 3) the weekday part is marked from the VIEWED day: Monday Oct 12 tags Mon–Fri, not the stored day's part", () => {
    host("other:o0", KLWA12);
    const tags = screen.getAllByTestId("flow:tag").map((e) => e.textContent);
    const text = screen.getByTestId("sheet-body").textContent!;
    expect(text).toContain("Mon–Fri: Release every onep.37 ↗Mon");
    expect(text).not.toMatch(/Sat–Sun: Keep up to 5(p\.37 ↗)?(Today|Mon(?!–))/);
    expect(tags.filter((t) => t === "Today" || t === "Mon")).toEqual(["Mon"]);
  });

  it("(T883 review 7) a step's tag is read once: the rule's press is named by its words alone", () => {
    host("other:o0", KLWA12);
    const press = screen.getByRole("button", { name: "Mon–Fri: Release every one" });
    expect(press.getAttribute("aria-label")).toBe("Mon–Fri: Release every one");
    expect(screen.getAllByTestId("flow:tag").filter((e) => e.textContent === "Mon")).toHaveLength(1);
  });

  it("(T883, owner 23:25 'The white fish repeats a lot at the top') a group sheet says its answer once: no head, no lone Rule book line; the multi-water link goes last", () => {
    host("other:o1", KLWA12);
    const body = screen.getByTestId("sheet-body"), text = body.textContent!;
    expect(screen.queryByTestId("kicker:scope")).toBeNull();
    expect(text).not.toMatch(/a day per angler across/);
    expect(text).toMatch(/Up to 15.*whitefish combined/);
    expect(text.indexOf("How this was decided")).toBeLessThan(text.indexOf("Fishing more than one water today?"));
    expect(text.trim().endsWith("See every regulation line ›")).toBe(true);
    // the only book links left are the rail's
    expect(text.match(/Rule book p\.\d+/g)).toBeNull();
  });

  it("(T810 P) a trout row's fish sheet: header, Here today (bold where bold), how to tell it, Sources in order; it closes as every sheet", () => {
    // a hatchery-only steelhead row with its bold stop line, from the shared vectors, through N's troutOf and P's builder
    const pick = TROUT_CASES.flatMap((c) => { const t = troutVmOf(c); return t.boxes.flatMap((b) => b.rows.map((r) => ({ t, b, r }))); })
      .find(({ b, r }) => (b.label ?? r.label) !== null && r.sheet.here.some((l) => l.bold) && r.sheet.here.some((l) => !l.bold) && r.sheet.src.length > 1)!;
    const { t, b, r } = pick, page = troutPage(t);
    const h = host(`tf:${r.key}`, page);
    expect(screen.getByRole("heading", { name: r.name })).toBeTruthy();
    expect(screen.getByTestId("sheet-sub").textContent).toBe(r.label ?? b.label);
    const body = screen.getByTestId("sheet-body");
    for (const s of [r.sheet.head, TROUT_SHEET.HERE, TROUT_SHEET.TELL, TROUT_SHEET.SOURCES, ...r.sheet.here.map((l) => l.t), ...r.coming])
      expect(body.textContent, s).toContain(s);
    // (T883 review 1) a trout sheet with no stored roles for its rules keeps `src` (at most 4 rules) as a list that claims
    // nothing: never the decided rail, no "So:" answer
    expect(within(body).queryByTestId("flow")).toBeNull();
    expect(body.textContent).not.toContain("So: ");
    // bold where the payload says bold, and only there
    const face = (s: string) => getComputedStyle(within(body).getByText(s)).fontFamily;
    const bold = face(r.sheet.head);
    expect(r.sheet.here.map((l) => face(l.t) === bold)).toEqual(r.sheet.here.map((l) => l.bold));
    // Sources: one row per index of `src`, in its order (each the rule's title), then "All sources ›"
    const titles = r.sheet.src.map((ix) => `Rule ${ix}`);
    const shown = titles.map((x) => body.textContent!.indexOf(x));
    expect(shown.every((i, k) => i >= 0 && (k === 0 || shown[k - 1]! < i)), JSON.stringify(shown)).toBe(true);
    fireEvent.click(screen.getByText(TROUT_SHEET.ALL_SOURCES));
    fireEvent.click(screen.getByText(TROUT_NAV.HATCH));
    fireEvent.click(screen.getByText(titles[0]!));
    expect(h.opened).toEqual(["src:trout", "hatch", `rule:k${r.sheet.src[0]}`]);
    // the table's every rule: `table.src`, in its order
    h.rerender("src:trout");
    expect(screen.getByRole("heading", { name: TROUT_SHEET.SOURCES })).toBeTruthy();
    const all = t.src.map((ix) => screen.getByTestId("sheet-body").textContent!.indexOf(`Rule ${ix}`));
    expect(all.every((i, k) => i >= 0 && (k === 0 || all[k - 1]! < i))).toBe(true);
    // closes as every sheet does: Done, the scrim, a flick on the handle (useSheetDrag)
    cleanup();
    const d = host(`tf:${r.key}`, page);
    act(() => { screen.getByLabelText("Done").click(); screen.getByLabelText("Close").click(); });
    act(() => { fireGestureHandler(getByGestureTestId("sheet-drag"), [{ translationY: 0 }, { translationY: 20 }, { translationY: 40, velocityY: 900 }]); });
    expect(d.onClose).toHaveBeenCalledTimes(3);
  });

  it("the sub line: the trout table's every-rule sheet names the water and the day (T810 cutover: where a row's sources sheet did)", () => {
    // the sub as core writes it for the page (sheets.ts: the water · the day; conformance trout.release.test.ts holds that)
    const t = troutVmOf(TROUT_CASES[0]!);
    host(TROUT_SOURCES_KEY, { ...VEDDER, sheet: (k: SheetKey) => (k === TROUT_SOURCES_KEY ? troutSourcesSheet(k, t, RULES, "Vedder River · Oct 10") : null) });
    expect(screen.getByTestId("sheet-sub").textContent).toBe("Vedder River · Oct 10");
  });
});

describe("the Regulations page's foot Sources button (T990)", () => {
  it("sits after the page's sections and opens the every-source sheet; Done closes it", () => {
    const sheet = vi.fn((k: SheetKey): SheetVM | null => (k === "src:all"
      ? { key: k, title: "Sources", sub: null, blocks: [{ k: "para", text: [{ t: "text", s: "every source" }] }] } : null));
    const page = { ...VEDDER, sheet } as RegsPage;
    render(<RegsScreen page={page} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} }} onDate={() => {}} />);
    expect(screen.getByTestId("regs-all-sources").compareDocumentPosition(screen.getByTestId("regs-legal")) & Node.DOCUMENT_POSITION_PRECEDING).toBeTruthy();
    fireEvent.click(screen.getByTestId("regs-all-sources-button"));
    expect(screen.getByRole("heading", { name: "Sources" })).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Done"));
    expect(sheet).toHaveBeenCalledWith("src:all");
  });
});

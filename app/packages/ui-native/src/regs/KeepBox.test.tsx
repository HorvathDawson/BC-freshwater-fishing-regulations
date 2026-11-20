/**
 * (T829) THE KEEP BOX and the RELEASE / CLOSED GROUPS, as ticket O will hand them a contract Box (trout-table-contract.md
 * 2.3): the strip, the rows with their two text sizes, the right band (an own / shared white box, the free "up to the N"
 * words beside each row), the group's one column word; each row one label in payload order; every press 44 × 44; large
 * text folds the band into lines under the rows. The words are the caller's: here a Box written as Python writes them.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { PixelRatio } from "react-native";
import { CVD, DARK, LIGHT } from "../theme";
import { TYPE } from "../type";
import { hitArea } from "./hit";
import { KeepBox, ShutGroup, type KeepBandVM, type KeepRowVM } from "./KeepBox";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const pic = (code: string, onPress?: () => void) => ({ label: `${code} — what it looks like`, testID: `pic:${code}`, more: "›", onPress });
/** (T810 cutover) a row whose picture opens a sheet IS the press, so the rows here open one, as every trout row does */
const row = (key: string, name: string, more: Partial<KeepRowVM> = {}): KeepRowVM =>
  ({ key, name, label: null, sub: null, lines: [], quiet: [], pic: pic(key, () => {}), ...more });
// round 15's Skeena box: brook trout under the box's 5, lake trout its own 3, four sharing "only 1 of these 4"
const ROWS: KeepRowVM[] = [
  row("EB", "Brook trout", { lines: ["30 cm (12 in) or longer"] }),
  row("LT", "Lake trout", { lines: ["30 cm (12 in) or longer"] }),
  row("GB", "Brown trout", { lines: ["30 cm (12 in) or longer"] }),
  row("CT", "Cutthroat trout", { lines: ["30 cm (12 in) or longer"] }),
  row("ST", "Steelhead", { label: "Hatchery only", sub: "a rainbow over 50 cm (20 in)",
                           lines: ["uses the 1 over 50 cm (20 in)", "after keeping 1, stop fishing this river today"],
                           quiet: ["Steelhead are known to be in this water."], date: "rule changes Nov 1" }),
  row("RB", "Rainbow trout", { lines: ["30–50 cm (12–20 in)"] }),
];
const BANDS: KeepBandVM[] = [
  { from: 0, to: 0, kind: "upto", text: "up to the 5" },
  { from: 1, to: 1, kind: "own", text: "only 3", date: "until Sep 14, then release" },
  { from: 2, to: 5, kind: "shared", text: "only 1 of these 4", date: "until Oct 31, then release" },
];
const box = (palette = LIGHT) =>
  render(<KeepBox head="Keep 5 in all" label={null} strip="only 1 over 50 cm (20 in)" tag={null} rows={ROWS} bands={BANDS} palette={palette} />);
const names = (el: HTMLElement) => within(el).getAllByTestId("keep-name").map((e) => e.textContent);

describe("(T829) <KeepBox>", () => {
  it("the strip across the top, then each band's rows beside its one cell: a shared white box spans its four rows", () => {
    box();
    expect([screen.getByTestId("keep-head").textContent, screen.getByTestId("keep-strip-text").textContent])
      .toEqual(["Keep 5 in all", "only 1 over 50 cm (20 in)"]);
    const groups = screen.getAllByTestId("box-group");
    expect(groups.map(names)).toEqual([["Brook trout"], ["Lake trout"], ["Brown trout", "Cutthroat trout", "Steelhead", "Rainbow trout"]]);
    // the free words of "up to the N" beside its row; a white box for an own and a shared band, its date small under it
    expect(groups.map((g) => [within(g).queryByTestId("keep-band-free")?.textContent ?? null, within(g).queryByTestId("keep-band-box")?.textContent ?? null]))
      .toEqual([["up to the 5", null], [null, "only 3until Sep 14, then release"], [null, "only 1 of these 4until Oct 31, then release"]]);
    // the white box is the card colour on the tint block
    expect(getComputedStyle(within(groups[1]!).getByTestId("keep-band-box")).backgroundColor).toBe(rgb(LIGHT.card));
    expect(getComputedStyle(screen.getByTestId("keep-box")).backgroundColor).toBe(rgb(LIGHT.tint));
  });

  it("(T810) a size band is drawn like a shared one: one white box spanning its rows, its text as given", () => {
    const text = "counts toward the 2 \u00b7 only 1 over 50 cm (20 in) between them";
    render(<KeepBox head="Keep 2" label={null} strip={null} tag={null} rows={ROWS.slice(0, 3)} palette={LIGHT}
      bands={[{ from: 0, to: 2, kind: "size", text }]} />);
    const groups = screen.getAllByTestId("box-group");
    expect(groups.length).toBe(1);
    expect(within(groups[0]!).getByTestId("keep-band-box").textContent).toBe(text);
    expect(names(groups[0]!).length).toBe(3);
  });

  it("two text sizes: today's rule bigger and bold, a coming change smaller and regular in the quiet colour", () => {
    box();
    const now = [screen.getByTestId("keep-head"), screen.getAllByTestId("keep-line")[0]!, screen.getAllByTestId("keep-band-text")[0]!];
    const next = [screen.getByTestId("keep-row-date"), screen.getAllByTestId("keep-band-date")[0]!, screen.getByTestId("keep-sub"), screen.getByTestId("keep-quiet")];
    const size = (e: HTMLElement) => parseFloat(getComputedStyle(e).fontSize);
    // (T810 cutover) the page's own scale: the bold body and the meta line (style-tokens: no size of the table's own)
    expect([...new Set(now.map(size))]).toEqual([TYPE.regsBodyBold.fontSize]);
    expect([...new Set(next.map(size))]).toEqual([TYPE.meta.fontSize]);
    expect(TYPE.regsBodyBold.fontSize).toBeGreaterThan(TYPE.meta.fontSize);
    expect([...new Set(next.map((e) => getComputedStyle(e).color))]).toEqual([rgb(LIGHT.sub)]);
    expect([...new Set(now.map((e) => getComputedStyle(e).color))]).toEqual([rgb(LIGHT.ink)]);
    // the filled label: the ink block, the card's colour for its words
    expect(getComputedStyle(screen.getByTestId("keep-label")).backgroundColor).toBe(rgb(LIGHT.ink));
  });

  it("each row reads as ONE label, its words in payload order: name, label, sub, lines, quiet, band, date (the row's press)", () => {
    box();
    const labels = screen.getAllByTestId("keep-row").map((e) => [e.getAttribute("role"), e.getAttribute("aria-label")]).map(([r, l]) => (r === "button" ? l : null));
    expect(labels).toEqual([
      "Brook trout. 30 cm (12 in) or longer. up to the 5",
      "Lake trout. 30 cm (12 in) or longer. only 3. until Sep 14, then release",
      "Brown trout. 30 cm (12 in) or longer",
      "Cutthroat trout. 30 cm (12 in) or longer",
      "Steelhead. Hatchery only. a rainbow over 50 cm (20 in). uses the 1 over 50 cm (20 in). after keeping 1, stop fishing this river today. "
        + "Steelhead are known to be in this water. rule changes Nov 1",
      "Rainbow trout. 30–50 cm (12–20 in). only 1 of these 4. until Oct 31, then release",
    ]);
  });

  it("a row whose picture opens a sheet is ONE named button reaching 44 × 44 (build spec P: a row press); one that opens nothing is no button and carries no label", () => {
    const opened: string[] = [];
    render(<KeepBox head={null} label={null} strip={null} tag={null} palette={LIGHT} bands={[]}
                    rows={[row("CO", "Coho", { pic: pic("CO", () => opened.push("sp:CO")) }), row("PK", "Pink", { pic: pic("PK") })]} />);
    const [b, plain] = screen.getAllByTestId("keep-row") as [HTMLElement, HTMLElement];
    expect([b.getAttribute("role"), b.getAttribute("aria-label"), hitArea(b.getAttribute("data-hitslop")!)])
      .toEqual(["button", "Coho", { w: 64, h: 44 }]);
    expect(b.contains(screen.getByTestId("pic:CO"))).toBe(true);
    fireEvent.click(screen.getByTestId("pic:CO"));   // a tap on the picture is a tap on the row
    expect(opened).toEqual(["sp:CO"]);
    expect([plain.getAttribute("role"), plain.getAttribute("aria-label"), screen.getByTestId("pic:PK").getAttribute("role")]).toEqual([null, null, null]);
    // no strip asked for: none drawn; a row no band covers sits beside an empty cell
    expect([screen.queryByTestId("box-strip"), screen.getAllByTestId("keep-band-empty").length]).toEqual([null, 2]);
  });

  it("(large text) the band folds away: each band's words a bold line under its last row, a shared band's rows on one rail", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(2);
    box();
    expect(screen.queryAllByTestId("keep-band")).toEqual([]);
    expect(screen.queryAllByTestId("keep-band-empty")).toEqual([]);
    const groups = screen.getAllByTestId("box-group");
    expect(groups.map((g) => within(g).queryAllByTestId("keep-band-line").map((e) => e.textContent)))
      .toEqual([["up to the 5"], ["only 3until Sep 14, then release"], ["only 1 of these 4until Oct 31, then release"]]);
    // under the shared band's LAST row (Rainbow trout), the four rows joined by a left rail
    const rows = within(groups[2]!).getAllByTestId("keep-row");
    expect(rows.map((r) => within(r).queryAllByTestId("keep-band-line").length)).toEqual([0, 0, 0, 1]);
    expect(getComputedStyle(rows[0]!.parentElement!).borderLeftWidth).toBe("3px");
  });
});

// (T810, owner 2026-11-18 round 4) the Vedder's box, cut down: six rows in "2 total from these 6" with "only 1 between them"
// over its last two, then the rainbow's own "counts toward the 4"
const VEDDER: KeepRowVM[] = [
  row("EB", "Brook trout", { lines: ["30 cm (12 in) or longer"] }), row("GB", "Brown trout", { lines: ["30 cm (12 in) or longer"] }),
  row("CT", "Cutthroat trout", { lines: ["30 cm (12 in) or longer"] }), row("ST", "Steelhead", { sub: "a rainbow over 50 cm (20 in)" }),
  row("DV", "Dolly Varden/bull trout", { lines: ["60 cm (24 in) or longer"] }), row("LT", "Lake trout", { lines: ["60 cm (24 in) or longer"] }),
  row("RB", "Rainbow trout", { lines: ["50 cm (20 in) or less"] }),
];
const V_BANDS: KeepBandVM[] = [{ from: 0, to: 5, kind: "shared", text: "2 total from these 6" }, { from: 6, to: 6, kind: "upto", text: "counts toward the 4" }];
const V_INNER: KeepBandVM[] = [{ from: 4, to: 5, kind: "shared", text: "only 1 between them" }];
const vedder = (inner: readonly KeepBandVM[] = V_INNER) =>
  render(<KeepBox head="Keep 4 in all" label="Hatchery only" strip={null} tag={null} rows={VEDDER} bands={V_BANDS} inner={inner} palette={LIGHT} />);

describe("(T810, round 4) a band's white box runs its rows' full height; a nested band sits inside it", () => {
  it("one white box per own / shared / possession / size band, stretched to the bottom of its last row, its words at the top", () => {
    box();
    const boxes = screen.getAllByTestId("keep-band-box");
    expect(boxes.map((b) => b.textContent)).toEqual(["only 3until Sep 14, then release", "only 1 of these 4until Oct 31, then release"]);
    // the box grows to its cell, and the cell to its rows (the row's flex line stretches it): no tint under the box
    for (const b of boxes) expect([getComputedStyle(b).flexGrow, getComputedStyle(b).backgroundColor]).toEqual(["1", rgb(LIGHT.card)]);
    for (const b of boxes) expect(b.firstElementChild?.getAttribute("data-testid")).toBe("keep-band-text");
    // an "up to the N" band keeps its free words on the tint, one per row
    expect(within(screen.getAllByTestId("box-group")[0]!).queryByTestId("keep-band-box")).toBeNull();
  });

  it("the Vedder: the shared band's rows cut where its nested band starts; the white box flush across both stretches", () => {
    vedder();
    const groups = screen.getAllByTestId("box-group");
    expect(groups.map(names)).toEqual([["Brook trout", "Brown trout", "Cutthroat trout", "Steelhead", "Dolly Varden/bull trout", "Lake trout"], ["Rainbow trout"]]);
    const parts = within(groups[0]!).getAllByTestId("box-stretch");
    expect(parts.map(names)).toEqual([["Brook trout", "Brown trout", "Cutthroat trout", "Steelhead"], ["Dolly Varden/bull trout", "Lake trout"]]);
    // the band's words once, at the top of its box; the second stretch continues the same white, no words, no gap
    expect(within(parts[0]!).getByTestId("keep-band-box").textContent).toBe("2 total from these 6");
    const more = within(parts[1]!).getByTestId("keep-band-box-more");
    expect([getComputedStyle(more).backgroundColor, getComputedStyle(more).paddingTop, getComputedStyle(more.parentElement!).paddingTop])
      .toEqual([rgb(LIGHT.card), "0px", "0px"]);
    expect([getComputedStyle(within(parts[0]!).getByTestId("keep-band-box")).paddingBottom, getComputedStyle(within(parts[0]!).getByTestId("keep-band")).paddingBottom])
      .toEqual(["0px", "0px"]);
  });

  it("the nested band: a 1 px hairline box inside its band's white box, beside just its rows, its words at its top", () => {
    vedder();
    const inner = screen.getByTestId("keep-inner");
    expect(inner.textContent).toBe("only 1 between them");
    expect(screen.getByTestId("keep-inner-text").textContent).toBe("only 1 between them");
    const cs = getComputedStyle(inner);
    expect([cs.borderTopWidth, cs.borderTopColor, cs.borderTopStyle, cs.flexGrow]).toEqual(["1px", rgb(LIGHT.line), "solid", "1"]);
    // inside the white box of the stretch that holds Dolly Varden and lake trout, and nowhere else
    const part = screen.getAllByTestId("box-stretch")[1]!;
    expect([within(part).getByTestId("keep-band-box-more").contains(inner), names(part)]).toEqual([true, ["Dolly Varden/bull trout", "Lake trout"]]);
    expect(screen.getAllByTestId("keep-inner").length).toBe(1);
  });

  it("a nested band at the band's top: its box under the band's words, in the first stretch; the rest flush below", () => {
    vedder([{ from: 0, to: 1, kind: "size", text: "only 1 over 50 cm (20 in) between them" }]);
    const parts = within(screen.getAllByTestId("box-group")[0]!).getAllByTestId("box-stretch");
    expect(parts.map(names)).toEqual([["Brook trout", "Brown trout"], ["Cutthroat trout", "Steelhead", "Dolly Varden/bull trout", "Lake trout"]]);
    const first = within(parts[0]!).getByTestId("keep-band-box");
    expect([...first.children].map((e) => e.getAttribute("data-testid"))).toEqual(["keep-band-text", "keep-inner"]);
    expect(within(parts[1]!).queryByTestId("keep-inner")).toBeNull();
  });

  it("the screen reader (rows that name themselves): a band's words once, on its last row; a nested band's on its own last row", () => {
    render(<KeepBox head={null} label={null} strip={null} tag={null} palette={LIGHT} bands={V_BANDS} inner={V_INNER}
                    rows={VEDDER.map((r) => ({ ...r, pic: pic(r.key, () => {}) }))} />);
    const labels = screen.getAllByTestId("keep-row").map((e) => e.getAttribute("aria-label"));
    expect(labels.slice(3)).toEqual([
      "Steelhead. a rainbow over 50 cm (20 in)",
      "Dolly Varden/bull trout. 60 cm (24 in) or longer",
      "Lake trout. 60 cm (24 in) or longer. 2 total from these 6. only 1 between them",
      "Rainbow trout. 50 cm (20 in) or less. counts toward the 4",
    ]);
  });

  it("(large text) the nested band's words a line under its last row, then its band's; no right band", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(2);
    vedder();
    expect([screen.queryAllByTestId("keep-band"), screen.queryAllByTestId("keep-inner")]).toEqual([[], []]);
    const rows = screen.getAllByTestId("keep-row");
    expect(rows.map((r) => within(r).queryAllByTestId("keep-band-line").map((e) => e.textContent)))
      .toEqual([[], [], [], [], [], ["only 1 between them", "2 total from these 6"], ["counts toward the 4"]]);
  });
});

describe("(T829) <ShutGroup>: the release and closed groups under the box", () => {
  const closed = [row("CH", "Chinook", { quiet: ["Sizes (DFO): chinook 30 cm or more to keep"] }),
                  row("x", "Eulachon closed all year: No fishing for eulachon", { pic: null })];

  it("one column word for the group, its shape mark and colour; faded pictures; a line with no fish wears the mark", () => {
    render(<ShutGroup kind="closed" head="Closed" date="reopens Nov 1" rows={closed} palette={LIGHT} />);
    const g = screen.getByTestId("keep-closed");
    expect([within(g).getAllByTestId("keep-col-word").map((e) => e.textContent), within(g).getByTestId("keep-col-date").textContent])
      .toEqual([["✕ Closed"], "reopens Nov 1"]);
    expect(getComputedStyle(within(g).getByTestId("keep-col-word")).color).toBe(rgb(LIGHT.closed));
    expect(names(g)).toEqual(["Chinook", "Eulachon closed all year: No fishing for eulachon"]);
    expect(getComputedStyle(within(g).getByTestId("keep-look")).opacity).toBe("0.6");
    const last = within(g).getAllByTestId("keep-row")[1]!;
    expect([within(last).queryByTestId("keep-look"), last.textContent]).toEqual([null, "✕Eulachon closed all year: No fishing for eulachon"]);
    // the column word and its date are the first row's label's end, never the mark
    expect(within(g).getAllByRole("button")[0]!.getAttribute("aria-label")).toBe("Chinook. Sizes (DFO): chinook 30 cm or more to keep. Closed. reopens Nov 1");
  });

  it("release: ↩ and the release colour; other (\"Check DFO\"): no mark, the quiet colour", () => {
    render(<ShutGroup kind="release" head="Release only" rows={[row("CO", "Coho")]} palette={DARK} />);
    expect([screen.getByTestId("keep-col-word").textContent, getComputedStyle(screen.getByTestId("keep-col-word")).color])
      .toEqual(["↩ Release only", rgb(DARK.restricted)]);
    cleanup();
    render(<ShutGroup kind="other" head="Check DFO" rows={[row("CO", "Coho")]} palette={LIGHT} />);
    expect([screen.getByTestId("keep-col-word").textContent, getComputedStyle(screen.getByTestId("keep-col-word")).color])
      .toEqual(["Check DFO", rgb(LIGHT.sub)]);
  });

  it("(large text) the column word becomes a line above its group", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(2);
    render(<ShutGroup kind="closed" head="Closed" rows={closed} palette={LIGHT} />);
    const word = screen.getByTestId("keep-col");
    expect(word.compareDocumentPosition(screen.getAllByTestId("keep-row")[0]!) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getAllByTestId("keep-col")).toHaveLength(1);
  });
});

describe("(T829) tokens only: every word of the box reads on its ground in light, dark and colour-blind", () => {
  for (const [name, p] of [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const)
    it(name, () => {
      // the strip and band words on the tint, the rows and white boxes on the card, the column words on the card
      const pairs = { "ink/tint": [p.ink, p.tint], "sub/tint": [p.sub, p.tint], "ink/card": [p.ink, p.card], "sub/card": [p.sub, p.card],
                      "closed/card": [p.closed, p.card], "restricted/card": [p.restricted, p.card], "card/ink (label)": [p.card, p.ink] };
      const low = Object.entries(pairs).filter(([, [a, b]]) => contrast(a!, b!) < 4.5).map(([k]) => k);
      expect(low).toEqual([]);
    });
});

/** WCAG relative-luminance contrast (as theme.test.ts) */
function contrast(a: string, b: string): number {
  const L = (hex: string) => {
    const h = hex.replace("#", "");
    const [r, g, b2] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16) / 255)
      .map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4)) as [number, number, number];
    return 0.2126 * r + 0.7152 * g + 0.0722 * b2;
  };
  const [x, y] = [L(a), L(b)].sort((m, n) => n - m) as [number, number];
  return (x + 0.05) / (y + 0.05);
}

function rgb(hex: string): string {
  const h = hex.replace("#", "");
  const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16));
  return `rgb(${r}, ${g}, ${b})`;
}

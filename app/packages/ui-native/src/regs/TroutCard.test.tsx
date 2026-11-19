/**
 * (T810 O) THE TROUT TABLE DRAWN, over every shared vector (`app/fixtures/trout/trout-vectors.generated.json`, V-py's; read,
 * never written): each frame through N's `troutOf`, then `TroutCard`. Every payload text is on the screen unchanged and
 * nothing else but navigation words; each row reads as N's golden label (`app/fixtures/golden/trout-a11y.json`); the year
 * bar is drawn exactly when `year` is set; the links, the presses (44 × 44) and their sheets; the two text sizes; the
 * filled label and the origin word; large text; tokens in light, dark and colour-blind.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { PixelRatio } from "react-native";
import { TROUT_NAV, troutOf, type RegsInputs, type SheetKey, type TroutValue, type TroutVM } from "@app/core";
import vectors from "../../../../fixtures/trout/trout-vectors.generated.json";
import golden from "../../../../fixtures/golden/trout-a11y.json";
import { CVD, DARK, LIGHT } from "../theme";
import { TYPE } from "../type";
import { hitArea } from "./hit";
import type { KeepActions } from "./LineText";
import { TroutCard } from "./TroutCard";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

interface Vector { readonly name: string; readonly day: string; readonly frame: TroutValue | null }
const CASES = (vectors as unknown as { cases: readonly Vector[] }).cases.filter((c) => c.frame !== null);
const A11Y = golden as Record<string, Record<string, string>>;

/** the case's table as N's view model builds it on its own day (the lead says "today"); the store's two bounds held open */
function vmOf(c: Vector): TroutVM {
  const [month, day] = c.day.split("-").map(Number) as [number, number];
  const inp = { trout: c.frame, rules: { has: () => true }, frames: { rows: { rows: { length: 1e6 } } } } as unknown as RegsInputs;
  const vm = troutOf(inp, { month, day }, { month, day });
  if (vm?.kind !== "table") throw new Error(`${c.name}: no table`);
  return vm;
}
const byName = (n: string) => vmOf(CASES.find((c) => c.name === n)!);

function mount(t: TroutVM, palette = LIGHT, kicker = false) {
  const sheets: SheetKey[] = [];
  const actions: KeepActions = { onSheet: (k) => sheets.push(k), onDay() {}, onTab() {}, onBook() {} };
  const r = render(<TroutCard t={t} palette={palette} actions={actions} kicker={kicker} todayAt={0.5} months={["J", "F"]} />);
  return { r, sheets };
}

/** every text the payload ships for this table (the strings O must draw unchanged) */
function payloadTexts(t: TroutVM): string[] {
  return [t.top.lead, t.top.sub, t.top.second, t.top.date, t.card, t.together?.text, t.year?.title,
    ...t.boxes.flatMap((b) => [b.head, b.label, b.strip, b.possession, b.tag, b.date, ...b.bands.flatMap((x) => [x.text, x.date]),
      ...b.inner.map((x) => x.text),
      ...b.rows.flatMap((r) => [r.name, r.origin, r.label, r.sub, ...r.lines, ...r.quiet, r.date])])]
    .filter((x): x is string => !!x);
}

// every vector mounted in turn: ~3 s alone, past the 5 s default when the whole suite shares the machine
describe("(T810 O) every vector drawn", { timeout: 30_000 }, () => {
  it(`${CASES.length} vectors: every payload text on screen, unchanged; no slot or brace left`, () => {
    expect(CASES.length).toBeGreaterThanOrEqual(52);
    for (const c of CASES) {
      const t = vmOf(c);
      const { r } = mount(t);
      const shown = r.container.textContent ?? "";
      const missing = payloadTexts(t).filter((s) => !shown.includes(s));
      expect(missing, c.name).toEqual([]);
      expect(/[{}]/.test(shown), c.name).toBe(false);
      expect(screen.getByTestId("trout-top").textContent, c.name).toBe(t.top.lead);
      expect(screen.getAllByTestId("keep-row").length, c.name).toBe(t.boxes.reduce((a, b) => a + b.rows.length, 0));
      cleanup();
    }
  });

  it("each row reads as ONE label, N's golden (payload texts in payload order), over every vector row", () => {
    let n = 0;
    for (const c of CASES) {
      mount(vmOf(c));
      // (T810 cutover) each row is its press (build spec P), named by that label
      const got = screen.getAllByTestId("keep-row").map((e) => [e.getAttribute("role"), e.getAttribute("aria-label")])
        .map(([role, l]) => (role === "button" ? l : `not a press: ${l}`));
      expect(got, c.name).toEqual(Object.values(A11Y[c.name]!));
      n += got.length;
      cleanup();
    }
    expect(n).toBeGreaterThan(200);
  });

  it("the year bar is drawn exactly when `year` is set (the Elk in winter yes, the Elk in July and every mixed water no)", () => {
    const drawn: string[] = [];
    for (const c of CASES) {
      const t = vmOf(c);
      mount(t);
      const bar = screen.queryByTestId("trout-year");
      expect(!!bar, c.name).toBe(t.year !== null);
      if (bar) drawn.push(c.name);
      cleanup();
    }
    expect(drawn).toContain("elk-nov15");
    expect(drawn).not.toContain("elk-jul15");
    mount(byName("elk-nov15"));
    expect([screen.getByTestId("trout-year-title").textContent, screen.getByTestId("trout-year-bar").getAttribute("aria-label")])
      .toEqual(["Trout and char through the year", "Trout and char through the year"]);
  });
});

describe("(T810 O) the links and the presses", { timeout: 30_000 }, () => {
  it("the hatchery link only where hatch_link; Sources always; 'Fished elsewhere today?' only with lines, unfolding them", () => {
    for (const c of CASES) {
      const t = vmOf(c);
      mount(t);
      expect(!!screen.queryByTestId("trout-hatch"), c.name).toBe(t.hatchLink !== null);
      expect(screen.getByTestId("trout-sources").textContent).toBe(TROUT_NAV.SOURCES);
      expect(!!screen.queryByTestId("trout-elsewhere"), c.name).toBe(t.elsewhere.length > 0);
      cleanup();
    }
    const t = byName("fraser-r2-mission-jul15");
    const { sheets } = mount(t);
    const away = screen.getByTestId("trout-elsewhere");
    expect([away.textContent, away.getAttribute("aria-expanded"), screen.queryByTestId("trout-elsewhere-lines")])
      .toEqual([TROUT_NAV.ELSEWHERE, "false", null]);
    fireEvent.click(away);
    expect(screen.getAllByTestId("trout-elsewhere-line").map((e) => e.textContent)).toEqual(t.elsewhere);
    expect(screen.getByTestId("trout-elsewhere").getAttribute("aria-expanded")).toBe("true");
    fireEvent.click(screen.getByTestId("trout-hatch"));
    fireEvent.click(screen.getByTestId("trout-sources"));
    fireEvent.click(screen.getByTestId("pic:ST"));
    fireEvent.click(screen.getByTestId("pic:RB"));
    // (T810 P) a row opens its own fish sheet (the row key, "/" and all); Sources the table's every rule
    expect(sheets).toEqual(["hatch", "src:trout", "tf:ST", "tf:RB"]);
    cleanup();
    const twin = CASES.map(vmOf).find((x) => x.boxes.some((b) => b.rows.some((r) => r.key.includes("/"))))!;
    const tk = twin.boxes.flatMap((b) => b.rows).find((r) => r.key.includes("/"))!.key;
    const m = mount(twin);
    fireEvent.click(screen.getByTestId(`pic:${tk}`));
    expect(m.sheets).toEqual([`tf:${tk}`]);
  });

  it("(T810 L elsewhere) a mixed list — the kept-elsewhere sentence, its row's worked lines, the groups' — drawn whole, as given, in order", () => {
    const MIXED = [
      "This lake's rule: 2 a day here. Trout and char other than steelhead you kept anywhere else count toward those 2.",
      "Kept 1 at a stream? Keep 1 more here.",
      "Kept 2 elsewhere? Keep 3 more here.",
      "Kept trout or char over 50 cm (20 in) elsewhere? It counts here too: at most 1 over 50 cm (20 in) a day in all.",
    ];
    const t: TroutVM = { ...byName("fraser-r2-mission-jul15"), elsewhere: MIXED, elsewhereLink: TROUT_NAV.ELSEWHERE };
    mount(t);
    fireEvent.click(screen.getByTestId("trout-elsewhere"));
    const lines = screen.getAllByTestId("trout-elsewhere-line");
    // every entry, the sentence first, nothing dropped, merged or reordered; each its own text a screen reader reaches
    expect(lines.map((e) => e.textContent)).toEqual(MIXED);
    expect(lines.filter((e) => e.closest("[aria-hidden='true']") || e.closest("[aria-label]"))).toEqual([]);
    cleanup();
    // a list with no worked line at all (one sentence) still shows the link and draws it
    mount({ ...t, elsewhere: [MIXED[0]!] });
    fireEvent.click(screen.getByTestId("trout-elsewhere"));
    expect(screen.getAllByTestId("trout-elsewhere-line").map((e) => e.textContent)).toEqual([MIXED[0]]);
  });

  it("every press reaches 44 × 44: the links and each row (the row is the press its picture opens through)", () => {
    mount(byName("fraser-r2-mission-jul15"));
    const rowOf = (id: string) => screen.getByTestId(id).closest<HTMLElement>("[data-testid='keep-row']")!;
    for (const [id, b] of [...["trout-hatch", "trout-sources", "trout-elsewhere"].map((i) => [i, screen.getByTestId(i)] as const),
                           ...["pic:ST", "pic:RB"].map((i) => [i, rowOf(i)] as const)]) {
      expect([b.getAttribute("role"), hitArea(b.getAttribute("data-hitslop")!)!.h >= 44, hitArea(b.getAttribute("data-hitslop")!)!.w >= 44], id)
        .toEqual(["button", true, true]);
    }
  });

  it("the kicker: the entry's name only where no folded row heads the table", () => {
    mount(byName("elk-nov15"), LIGHT, true);
    expect(screen.getByTestId("kicker:trout").textContent).toBe(TROUT_NAV.ENTRY);
    expect(screen.queryByTestId("trout-hatch")).toBeNull();   // the kicker's line holds the hatchery link only where hatch_link
    cleanup();
    mount(byName("elk-nov15"));
    expect(screen.queryByTestId("kicker:trout")).toBeNull();
  });
});

describe("(T810 O) the look: two text sizes, the filled label, the origin word, the groups", () => {
  it("today's rule bigger and in ink; a coming change smaller, regular, quiet", () => {
    mount(byName("elk-nov15"));
    const size = (e: HTMLElement) => parseFloat(getComputedStyle(e).fontSize);
    expect([size(screen.getByTestId("trout-top")), size(screen.getByTestId("trout-top-date"))]).toEqual([TYPE.screen.fontSize, TYPE.meta.fontSize]);   // the owner's 23 px top line (style-tokens SIZE_ALLOW)
    expect(getComputedStyle(screen.getByTestId("trout-top-date")).color).toBe(rgb(LIGHT.sub));
    cleanup();
    mount(byName("shamrock-jul15"));
    expect([size(screen.getByTestId("trout-card-line")), getComputedStyle(screen.getByTestId("trout-card-line")).color]).toEqual([TYPE.meta.fontSize, rgb(LIGHT.sub)]);
    cleanup();
    mount(byName("fraser-r2-mission-jul15"));
    const st = screen.getAllByTestId("keep-row").find((r) => within(r).queryByText("Steelhead"))!;
    expect([size(within(st).getAllByTestId("keep-line")[0]!), size(within(st).getByTestId("keep-row-date"))]).toEqual([TYPE.regsBodyBold.fontSize, TYPE.meta.fontSize]);
  });

  it("HATCHERY ONLY: the filled label once on the box's top line (Fraser R2), or beside the one fish's name (Windsor's char)", () => {
    mount(byName("fraser-r2-mission-jul15"));
    const box = screen.getByTestId("trout-box:0");
    expect(within(within(box).getByTestId("box-strip")).getByTestId("keep-label").textContent).toBe("Hatchery only");
    expect(within(box).getAllByTestId("keep-label")).toHaveLength(1);
    expect(getComputedStyle(within(box).getByTestId("keep-label")).backgroundColor).toBe(rgb(LIGHT.ink));
    cleanup();
    mount(byName("windsor-jul15"));
    const dv = screen.getAllByTestId("keep-row").find((r) => within(r).queryByText("Dolly Varden/bull trout"))!;
    expect(within(dv).getByTestId("keep-label").textContent).toBe("Hatchery only");
    // the twin rows: the origin word itself beside the name, never a label, in the payload's row order (G2: Windsor's
    // hatchery rows first, then its wild ones)
    const twins = byName("windsor-jul15").boxes.flatMap((b) => b.rows).filter((r) => r.origin !== null);
    expect(screen.getAllByTestId("keep-origin").map((e) => e.textContent)).toEqual(twins.map((r) => r.origin));
    expect(new Set(twins.map((r) => r.origin))).toEqual(new Set(["hatchery", "wild"]));
  });

  it("(G2 Q9) a twin row's origin is a badge: hatchery FILLED as the Hatchery only label, wild OUTLINED; the word as is, the row's label unchanged", () => {
    for (const [name, p] of [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const) {
      const t = byName("dodd-jul15");
      mount(t, p);
      const twins = t.boxes.flatMap((b) => b.rows).filter((r) => r.origin !== null);
      const badges = screen.getAllByTestId("keep-origin");
      expect(badges.map((e) => e.textContent), name).toEqual(twins.map((r) => r.origin));
      expect(new Set(twins.map((r) => r.origin)), name).toEqual(new Set(["hatchery", "wild"]));
      const label = screen.getAllByTestId("keep-label")[0]!, ls = getComputedStyle(label);
      for (const b of badges) {
        const s = getComputedStyle(b), word = getComputedStyle(b.firstElementChild!);
        const filled = b.textContent === "hatchery";
        // filled: the label's own look (ink ground, card word); outlined: no ground, an ink border and an ink word
        const clear = /, 0\)$/.test(s.backgroundColor);   // no ground: a fully transparent fill
        expect([clear ? "none" : s.backgroundColor, s.borderTopColor, s.borderTopWidth, word.color, word.textTransform], `${name} ${b.textContent}`).toEqual(filled
          ? [ls.backgroundColor, rgb(p.ink), "1px", getComputedStyle(label.firstElementChild!).color, "uppercase"]
          : ["none", rgb(p.ink), "1px", rgb(p.ink), "uppercase"]);
        // one size: the same padding and border as the label
        expect([s.paddingLeft, s.paddingTop, s.borderTopWidth], name).toEqual([ls.paddingLeft, ls.paddingTop, ls.borderTopWidth]);
      }
      // the row still reads as N's golden label (the word as is), and it is still a 44 × 44 press (the row, its picture in it)
      const row = screen.getAllByTestId("keep-row").find((r) => within(r).queryByTestId("keep-origin"))!;
      expect([row.getAttribute("role"), row.getAttribute("aria-label")], name).toEqual(["button", A11Y["dodd-jul15"]![twins[0]!.key]]);
      expect(row.contains(screen.getByTestId(`pic:${twins[0]!.key}`)), name).toBe(true);
      expect([hitArea(row.getAttribute("data-hitslop")!)!.h >= 44, hitArea(row.getAttribute("data-hitslop")!)!.w >= 44], name).toEqual([true, true]);
      cleanup();
    }
  });

  it("release and closed groups under their head word, with the shape marks; the together line under the boxes", () => {
    mount(byName("fraser-r3-sep15"));
    expect(screen.getAllByTestId("keep-col-word").map((e) => e.textContent)).toEqual(["↩ Release", "✕ Closed"]);
    cleanup();
    // (G2) the lakes' own count under the top line only: `second` without `together` (Echoes), no together line
    const e = byName("echoes-jul15");
    mount(e);
    expect([screen.getByTestId("trout-second").textContent, screen.queryByTestId("trout-together"), e.together]).toEqual([e.top.second, null, null]);
    cleanup();
    // a limit across two boxes is said under the top line AND under the table; G2's vectors hold none today (Lois is one
    // box now), so its frame is Echoes' with a together in the second's words, as the validator requires
    const t: TroutVM = { ...e, together: { text: e.top.second!, src: [] } };
    mount(t);
    expect(screen.getByTestId("trout-together").textContent).toBe(t.together!.text);
    expect(screen.getByTestId("trout-second").textContent).toBe(t.top.second);
    expect(screen.getByTestId("trout-boxes").compareDocumentPosition(screen.getByTestId("trout-together")) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("(G2 Q4) a box's possession: its own line directly under the box's top line, today's size, the words unchanged", () => {
    const t = byName("bowron-jul15"), b = t.boxes[0]!;
    expect(b.possession).toBe("In your possession: no more than 2 trout and char in all");
    mount(t);
    const strip = within(screen.getByTestId("trout-box:0")).getByTestId("box-strip");
    const line = within(strip).getByTestId("keep-possession");
    expect(line.textContent).toBe(b.possession);
    // its own line: a sibling right after the top line's row (head, label, strip, tag), not inside it
    const top = within(strip).getByTestId("keep-head").parentElement!;
    expect([within(top).queryByTestId("keep-possession"), top.nextElementSibling]).toEqual([null, line]);
    expect([parseFloat(getComputedStyle(line).fontSize), getComputedStyle(line).color]).toEqual([TYPE.regsBodyBold.fontSize, rgb(LIGHT.ink)]);
    expect(screen.getAllByTestId("keep-possession")).toHaveLength(1);
    cleanup();
    // every other vector box: no possession, no line
    for (const c of CASES) {
      const v = vmOf(c);
      mount(v);
      expect(screen.queryAllByTestId("keep-possession").length, c.name).toBe(v.boxes.filter((x) => x.possession).length);
      cleanup();
    }
  });

  it("(trout 4) the Vedder: the sub under the top line in its face a size down; the card date grey under them; the nested band in its band's box", () => {
    const t = byName("vedder-jul15");
    expect(t.top.sub).not.toBeNull();
    mount(t);
    const top = screen.getByTestId("trout-top"), sub = screen.getByTestId("trout-sub");
    expect(sub.textContent).toBe(t.top.sub);
    // the full sentence is the lead's, then the sub: drawn in that order, one under the other
    expect(top.compareDocumentPosition(sub) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    const [ts, ss] = [getComputedStyle(top), getComputedStyle(sub)];
    expect([ss.fontFamily, parseFloat(ss.fontSize), ss.color]).toEqual([ts.fontFamily, TYPE.lead.fontSize, rgb(LIGHT.ink)]);
    expect(parseFloat(ss.fontSize)).toBeLessThan(parseFloat(ts.fontSize));
    const inner = t.boxes.flatMap((b) => b.inner);
    expect(screen.getAllByTestId("keep-inner-text").map((e) => e.textContent)).toEqual(inner.map((x) => x.text));
    if (t.card) {
      const card = screen.getByTestId("trout-card-line");
      expect([card.textContent, getComputedStyle(card).color]).toEqual([t.card, rgb(LIGHT.sub)]);
      expect(sub.compareDocumentPosition(card) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    }
  });

  it("(trout 4) no sub: no subtitle drawn", () => {
    mount(byName("fraser-r3-sep15"));
    expect(screen.queryByTestId("trout-sub")).toBeNull();
  });

  it("(large text) the band folds into bold lines under the rows, and the top line steps down a size", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(2);
    mount(byName("skeena-sedan-mill-jul15"));
    expect(screen.queryAllByTestId("keep-band")).toEqual([]);
    expect(screen.getAllByTestId("keep-band-line").length).toBeGreaterThan(0);
    expect(parseFloat(getComputedStyle(screen.getByTestId("trout-top")).fontSize)).toBe(TYPE.lead.fontSize);
  });
});

describe("(T810 O) tokens only: the table's words read on their ground in light, dark and colour-blind", () => {
  for (const [name, p] of [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const)
    it(name, () => {
      // the card's own: the top line and its quiet lines on the card, the links in the accent, the year bar's title
      const pairs = { "ink/card": [p.ink, p.card], "sub/card": [p.sub, p.card], "accent/card": [p.accent, p.card],
                      "ink/tint": [p.ink, p.tint], "sub/tint": [p.sub, p.tint], "card/ink (label)": [p.card, p.ink],
                      "closed/card": [p.closed, p.card], "restricted/card": [p.restricted, p.card] };
      expect(Object.entries(pairs).filter(([, [a, b]]) => contrast(a!, b!) < 4.5).map(([k]) => k)).toEqual([]);
      const t = byName("fraser-r3-sep15");
      mount(t, p);
      expect(getComputedStyle(screen.getByTestId("trout-top")).color).toBe(rgb(p.ink));
      expect(getComputedStyle(within(screen.getByTestId("trout-sources")).getByText(TROUT_NAV.SOURCES)).color).toBe(rgb(p.accent));
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

describe("(T907) a row's tributary source: its own line under the name, read once by its row", () => {
  const LINE = "Kootenay Lake — Main Body’s rule for its tributaries";
  const TAG = "Kootenay Lake — Main Body’s tributaries";
  /** the first vector's table with Python's line (T892) and, unless `line only`, its tag (T907) on its first row */
  function withSource(lineOnly = false): TroutVM {
    const c = CASES[0]!, f = c.frame!, b0 = f.table.boxes[0]!;
    const put = { down: LINE, ...(lineOnly ? {} : { tag: TAG }) };
    const frame = { ...f, table: { ...f.table, boxes: [{ ...b0, rows: b0.rows.map((r, i) => (i === 0 ? { ...r, ...put } : r)) },
      ...f.table.boxes.slice(1)] } } as TroutValue;
    return vmOf({ ...c, frame });
  }
  it("drawn on its row only, under the name, as given; its row's label says it once", () => {
    for (const p of [LIGHT, DARK, CVD]) {
      mount(withSource(), p);
      const got = screen.getAllByTestId("keep-row-tag");
      expect(got.map((e) => e.textContent)).toEqual([TAG]);
      const row = screen.getAllByTestId("keep-row")[0]!;
      // its own line: in the row's text column, not inside the name's line (which wraps the name and its badges)
      expect(within(row).getByTestId("keep-name").parentElement!.contains(got[0]!)).toBe(false);
      expect(within(row).getByTestId("keep-name").parentElement!.parentElement!.contains(got[0]!)).toBe(true);
      expect(row.getAttribute("aria-label")!.split(TAG)).toHaveLength(2);
      expect(row.textContent!.includes(LINE)).toBe(false);                                  // the grey line is gone
      cleanup();
    }
  });
  it("(round 2, owner ~12:27 \"the baddly formattred double line tag\", ~12:50 \"muted grey not purple idea same size as the dates text\") no box to break: the date line's face and muted colour, no capitals or tracking, a rule of that colour on its left only", () => {
    for (const p of [LIGHT, DARK, CVD]) {
      mount(withSource(), p);
      const tag = screen.getByTestId("keep-row-tag"), word = within(tag).getByTestId("keep-row-tag-text");
      const t = getComputedStyle(word), b = getComputedStyle(tag);
      expect([parseFloat(t.fontSize), t.color, t.textTransform, t.letterSpacing])
        .toEqual([TYPE.meta.fontSize, rgb(p.sub), "none", "normal"]);            // the kicker: uppercase, 1px
      expect([b.borderLeftWidth, b.borderLeftColor, b.borderTopWidth || "0px", b.borderRightWidth || "0px", b.alignSelf])
        .toEqual(["2px", rgb(p.sub), "0px", "0px", "flex-start"]);                       // hugs its text: no column-wide box
      // the box head's own-water tag keeps its outlined kicker
      expect(within(tag).queryByTestId("kicker:tag")).toBeNull();
      cleanup();
    }
  });
  it("a release from T892 to T907 (the line alone) and the vectors (neither): nothing drawn", () => {
    mount(withSource(true));
    expect([screen.queryAllByTestId("keep-row-tag").length, document.body.textContent!.includes(LINE)]).toEqual([0, false]);
    cleanup();
    mount(vmOf(CASES[0]!));
    expect(screen.queryAllByTestId("keep-row-tag")).toEqual([]);
  });
});

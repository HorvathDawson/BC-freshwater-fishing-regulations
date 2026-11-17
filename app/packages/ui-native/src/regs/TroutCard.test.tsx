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
  return [t.top.lead, t.top.second, t.top.date, t.card, t.together?.text, t.year?.title,
    ...t.boxes.flatMap((b) => [b.head, b.label, b.strip, b.tag, b.date, ...b.bands.flatMap((x) => [x.text, x.date]),
      ...b.rows.flatMap((r) => [r.name, r.origin, r.label, r.sub, ...r.lines, ...r.quiet, r.date])])]
    .filter((x): x is string => !!x);
}

describe("(T810 O) every vector drawn", () => {
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
      const got = screen.getAllByRole("group").map((e) => e.getAttribute("aria-label"));
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

describe("(T810 O) the links and the presses", () => {
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

  it("every press reaches 44 × 44: the links and each row's picture", () => {
    mount(byName("fraser-r2-mission-jul15"));
    for (const id of ["trout-hatch", "trout-sources", "trout-elsewhere", "pic:ST", "pic:RB"]) {
      const b = screen.getByTestId(id);
      expect([b.getAttribute("role"), hitArea(b.getAttribute("data-hitslop")!)!.h >= 44, hitArea(b.getAttribute("data-hitslop")!)!.w >= 44], id)
        .toEqual(["button", true, true]);
    }
  });

  it("the kicker: the entry's name only where no folded row heads the table", () => {
    mount(byName("elk-nov15"), LIGHT, true);
    expect(screen.getByTestId("trout-kicker").textContent).toBe(TROUT_NAV.ENTRY);
    expect(screen.queryByTestId("trout-hatch")).toBeNull();   // the kicker's line holds the hatchery link only where hatch_link
    cleanup();
    mount(byName("elk-nov15"));
    expect(screen.queryByTestId("trout-kicker")).toBeNull();
  });
});

describe("(T810 O) the look: two text sizes, the filled label, the origin word, the groups", () => {
  it("today's rule bigger and in ink; a coming change smaller, regular, quiet", () => {
    mount(byName("elk-nov15"));
    const size = (e: HTMLElement) => parseFloat(getComputedStyle(e).fontSize);
    expect([size(screen.getByTestId("trout-top")), size(screen.getByTestId("trout-top-date"))]).toEqual([TYPE.screen.fontSize, TYPE.meta.fontSize]);
    expect(getComputedStyle(screen.getByTestId("trout-top-date")).color).toBe(rgb(LIGHT.sub));
    cleanup();
    mount(byName("shamrock-jul15"));
    expect([size(screen.getByTestId("trout-card-line")), getComputedStyle(screen.getByTestId("trout-card-line")).color]).toEqual([TYPE.meta.fontSize, rgb(LIGHT.sub)]);
    cleanup();
    mount(byName("fraser-r2-mission-jul15"));
    const st = screen.getAllByTestId("keep-row").find((r) => within(r).queryByText("Steelhead"))!;
    expect([size(within(st).getAllByTestId("keep-line")[0]!), size(within(st).getByTestId("keep-row-date"))]).toEqual([TYPE.legend.fontSize, TYPE.meta.fontSize]);
  });

  it("HATCHERY ONLY: the filled label once on the box's top line (Fraser R2), or beside the one fish's name (Windsor's char)", () => {
    mount(byName("fraser-r2-mission-jul15"));
    const box = screen.getByTestId("trout-box:0");
    expect(within(within(box).getByTestId("keep-strip")).getByTestId("keep-label").textContent).toBe("Hatchery only");
    expect(within(box).getAllByTestId("keep-label")).toHaveLength(1);
    expect(getComputedStyle(within(box).getByTestId("keep-label")).backgroundColor).toBe(rgb(LIGHT.ink));
    cleanup();
    mount(byName("windsor-jul15"));
    const dv = screen.getAllByTestId("keep-row").find((r) => within(r).queryByText("Dolly Varden/bull trout"))!;
    expect(within(dv).getByTestId("keep-label").textContent).toBe("Hatchery only");
    // the twin rows: the origin word itself beside the name, never a label
    expect(screen.getAllByTestId("keep-origin").map((e) => e.textContent).slice(0, 2)).toEqual(["hatchery", "wild"]);
  });

  it("release and closed groups under their head word, with the shape marks; the together line under the boxes", () => {
    mount(byName("fraser-r3-sep15"));
    expect(screen.getAllByTestId("keep-col-word").map((e) => e.textContent)).toEqual(["↩ Release", "✕ Closed"]);
    cleanup();
    const t = byName("lois-jul15");
    mount(t);
    expect(screen.getByTestId("trout-together").textContent).toBe(t.together!.text);
    expect(screen.getByTestId("trout-second").textContent).toBe(t.top.second);
    expect(screen.getByTestId("trout-boxes").compareDocumentPosition(screen.getByTestId("trout-together")) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
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

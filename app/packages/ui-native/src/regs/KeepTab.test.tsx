/**
 * T21 render: the Keep tab mounted over the FROZEN golden pages (fixtures/*.json, written by `UPDATE_REGS_FIXTURES=1` from
 * buildRegsPage over the parity store — never hand-edited). The blocks draw in `keep.order`; every block's words are the
 * VM's; links say where they go. jsdom cannot open a store, so these are the frozen VMs.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { PixelRatio } from "react-native";
import { GLANCE_WORDS, SALMON_WORDS, troutOf, type KeepVM, type RegsInputs, type RegsPage, type SheetKey, type TroutValue } from "@app/core";
import vectors from "../../../../fixtures/trout/trout-vectors.generated.json";
import { hitArea } from "./hit";
import { KeepTab } from "./KeepTab";
import { LIGHT } from "../theme";
import type { KeepActions } from "./LineText";
import vedder1010 from "./fixtures/vedder_1010.json";
import vedder615 from "./fixtures/vedder_615.json";
import glenn501 from "./fixtures/glenn_501.json";
import klwa1010 from "./fixtures/klwa_1010.json";
import bc1501 from "./fixtures/bc1_501.json";
import x35p1010 from "./fixtures/x35p_1010.json";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const keepOf = (f: unknown) => (f as RegsPage).keep as KeepVM;
const TEST_ID = { strip: "keep-strip", inherited: "keep-inherited", banner: "keep-banner", otherFish: "keep-otherfish",
  possession: "keep-possession", whereNot: "keep-wherenot", outside: "keep-outside", trout: "keep-trout" } as const;
const idOf = (b: KeepVM["order"][number]) => typeof b === "string" ? TEST_ID[b] : "group" in b ? "keep-group" : "plain" in b ? "keep-plain" : "keep-caveat";

function mount(keep: KeepVM) {
  const sheets: SheetKey[] = [], books: number[] = [];
  const actions: KeepActions = { onSheet: (k) => sheets.push(k), onDay() {}, onTab() {}, onBook: (p) => books.push(p) };
  const r = render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions} />);
  // the tab's own blocks (T810 cutover: the trout table inside its block carries `keep-*` ids of its own: KeepBox's rows)
  const drawn = [...r.container.querySelector("[data-testid='keep-tab']")!.children].map((e) => e.getAttribute("data-testid"))
    .filter((t): t is string => !!t && t.startsWith("keep-"));
  return { r, sheets, books, drawn };
}

describe("<KeepTab> over the frozen pages", () => {
  it("vedder_1010: the blocks in keep.order — strip, the trout table, Other fish, possession, where-not", () => {
    const keep = keepOf(vedder1010), { drawn } = mount(keep);
    expect(drawn).toEqual(["keep-strip", "keep-trout", "keep-otherfish", "keep-possession", "keep-wherenot"]);
    expect(drawn).toEqual(keep.order.map(idOf));
    expect(screen.getByTestId("keep-possession").textContent).toBe("Possession2 days’ quota›");
  });

  it("vedder_1010 (rows 32): the white sturgeon row's stored stamp drawn under it, its document a link to the Licence tab", () => {
    const tabs: string[] = [];
    render(<KeepTab keep={keepOf(vedder1010)} dateLabel="Oct 10" palette={LIGHT}
                    actions={{ onSheet() {}, onDay() {}, onTab: (t) => tabs.push(t), onBook() {} }} />);
    expect(screen.getAllByTestId("otherfish-stamp").map((e) => e.textContent)).toEqual([
      "You need White Sturgeon Conservation Licence › only if you fish for white sturgeon, even to release them"]);
    fireEvent.click(screen.getByText("White Sturgeon Conservation Licence ›"));
    expect(tabs).toEqual(["licence"]);
  });

  it("vedder_615 (closed): strip and banner — no where-not on a closed day (owner 2026-11-04); the banner's words are the VM's", () => {
    const keep = keepOf(vedder615), { drawn } = mount(keep);
    expect(drawn).toEqual(["keep-strip", "keep-banner"]);
    expect(screen.getByTestId("keep-banner").textContent).toBe(
      "No fishing for any game fish Jun 1–30.Jun 1–30. This waterSourceRule book p.22 ↗See the rules from then ›");
  });

  it("glenn_501: the inherited line right after the strip, with its caution", () => {
    const keep = keepOf(glenn501), { drawn } = mount(keep);
    expect(drawn.slice(0, 2)).toEqual(["keep-strip", "keep-inherited"]);
    expect(screen.getByTestId("keep-inherited").textContent).toBe(
      "By another water’s ruleOpen Apr 1–Jun 14: the Duncan River’s exemption from the spring stream closure covers its tributaries. Rule book p.36 ↗"
      + "Trout Lake’s row, nearer this stream, prints no such exemption; the Duncan River’s is applied here because its row covers all its tributaries.");
  });

  // (T810 cutover) retired: "glenn_501: the group's year window under its head block, before the tree" — no Keep tab
  // draws a group card since the cutover; the trout table's year is its year bar, TroutCard.test "the year bar is drawn
  // exactly when `year` is set (…)"

  it("links say where they go: a Rule book page, an Other fish row, the trout table's 'Sources ›'", () => {
    const { sheets, books } = mount(keepOf(vedder1010));
    fireEvent.click(screen.getAllByText("Rule book p.9 ↗")[0]!);
    fireEvent.click(screen.getByText("Kokanee"));
    fireEvent.click(screen.getByTestId("trout-sources"));
    expect([books, sheets]).toEqual([[11], ["other:o0", "src:trout"]]);
  });
});

describe("(T227, owner 2026-10-28) the salmon bar at the top, and the glance rows", () => {
  const STRIP = { title: "Salmon (DFO)", status: "release", mark: "✓ ", word: "Release only", when: "today", soon: null,
    generalTitle: "Game fish (B.C. rules)", legend: [{ drawn: "release", label: "release only" }, { drawn: "closed", label: "✕ closed" }],
    runs: [{ drawn: "release", days: 300 }, { drawn: "closed", days: 65 }] } as const;
  const actions: KeepActions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };

  it("drawn on top, right above the provincial bar (owner 2026-11-12), the two named; none without salmon (the bar unnamed)", () => {
    const keep = keepOf(vedder1010);
    const r = render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions} salmonStrip={STRIP as never} />);
    const ids = [...r.container.querySelectorAll("[data-testid='keep-strip'],[data-testid='salmon-strip'],[data-testid='keep-trout']")]
      .map((e) => e.getAttribute("data-testid"));
    expect(ids).toEqual(["salmon-strip", "keep-strip", "keep-trout"]);   // (T810 cutover) the trout table, where the group was
    expect([screen.getByTestId("keep-strip-title").textContent, screen.getByTestId("salmon-strip-title").textContent])
      .toEqual(["Game fish (B.C. rules)", "Salmon (DFO)"]);
    expect(screen.getByTestId("salmon-strip-st:release").textContent).toBe("✓ Release only");
    cleanup();
    render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions} />);
    expect([screen.queryByTestId("salmon-strip"), screen.queryByTestId("keep-strip-title")]).toEqual([null, null]);
  });

  it("the salmon bar is the game-fish bar's shape: one bar, the next change, the legend — nothing under it", () => {
    render(<KeepTab keep={keepOf(vedder1010)} dateLabel="Oct 10" palette={LIGHT} actions={actions}
                    salmonStrip={{ ...STRIP, soon: { kind: "soon", text: "Closed from Nov 1" } } as never} />);
    const s = screen.getByTestId("salmon-strip");
    // (jsdom lays nothing out: the marker and its date draw only at a measured width)
    expect([s.querySelectorAll("[data-testid=salmon-strip-bar]").length, within(s).getByText("Closed from Nov 1") !== null,
            screen.getByTestId("salmon-strip-legend").textContent])
      .toEqual([1, true, "release only✕ closed"]);
  });

  it("the WHOLE glance row opens its card; a row with nothing more is plain (no Details, not a button)", () => {
    const keep = keepOf(vedder1010);
    render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions}
                    glance={{ salmon: { row: { source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: null, tone: "keep" }, card: () => <span>salmon card</span> } }} />);
    const row = screen.getByTestId("glance:salmon");
    fireEvent.click(row.querySelector("[role=button]")!.firstChild as Element);   // the picture, not the Details button
    expect(screen.getByTestId("glance-body:salmon").textContent).toBe("salmon card");
    cleanup();
    render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions}
                    glance={{ salmon: { row: { source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: "closed", tone: "closed", fold: false }, card: () => <span>x</span> } }} />);
    const plain = screen.getByTestId("glance:salmon");
    expect([plain.querySelector("[role=button]"), plain.querySelector("[data-testid=glance-toggle]"), plain.textContent])
      .toEqual([null, null, "Salmonclosed"]);
  });
});

describe("(T531, owner 2026-11-09) each calendar owns its species", () => {
  const STRIP = { title: "Salmon (DFO)", status: "release", mark: "✓ ", word: "Release only", when: "today", soon: null,
    generalTitle: "Game fish (B.C. rules)", legend: [{ drawn: "release", label: "Release only" }, { drawn: "closed", label: "✕ Closed" }],
    runs: [{ drawn: "release", days: 300 }, { drawn: "closed", days: 65 }] } as const;
  const actions: KeepActions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };
  const salmon = { row: { source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: null, tone: "keep" }, card: () => <span>salmon card</span> } as const;
  const group = (r: ReturnType<typeof render>) => r.container.querySelector("[data-testid='keep-tab']") as HTMLElement;

  it("Vedder: Salmon (bar, its Salmon row) on top, then game fish (strip, its trout fold and Other fish rows); no 'Species and limits' list", () => {
    const keep = keepOf(vedder1010);
    const r = render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions} salmonStrip={STRIP as never} glance={{ salmon }} />);
    const ids = [...group(r).querySelectorAll("[data-testid='keep-strip'],[data-testid='glance:trout'],[data-testid='keep-species'],[data-testid='species-rows'],[data-testid='salmon-block'],[data-testid='salmon-strip'],[data-testid='glance:salmon']")]
      .map((e) => e.getAttribute("data-testid"));
    expect(ids).toEqual(["salmon-block", "salmon-strip", "species-rows", "glance:salmon", "keep-strip", "species-rows", "glance:trout"]);
    expect(screen.queryByTestId("keep-otherfish")).toBeNull();
    expect(screen.queryByText("Species and limits")).toBeNull();
  });

  it("a species appears only under its own source's block", () => {
    const keep = keepOf(vedder1010);
    render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions} salmonStrip={STRIP as never} glance={{ salmon }} />);
    const dfo = screen.getByTestId("salmon-block"), strip = screen.getByTestId("keep-strip");
    expect(within(dfo).getByTestId("glance:salmon")).toBeTruthy();                       // the DFO row: under the salmon block
    expect(within(dfo).queryByTestId("glance:trout")).toBeNull();                      // no B.C. trout fold in it
    expect(within(dfo).queryByText("Kokanee")).toBeNull();                               // no B.C. Other fish row in it
    const bc = strip.nextElementSibling as HTMLElement | null;
    if (!bc) throw new Error("no rows under the game-fish calendar");                                  // the rows right under the game-fish calendar
    expect(within(bc).getByTestId("glance:trout")).toBeTruthy();
    expect(within(bc).getByText("Kokanee")).toBeTruthy();
    expect(within(bc).queryByTestId("glance:salmon")).toBeNull();                        // and no salmon row among them
    expect(screen.getAllByTestId("glance:salmon")).toHaveLength(1);
  });

  it("a water with only B.C. rules has one block: the game-fish heading, its rows, no salmon block", () => {
    const r = render(<KeepTab keep={keepOf(vedder1010)} dateLabel="Oct 10" palette={LIGHT} actions={actions} glance={{ salmon: null }} />);
    expect(screen.getByTestId("keep-strip-title").textContent).toBe("Game fish (B.C. rules)");
    expect([screen.queryByTestId("salmon-block"), screen.queryByTestId("keep-otherfish")]).toEqual([null, null]);
    expect(within(group(r)).getByText("Kokanee")).toBeTruthy();
  });

  it("Details, rows and the possession row still work from inside the blocks", () => {
    const sheets: SheetKey[] = [];
    render(<KeepTab keep={keepOf(vedder1010)} dateLabel="Oct 10" palette={LIGHT} actions={{ ...actions, onSheet: (k) => sheets.push(k) }}
                    salmonStrip={STRIP as never} glance={{ salmon }} />);
    fireEvent.click(within(screen.getByTestId("glance:salmon")).getByRole("button"));
    expect(screen.getByTestId("glance-body:salmon").textContent).toBe("salmon card");
    fireEvent.click(screen.getByText("Kokanee"));
    fireEvent.click(screen.getByTestId("keep-possession"));
    expect(sheets.length).toBe(2);
  });

  it("STRICT: across the fixture waters, every DFO row sits under the Salmon (DFO) heading — with a DFO calendar and without one (Glenn Creek)", () => {
    const closed = { source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: "closed", tone: "closed", fold: false } as const;
    for (const [fx, strip] of [[vedder1010, STRIP], [glenn501, null], [glenn501, STRIP], [klwa1010, null], [bc1501, null], [x35p1010, null]] as const) {
      for (const sal of [salmon, { row: closed, card: () => <span>x</span> }] as const) {
        const r = render(<KeepTab keep={keepOf(fx)} dateLabel="Oct 10" palette={LIGHT} actions={actions} salmonStrip={strip as never} glance={{ salmon: sal as never }} />);
        const block = within(group(r)).getByTestId("salmon-block");
        expect(within(block).getByTestId("salmon-strip-title").textContent).toBe("Salmon (DFO)");
        expect(within(block).getAllByTestId("glance:salmon")).toHaveLength(1);
        expect(group(r).querySelectorAll("[data-testid='glance:salmon']")).toHaveLength(1);          // and nowhere else
        const bc = group(r).querySelector("[data-testid='keep-strip']")?.nextElementSibling;
        if (bc) expect(within(bc as HTMLElement).queryByTestId("glance:salmon")).toBeNull();           // never under the B.C. block
        if (!strip) expect(within(block).queryByTestId("salmon-strip-bar")).toBeNull();                // no calendar: no bar, no legend
        cleanup();
      }
    }
  });

  it("no DFO calendar: the Salmon (DFO) block says its status ('Closed') under its heading, still above the B.C. block", () => {
    const closed = { source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: "closed", tone: "closed", fold: false } as const;
    const r = render(<KeepTab keep={keepOf(glenn501)} dateLabel="Oct 10" palette={LIGHT} actions={actions}
                              glance={{ salmon: { row: closed, card: () => <span>x</span> } }} />);
    expect(screen.getByTestId("salmon-bare-st:closed").textContent).toBe("Closed");
    const ids = [...group(r).querySelectorAll("[data-testid='keep-strip'],[data-testid='salmon-block']")].map((e) => e.getAttribute("data-testid"));
    expect(ids).toEqual(["salmon-block", "keep-strip"]);
  });

  it("(owner 2026-11-12 \"salmon … on top when salmon rules exist\") STRICT: across the fixture waters, the WHOLE Salmon (DFO) block — bar, status, its row — is drawn above the game-fish calendar and its rows", () => {
    const closed = { source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: "closed", tone: "closed", fold: false } as const;
    let checked = 0;
    for (const [fx, strip] of [[vedder1010, STRIP], [vedder1010, null], [vedder615, STRIP], [glenn501, null], [glenn501, STRIP], [klwa1010, null], [bc1501, null], [x35p1010, null]] as const) {
      for (const sal of [salmon, { row: closed, card: () => <span>x</span> }] as const) {
        const r = render(<KeepTab keep={keepOf(fx)} dateLabel="Oct 10" palette={LIGHT} actions={actions} salmonStrip={strip as never} glance={{ salmon: sal as never }} />);
        const block = within(group(r)).getByTestId("salmon-block");
        const bc = group(r).querySelector("[data-testid='keep-strip']");
        if (bc) {
          // the salmon block ends before the game-fish calendar starts (not merely starts before it)
          expect(block.contains(bc)).toBe(false);
          expect(block.compareDocumentPosition(bc) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
          for (const row of [...group(r).querySelectorAll("[data-testid^='glance:group'],[data-testid='keep-possession']")])
            expect(block.compareDocumentPosition(row) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
          checked++;
        }
        cleanup();
      }
    }
    expect(checked).toBeGreaterThan(4);
  });

  it("the legend is plain ink words with the first letter capital: '✓ Open', 'Fly fishing only'", () => {
    const keep = keepOf(vedder1010);
    render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions} />);
    expect(screen.getAllByTestId("strip-legend-item").map((e) => e.textContent)).toEqual(keep.strip!.legend.map((l) =>
      (l.mark ? `${l.mark} ` : "") + l.label[0]!.toUpperCase() + l.label.slice(1)));
    expect(screen.getAllByTestId("strip-legend-item")[0]!.textContent).toBe("✓ Open");
  });
});

describe("(T810 O, S; owner about 17:15 \"Salmon + trout folds\") the trout table's one fold, and the arrow", () => {
  const actions: KeepActions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };
  const salmon = { row: { source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: "closed", tone: "closed" }, card: () => <span>salmon card</span> } as const;
  /** Vedder's page with a trout table (a vector's frame, through N's troutOf), its block before the group */
  const withTrout = (name = "fraser-r2-mission-jul15"): KeepVM => {
    const c = (vectors as unknown as { cases: { name: string; day: string; frame: TroutValue | null }[] }).cases.find((x) => x.name === name)!;
    const [month, day] = c.day.split("-").map(Number) as [number, number];
    const inp = { trout: c.frame, rules: { has: () => true }, frames: { rows: { rows: { length: 1e6 } } } } as unknown as RegsInputs;
    const keep = keepOf(vedder1010);
    // (T810 cutover) the frozen Vedder already draws its own table in keep.order: the vector's table takes its place
    return { ...keep, trout: troutOf(inp, { month, day }, { month, day }),
             order: keep.order.includes("trout") ? keep.order : ["strip", "trout", ...keep.order.slice(1)] };
  };
  const tab = (keep: KeepVM, glance = true) =>
    render(<KeepTab keep={keep} dateLabel="Jul 15" palette={LIGHT} actions={actions} {...(glance ? { glance: { salmon } } : {})} />);

  it("the salmon and trout folds end in the arrow, no 'Details'; the other fish's group fold keeps its Details", () => {
    tab(withTrout());
    for (const k of ["salmon", "trout"]) {
      const f = screen.getByTestId(`glance:${k}`);
      expect([within(f).queryByTestId("glance-arrow") !== null, within(f).queryByTestId("glance-toggle"), within(f).queryByText(GLANCE_WORDS.show)], k)
        .toEqual([true, null, null]);
    }
    // (T810 cutover) retired: "the other fish's group fold keeps its Details" — no Keep tab draws a group fold since the
    // cutover (the groups left sit in the Other fish list, whose rows open sheets: KeepTab.test "links say where they go")
    expect(screen.getAllByTestId("glance:trout")).toHaveLength(1);   // ONE trout fold, the table's
  });

  it("the whole row is one 44 px press, open or folded by aria-expanded, named by exactly its visible words", () => {
    tab(withTrout());
    const press = within(screen.getByTestId("glance:trout")).getByRole("button");
    expect([press.getAttribute("aria-label"), press.getAttribute("aria-expanded"), hitArea(press.getAttribute("data-hitslop")!)!.h >= 44])
      .toEqual(["Trout and char 2 a day", "false", true]);
    expect(press.textContent).toBe("Trout and char2 a day");
    fireEvent.click(press);
    expect(press.getAttribute("aria-expanded")).toBe("true");
    expect(within(screen.getByTestId("glance-body:trout")).getByTestId("keep-trout")).toBeTruthy();
    expect(within(screen.getByTestId("glance-body:trout")).queryByTestId("kicker:trout")).toBeNull();   // the fold names it
    cleanup();
    // a fold's date (the Elk in winter) is a visible word, so in the name too
    tab(withTrout("elk-nov15"));
    const elk = within(screen.getByTestId("glance:trout")).getByRole("button");
    expect(elk.getAttribute("aria-label")).toBe("Trout and char Release until Mar 31, then closed to all fishing");
    expect(within(elk).getByTestId("glance-date").textContent).toBe("until Mar 31, then closed to all fishing");
    const s = within(screen.getByTestId("glance:salmon")).getByRole("button");
    expect([s.getAttribute("aria-label"), s.getAttribute("aria-expanded")]).toEqual(["Salmon closed", "false"]);
  });

  it("(2× text) the value goes under the name, the arrow stays at the end", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(2);
    tab(withTrout());
    const press = within(screen.getByTestId("glance:trout")).getByRole("button");
    expect(within(press).getByTestId("glance-value").parentElement!.textContent).toBe("Trout and char2 a day");   // the name's column
    expect(press.lastElementChild!.getAttribute("data-testid")).toBe("glance-arrow");
  });

  it("(S, no dev switch) a page with no table: the salmon fold still ends in the arrow; the Other fish rows are drawn as before", () => {
    const v = keepOf(vedder1010);
    const plain: KeepVM = { ...v, trout: null, order: v.order.filter((b) => b !== "trout") };   // a page with no table (Q-D3)
    tab(plain);
    const f = screen.getByTestId("glance:salmon");
    expect([within(f).queryByTestId("glance-arrow") !== null, within(f).queryByTestId("glance-toggle")]).toEqual([true, null]);
    expect(screen.queryByTestId("glance:trout")).toBeNull();
    expect(screen.getByTestId("keep-tab").textContent).toContain("Kokanee");
  });

  it("no at-a-glance table: the trout table in full where its block is, named by its kicker", () => {
    const { r } = mount(withTrout());
    // the tab's own blocks (the keep box inside the table carries T829's `keep-strip` / `keep-group` ids of its own)
    expect([...r.container.querySelector("[data-testid='keep-tab']")!.children].map((e) => e.getAttribute("data-testid")).slice(0, 4))
      .toEqual(["keep-strip", "keep-trout", "keep-otherfish", "keep-possession"]);
    expect(screen.getByTestId("kicker:trout").textContent).toBe("Trout and char");
  });
});

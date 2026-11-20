/**
 * (T829, owner 2026-11-16: "B: trout keep box") the salmon species rows in the trout table's look: the species open today
 * in the tint box, each one's numbers in the white box on the right; closed species, the eulachon line (and a rule-book
 * closure) in the "✕ Closed" group under it; the badge, the year bar and the bait-ban line stay. SAME DATA: every word on
 * the box and the groups is the payload's (core `salmonCard` / `salmonTable`) or a words value — none composed here.
 * The water is the Skeena below Terrace in the owner's mockup (Aug 15), cut down: chinook and chum closed, coho 4 a day
 * only 2 over 50 cm, sockeye open with no number of its own, pink 2 a day; Region 6's bucket; eulachon closed; bait ban.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { SALMON_WORDS, WORDS, salmonGlance, salmonStripOf, salmonTable, type KeepVM, type RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { hitArea } from "./hit";
import { SalmonCard } from "./SalmonCard";
import { KeepTab } from "./KeepTab";
import { StripWidthHint } from "./YearStrip";
import vedder1010 from "./fixtures/vedder_1010.json";
import { BAIT, EULACHON, skeenaCard } from "./fixtures/salmonSkeena";

afterEach(cleanup);

const card = skeenaCard;
const names = (id: string) => within(screen.getByTestId(id)).getAllByTestId("keep-name").map((e) => e.textContent);

describe("(T829) the salmon card in the trout keep box", () => {
  it("the box holds the open species, each with its limit in the white box; DFO's day total across the top", () => {
    render(<SalmonCard card={card()} palette={LIGHT} />);
    const box = screen.getByTestId("salmon-keep-box");
    expect(names("salmon-keep-box")).toEqual(["Coho", "Sockeye", "Pink"]);
    expect(within(box).getByTestId("keep-head").textContent).toBe("Up to 4 salmon a day in total, fresh and tidal waters combined · 8 in possession");
    // each species' row with its own cell: coho's and pink's numbers in a white box (their dates small under them),
    // sockeye (no number of its own) its status word, free
    const cells = within(box).getAllByTestId("box-group").map((g) => [within(g).getByTestId("keep-name").textContent,
      within(g).queryByTestId("keep-band-box")?.textContent ?? null, within(g).queryByTestId("keep-band-free")?.textContent ?? null]);
    expect(cells).toEqual([["Coho", "4 a day · only 2 over 50 cmJul 15–Aug 31", null], ["Sockeye", null, "Open"],
                           ["Pink", "2 a dayApr 1–Mar 31", null]]);
    // the open ones' sizes stay on their rows
    expect(within(box).getAllByTestId("keep-quiet").map((e) => e.textContent)).toContain("Sizes (DFO): coho 30 cm or more to keep");
  });

  it("the ✕ Closed group under the box holds the closed species, then the eulachon line", () => {
    render(<SalmonCard card={card()} palette={LIGHT} />);
    const g = screen.getByTestId("salmon-closed-group");
    expect(names("salmon-closed-group")).toEqual(["Chinook", "Chum", EULACHON]);
    expect(within(g).getAllByTestId("keep-col-word").map((e) => e.textContent)).toEqual(["✕ Closed"]);
    expect(screen.getByTestId("salmon-keep-box").compareDocumentPosition(g) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    // no species anywhere else: one row each, five in all, and the eulachon line is not a gear line
    expect(screen.getAllByTestId("keep-name").map((e) => e.textContent)).toEqual(["Coho", "Sockeye", "Pink", "Chinook", "Chum", EULACHON]);
    expect(screen.queryAllByTestId("salmon-card-block-gear")).toEqual([]);
  });

  it("the badge, the head's chip and the bait-ban line stay", () => {
    render(<SalmonCard card={card()} palette={LIGHT} />);
    const el = screen.getByTestId("salmon-card");
    expect(el.getAttribute("aria-label")).toBe("Salmon, DFO: Open today");
    expect(within(el).getByText("Open today")).toBeTruthy();
    expect(screen.getByTestId("salmon-gear-provincial").textContent).toBe(BAIT);
    expect(screen.getByTestId("salmon-closed-group").compareDocumentPosition(screen.getByTestId("salmon-gear-provincial"))
      & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("on the Keep tab the salmon year bar stays above the opened card", () => {
    const c = card();
    const strip = salmonStripOf(c, { runs: [], line: [], todayAt: 226 / 365 });
    expect(strip).not.toBeNull();
    const actions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };
    render(
      <StripWidthHint.Provider value={300}>
        <KeepTab keep={(vedder1010 as unknown as RegsPage).keep as KeepVM} dateLabel="Aug 15" palette={LIGHT} actions={actions}
                 salmonStrip={strip} glance={{ salmon: { row: salmonGlance(c), card: () => <SalmonCard card={c} palette={LIGHT} /> } }} />
      </StripWidthHint.Provider>,
    );
    fireEvent.click(within(screen.getByTestId("glance:salmon")).getByRole("button"));
    const bar = screen.getByTestId("salmon-strip-bar");
    expect(bar.compareDocumentPosition(screen.getByTestId("salmon-keep-box")) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("every word in the box and the groups is the payload's or a words value", () => {
    const c = card();
    const allowed = new Set<string>();
    const take = (x: unknown): void => {
      if (typeof x === "string") allowed.add(x.trim());
      else if (Array.isArray(x)) x.forEach(take);
      else if (x && typeof x === "object") Object.values(x).forEach(take);
    };
    take(c); take(salmonTable(c)); take(SALMON_WORDS); take(WORDS);
    render(<SalmonCard card={c} palette={LIGHT} />);
    const drawn: string[] = [];
    for (const id of ["salmon-keep-box", "salmon-closed-group"]) {
      const walk = document.createTreeWalker(screen.getByTestId(id), NodeFilter.SHOW_TEXT);
      for (let n; (n = walk.nextNode());) if (n.textContent!.trim()) drawn.push(n.textContent!.trim());
    }
    expect(drawn.length).toBeGreaterThan(15);
    expect(drawn.filter((t) => !allowed.has(t))).toEqual([]);
  });

  it("a11y: each species row one label in payload order; every press named and 44 × 44", () => {
    render(<SalmonCard card={card()} palette={LIGHT} />);
    // (T810 cutover) the row is its press: its one label is the button's name
    const labels = within(screen.getByTestId("salmon-keep-box")).getAllByTestId("keep-row").map((e) => e.getAttribute("aria-label"));
    expect(labels[0]).toBe("Coho. Sizes (DFO): coho 30 cm or more to keep. Region 6 · Skeena River · B(ii). 4 a day · only 2 over 50 cm. Jul 15–Aug 31");
    for (const b of within(screen.getByTestId("salmon-card")).getAllByRole("button")) {
      expect(b.getAttribute("aria-label") || b.textContent, b.outerHTML.slice(0, 80)).toBeTruthy();
      const area = hitArea(b.getAttribute("data-hitslop") ?? "");
      expect(area && area.w >= 44 && area.h >= 44, b.getAttribute("data-testid") ?? "").toBe(true);
    }
  });
});

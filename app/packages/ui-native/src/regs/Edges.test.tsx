/**
 * T42 (C): the edge states on screen. Outside B.C. is core `buildRegsPage` over a place with no key (no store needed);
 * tidal water (x31p_1010), the hours window (x15p2_1010) and an unnamed section (handle 1178240) are the frozen pages
 * (fixtures/edge_*.json, buildRegsPage over the parity store — conformance/src/regs/fixtures.ts, never hand-edited).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { Text } from "react-native";
import { buildRegsPage, type RegsInputs, type RegsPage, type SheetKey } from "@app/core";
import { DARK, LIGHT } from "../theme";
import { KeepTab } from "./KeepTab";
import { RegsHeader } from "./RegsHeader";
import { RegsScreen } from "./RegsScreen";
import { createPinStore } from "./pinStore";
import type { KeepActions } from "./LineText";
import x31p from "./fixtures/edge_x31p_1010.json";
import x15p2 from "./fixtures/edge_x15p2_1010.json";
import unnamed from "./fixtures/edge_unnamed_1178240.json";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const OCT10 = { month: 10, day: 10 };
const asPage = (f: unknown) => ({ ...(f as RegsPage), sheet: () => null }) as RegsPage;
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };

/** A handle outside B.C.: what T09 `loadRegsInputs` gives (no key, place, item or frames). */
const outsidePage = (): RegsPage => buildRegsPage({
  target: { kind: "section", handle: 2_000_000_000 }, day: OCT10, weekday: "Saturday", calendarYear: 2026,
  item: null, partIx: 0, place: null, key: null, s: null, cell: null, frames: null, year: null, ruleset: null,
  rules: { get: (rix: number) => ({ rix, missing: true }), has: () => false, size: 0 }, lics: new Map(), mergedParts: null,
  statics: { exportSpecies: { fish: {}, groups: {} }, fish: [], glossary: { terms: [] } },
} as unknown as RegsInputs, { today: OCT10, who: { age: "adult", residency: "resident" } });

function mountKeep(page: RegsPage) {
  const sheets: SheetKey[] = [], books: number[] = [];
  const actions: KeepActions = { onSheet: (k) => sheets.push(k), onDay() {}, onTab() {}, onBook: (p) => books.push(p) };
  render(<KeepTab keep={page.keep} dateLabel={page.header.date} palette={LIGHT} actions={actions} />);
  return { sheets, books };
}

describe("8. outside B.C.", () => {
  it("one line, the outside foot; no section below the Keep content; only the Keep tab listed", () => {
    const page = outsidePage();
    // the water screen's rule (WaterScreen.tsx): outside B.C. no Gear / Methods / Licence slot is handed
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                       gear={page.gear ? <Text>gear</Text> : undefined} methods={page.methods ? <Text>methods</Text> : undefined}
                       licence={page.licence ? <Text>licence</Text> : undefined} />);
    expect(screen.getByTestId("keep-tab").textContent).toBe("This part of the water is outside B.C.: no B.C. rules apply.");
    expect(screen.getByTestId("keep-outside").textContent).toBe("This part of the water is outside B.C.: no B.C. rules apply.");
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid"))).toEqual(["keep-tab"]);
    expect(screen.queryByTestId("regs-licence")).toBeNull();
    expect(within(screen.getByRole("tablist", { name: "Section" })).getAllByRole("tab").map((t) => t.textContent)).toEqual(["What you can keep"]);
  });
});

describe("9. tidal water (x31p_1010)", () => {
  it("the banner's words alone: no year strip, no group, no Other fish, possession or where-not", () => {
    const page = asPage(x31p);
    mountKeep(page);
    expect([...screen.getByTestId("keep-tab").querySelectorAll("[data-testid^='keep-']")].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-banner"]);
    expect(screen.getByTestId("keep-banner").textContent).toBe("Tidal water" + "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp. See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required.");
    expect([screen.queryByTestId("keep-strip"), screen.queryByTestId("keep-group"), screen.queryByRole("heading", { name: /trout/i })]).toEqual([null, null, null]);
  });
  it("the header dot is faint (the sub grey), not a status colour", () => {
    render(<RegsHeader header={asPage(x31p).header} palette={LIGHT} tabs={createPinStore()} onDate={() => {}} onTab={() => {}} />);
    expect(getComputedStyle(screen.getByTestId("regs-dot")).backgroundColor).toBe(rgb(LIGHT.sub));
  });
});

describe("10. an unnamed section (handle 1178240, D11 a)", () => {
  const h = asPage(unnamed).header;
  it("the store's place name as the heading; no Map or Parts button even with a handler", () => {
    render(<RegsHeader header={h} palette={LIGHT} tabs={createPinStore()} onDate={() => {}} onTab={() => {}} onAction={() => {}} />);
    expect(within(screen.getByTestId("regs-header")).getByRole("heading").textContent).toBe("Unnamed stream · Region 5");
    expect(screen.queryByRole("button", { name: /map|parts/i })).toBeNull();
    expect(screen.queryByTestId("regs-action")).toBeNull();
    expect(screen.getByTestId("regs-place").textContent).toBe("");
  });
  it("header-one-shape: the empty label slot is the height of a named water's", () => {
    const style = (header: RegsPage["header"]) => {
      render(<RegsHeader header={header} palette={LIGHT} tabs={createPinStore()} onDate={() => {}} onTab={() => {}} />);
      const s = getComputedStyle(screen.getByTestId("regs-place")).height;
      cleanup();
      return s;
    };
    const named = style(asPage(vedder1010).header), none = style(h);
    expect(named).toBe(none);
    expect(named).not.toBe("");
  });
});

describe("11. the timed caveat (x15p2_1010, D10 a)", () => {
  const page = asPage(x15p2), caveat = page.keep.caveats[0]!;
  it("'⚠ At certain times' as the block's label, in the sub grey; the block marked by a restricted rule on its left", () => {
    expect(caveat.kind).toBe("timed");
    for (const palette of [LIGHT, DARK]) {
      render(<KeepTab keep={page.keep} dateLabel={page.header.date} palette={palette} actions={{ ...NOOP, onTab() {} }} />);
      const k = screen.getByTestId("kicker:timed");
      expect(k.textContent).toBe("⚠ At certain times");
      expect(getComputedStyle(k).color).toBe(rgb(palette.sub));
      const block = screen.getByTestId("keep-caveat");
      expect([getComputedStyle(block).borderLeftColor, getComputedStyle(block).borderLeftWidth]).toEqual([rgb(palette.restricted), "3px"]);
      cleanup();
    }
  });
  it("each item a button opening its rule's sheet; the Rule book link after it, not inside it", () => {
    const { sheets, books } = mountKeep(page);
    const items = screen.getAllByTestId("keep-caveat-item");
    expect(items.map((e) => e.textContent)).toEqual(["No fishing for any game fish60 min after sunset–60 min before sunriseRule book p.23 ↗"]);
    const btn = within(items[0]!).getByRole("button");
    expect(btn.textContent).toBe("No fishing for any game fish60 min after sunset–60 min before sunrise");
    fireEvent.click(btn);
    expect(sheets).toEqual([`rule:${caveat.items[0]!.rule}`]);
    fireEvent.click(within(items[0]!).getByText("Rule book p.23 ↗"));
    expect([sheets.length, books.length]).toEqual([1, 1]);
  });
});

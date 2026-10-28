/**
 * (T211, option A) THE SALMON CARD on the Regulations page: framed apart under the provincial rules, its own as-of line,
 * "Salmon — closed" when every species is closed (an unlisted water included), the species grouped otherwise, and a DFO
 * place not drawn yet tagged "Location not yet mapped". Words are core's (`salmonCard`, pinned whole there); this pins
 * where they land. And the map's Standard / Salmon toggle: a radio pair whose press only sets the choice.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { salmonCard, type RegsPage, type SalmonFacts } from "@app/core";
import { LIGHT } from "../theme";
import { RegsScreen } from "./RegsScreen";
import { SalmonCard } from "./SalmonCard";
import { RegsLayerToggle } from "../shell/RegsLayerToggle";
import type { MapColouring } from "../shell/useMapColouring";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 3,
  chainOf: (sid) => (sid === 101 ? 1 : 0),
  payload: {
    format: "salmon/1", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations",
    default: 0,
    regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/6" } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "closed", t: "No fishing for chinook (Apr 1–Mar 31)" },
               { s: "open", t: "4 per day, only 2 over 50 cm (Jul 15–Aug 23)" }],
    gears: [["No natural bait allowed (Jan 1–Dec 31)"]],
    chains: [
      { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Kispiox River", l: [], sp: [[0, 1], [0, 1, 196, 2, 236, 1], [0, 1], [0, 1], [0, 0]], g: [0, 0], m: [0, 3] },
      { r: ["6"], w: "Kispiox River — at the resort", l: [], sp: all(1), g: [0, -1], m: [0, 3] },
    ],
    unmapped: [{ items: ["gnis:4674"], r: "6", w: "Kispiox River — at the resort", chain: 2 }],
  },
};
const JUL20 = new Date(2026, 6, 20), SEP15 = new Date(2026, 8, 15);
const page = { ...(vedder1010 as unknown as RegsPage), sheet: () => null } as RegsPage;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };

describe("the Salmon card", () => {
  it("draws under the provincial rules, framed, with its own source and as-of line", () => {
    const card = salmonCard(FACTS, { sids: [101], item: null }, JUL20);
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                       salmon={<SalmonCard card={card} palette={LIGHT} />} />);
    const el = screen.getByTestId("salmon-card");
    // ABOVE the Keep tab (owner 2026-10-28)
    expect(screen.getByTestId("keep-tab").compareDocumentPosition(el) & Node.DOCUMENT_POSITION_PRECEDING).toBeTruthy();
    expect(el.getAttribute("aria-label")).toBe("Salmon, DFO: Open today");
    const rows = screen.getAllByTestId("salmon-card-block-row").map((r) => r.textContent!);
    expect(rows.map((r, i) => [r.startsWith(["Chinook · sockeye · pink", "Coho", "Chum"][i]!), r.includes(["Closed", "Open", "Closed"][i]!)]))
      .toEqual([[true, true], [true, true], [true, true]]);
    expect(rows[1]).toContain("4 per day, only 2 over 50 cm (Jul 15–Aug 23)");
    // each species of a row is pictured, both looks, and opens its fish sheet
    expect(within(screen.getAllByTestId("salmon-card-block-row")[0]!).getAllByTestId("salmon-look").length).toBe(6);
    expect(screen.getByTestId("pic:CO").textContent).toBe("oceanspawningCoho salmon ›");
    expect(screen.getByTestId("salmon-card-block-gear").textContent).toBe("No natural bait allowed (Jan 1–Dec 31)");
    expect([screen.getByTestId("salmon-source").textContent, screen.getByTestId("salmon-asof").textContent])
      .toEqual(["Fisheries and Oceans Canada · Region 6 Skeena", "DFO page dated Aug 28, 2026"]);
    expect(screen.getByTestId("salmon-credit").textContent).toContain("Fisheries and Oceans Canada (DFO)");
    // the release's own file: no feed line
    expect(screen.queryByTestId("salmon-checked")).toBeNull();
  });

  it("(T211 part 2) read through the live feed: the capture's time under the page date", () => {
    const card = { ...salmonCard(FACTS, { sids: [101], item: null }, JUL20), checked: "Checked with DFO Oct 27, 2026, 11:04 am" };
    render(<SalmonCard card={card} palette={LIGHT} />);
    expect(screen.getByTestId("salmon-checked").textContent).toBe("Checked with DFO Oct 27, 2026, 11:04 am");
  });

  it("every species closed: ONE 'Salmon — closed' row", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [101], item: null }, SEP15)} palette={LIGHT} />);
    const rows = screen.getAllByTestId("salmon-card-block-row");
    expect(rows.length).toBe(1);
    expect(rows[0]!.textContent!.startsWith("Salmon — closed")).toBe(true);
    expect(within(rows[0]!).getAllByTestId("salmon-look").length).toBe(10);       // five species, two looks each
    expect(screen.getByTestId("salmon-card").getAttribute("aria-label")).toBe("Salmon, DFO: Closed today");
  });

  it("A WATER DFO DOES NOT LIST reads 'Salmon — closed', and says why (owner 2026-10-27)", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [7], item: "gnis:1" }, JUL20)} palette={LIGHT} />);
    expect(screen.getAllByTestId("salmon-card-block-row").map((r) => r.textContent))
      .toEqual([expect.stringMatching(/^Salmon — closedClosedNot opened by DFO: closed to salmon fishing/)]);
    expect(screen.getByTestId("salmon-not-listed").textContent)
      .toBe("DFO does not list this water for salmon. It is closed to salmon fishing.");
  });

  it("a DFO place on this water not drawn yet: 'Location not yet mapped', on the card — never on the map", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [101], item: "gnis:4674" }, JUL20)} palette={LIGHT} />);
    expect(screen.getByTestId("salmon-unmapped-tag").textContent).toBe("Location not yet mapped");
    expect(screen.getByTestId("salmon-unmapped").textContent).toContain("Kispiox River — at the resort");
    expect(FACTS.chainOf(999)).toBe(0);     // its chain is placed on no section: the map draws the water unlisted
  });
});

describe("the Standard / Salmon switch (under the Layers button, T213)", () => {
  it("a radio pair; a press sets the choice and nothing else; no Bait & fly (marks on Standard)", () => {
    const set: string[] = [];
    const col = { regsLayer: "standard", setRegsLayer: (p: string) => set.push(p) } as unknown as MapColouring;
    render(<RegsLayerToggle col={col} palette={LIGHT} />);
    const [std, sal] = [screen.getByTestId("regs-paint-standard"), screen.getByTestId("regs-paint-salmon")];
    expect([std, sal].map((e) => [e!.getAttribute("aria-checked"), e!.textContent]))
      .toEqual([["true", "Standard"], ["false", "Salmon"]]);
    expect(screen.queryByTestId("regs-paint-method")).toBeNull();
    fireEvent.click(sal!);
    expect(set).toEqual(["salmon"]);
  });
});

describe("a species' picture opens its fish sheet", () => {
  it("sp:CO on a press", () => {
    const opened: string[] = [];
    const card = salmonCard(FACTS, { sids: [101], item: null }, JUL20);
    render(<SalmonCard card={card} palette={LIGHT} actions={{ onSheet: (k: string) => opened.push(k) } as never} />);
    fireEvent.click(screen.getByTestId("pic:CO"));
    expect(opened).toEqual(["sp:CO"]);
  });
});

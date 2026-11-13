/**
 * (T211, option A) THE SALMON CARD on the Regulations page: framed apart under the provincial rules, its own as-of line,
 * "Salmon — closed" when every species is closed (an unlisted water included), the species grouped otherwise, and a DFO
 * place not drawn yet tagged "Location not yet mapped". Words are core's (`salmonCard`, pinned whole there); this pins
 * where they land. And the map's Standard / Salmon toggle: a radio pair whose press only sets the choice.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { FISH, salmonCard, salmonRowTexts, type RegsPage, type SalmonFacts } from "@app/core";
import { LIGHT } from "../theme";
import { SPINNER_TIMING } from "../LoadingState";
import { hitArea } from "./hit";
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
    regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/6", shared: [], sizes: [] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "closed", t: "No fishing for chinook (Apr 1–Mar 31)" },
               { s: "open", t: "4 per day, only 2 over 50 cm (Jul 15–Aug 23)" }],
    gears: [["No natural bait allowed (Jan 1–Dec 31)"]],
    chains: [
      { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
      // (T668 fix 1) the section names the water's undrawn place (`u`), as the pipeline builds it; the place's own chain is it
      { r: ["6"], w: "Kispiox River", l: [], u: ["6:resort"], sp: [[0, 1], [0, 1, 196, 2, 236, 1], [0, 1], [0, 1], [0, 0]], g: [0, 0], m: [0, 3] },
      { r: ["6"], w: "Kispiox River — at the resort", l: ["6:resort"], sp: all(1), g: [0, -1], m: [0, 3], n: [0, 1] },
    ],
    unmapped: [{ items: ["gnis:4674"], r: "6", w: "Kispiox River — at the resort", chain: 2 }],
  },
};
const JUL20 = new Date(2026, 6, 20), SEP15 = new Date(2026, 8, 15);
const page = { ...(vedder1010 as unknown as RegsPage), sheet: () => null } as RegsPage;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };

describe("the Salmon card", () => {
  it("(T320, T332) the bucket total over the species rows, and each open species' numbers on its row — the quota tree", () => {
    const P = FACTS.payload;
    const tier = (t: string, n: number, says: string, more: object = {}) => ({ node: "tier", id: "t1", tier: t, n, says, by: 0, children: [], ...more });
    const facts = { ...FACTS, payload: { ...P,
      regions: { "6": { ...P.regions["6"]!, bucket: { tree: tier("total", 4, "Up to 4 salmon a day in total"), possession: 8,
                                                     t: "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4." } } },
      verdicts: P.verdicts.map((v, i) => (i === 2 ? { ...v, tree: tier("count", 4, "4 a day", { species: ["CO"],
        children: [tier("size", 2, "only 2 over 50 cm", { over_cm: 50 })] }) } : v)) } } as SalmonFacts;
    render(<SalmonCard card={salmonCard(facts, { sids: [101], item: null }, JUL20)} palette={LIGHT} />);
    expect(screen.getByTestId("salmon-bucket-line").textContent).toBe("Up to 4 salmon a day in total · 8 in possession");
    expect(screen.getAllByTestId("salmon-quota").map((e) => e.textContent)).toEqual(["4 a day · only 2 over 50 cm"]);
    // the trout presenter's runs: each tier's number its own run (tierWords), the bucket's and the quota's
    expect(screen.getAllByTestId("n:big").map((e) => e.textContent)).toEqual(["4", "4", "2"]);
    // nothing kept today: no bucket line
    cleanup();
    render(<SalmonCard card={salmonCard(facts, { sids: [101], item: null }, SEP15)} palette={LIGHT} />);
    expect(screen.queryByTestId("salmon-bucket-line")).toBeNull();
  });

  it("(T671 D3) the bucket's period limits are drawn under the total, each in the store's words, while a species it names may be kept", () => {
    const P = FACTS.payload;
    const tier = (t: string, n: number, says: string, more: object = {}) => ({ node: "tier", id: "t1", tier: t, n, says, by: 0, children: [], ...more });
    const annual = tier("size", 10, "The total annual limit for coho salmon over 50 cm is 10 fish from all fresh waters combined.",
                        { over_cm: 50, period: "annual", id: "a", species: ["CO"] });
    const chinook = tier("size", 10, "The total annual limit for chinook salmon over 50 cm is 10 fish.",
                         { over_cm: 50, period: "annual", id: "b", species: ["CH"] });
    const facts = { ...FACTS, payload: { ...P,
      regions: { "6": { ...P.regions["6"]!, bucket: { tree: tier("total", 4, "Up to 4 salmon a day in total", { children: [annual, chinook] }), possession: 8,
                                                     t: "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4." } } },
      verdicts: P.verdicts.map((v, i) => (i === 2 ? { ...v, tree: tier("count", 4, "4 a day", { species: ["CO"] }) } : v)) } } as SalmonFacts;
    render(<SalmonCard card={salmonCard(facts, { sids: [101], item: null }, JUL20)} palette={LIGHT} />);
    // coho may be kept (the chinook limit names a species that may not: not drawn); the line is the whole sentence
    expect(screen.getAllByTestId("salmon-bucket-tier").map((e) => (e.textContent ?? "").replace(/\u00a0/g, " ")))
      .toEqual(["The total annual limit for coho salmon over 50 cm is 10 fish from all fresh waters combined."]);
    // the total stays first
    expect(screen.getByTestId("salmon-bucket-line").textContent).toBe("Up to 4 salmon a day in total · 8 in possession");
  });

  it("draws under the provincial rules, framed, with its own source and as-of line", () => {
    const card = salmonCard(FACTS, { sids: [101], item: null }, JUL20);
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                       salmon={<SalmonCard card={card} palette={LIGHT} />} />);
    const el = screen.getByTestId("salmon-card");
    // ABOVE the Keep tab (owner 2026-10-28)
    expect(screen.getByTestId("keep-tab").compareDocumentPosition(el) & Node.DOCUMENT_POSITION_PRECEDING).toBeTruthy();
    expect(el.getAttribute("aria-label")).toBe("Salmon, DFO: Open today");
    // (T318) ONE row per species, today's status; DFO's words once under the table; ONE picture each (T394: the spawning look)
    const rows = screen.getAllByTestId("salmon-species-row").map((r) => r.textContent!);
    expect(rows.map((r, i) => [r.startsWith(["Chinook", "Coho", "Sockeye", "Pink", "Chum"][i]!), r.endsWith(["Closed", "Open", "Closed", "Closed", "Closed"][i]!)]))
      .toEqual([[true, true], [true, true], [true, true], [true, true], [true, true]]);
    expect(screen.getAllByTestId("salmon-says-line").map((e) => e.textContent)).toContain("4 per day, only 2 over 50 cm (Jul 15–Aug 23)");
    expect(screen.getAllByTestId("salmon-look").length).toBe(5);
    expect(screen.getByTestId("pic:CO").textContent).toBe("Coho ›");
    expect(screen.getByTestId("salmon-card-block-gear").textContent).toBe("No natural bait allowed (Jan 1–Dec 31)");
    // (T482) DFO's source block is a fold, closed by default: its summary only, then everything on a press
    const summary = screen.getByTestId("salmon-source-summary");
    expect([summary.textContent, summary.getAttribute("aria-expanded"), screen.queryByTestId("salmon-source")])
      .toEqual(["Source (DFO)", "false", null]);
    expect(screen.queryByTestId("salmon-note")).toBeNull();
    fireEvent.click(summary);
    expect(screen.getByTestId("salmon-note").textContent).toBe("Salmon are set by DFO, not the provincial regulations above. Both apply.");
    expect([screen.getByTestId("salmon-source").textContent, screen.getByTestId("salmon-asof").textContent])
      .toEqual(["Fisheries and Oceans Canada · Region 6 Skeena", "DFO page dated Aug 28, 2026"]);
    expect(screen.getByTestId("salmon-credit").textContent).toContain("Fisheries and Oceans Canada (DFO)");
    // the release's own file: no feed line
    expect(screen.queryByTestId("salmon-checked")).toBeNull();
  });

  it("(T482) the page's salmon hours are one line in the section (the daylight note folds into it)", () => {
    const hours = "Salmon fishing in daylight only: one hour before sunrise to one hour after sunset";
    render(<SalmonCard card={{ ...salmonCard(FACTS, { sids: [101], item: null }, JUL20), hours }} palette={LIGHT} />);
    expect(screen.getByTestId("salmon-hours").textContent).toBe(hours);
    cleanup();
    render(<SalmonCard card={{ ...salmonCard(FACTS, { sids: [101], item: null }, JUL20), hours: null }} palette={LIGHT} />);
    expect(screen.queryByTestId("salmon-hours")).toBeNull();
  });

  it("(T211 part 2) read through the live feed: the capture's time under the page date", () => {
    const card = { ...salmonCard(FACTS, { sids: [101], item: null }, JUL20), checked: "Checked with DFO Oct 27, 2026, 11:04 am PT" };
    render(<SalmonCard card={card} palette={LIGHT} />);
    fireEvent.click(screen.getByTestId("salmon-source-summary"));
    expect(screen.getByTestId("salmon-checked").textContent).toBe("Checked with DFO Oct 27, 2026, 11:04 am PT");
  });

  it("(T394) each species row wears its SPAWNING look (owner 2026-10-31, T343 item 6), never the ocean one", () => {
    const images = Object.fromEntries(Object.entries(FISH).map(([code, looks]) => [code, looks.map((l) => ({ thumb: { uri: `thumbs/${l.file}` }, full: { uri: l.file } }))]));
    render(<SalmonCard card={salmonCard(FACTS, { sids: [101], item: null }, JUL20)} palette={LIGHT} images={images as never} />);
    const looks = screen.getAllByTestId("salmon-look").map((e) => e.outerHTML);
    expect(looks.length).toBe(5);
    expect(looks.every((h) => h.includes("-spawning-phase.webp") && !h.includes("-ocean-phase.webp"))).toBe(true);
    // (T405) a row draws the thumbnail, never the full picture
    expect(looks.every((h) => h.includes("thumbs/"))).toBe(true);
  });

  it("every species closed: five closed species rows and DFO's closed words said ONCE (not per picture)", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [101], item: null }, SEP15)} palette={LIGHT} />);
    expect(screen.getAllByTestId("salmon-species-status").map((e) => e.textContent)).toEqual(Array(5).fill("Closed"));
    expect(screen.getAllByTestId("salmon-says-line").length).toBe(2);               // the chinook line and the not-opened line, once
    expect(screen.getByTestId("salmon-card").getAttribute("aria-label")).toBe("Salmon, DFO: Closed today");
  });

  it("A WATER DFO DOES NOT LIST reads 'Salmon — closed', and says why (owner 2026-10-27)", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [7], item: "gnis:1" }, JUL20)} palette={LIGHT} />);
    expect(screen.getAllByTestId("salmon-species-status").map((e) => e.textContent)).toEqual(Array(5).fill("Closed"));
    // (T502) DFO's "not opened" words repeat the five Closed rows: not said under the table; the line below says why
    expect(screen.queryAllByTestId("salmon-says-line")).toEqual([]);
    expect(screen.getByTestId("salmon-not-listed").textContent)
      .toBe("DFO does not list this water for salmon. It is closed to salmon fishing.");
  });

  it("(T372) a DFO place on this water not drawn yet: the trout card's caveat block on the card — never on the map", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [101], item: "gnis:4674" }, JUL20)} palette={LIGHT} />);
    // folded to its head; opened, the place and its day ("Closed to salmon fishing": every species closed, said once)
    expect(screen.queryByTestId("keep-caveat-item")).toBeNull();
    fireEvent.click(screen.getByTestId("caveat-fold"));
    const item = screen.getByTestId("keep-caveat-item").textContent!;
    expect([item.includes("Closed to salmon fishing"), item.includes("Where: Kispiox River — at the resort")]).toEqual([true, true]);
  });
});

describe("(T425) the Trout / Salmon switch (the map rail, under locate + layers)", () => {
  it("a radio pair of fish over small-caps words; a press sets the choice and nothing else; no Bait & fly (marks on Standard)", () => {
    const set: string[] = [];
    const col = { regsLayer: "standard", setRegsLayer: (p: string) => set.push(p) } as unknown as MapColouring;
    render(<RegsLayerToggle col={col} palette={LIGHT} />);
    const [std, sal] = [screen.getByTestId("regs-paint-standard"), screen.getByTestId("regs-paint-salmon")];
    expect([std, sal].map((e) => [e!.getAttribute("aria-checked"), e!.textContent]))
      .toEqual([["true", "Trout"], ["false", "Salmon"]]);
    expect(getComputedStyle(within(std).getByText("Trout")).textTransform).toBe("uppercase");
    expect(screen.getByTestId("regs-paint").getAttribute("role")).toBe("radiogroup");
    expect([std, sal].map((e) => e!.getAttribute("role"))).toEqual(["radio", "radio"]);
    // named and hinted aloud; the chosen one wears the accent bar
    expect([std, sal].map((e) => e!.getAttribute("aria-label")))
      .toEqual(["Colour by trout and other species' rules", "Colour by salmon openings"]);
    expect(screen.getByTestId("regs-paint-bar-standard")).toBeTruthy();
    expect(screen.queryByTestId("regs-paint-bar-salmon")).toBeNull();
    // 44 x 44 to the touch
    expect([std, sal].map((e) => hitArea(e!.getAttribute("data-hitslop")!))).toEqual([{ w: 44, h: 44 }, { w: 44, h: 44 }]);
    expect(screen.queryByTestId("regs-paint-method")).toBeNull();
    fireEvent.click(sal!);
    expect(set).toEqual(["salmon"]);
  });

  it("wears the app's fish art when it is handed down (rainbow trout, chinook), the fish icon when it is not", () => {
    const col = { regsLayer: "standard", setRegsLayer: () => {} } as unknown as MapColouring;
    const art = { RB: [{ thumb: "rb.png", full: "rb-full.png" }], CH: [{ thumb: "ch.png", full: "ch-full.png" }] };
    render(<RegsLayerToggle col={col} palette={LIGHT} fishImages={art as never} />);
    const src = (k: string) => screen.getByTestId(`regs-paint-${k}`).querySelector("img")?.getAttribute("src");
    expect([src("standard"), src("salmon")].map((x) => x?.split("/").pop())).toEqual(["rb.png", "ch.png"]);
    cleanup();
    render(<RegsLayerToggle col={col} palette={LIGHT} />);
    expect(screen.getAllByTestId("icon-fish")).toHaveLength(2);
  });

  it("T182 (web): while the first press's salmon file loads, Salmon is the choice and busy; (T257) the fish turns in its icon's place", () => {
    vi.useFakeTimers();
    try {
      const col = { regsLayer: "salmon", salmonLoading: true, setRegsLayer: () => {} } as unknown as MapColouring;
      render(<RegsLayerToggle col={col} palette={LIGHT} />);
      const sal = screen.getByTestId("regs-paint-salmon");
      expect([sal.getAttribute("aria-checked"), sal.getAttribute("aria-busy"), sal.textContent])
        .toEqual(["true", "true", "Salmon"]);
      // a quick load shows no spinner; a slow one shows the one spinner after its delay
      expect(within(sal).queryByRole("progressbar")).toBeNull();
      act(() => { vi.advanceTimersByTime(SPINNER_TIMING.showAfterMs); });
      expect(within(sal).getByRole("progressbar")).toBeTruthy();
    } finally { vi.useRealTimers(); }
  });
});

describe("(T318) every DFO rule on a water is still reachable in the table layout", () => {
  it("each row word, size, species note, place word, region rule and water rule is on the card (the caveat once opened)", () => {
    const n = (id: string, k: string, t: string, extra: object = {}) => ({ id, k, t, ...extra });
    const f: SalmonFacts = { ...FACTS, payload: { ...FACTS.payload, regions: { "6": { ...FACTS.payload.regions["6"]!, notes: [
      n("6:daylight:a", "daylight", "You can only fish for salmon in Region 6 during daylight hours."),
      n("6:min_size:b", "min_size", "All retained coho must measure 25 cm or more.", { sp: ["CO"], lengths: [{ min_cm: 25 }] }),
      n("6:annual:c", "annual", "The annual coho limit is 10.", { sp: ["CO"], n: 10 }),
    ], sizes: [{ sp: ["CO"], min: { min_cm: 25 } }] } } } };
    for (const [day, item] of [[JUL20, "gnis:4674"], [SEP15, "gnis:4674"], [JUL20, null]] as const) {
      const card = salmonCard(f, { sids: [101], item }, day);
      render(<SalmonCard card={card} palette={LIGHT} />);
      screen.queryAllByTestId("caveat-fold").forEach((b) => fireEvent.click(b));
      const text = screen.getByTestId("salmon-card").textContent!;
      // (T502) a row's words its species rows already say (`repeats`) are said by those rows, not under the table
      for (const t of salmonRowTexts(card)) if (!card.rows.some((r) => r.repeats?.includes(t))) expect(text, t).toContain(t);
      for (const x of [...(card.caveat?.items ?? []), ...(card.caveat?.later ?? [])]) for (const t of [x.where!, ...(x.detail ?? [])]) expect(text, t).toContain(t);
      for (const g of card.regionRules) for (const r of g.rules) expect(text, r.id).toContain(r.line);
      for (const t of card.waterRules) expect(text, t).toContain(t);
      cleanup();
    }
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

describe("(owner 2026-10-28) the region header and the provincial gear on the card", () => {
  const n = (id: string, k: string, t: string, extra: object = {}) => ({ id, k, t, ...extra });
  const WITH_HEADER: SalmonFacts = { ...FACTS, payload: { ...FACTS.payload,
    regions: { "6": { ...FACTS.payload.regions["6"]!, notes: [
      n("6:daylight:a", "daylight", "You can only fish for salmon in Region 6 during daylight hours."),
      n("6:min_size:b", "min_size", "All retained coho must measure 25 cm or more.", { sp: ["CO"], lengths: [{ min_cm: 25 }] }),
      n("6:aggregate:c", "aggregate", "Kispiox watershed: the aggregate daily limit of hatchery-marked coho is one (1).",
        { sp: ["CO"], n: 1, w: [{ n: "Kispiox River", ws: true, key: "kispiox river+ws" }] }),
    ], sizes: [{ sp: ["CO"], min: { min_cm: 25 } }],
    // (T356) the pipeline's `quota.shared_buckets` for the aggregate note
    shared: [{ id: "6:aggregate:c", t: "Kispiox watershed: the aggregate daily limit of hatchery-marked coho is one (1).",
               w: ["kispiox river+ws"], tree: { node: "tier", id: "t0", tier: "total", n: 1, by: 0, children: [], species: ["CO"],
               whose: "the Kispiox River watershed", origin: "hatchery",
               says: "Up to 1 hatchery-marked coho a day in total across the Kispiox River watershed" } as never }] } },
    scopes: { "6": { "kispiox river+ws": ["gnis:77"] } } } };   // gnis:77: a Kispiox trib (gnis:4674 has an undrawn place)

  it("region rules in their own block, the water's coho note and sizes on the open coho row, the gear line", () => {
    render(<SalmonCard card={salmonCard(WITH_HEADER, { sids: [101], item: "gnis:77", gear: ["bait"] }, JUL20)} palette={LIGHT} />);
    const block = screen.getByTestId("salmon-region-rules");
    expect(within(block).getByText("Region rules (DFO) · Region 6 Skeena")).toBeTruthy();
    expect(within(block).getByText("You can only fish for salmon in Region 6 during daylight hours.")).toBeTruthy();
    expect(screen.getByText(/Kispiox watershed: the aggregate daily limit/)).toBeTruthy();
    expect(screen.getByTestId("salmon-limit").textContent).toBe("Sizes (DFO): coho 25 cm or more to keep");
    expect(screen.getByTestId("salmon-gear-provincial").textContent).toBe("B.C. rules, also for salmon: Bait ban: no bait of any kind.");
  });

  it("(T309) region rules are rows: kind chip, species tag, one line; the whole wording folds; Copy", () => {
    const long = "Region 6 Skeena: all retained coho must measure 25 cm or more. Measure from the tip of the snout to the fork of the tail.";
    const f: SalmonFacts = { ...WITH_HEADER, payload: { ...WITH_HEADER.payload, regions: { "6": { ...WITH_HEADER.payload.regions["6"]!,
      notes: [n("6:other:z", "other", long, { sp: ["CH"] })] } } } };
    render(<SalmonCard card={salmonCard(f, { sids: [101], item: "gnis:77" }, JUL20)} palette={LIGHT} />);
    const row = screen.getByTestId("salmon-rule");
    expect(within(row).getByTestId("salmon-rule-kind").textContent).toBe("Rule");
    expect(within(row).getByText("Chinook")).toBeTruthy();
    expect(within(row).getByTestId("salmon-rule-line").textContent).toBe("Region 6 Skeena: all retained coho must measure 25 cm or more.");
    expect(within(row).queryByTestId("salmon-rule-full")).toBeNull();
    fireEvent.click(within(row).getByTestId("salmon-rule-fold"));
    expect(within(row).getByTestId("salmon-rule-full").textContent).toBe(long);
    // (T342) no Copy button: the words copy with the phone's own long-press
    expect(within(row).queryByTestId("salmon-rule-copy")).toBeNull();
  });

  it("(T227) a region note about the coho sits on the coho row, not in the region block", () => {
    const f: SalmonFacts = { ...WITH_HEADER, payload: { ...WITH_HEADER.payload, regions: { "6": { ...WITH_HEADER.payload.regions["6"]!,
      notes: [...WITH_HEADER.payload.regions["6"]!.notes!, n("6:annual:d", "annual", "The annual coho limit is 10.", { sp: ["CO"], n: 10 })] } } } };
    render(<SalmonCard card={salmonCard(f, { sids: [101], item: "gnis:9" }, JUL20)} palette={LIGHT} />);
    expect(screen.getAllByTestId("salmon-says-line").map((e) => e.textContent)).toContain("The annual coho limit is 10.");
    expect(screen.getByTestId("salmon-region-rules").textContent).not.toContain("annual coho");
    expect(screen.getByTestId("salmon-region-rules").textContent).not.toContain("25 cm");
  });

  it("another water in the region: the region block only; a closed water: no gear line", () => {
    render(<SalmonCard card={salmonCard(WITH_HEADER, { sids: [7], item: "gnis:1", gear: ["bait"] }, JUL20)} palette={LIGHT} />);
    expect(screen.queryByTestId("salmon-region-rules")).toBeNull();                // unlisted: no DFO region
    expect(screen.queryByTestId("salmon-gear-provincial")).toBeNull();
    cleanup();
    render(<SalmonCard card={salmonCard(WITH_HEADER, { sids: [101], item: "gnis:9", gear: [] }, JUL20)} palette={LIGHT} />);
    expect(screen.getByTestId("salmon-region-rules")).toBeTruthy();
    expect(screen.queryByText(/Kispiox watershed/)).toBeNull();
  });
});

describe("(T321) DFO's words on the card", () => {
  it("a water with an unparsed DFO row shows the 'wording changed' block: species, dates, DFO's words", () => {
    const facts = { ...FACTS, payload: { ...FACTS.payload,
      chains: FACTS.payload.chains.map((c, i) => (i === 1 ? { ...c, l: ["6:k"] } : c)),
      x: { "6:k": [{ k: "new", sp: "Coho", d: "Jul 15 to Aug 23", t: "4 per day, fish from shore on Sundays" },
                   { k: "note", sp: "Chinook", d: "", t: "No fishing for chinook. (see exception below)" }] } } } as SalmonFacts;
    render(<SalmonCard card={salmonCard(facts, { sids: [101], item: null }, JUL20)} palette={LIGHT} />);
    const box = screen.getByTestId("salmon-wording-changed");
    expect(box.textContent).toContain("DFO changed the wording here — read their text");
    expect(box.textContent).toContain("Coho · Jul 15 to Aug 23");
    expect(within(box).getByTestId("salmon-words-text").textContent).toBe("4 per day, fish from shore on Sundays");
    expect(screen.getByTestId("salmon-wording-notes").textContent).toContain("Chinook · All year");
    cleanup();
    render(<SalmonCard card={salmonCard(FACTS, { sids: [101], item: null }, JUL20)} palette={LIGHT} />);
    expect(screen.queryByTestId("salmon-wording-changed")).toBeNull();
  });
});

/**
 * The menu's words and VMs (core/menu.ts). Whole strings (AGENTS 33): every sentence a page draws is pinned as the
 * reader will see it.
 */
import { describe, expect, it } from "vitest";
import {
  aboutVM, agoWords, contactVM, diagnosticRows, glossaryRows, isoDayWords, mailtoUrl, menuVM, regsReportWhere,
  reportMessage, sourcesVM, whereLines, type Diagnostics,
} from "./menu";
import { FISH_CREDITS } from "./regs/fish.generated";
import type { GlossaryTerm } from "./regs/inputs";
import { MENU_WORDS } from "./menu";

const term = (id: string, t: string, says: string): GlossaryTerm => ({ id, term: t, says, pages: [80], quote: "", source: "" });
const TERMS = [
  term("single_barbless_hook", "Single barbless hook", "A hook with one point and no barb."),
  term("bait", "Bait", "Anything an angler offers a fish to eat, natural or scented."),
  term("artificial_fly", "Artificial fly", "A single barbless hook dressed with feathers, hair or thread."),
  term("éclosion", "Éclosion", "A made-up accented term."),
];

describe("the drawer", () => {
  it("lists the six pages in the reader's order, each with its hint", () => {
    expect(menuVM()).toEqual({
      title: "Menu",
      items: [
        { k: "glossary", label: "Glossary", hint: "What the regulations' words mean" },
        { k: "sources", label: "Data sources & dates", hint: "Where each answer comes from, and how fresh it is" },
        { k: "display", label: "Display", hint: "Light, dark or colour-blind colours" },
        { k: "about", label: "About", hint: "What this app is, and what it is not" },
        { k: "report", label: "Report a problem", hint: "Something wrong, missing or confusing" },
        { k: "contact", label: "Contact", hint: "Get in touch" },
      ],
    });
  });
});

describe("the glossary", () => {
  it("lists every term alphabetically, accents folded, with its own plain words", () => {
    expect(glossaryRows(TERMS).map((r) => r.term)).toEqual(["Artificial fly", "Bait", "Éclosion", "Single barbless hook"]);
    expect(glossaryRows(TERMS)[0]).toEqual({ id: "artificial_fly", term: "Artificial fly",
                                             says: "A single barbless hook dressed with feathers, hair or thread." });
  });

  it("puts a term whose NAME matches before one whose words do", () => {
    expect(glossaryRows(TERMS, "barbless").map((r) => r.id)).toEqual(["single_barbless_hook", "artificial_fly"]);
  });

  it("needs every word of the query, in any case, accents folded", () => {
    expect(glossaryRows(TERMS, "HOOK single").map((r) => r.id)).toEqual(["single_barbless_hook", "artificial_fly"]);
    expect(glossaryRows(TERMS, "hook natural").map((r) => r.id)).toEqual([]);
    expect(glossaryRows(TERMS, "eclosion").map((r) => r.id)).toEqual(["éclosion"]);
    expect(glossaryRows(TERMS, "  ").length).toBe(4);
    expect(glossaryRows(TERMS, "sturgeon")).toEqual([]);
  });
});

describe("dates", () => {
  it("writes an ISO day in words and refuses anything else", () => {
    expect(isoDayWords("2026-07-15")).toBe("Jul 15, 2026");
    expect(isoDayWords("2026-13-01")).toBeNull();
    expect(isoDayWords("20260715")).toBeNull();
    expect(isoDayWords(null)).toBeNull();
  });

  it("says how old a live value is, at each edge, in the app's one convention", () => {
    const now = Date.UTC(2026, 9, 21, 18);
    const at = (ms: number) => new Date(now - ms).toISOString();
    expect(agoWords(at(29 * 60_000), now)).toBe("just now");
    expect(agoWords(at(30 * 60_000), now)).toBe("1h ago");
    expect(agoWords(at(47 * 3_600_000), now)).toBe("47h ago");
    expect(agoWords(at(48 * 3_600_000), now)).toBe("2d ago");
    expect(agoWords("not a date", now)).toBe("unknown age");
  });
});

describe("about", () => {
  it("says what the app is, that it is not the official regulations, and every version", () => {
    expect(aboutVM({ appBuild: "1.2.0 (34)", release: "r-6960fe8201e3", tiles: "d768d6e135e62b99.c9f9604f12bf",
                     edition: "2025–2027 BC Freshwater Fishing Regulations Synopsis" }, "https://example.test/s.pdf"))
      .toEqual({
        what: "Can I Fish This? reads British Columbia's freshwater fishing regulations onto a map: tap a stream or a "
          + "lake and it says what the regulations allow there on the day you choose, alongside what the water is doing.",
        notOfficial: "This app is not the official regulations. It is built from the B.C. Freshwater Fishing "
          + "Regulations Synopsis and other public sources, and it can be wrong or out of date. The Synopsis, the "
          + "Province's in-season changes and federal notices are the law: check them before you fish.",
        official: { label: "Open the official Synopsis", url: "https://example.test/s.pdf" },
        versionsTitle: "Versions",
        versions: [
          { label: "App build", value: "1.2.0 (34)" },
          { label: "Data release", value: "r-6960fe8201e3" },
          { label: "Map tiles", value: "d768d6e135e62b99.c9f9604f12bf" },
          { label: "Regulations", value: "2025–2027 BC Freshwater Fishing Regulations Synopsis" },
        ],
      });
  });

  it("names the development bundle and says what is not stated, never inventing a version", () => {
    expect(aboutVM({ appBuild: null, release: null, tiles: null, edition: null }, "u").versions).toEqual([
      { label: "App build", value: "Not stated" },
      { label: "Data release", value: "Development bundle" },
      { label: "Map tiles", value: "Not stated" },
      { label: "Regulations", value: "Not stated" },
    ]);
  });
});

describe("data sources & dates", () => {
  const now = Date.UTC(2026, 9, 21, 18);
  it("dates each source from the release's own block and from what the app fetched", () => {
    const vm = sourcesVM({
      sources: { synopsis: { title: "2025–2027 BC Freshwater Fishing Regulations Synopsis", date: null },
                 dfo: { date: "2026-08-28" }, fwa: { date: "2026-07-15" }, basemap: { date: "2026-03-05" },
                 hydat: { date: "2026-07-17" } },
      validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", feedFetchedAt: new Date(now - 7 * 3_600_000).toISOString(), now,
    });
    expect(vm.intro).toBe("Every answer in this app comes from one of these. Each says when it was last updated.");
    expect(vm.rows).toEqual([
      { k: "synopsis", name: "2025–2027 BC Freshwater Fishing Regulations Synopsis",
        what: "The Province of British Columbia's printed regulations, read water by water",
        when: "Date not recorded in this release" },
      { k: "inseason", name: "In-season regulation changes",
        what: "The Province's changes to the Synopsis, announced during the season",
        when: "Not read by this app: check them before you fish" },
      { k: "dfo", name: "Fisheries and Oceans Canada salmon regulations",
        what: "The federal salmon limits, openings and closures for B.C.'s fresh water, by region",
        when: "Updated Aug 28, 2026" },
      { k: "fwa", name: "B.C. Freshwater Atlas",
        what: "Every stream and lake on the map, from the Province of British Columbia",
        when: "Updated Jul 15, 2026" },
      { k: "tiles", name: "Map tiles", what: "The atlas drawn for the map, with every regulation's section in it",
        when: "Version d768d6e135e62b99.c9f9604f12bf" },
      { k: "basemap", name: "Basemap", what: "Roads, towns and terrain: © OpenStreetMap contributors, Protomaps",
        when: "Updated Mar 5, 2026" },
      { k: "hydat", name: "Gauge records (HYDAT)",
        what: "Past years of flow and level, Environment and Climate Change Canada", when: "Updated Jul 17, 2026" },
      { k: "feed", name: "Live gauge readings",
        what: "Environment and Climate Change Canada, and the BC River Forecast Centre's forecasts",
        when: "Checked 7h ago" },
      { k: "weather", name: "Weather", what: "Open-Meteo.com (CC BY 4.0)", when: "Fetched when a spot is saved" },
      { k: "imagery", name: "Satellite imagery", what: "Shown only while online; never stored on this device",
        when: "Live, when online" },
    ]);
    expect(vm.footnote).toBe("The Province's in-season changes and Fisheries and Oceans Canada's notices can change a "
      + "regulation at short notice. Check them before you fish.");
  });

  it("(T43) credits every fish picture the app can show, once: the manifest's own lines (FISH_CREDITS), under \"Fish pictures\"", () => {
    const vm = sourcesVM({ sources: null, validUntil: null, tiles: null, feedFetchedAt: null, now });
    expect(vm.pictures.label).toBe("Fish pictures");
    expect(vm.pictures.lines).toBe(FISH_CREDITS);
    expect([vm.pictures.lines.length, vm.pictures.lines[0], vm.pictures.lines.at(-1)]).toEqual([39,
      "Arctic grayling — The Fishes of Alaska (1907), A. Hoen & Co. (Commons)",
      "Yellow perch — Freshwater Fisheries Society of BC (GoFishBC)"]);
  });

  it("says how long the edition is in force when the release dates no printing", () => {
    const vm = sourcesVM({ sources: { synopsis: { title: "2025–2027 Synopsis", date: null } }, validUntil: "2027-03-31",
                           tiles: null, feedFetchedAt: null, now });
    expect(vm.rows[0]).toEqual({ k: "synopsis", name: "2025–2027 Synopsis",
      what: "The Province of British Columbia's printed regulations, read water by water", when: "In force until Mar 31, 2027" });
    // a printing date, when one is recorded, is what the row says
    expect(sourcesVM({ sources: { synopsis: { title: "S", date: "2025-03-20" } }, validUntil: "2027-03-31", tiles: null,
                       feedFetchedAt: null, now }).rows[0]!.when).toBe("Updated Mar 20, 2025");
  });

  it("says not recorded where the release has no block (an older release, the dev bundle) — never today's date", () => {
    const vm = sourcesVM({ sources: null, validUntil: null, tiles: null, feedFetchedAt: null, now });
    expect(Object.fromEntries(vm.rows.map((r) => [r.k, r.when]))).toEqual({
      synopsis: "Date not recorded in this release", inseason: "Not read by this app: check them before you fish",
      dfo: "Date not recorded in this release",
      fwa: "Date not recorded in this release", tiles: "Date not recorded in this release",
      basemap: "Date not recorded in this release", hydat: "Date not recorded in this release",
      feed: "Not checked yet", weather: "Fetched when a spot is saved", imagery: "Live, when online",
    });
    expect(vm.rows[0]!.name).toBe("Freshwater Fishing Regulations Synopsis");
  });

  it("refuses a malformed date rather than printing it", () => {
    const vm = sourcesVM({ sources: { fwa: { date: "July" }, dfo: { date: "2026-02-30x" } }, validUntil: "soon",
                           tiles: null, feedFetchedAt: "yesterday", now });
    expect(vm.rows.find((r) => r.k === "fwa")!.when).toBe("Date not recorded in this release");
    expect(vm.rows.find((r) => r.k === "dfo")!.when).toBe("Date not recorded in this release");
    expect(vm.rows.find((r) => r.k === "synopsis")!.when).toBe("Date not recorded in this release");
    expect(vm.rows.find((r) => r.k === "feed")!.when).toBe("Not checked yet");
  });
});

describe("contact", () => {
  it("is a placeholder until the owner supplies one", () => {
    expect(contactVM(null)).toEqual({ kind: "none", intro: "Questions, corrections and ideas are welcome.",
      text: "Contact details are coming soon. Until then, use Report a problem to share a note from your device." });
    expect(contactVM("  ").kind).toBe("none");
    expect(contactVM("not an address").kind).toBe("none");
  });

  it("reads an email address or a web link from the one config value", () => {
    expect(contactVM("hello@example.org")).toEqual({ kind: "email", intro: "Questions, corrections and ideas are welcome.",
      label: "Email", value: "hello@example.org", url: "mailto:hello@example.org" });
    expect(contactVM("https://example.org/contact")).toEqual({ kind: "link", intro: "Questions, corrections and ideas are welcome.",
      label: "Website", value: "https://example.org/contact", url: "https://example.org/contact" });
  });
});

describe("report a problem", () => {
  const d: Diagnostics = {
    appBuild: "1.2.0 (34)", platform: "android 15", release: "r-6960fe8201e3", tiles: "d768d6e135e62b99.c9f9604f12bf",
    where: { screen: "water · regs", tab: "Gear", place: "Vedder River", water: "356308001", part: 2, section: 812345,
             day: "Tue, Oct 21" },
    errors: ["18:02 map: style failed", "18:03 regs store: fetch failed"],
  };

  it("lists the place the reader was in, one line each", () => {
    expect(whereLines(d.where)).toEqual(["Screen: water · regs", "Tab: Gear", "Water: Vedder River (356308001)",
                                         "Part: 2", "Section: 812345", "Day: Tue, Oct 21"]);
    expect(whereLines({ screen: "map" })).toEqual(["Screen: map"]);
    expect(whereLines({ part: 0 })).toEqual(["Part: 0"]);
    expect(whereLines({ part: 1, partLabel: "Mouth → Slesse Creek" })).toEqual(["Part: 1 (Mouth → Slesse Creek)"]);
    expect(whereLines({ part: null, partLabel: "Whole water" })).toEqual(["Part: (Whole water)"]);
    expect(whereLines({})).toEqual([]);
  });

  it("shows every diagnostic it would send, and drops the empty ones", () => {
    expect(diagnosticRows(d).map((r) => [r.k, r.label, r.lines.length])).toEqual([
      ["app", "App build", 1], ["platform", "Platform", 1], ["release", "Data release", 1], ["tiles", "Map tiles", 1],
      ["where", "Where you were", 6], ["errors", "Recent errors", 2]]);
    expect(diagnosticRows({ ...d, release: null, errors: [] }).map((r) => r.k)).toEqual(["app", "platform", "tiles", "where"]);
  });

  it("composes exactly what the reader wrote and the rows they left on", () => {
    expect(reportMessage("  The closure is wrong.  ", d, { errors: false, tiles: false })).toEqual({
      subject: "Can I Fish This? problem report: Vedder River · Gear",
      body: "The closure is wrong.\n\n--\nApp build: 1.2.0 (34)\nPlatform: android 15\nData release: r-6960fe8201e3\n"
        + "Where you were:\n  Screen: water · regs\n  Tab: Gear\n  Water: Vedder River (356308001)\n  Part: 2\n"
        + "  Section: 812345\n  Day: Tue, Oct 21",
    });
  });

  it("with every row off, sends the words alone under a plain subject", () => {
    const off = { app: false, platform: false, release: false, tiles: false, where: false, errors: false };
    expect(reportMessage("Typo on the About page.", d, off)).toEqual({
      subject: "Can I Fish This? problem report", body: "Typo on the About page." });
  });

  it("encodes a mailto the way mail apps read it", () => {
    expect(mailtoUrl("hello@example.org", { subject: "A & B", body: "line 1\nline 2" }))
      .toBe("mailto:hello@example.org?subject=A%20%26%20B&body=line%201%0D%0Aline%202");
  });
});

describe("report a problem, from a regulations tab", () => {
  const header = { name: "Vedder River", dayWord: "Today", date: "Oct 21",
                   place: { full: "Vedder Crossing → Fraser River", short: "→ Fraser" },
                   tabs: [{ tab: "keep", label: "Keep" }, { tab: "gear", label: "Gear" },
                          { tab: "methods", label: "Methods" }, { tab: "licence", label: "Licence" }] } as const;

  it("prefills the water, the part, the day and the tab, in the tab's own words", () => {
    expect(regsReportWhere({ target: { kind: "part", item: "356308001", partIx: 2 }, header }, "gear")).toEqual({
      screen: "water · regulations", tab: "Gear", place: "Vedder River", water: "356308001", part: 2,
      partLabel: "Vedder Crossing → Fraser River", section: null, day: "Today, Oct 21" });
  });

  it("names the section of an unnamed tap, and a tab the header does not list by its key", () => {
    expect(regsReportWhere({ target: { kind: "section", handle: 812345 }, header: { ...header, tabs: [], place: null } },
                           "licence"))
      .toEqual({ screen: "water · regulations", tab: "licence", place: "Vedder River", water: null, part: null,
                 partLabel: null, section: 812345, day: "Today, Oct 21" });
  });

  it("words the link at the foot of each tab", () => {
    expect(MENU_WORDS.report.fromRegs).toBe("Report a problem with this page");
  });
});


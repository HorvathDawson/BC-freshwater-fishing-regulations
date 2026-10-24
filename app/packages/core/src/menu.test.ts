/**
 * The menu's words and VMs (core/menu.ts). Whole strings (AGENTS 33): every sentence a page draws is pinned as the
 * reader will see it.
 */
import { describe, expect, it } from "vitest";
import {
  aboutVM, contactVM, diagnosticRows, regsLapsed, regsLapsedNote, sourceFreshness, glossaryRows, isoDayWords,
  mailtoUrl, regsReportWhere, reportMessage, sourcesVM, whereLines, type Diagnostics, type SourceDateKey, type SourcesInput,
} from "./menu";
import { FISH_CREDITS } from "./regs/fish.generated";
import { IMAGERY_ATTRIBUTION, IMAGERY_CREDIT } from "./imagery";
import { dataVM } from "./more";
import type { GlossaryTerm } from "./regs/inputs";
import { MENU_WORDS } from "./menu";
import { agoWords, STALE_AFTER_MS } from "./freshness";
import { hydatNotice, STALE_AFTER, type HydatNotice } from "./staleness";

const term = (id: string, t: string, says: string): GlossaryTerm => ({ id, term: t, says, pages: [80], quote: "", source: "" });
const TERMS = [
  term("single_barbless_hook", "Single barbless hook", "A hook with one point and no barb."),
  term("bait", "Bait", "Anything an angler offers a fish to eat, natural or scented."),
  term("artificial_fly", "Artificial fly", "A single barbless hook dressed with feathers, hair or thread."),
  term("éclosion", "Éclosion", "A made-up accented term."),
];

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
    expect(agoWords(at(20_000), now)).toBe("just now");
    expect(agoWords(at(29 * 60_000), now)).toBe("29 min ago");
    expect(agoWords(at(60 * 60_000), now)).toBe("1h ago");
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

describe("sources and credits (T167: references apart — no dates, no freshness)", () => {
  it("names every source as the More tab does, says who publishes it and on what terms, whole", () => {
    const vm = sourcesVM({ edition: "2025–2027 BC Freshwater Fishing Regulations Synopsis",
                           tiles: "d768d6e135e62b99.c9f9604f12bf" });
    expect(vm.intro).toBe("Who publishes the data this app reads, and on what terms.");
    expect(vm.rows).toEqual([
      { k: "synopsis", name: "Synopsis", from: "Province of British Columbia. The printed regulations this app reads.",
        detail: "2025–2027 BC Freshwater Fishing Regulations Synopsis" },
      { k: "inseason", name: "In-season changes", from: "Province of British Columbia. Not read by this app.", detail: null },
      { k: "dfo", name: "DFO salmon", from: "Fisheries and Oceans Canada. The federal salmon limits and closures.",
        detail: null },
      { k: "fwa", name: "Freshwater Atlas", from: "Province of British Columbia. Contains information licensed under the "
        + "Open Government Licence – British Columbia.", detail: null },
      { k: "tiles", name: "Map tiles", from: "Drawn for this app from the Freshwater Atlas and the regulations.",
        detail: "Version d768d6e135e62b99.c9f9604f12bf" },
      { k: "basemap", name: "Basemap", from: "© OpenStreetMap contributors. © Protomaps.", detail: null },
      { k: "hydat", name: "HYDAT", from: "Environment and Climate Change Canada. Historical hydrometric data, under the "
        + "Open Government Licence – Canada.", detail: null },
      { k: "feed", name: "Live gauge readings", from: "Environment and Climate Change Canada. Real-time hydrometric data, "
        + "under the Open Government Licence – Canada.", detail: null },
      { k: "forecast", name: "River forecasts",
        from: "BC River Forecast Centre. The CLEVER, COFFEE and ELF river forecasts, used with permission.", detail: null },
      { k: "weather", name: "Weather", from: "Weather data by Open-Meteo.com, CC BY 4.0.", detail: null },
      { k: "imagery", name: "Satellite imagery", from: "Powered by Esri · Source: Esri, Vantor, Earthstar Geographics, "
        + "and the GIS User Community. Shown only while online, never stored.", detail: null },
    ]);
  });

  it("carries no date and no freshness word — those are the More tab's, drawn once", () => {
    const vm = sourcesVM({ edition: null, tiles: null });
    const text = JSON.stringify(vm.rows);
    expect([/\b20\d\d-\d\d-\d\d\b|\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d+, 20\d\d\b/.test(text),
            /Up to date|[Oo]ut of date|Unknown|Newer release out/.test(text), "badge" in vm.rows[0]!, "when" in vm.rows[0]!])
      .toEqual([false, false, false, false]);
    // no edition, no tile version: nothing said, never a placeholder
    expect(vm.rows.filter((r) => r.detail !== null)).toEqual([]);
  });

  it("(T43) credits every fish picture the app can show, once: the manifest's own lines (FISH_CREDITS), under \"Fish pictures\"", () => {
    const vm = sourcesVM({ edition: null, tiles: null });
    expect(vm.pictures.label).toBe("Fish pictures");
    expect(vm.pictures.lines).toBe(FISH_CREDITS);
    expect([vm.pictures.lines.length, vm.pictures.lines[0], vm.pictures.lines.at(-1)]).toEqual([39,
      "Arctic grayling — The Fishes of Alaska (1907), A. Hoen & Co. (Commons)",
      "Yellow perch — Freshwater Fisheries Society of BC (GoFishBC)"]);
  });

  it("the imagery credit is the map's own (Esri's required words, IMAGERY_CREDIT), never a second wording", () => {
    expect(IMAGERY_CREDIT).toBe(`Satellite imagery: ${IMAGERY_ATTRIBUTION}`);
    expect(sourcesVM({ edition: null, tiles: null }).rows.find((r) => r.k === "imagery")!.from
      .startsWith(`${IMAGERY_ATTRIBUTION}.`)).toBe(true);
  });
});

describe("freshness: each source against its own cadence (owner 2026-10-22)", () => {
  const H = 3_600_000, D = 24 * H;
  const at = (k: SourceDateKey, s: Partial<SourcesInput>, now: number) =>
    `${k} ${sourceFreshness(k, { sources: null, validUntil: null, tiles: null, feedFetchedAt: null, now, ...s }).label}`;
  const now = Date.parse("2026-10-22T18:00:00Z");

  it("ONE table names every feed's rule (core staleness.ts): gauges in hours, forecasts in a day and a half, sources " +
     "in days, the regulations and the map by their release", () => {
    expect(STALE_AFTER).toEqual({
      feed: { by: "age", ms: 2 * H }, feedCheck: { by: "age", ms: 30 * 60_000 }, forecast: { by: "age", ms: 36 * H },
      dfo: { by: "age", ms: 120 * D }, fwa: { by: "age", ms: 730 * D }, basemap: { by: "age", ms: 365 * D },
      hydat: { by: "notice", ms: 548 * D },
      synopsis: { by: "release" }, tiles: { by: "release" },
      inseason: { by: "notRead" }, weather: { by: "live" }, imagery: { by: "live" },
    });
    // ONE line for "the feed is old": the Data block and the sync icon draw the line a reading's age is held to (P02)
    expect(STALE_AFTER.feed.ms).toBe(STALE_AFTER_MS);
  });

  it("the live feed: up to date through 2 h, likely out of date past it, unknown when never reached", () => {
    const fed = (ago: number) => at("feed", { feedFetchedAt: new Date(now - ago).toISOString() }, now);
    expect([fed(0), fed(2 * H), fed(2 * H + 60_000), fed(27 * H), at("feed", {}, now)]).toEqual([
      "feed Up to date", "feed Up to date", "feed Likely out of date", "feed Likely out of date", "feed Unknown"]);
  });

  it("(T68) forecasts: by their newest run's issue time, 36 h; none running is as fresh as the feed's check; the " +
     "Details page says each in its own words", () => {
    const fc = (s: Partial<SourcesInput>) => at("forecast", s, now);
    const iso = (ago: number) => new Date(now - ago).toISOString();
    expect([fc({ forecastIssuedAt: iso(36 * H) }), fc({ forecastIssuedAt: iso(36 * H + 60_000) }),
            fc({ forecastIssuedAt: null, feedFetchedAt: iso(H) }), fc({ forecastIssuedAt: null, feedFetchedAt: iso(3 * H) }),
            fc({ forecastIssuedAt: undefined, feedFetchedAt: iso(H) }), fc({})]).toEqual([
      "forecast Up to date", "forecast Likely out of date", "forecast Up to date", "forecast Likely out of date",
      "forecast Unknown", "forecast Unknown"]);
    const row = (s: Partial<SourcesInput>) => dataVM({ sources: null, validUntil: null, tiles: null, feedFetchedAt: null,
                                                      now, newRelease: null, ...s }).rows.find((r) => r.k === "forecast")!.when;
    expect([row({ forecastIssuedAt: iso(5 * H), feedFetchedAt: iso(H) }), row({ forecastIssuedAt: null, feedFetchedAt: iso(H) }),
            row({ feedFetchedAt: iso(H) }), row({})]).toEqual([
      "Issued 5h ago", "None running now", "No date recorded", "Not checked yet"]);
  });

  it("each release-dated source at its edge: the day inside, the day past", () => {
    // at midnight UTC, so a date N days back is exactly N days old
    const midnight = Date.parse("2026-10-22T00:00:00Z");
    const day = (k: "dfo" | "fwa" | "basemap" | "hydat", days: number) =>
      at(k, { sources: { [k]: { date: new Date(midnight - days * D).toISOString().slice(0, 10) } } }, midnight);
    expect([day("dfo", 120), day("dfo", 121), day("fwa", 730), day("fwa", 731), day("basemap", 365),
            day("basemap", 366), day("hydat", 548), day("hydat", 549)]).toEqual([
      "dfo Up to date", "dfo Likely out of date", "fwa Up to date", "fwa Likely out of date",
      "basemap Up to date", "basemap Likely out of date", "hydat Up to date", "hydat Likely out of date"]);
  });

  describe("HYDAT: the feed's maintenance notice decides, its age only when no notice word reaches the app " +
           "(owner 2026-10-22 late)", () => {
    const DUE = { kind: "hydat", since: "2026-10-22T21:00:00Z", current: "20260717", latest: "20261020",
                  action: "scripts/update-hydat.sh" };
    const ROSTER = { kind: "roster", since: "2026-10-22T21:00:00Z", current: "data/source/bc_hydrometric_stations.json",
                     latest: "+08NM139", action: "rm …" };
    const CHECKED = { release: "20260717", latest: "20260717", stale: false };

    it("reads the index: a hydat notice is due; a checked run with none is clear AS OF the newest HYDAT listed (an " +
       "ISO day); an older index or an unchecked run says nothing", () => {
      expect([
        hydatNotice(null),
        hydatNotice(undefined),
        // an index from before `maintenance`: the age rule
        hydatNotice({ hydat: { release: "20250101", latest: "20260717", stale: true } }),
        hydatNotice({ maintenance: [DUE], hydat: { release: "20260717", latest: "20261020", stale: true } }),
        // the listing failed this run, but the publisher kept the notice: still due
        hydatNotice({ maintenance: [DUE], hydat: { release: "20260717", latest: null, stale: false } }),
        hydatNotice({ maintenance: [ROSTER], hydat: CHECKED }),
        hydatNotice({ maintenance: [], hydat: CHECKED }),
        // the listing could not be read and nothing was due before: not checked, the age rule
        hydatNotice({ maintenance: [], hydat: { release: "20260717", latest: null, stale: false } }),
        // no envelope in use: nothing to compare
        hydatNotice({ maintenance: [], hydat: { release: null, latest: "20260717", stale: false } }),
        // a `latest` that is no day: no word
        hydatNotice({ maintenance: [], hydat: { release: "20260717", latest: "July", stale: false } }),
        hydatNotice({ maintenance: [] }),
      ]).toEqual([null, null, null, "due", "due", { current: "2026-07-17" }, { current: "2026-07-17" }, null, null,
                  null, null]);
    });

    it("current ONLY when the feed is clear AND the app's release carries that same HYDAT; clear but the release " +
       "older: the app's data has not caught up; due: newer release out; no word or no release date: the age line",
       () => {
      const midnight = Date.parse("2026-10-22T00:00:00Z");
      const day = (days: number) => new Date(midnight - days * D).toISOString().slice(0, 10);
      const one = (date: string | null, n: HydatNotice | null | undefined) =>
        sourceFreshness("hydat", { validUntil: null, tiles: null, feedFetchedAt: null, now: midnight,
                                   sources: date === null ? {} : { hydat: { date } }, hydatNotice: n });
      expect([
        one(day(10), "due"),
        one(day(1000), "due"),
        // the feed is clear and the release measured against the same HYDAT: up to date, however old
        one(day(10), { current: day(10) }),
        one(day(1000), { current: day(1000) }),
        // MUTATION PIN: the feed caught up (owner ran update-hydat.sh) but the data release has not shipped yet
        one(day(10), { current: day(5) }),
        one(day(1000), { current: day(5) }),
        // clear, but the release states no HYDAT date: the age line (no age: unknown)
        one(null, { current: day(5) }),
        // no word (an old index): the age line
        one(day(10), null),
        one(day(548), undefined),
        one(day(549), undefined),
      ]).toEqual([
        { kind: "stale", label: "Newer release out" },
        { kind: "stale", label: "Newer release out" },
        { kind: "fresh", label: "Up to date" },
        { kind: "fresh", label: "Up to date" },
        { kind: "stale", label: "Newer release out" },
        { kind: "stale", label: "Newer release out" },
        { kind: "unknown", label: "Unknown" },
        { kind: "fresh", label: "Up to date" },
        { kind: "fresh", label: "Up to date" },
        { kind: "stale", label: "Likely out of date" },
      ]);
    });

    it("the More tab's HYDAT row, whole: the release's date, and the notice's state", () => {
      const row = (n: HydatNotice | null) => dataVM({
        sources: { hydat: { date: "2026-07-17" } }, validUntil: null, tiles: null, feedFetchedAt: null, now,
        hydatNotice: n, newRelease: null,
      }).rows.find((r) => r.k === "hydat")!;
      expect([row("due"), row({ current: "2026-07-17" }), row({ current: "2026-10-20" }), row(null)]
        .map((r) => `${r.shown} | ${r.label}`)).toEqual([
        "Jul 17, 2026 · Newer release out | HYDAT: Jul 17, 2026. Newer release out.",
        "Jul 17, 2026 | HYDAT: Jul 17, 2026. Up to date.",
        "Jul 17, 2026 · Newer release out | HYDAT: Jul 17, 2026. Newer release out.",
        "Jul 17, 2026 | HYDAT: Jul 17, 2026. Up to date.",
      ]);
    });

    it("a newer DATA release does not touch the HYDAT line: it is not release-ruled", () => {
      expect(sourceFreshness("hydat", { sources: { hydat: { date: "2026-07-17" } }, validUntil: null, tiles: null,
                                        feedFetchedAt: null, now, newRelease: "r-new",
                                        hydatNotice: { current: "2026-07-17" } }))
        .toEqual({ kind: "fresh", label: "Up to date" });
    });
  });

  it("the regulations: in force THROUGH their last day on the reader's calendar, OUT of date the day after, unknown " +
     "with no last day — and likely out of date once a newer release is known", () => {
    const last = "2027-03-31";
    // local times: the release's last day is a B.C. calendar day, not a UTC one (UTC midnight is 5 pm on Mar 31 here)
    expect([at("synopsis", { validUntil: last }, new Date(2027, 2, 31, 23, 59).getTime()),
            at("synopsis", { validUntil: last }, new Date(2027, 3, 1, 0, 0).getTime()),
            at("synopsis", { validUntil: null }, now),
            at("synopsis", { validUntil: "soon" }, now),
            at("synopsis", { validUntil: last, newRelease: "r-new" }, now),
            at("synopsis", { validUntil: null, newRelease: "r-new" }, now)])
      .toEqual(["synopsis Up to date", "synopsis Out of date", "synopsis Unknown", "synopsis Unknown",
                "synopsis Likely out of date", "synopsis Likely out of date"]);
  });

  it("ONE lapse rule (regsLapsed) and its one sentence (regsLapsedNote): none without a stamp or before the day after", () => {
    const last = "2027-03-31";
    const eve = new Date(2027, 2, 31, 23, 59).getTime(), next = new Date(2027, 3, 1, 0, 0).getTime();
    expect([regsLapsed(last, eve), regsLapsed(last, next), regsLapsed(null, next), regsLapsed(undefined, next),
            regsLapsed("2027-3-31", next)]).toEqual([false, true, false, false, false]);
    expect([regsLapsedNote(last, eve), regsLapsedNote(last, next), regsLapsedNote(null, next)]).toEqual([
      null,
      "Out of date: these regulations were valid to Mar 31, 2027. Check the current Synopsis and the Province's " +
        "notices before you fish.",
      null]);
  });

  it("the map tiles by the proven pair and the release; the in-season changes not read; weather and imagery live", () => {
    expect([at("tiles", { tilesPaired: true }, now), at("tiles", { tilesPaired: false }, now),
            at("tiles", { tilesPaired: null }, now), at("tiles", { tilesPaired: true, newRelease: "r-new" }, now),
            at("inseason", {}, now), at("weather", {}, now), at("imagery", {}, now)])
      .toEqual(["tiles Up to date", "tiles Likely out of date", "tiles Unknown", "tiles Likely out of date",
                "inseason Not read", "weather Live", "imagery Live"]);
  });

  it("MUTATION PIN: the gauges and the regulations do not share one line — a feed past its hours is stale while the " +
     "regulations and the map, months into their release, are not; a newer release dates them while the feed is fresh", () => {
    expect([STALE_AFTER.feed.by, STALE_AFTER.synopsis.by, STALE_AFTER.tiles.by]).toEqual(["age", "release", "release"]);
    expect(STALE_AFTER.forecast.ms).not.toBe(STALE_AFTER.feed.ms);
    // the printing 19 months old, the edition still in force, the pair proven: only the gauge feed's 2 h has passed
    const release = { validUntil: "2027-03-31", tilesPaired: true, sources: { synopsis: { date: "2025-03-20" } } };
    const gaugeOld = { ...release, feedFetchedAt: new Date(now - 2 * H - 60_000).toISOString() };
    expect([at("feed", gaugeOld, now), at("synopsis", gaugeOld, now), at("tiles", gaugeOld, now)])
      .toEqual(["feed Likely out of date", "synopsis Up to date", "tiles Up to date"]);
    const releaseNew = { ...release, newRelease: "r-new", feedFetchedAt: new Date(now - 60_000).toISOString() };
    expect([at("feed", releaseNew, now), at("synopsis", releaseNew, now), at("tiles", releaseNew, now)])
      .toEqual(["feed Up to date", "synopsis Likely out of date", "tiles Likely out of date"]);
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

  it("names no tab for the page-foot link: the whole page is the subject (T41 Q3 review: the foot is only reached on Licence)", () => {
    const w = regsReportWhere({ target: { kind: "part", item: "356308001", partIx: 2 }, header }, null);
    expect(w).toEqual({ screen: "water · regulations", tab: null, place: "Vedder River", water: "356308001", part: 2,
                        partLabel: "Vedder Crossing → Fraser River", section: null, day: "Today, Oct 21" });
    expect(whereLines(w).some((l) => l.startsWith("Tab:"))).toBe(false);
    const d = { appBuild: null, platform: null, release: null, tiles: null, where: w, errors: [] };
    expect(reportMessage("x", d, {}).subject).toBe("Can I Fish This? problem report: Vedder River");
  });

  it("words the link at the foot of the Regulations page", () => {
    expect(MENU_WORDS.report.fromRegs).toBe("Report a problem with this page");
  });
});


/**
 * The More tab and the sync icon (more.ts), whole strings (AGENTS 33): the Data block every reader sees at the top of
 * More — (T167) every source the app reads, one row each, and the in-season warning — its summary badge, the tab's
 * sections, and the sync icon's three states as said aloud.
 */
import { describe, expect, it } from "vitest";
import { DATA_ROWS, dataVM, menuVM, syncVM, type DataInput } from "./more";
import { SOURCE_ORDER } from "./menu";
import { STALE_AFTER } from "./staleness";

const NOW = Date.parse("2026-10-22T18:00:00Z");
const H = 3_600_000;
const LIVE: DataInput = {
  // the live release today: an edition title and no last day; the atlas, basemap and HYDAT dated
  sources: { synopsis: { title: "2025–2027 BC Freshwater Fishing Regulations Synopsis", date: null },
             dfo: { date: "2026-08-28" }, fwa: { date: "2026-07-15" }, basemap: { date: "2026-03-05" },
             hydat: { date: "2026-07-17" } },
  validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
  feedFetchedAt: new Date(NOW - 27 * H).toISOString(), now: NOW, newRelease: null,
  forecastIssuedAt: new Date(NOW - 9 * H).toISOString(),
};
/** a row as drawn (name | right-hand text) and as said aloud */
const rows = (d: DataInput) => dataVM(d).rows.map((r) => `${r.name} | ${r.shown}`);
const row = (d: Partial<DataInput>, k: string) => dataVM({ ...LIVE, ...d }).rows.find((r) => r.k === k)!;
const sum = (d: DataInput) => { const v = dataVM(d); return `${v.summary.kind}: ${v.summary.label}`; };

describe("the Data block: always on More, compact — every source, one row each (T167)", () => {
  it("every source the app reads, in the owner's names and order: the date or age, the state in words only when " +
     "stale", () => {
    expect(rows(LIVE)).toEqual([
      "Synopsis | 2025–2027 edition",
      "DFO salmon | Aug 28, 2026",
      "Freshwater Atlas | Jul 15, 2026",
      "Map tiles | Matches the data",
      "Basemap | Mar 5, 2026",
      "HYDAT | Jul 17, 2026",
      "Live gauge readings | 27h ago · Likely out of date",
      "River forecasts | Issued 9h ago",
      "Weather | Per saved spot",
      "Satellite imagery | Online only",
    ]);
  });

  it("every row is said aloud whole: name, date, state — the state the dot draws", () => {
    expect(dataVM(LIVE).rows.map((r) => r.label)).toEqual([
      "Synopsis: 2025–2027 edition. Unknown.",
      "DFO salmon: Aug 28, 2026. Up to date.",
      "Freshwater Atlas: Jul 15, 2026. Up to date.",
      "Map tiles: Matches the data. Up to date.",
      "Basemap: Mar 5, 2026. Up to date.",
      "HYDAT: Jul 17, 2026. Up to date.",
      "Live gauge readings: 27h ago. Likely out of date.",
      "River forecasts: Issued 9h ago. Up to date.",
      "Weather: Per saved spot. Live.",
      "Satellite imagery: Online only. Live.",
    ]);
  });

  it("the rows are the app's ONE source list (SOURCE_ORDER) less the in-season changes, which are the warning", () => {
    expect([...DATA_ROWS]).toEqual(SOURCE_ORDER.filter((k) => k !== "inseason"));
    expect(dataVM(LIVE).rows.map((r) => r.k)).toEqual([...DATA_ROWS]);
  });

  it("the in-season changes: never a row, always the warning, with the Province's page", () => {
    expect(dataVM(LIVE).inseason).toEqual({
      title: "In-season changes are not read",
      text: "The Province can change a regulation during the season. Check its notices before you fish.",
      link: { label: "Province's in-season changes",
              url: "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations#In-Season-changes" },
    });
  });

  it("the map tiles row gives the tiles' state, never their hash (the version is the Sources page's)", () => {
    expect([row({ tilesPaired: true }, "tiles"), row({ tilesPaired: false }, "tiles"), row({ tilesPaired: null }, "tiles")]
      .map((r) => `${r.shown} / ${r.label}`)).toEqual([
      "Matches the data / Map tiles: Matches the data. Up to date.",
      "Out of step with the data · Likely out of date / Map tiles: Out of step with the data. Likely out of date.",
      "Not proven / Map tiles: Not proven. Unknown.",
    ]);
    expect(dataVM(LIVE).rows.some((r) => r.shown.includes("d768d6e1") || r.label.includes("d768d6e1"))).toBe(false);
  });

  it("the synopsis row: the last day in force, else the printing, else the edition's years, else not recorded", () => {
    const syn = (d: Partial<DataInput>) => row(d, "synopsis").shown;
    expect([
      syn({ validUntil: "2027-03-31" }),
      syn({ sources: { synopsis: { title: "S", date: "2025-03-20" } } }),
      syn({ sources: null }),
    ]).toEqual(["Valid to Mar 31, 2027", "Printed Mar 20, 2025", "No date recorded"]);
  });

  it("a dated source with no date recorded, or one that is not a day, says so — never a guess", () => {
    expect([row({ sources: { ...LIVE.sources, dfo: { date: null } } }, "dfo"),
            row({ sources: { ...LIVE.sources, fwa: { date: "July" } } }, "fwa")].map((r) => r.label))
      .toEqual(["DFO salmon: No date recorded. Unknown.", "Freshwater Atlas: No date recorded. Unknown."]);
  });

  it("past the last day the synopsis row says OUT of date (not 'likely') and the block leads with it", () => {
    // the reader's own calendar: the evening of the last day is still in force, the next morning is not
    const at = (now: number) => {
      // every other source dated the same week, so the synopsis row is the only one that can go stale
      const day = { date: new Date(now - 7 * 24 * H).toISOString().slice(0, 10) };
      const sources = { synopsis: LIVE.sources!.synopsis, dfo: day, fwa: day, basemap: day, hydat: day };
      const v = dataVM({ ...LIVE, sources, validUntil: "2027-03-31", feedFetchedAt: new Date(now - H).toISOString(),
                         forecastIssuedAt: new Date(now - H).toISOString(), now });
      return `${v.rows[0]!.name} | ${v.rows[0]!.shown} | ${v.summary.label}`;
    };
    expect([at(new Date(2027, 2, 31, 23, 59).getTime()), at(new Date(2027, 3, 1, 0, 1).getTime())]).toEqual([
      "Synopsis | Valid to Mar 31, 2027 | All up to date",
      "Synopsis | Valid to Mar 31, 2027 · Out of date | Regulations out of date",
    ]);
  });

  it("HYDAT follows the feed's notice: due is a newer release, current never by age; the sync icon never hears of it", () => {
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31", feedFetchedAt: new Date(NOW - H).toISOString() };
    const old = { ...fresh.sources, hydat: { date: "2023-01-01" } };
    const hy = (d: DataInput) => `${dataVM(d).rows.find((r) => r.k === "hydat")!.shown} | ${sum(d)}`;
    expect([
      hy({ ...fresh, hydatNotice: "due" }),
      hy({ ...fresh, hydatNotice: { current: "2023-01-01" }, sources: old }),
      // the feed is clear on a newer HYDAT than the release carries: the app's data has not caught up
      hy({ ...fresh, hydatNotice: { current: "2026-07-17" }, sources: old }),
      hy({ ...fresh, hydatNotice: null, sources: old }),
    ]).toEqual([
      "Jul 17, 2026 · Newer release out | stale: 1 likely out of date",
      "Jan 1, 2023 | fresh: All up to date",
      "Jan 1, 2023 · Newer release out | stale: 1 likely out of date",
      "Jan 1, 2023 · Likely out of date | stale: 1 likely out of date",
    ]);
    // the notice adds no "newer data" note (that is the DATA release)
    expect(dataVM({ ...fresh, hydatNotice: "due" }).newRelease).toBeNull();
  });

  it("a lapsed edition leads the summary (never folded into a 'likely' count); other stale rows follow it", () => {
    const now = new Date(2027, 3, 1, 9).getTime();
    const day = { date: new Date(now - 7 * 24 * H).toISOString().slice(0, 10) };
    const sources = { synopsis: LIVE.sources!.synopsis, dfo: day, fwa: day, basemap: day, hydat: day };
    const base: DataInput = { ...LIVE, sources, validUntil: "2027-03-31", feedFetchedAt: new Date(now - H).toISOString(),
                              forecastIssuedAt: new Date(now - H).toISOString(), now };
    expect([
      sum(base),
      sum({ ...base, tilesPaired: false }),
      sum({ ...base, tilesPaired: false, sources: { ...sources, dfo: { date: "2026-01-01" } } }),
      sum({ ...base, newRelease: "r-new" }),
    ]).toEqual([
      "stale: Regulations out of date",
      "stale: Regulations out of date · 1 likely out of date",
      "stale: Regulations out of date · 2 likely out of date",
      "stale: Newer data available",
    ]);
  });

  it("a newer release known: the synopsis and the map tiles are likely out of date; the flows keep their own hours", () => {
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31", feedFetchedAt: new Date(NOW - H).toISOString() };
    expect(rows({ ...fresh, newRelease: "r-new" })).toEqual([
      "Synopsis | Valid to Mar 31, 2027 · Likely out of date",
      "DFO salmon | Aug 28, 2026",
      "Freshwater Atlas | Jul 15, 2026",
      "Map tiles | Matches the data · Likely out of date",
      "Basemap | Mar 5, 2026",
      "HYDAT | Jul 17, 2026",
      "Live gauge readings | 1h ago",
      "River forecasts | Issued 9h ago",
      "Weather | Per saved spot",
      "Satellite imagery | Online only",
    ]);
  });

  it("the live gauge readings at their 2 h edge, whole: a minute inside, a minute past", () => {
    const flows = (ago: number) => row({ feedFetchedAt: new Date(NOW - ago).toISOString() }, "feed");
    expect([flows(2 * H - 60_000), flows(2 * H + 60_000)].map((r) => r.label))
      .toEqual(["Live gauge readings: 2h ago. Up to date.", "Live gauge readings: 2h ago. Likely out of date."]);
    expect(STALE_AFTER.feed).toMatchObject({ by: "age" });
  });

  it("(T68) the river forecasts row, from the feed index's newest run: whole, at its 36 h edge, none running, no word", () => {
    const fc = (d: Partial<DataInput>) => row(d, "forecast").label;
    expect([
      fc({ forecastIssuedAt: new Date(NOW - 36 * H + 60_000).toISOString() }),
      fc({ forecastIssuedAt: new Date(NOW - 36 * H - 60_000).toISOString() }),
      // the index says no run is held (out of season): the check is as fresh as the feed that made it
      fc({ forecastIssuedAt: null, feedFetchedAt: new Date(NOW - H).toISOString() }),
      fc({ forecastIssuedAt: null }),
      // an index from before T68 says nothing about forecasts; a feed not reached says nothing at all
      fc({ forecastIssuedAt: undefined }),
      fc({ forecastIssuedAt: undefined, feedFetchedAt: null }),
    ]).toEqual([
      "River forecasts: Issued 36h ago. Up to date.",
      "River forecasts: Issued 36h ago. Likely out of date.",
      "River forecasts: None running now. Up to date.",
      "River forecasts: None running now. Likely out of date.",
      "River forecasts: No date recorded. Unknown.",
      "River forecasts: Not checked yet. Unknown.",
    ]);
  });

  it("(T68 mutation pin) forecasts keep their own day and a half: a 3 h old run is up to date where 3 h old flows " +
     "are not, and an old run counts in the block's summary", () => {
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31" };
    const v = dataVM({ ...fresh, feedFetchedAt: new Date(NOW - 3 * H).toISOString(),
                       forecastIssuedAt: new Date(NOW - 3 * H).toISOString() });
    expect(v.rows.filter((r) => r.k === "feed" || r.k === "forecast").map((r) => `${r.k} ${r.badge.label}`))
      .toEqual(["feed Likely out of date", "forecast Up to date"]);
    expect(sum({ ...fresh, feedFetchedAt: new Date(NOW - H).toISOString(),
                 forecastIssuedAt: new Date(NOW - 40 * H).toISOString() })).toBe("stale: 1 likely out of date");
  });

  it("ONE summary badge: a newer release, else how many rows are stale (every source counts), else unknown, else fresh", () => {
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31", feedFetchedAt: new Date(NOW - H).toISOString() };
    expect([
      sum({ ...LIVE, newRelease: "r-new" }),
      sum({ ...LIVE, tilesPaired: false }),
      sum(LIVE),
      // DFO past its cadence: a row like any other, so it counts (there is no second list to send the reader to)
      sum({ ...fresh, sources: { ...fresh.sources, dfo: { date: "2026-01-01" } } }),
      sum({ ...fresh, validUntil: null }),
      sum({ ...fresh, sources: { ...fresh.sources, fwa: undefined } }),
      sum(fresh),
    ]).toEqual([
      "stale: Newer data available",
      "stale: 2 likely out of date",
      "stale: 1 likely out of date",
      "stale: 1 likely out of date",
      "unknown: Some dates not recorded",
      "unknown: Some dates not recorded",
      "fresh: All up to date",
    ]);
    // the summary's count is the number of stale dots drawn, whatever the mix
    const odd: DataInput = { ...LIVE, tilesPaired: false, sources: { ...LIVE.sources, dfo: { date: "2026-01-01" } } };
    expect([sum(odd), dataVM(odd).rows.filter((r) => r.badge.kind === "stale").length])
      .toEqual(["stale: 3 likely out of date", 3]);
    expect([dataVM({ ...LIVE, newRelease: "r-new" }).newRelease, dataVM(LIVE).newRelease])
      .toEqual(["A newer data release is available. Reopen the app to use it.", null]);
  });
});

describe("the More tab, top to bottom", () => {
  it("back to the previous tab, the Data block, Display, the pages as rows (Sources and credits once), Contact inline", () => {
    const vm = menuVM({ data: LIVE, back: "Conditions", contact: null });
    expect({
      title: vm.title, back: vm.back, data: vm.data.title, display: vm.display, rows: vm.rows, contact: vm.contact,
    }).toEqual({
      title: "More",
      back: { to: "Conditions", label: "Back to Conditions" },
      data: "Data",
      display: { title: "Display", palette: "Colours",
                 note: "The colour-blind palette changes the hues and nothing else. It has a light ground only.",
                 options: [{ k: "light", t: "Light" }, { k: "dark", t: "Dark" }, { k: "cvd", t: "Colour-blind" }] },
      rows: [
        { k: "glossary", label: "Glossary", hint: "What the regulations' words mean" },
        { k: "sources", label: "Sources and credits", hint: "Who publishes the data, and on what terms" },
        { k: "about", label: "About", hint: "What this app is, and what it is not" },
        { k: "report", label: "Report a problem", hint: "Something wrong, missing or confusing" },
      ],
      contact: { title: "Contact", vm: { kind: "none", intro: "Questions, corrections and ideas are welcome.",
        text: "Contact details are coming soon. Until then, use Report a problem to share a note from your device." } },
    });
  });
});

describe("the sync icon: fresh, needs a refresh, checking — said aloud", () => {
  const sync = (s: Partial<Parameters<typeof syncVM>[0]>) => {
    const v = syncVM({ feed: true, feedFetchedAt: new Date(NOW - H).toISOString(), checkedAt: NOW - 60_000,
                       checking: false, newRelease: null, now: NOW, ...s });
    return `${v.kind} | ${v.label}`;
  };
  it("the dot means a tap or a reopen fixes it: this device's check past the feed's 30-minute cadence, the feed not reached, a newer release", () => {
    expect(STALE_AFTER.feedCheck).toEqual({ by: "age", ms: 30 * 60_000 });
    expect([
      sync({}),
      sync({ checking: true }),
      sync({ newRelease: "r-new" }),
      sync({ checkedAt: NOW - 30 * 60_000 }),
      sync({ checkedAt: NOW - 31 * 60_000 }),
      sync({ checkedAt: NOW - 5 * H }),
      sync({ checkedAt: null, feedFetchedAt: null }),
      sync({ feed: false, feedFetchedAt: null, checkedAt: null }),
    ]).toEqual([
      "fresh | Refresh data: up to date",
      "checking | Refresh data: checking now",
      "stale | Refresh data: a newer data release is available; reopen the app to use it",
      "fresh | Refresh data: up to date",
      "stale | Refresh data: last checked on this device 31 min ago",
      "stale | Refresh data: last checked on this device 5h ago",
      "stale | Refresh data: river flows could not be reached",
      "fresh | Refresh data: up to date",
    ]);
    expect(syncVM({ feed: true, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: null, now: NOW }).hint)
      .toBe("Checks the river flows, the salmon and in-season feeds and the data release again");
  });

  it("a feed the SERVER has not published lately is no refresh prompt (a tap cannot fix it): no dot, said quietly", () => {
    // the live release today: the feed 27h old on the server, this device checked a minute ago
    expect([
      sync({ feedFetchedAt: new Date(NOW - 27 * H).toISOString() }),
      sync({ feedFetchedAt: new Date(NOW - 2 * H).toISOString() }),
      sync({ feedFetchedAt: new Date(NOW - 27 * H).toISOString(), checkedAt: NOW - 2 * H }),
    ]).toEqual([
      "fresh | Refresh data: nothing newer to fetch; river flows last published 27h ago",
      "fresh | Refresh data: up to date",
      "stale | Refresh data: last checked on this device 2h ago",
    ]);
  });
});

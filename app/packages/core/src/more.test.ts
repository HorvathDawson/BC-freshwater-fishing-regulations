/**
 * The More tab and the sync icon (more.ts), whole strings (AGENTS 33): the Data block every reader sees at the top of
 * More, its summary badge, the tab's sections, and the sync icon's three states as said aloud.
 */
import { describe, expect, it } from "vitest";
import { dataVM, menuVM, syncVM, type DataInput } from "./more";
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
};
const lines = (d: DataInput) => dataVM(d).lines.map((l) => `${l.name} · ${l.when} · ${l.badge.label}`);

describe("the Data block: always at the top of More, one line per source", () => {
  it("each line names the source, its date in few words and its freshness — the owner's seven, in order", () => {
    expect(lines(LIVE)).toEqual([
      "Regulations · 2025–2027 edition · Unknown",
      "In-season changes · Check the Province's notices · Not read",
      "River flows · Checked 27h ago · Likely out of date",
      "Weather · With each saved spot · Live",
      "Map · Matches the data · Up to date",
      "Basemap · Updated Mar 5, 2026 · Up to date",
      "Satellite imagery · Online only · Live",
    ]);
  });

  it("the regulations line: the last day in force, else the printing, else the edition's years, else not recorded", () => {
    const syn = (d: Partial<DataInput>) => dataVM({ ...LIVE, ...d }).lines[0]!.when;
    expect([
      syn({ validUntil: "2027-03-31" }),
      syn({ sources: { synopsis: { title: "S", date: "2025-03-20" } } }),
      syn({ sources: null }),
    ]).toEqual(["Valid to Mar 31, 2027", "Printed Mar 20, 2025", "Date not recorded"]);
  });

  it("past the last day the regulations line says OUT of date (not 'likely') and the block counts it", () => {
    // the reader's own calendar: the evening of the last day is still in force, the next morning is not
    const at = (now: number) => {
      // every other source dated the same week, so the regulations line is the only one that can go stale
      const day = { date: new Date(now - 7 * 24 * H).toISOString().slice(0, 10) };
      const sources = { synopsis: LIVE.sources!.synopsis, dfo: day, fwa: day, basemap: day, hydat: day };
      const v = dataVM({ ...LIVE, sources, validUntil: "2027-03-31", feedFetchedAt: new Date(now - H).toISOString(), now });
      return `${v.lines[0]!.name} · ${v.lines[0]!.when} · ${v.lines[0]!.badge.kind} ${v.lines[0]!.badge.label} | ${v.summary.label}`;
    };
    expect([at(new Date(2027, 2, 31, 23, 59).getTime()), at(new Date(2027, 3, 1, 0, 1).getTime())]).toEqual([
      "Regulations · Valid to Mar 31, 2027 · fresh Up to date | All up to date",
      "Regulations · Valid to Mar 31, 2027 · stale Out of date | Regulations out of date",
    ]);
  });

  it("HYDAT (on Details alone) follows the feed's notice: due counts as out of date, current never by age; the " +
     "sync icon never hears of it", () => {
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31", feedFetchedAt: new Date(NOW - H).toISOString() };
    const old = { ...fresh.sources, hydat: { date: "2023-01-01" } };
    const sum = (d: DataInput) => { const v = dataVM(d); return `${v.summary.kind}: ${v.summary.label}`; };
    expect([
      sum({ ...fresh, hydatNotice: "due" }),
      sum({ ...fresh, hydatNotice: { current: "2023-01-01" }, sources: old }),
      // the feed is clear on a newer HYDAT than the release carries: the app's data has not caught up
      sum({ ...fresh, hydatNotice: { current: "2026-07-17" }, sources: old }),
      sum({ ...fresh, hydatNotice: null, sources: old }),
      sum({ ...fresh, sources: old }),
    ]).toEqual([
      "stale: 1 likely out of date · see Details",
      "fresh: All up to date",
      "stale: 1 likely out of date · see Details",
      "stale: 1 likely out of date · see Details",
      "stale: 1 likely out of date · see Details",
    ]);
    // the block draws no HYDAT line; the notice adds no line and no "newer data" note (that is the DATA release)
    expect(lines({ ...fresh, hydatNotice: "due" })).toEqual(lines(fresh));
    expect(dataVM({ ...fresh, hydatNotice: "due" }).newRelease).toBeNull();
  });

  it("a lapsed edition leads the summary (never folded into a 'likely' count); other stale lines follow it", () => {
    const now = new Date(2027, 3, 1, 9).getTime();
    const day = { date: new Date(now - 7 * 24 * H).toISOString().slice(0, 10) };
    const sources = { synopsis: LIVE.sources!.synopsis, dfo: day, fwa: day, basemap: day, hydat: day };
    const base: DataInput = { ...LIVE, sources, validUntil: "2027-03-31", feedFetchedAt: new Date(now - H).toISOString(), now };
    const sum = (d: DataInput) => { const v = dataVM(d); return `${v.summary.kind}: ${v.summary.label}`; };
    expect([
      sum(base),
      sum({ ...base, tilesPaired: false }),
      sum({ ...base, sources: { ...sources, dfo: { date: "2026-01-01" } } }),
      sum({ ...base, newRelease: "r-new" }),
    ]).toEqual([
      "stale: Regulations out of date",
      "stale: Regulations out of date · 1 likely out of date",
      "stale: Regulations out of date · 1 more in Details",
      "stale: Newer data available",
    ]);
  });

  it("a newer release known: the regulations and the map lines are likely out of date; the flows keep their own hours", () => {
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31", feedFetchedAt: new Date(NOW - H).toISOString() };
    expect(lines({ ...fresh, newRelease: "r-new" })).toEqual([
      "Regulations · Valid to Mar 31, 2027 · Likely out of date",
      "In-season changes · Check the Province's notices · Not read",
      "River flows · Checked 1h ago · Up to date",
      "Weather · With each saved spot · Live",
      "Map · Matches the data · Likely out of date",
      "Basemap · Updated Mar 5, 2026 · Up to date",
      "Satellite imagery · Online only · Live",
    ]);
  });

  it("the River flows line at its 2 h edge, whole: a minute inside, a minute past", () => {
    const flows = (ago: number) => dataVM({ ...LIVE, feedFetchedAt: new Date(NOW - ago).toISOString() })
      .lines.find((l) => l.k === "feed")!;
    expect([flows(2 * H - 60_000), flows(2 * H + 60_000)].map((l) => `${l.name} · ${l.when} · ${l.badge.label}`))
      .toEqual(["River flows · Checked 2h ago · Up to date", "River flows · Checked 2h ago · Likely out of date"]);
  });

  it("the map line by the proven pair; the flows line before the feed answers", () => {
    const at = (d: Partial<DataInput>, k: string) => dataVM({ ...LIVE, ...d }).lines.find((l) => l.k === k)!;
    expect([at({ tilesPaired: false }, "tiles"), at({ tilesPaired: null }, "tiles"), at({ feedFetchedAt: null }, "feed")]
      .map((l) => `${l.when} · ${l.badge.label}`))
      .toEqual(["Out of step with the data · Likely out of date", "Not proven · Unknown", "Not checked yet · Unknown"]);
  });

  it("ONE summary badge for the block: a newer release, else how many are stale, else unknown, else all up to date", () => {
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31", feedFetchedAt: new Date(NOW - H).toISOString() };
    const sum = (d: DataInput) => { const v = dataVM(d); return `${v.summary.kind}: ${v.summary.label}`; };
    expect([
      sum({ ...LIVE, newRelease: "r-new" }),
      sum({ ...LIVE, tilesPaired: false }),
      sum(LIVE),
      sum({ ...fresh, validUntil: null }),
      sum(fresh),
    ]).toEqual([
      "stale: Newer data available",
      "stale: 2 likely out of date",
      "stale: 1 likely out of date",
      "unknown: Some dates not recorded",
      "fresh: All up to date",
    ]);
    expect([dataVM({ ...LIVE, newRelease: "r-new" }).newRelease, dataVM(LIVE).newRelease])
      .toEqual(["A newer data release is available. Reopen the app to use it.", null]);
  });

  it("the summary counts ONLY the lines the block draws: a stale source listed on Details alone sends the reader there", () => {
    // DFO, the atlas (FWA) and HYDAT are on the Data sources & dates page, not in the block: with DFO past its 120 days
    // (Dec 26, 2026 on the live dates) the block must not say "2 likely out of date" over one red dot
    const fresh: DataInput = { ...LIVE, validUntil: "2027-03-31", feedFetchedAt: new Date(NOW - H).toISOString() };
    const oldDfo = { ...fresh.sources, dfo: { date: "2026-01-01" } };
    const sum = (d: DataInput) => { const v = dataVM(d); return `${v.summary.kind}: ${v.summary.label}`; };
    const dots = (d: DataInput) => dataVM(d).lines.filter((l) => l.badge.kind === "stale").length;
    expect([
      sum({ ...fresh, sources: oldDfo }),
      sum({ ...LIVE, sources: { ...LIVE.sources, dfo: { date: "2026-01-01" } } }),
      sum({ ...fresh, sources: { ...fresh.sources, fwa: undefined } }),
    ]).toEqual([
      "stale: 1 likely out of date · see Details",
      "stale: 1 likely out of date · 1 more in Details",
      "unknown: Some dates not recorded · see Details",
    ]);
    expect([dots({ ...fresh, sources: oldDfo }), dots({ ...LIVE, sources: { ...LIVE.sources, dfo: { date: "2026-01-01" } } })])
      .toEqual([0, 1]);
  });
});

describe("the More tab, top to bottom", () => {
  it("back to the previous tab, the Data block, Display, the pages as rows, Contact inline", () => {
    const vm = menuVM({ data: LIVE, back: "Conditions", contact: null });
    expect({
      title: vm.title, back: vm.back, data: [vm.data.title, vm.data.details, vm.data.detailsLabel, vm.data.detailsHint],
      display: vm.display, rows: vm.rows, contact: vm.contact,
    }).toEqual({
      title: "More",
      back: { to: "Conditions", label: "Back to Conditions" },
      data: ["Data", "Details", "Data sources & dates", "Every source, its date and where it comes from"],
      display: { title: "Display", palette: "Colours",
                 note: "The colour-blind palette changes the hues and nothing else. It has a light ground only.",
                 options: [{ k: "light", t: "Light" }, { k: "dark", t: "Dark" }, { k: "cvd", t: "Colour-blind" }] },
      rows: [
        { k: "glossary", label: "Glossary", hint: "What the regulations' words mean" },
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
      "stale | Refresh data: last checked on this device 1h ago",
      "stale | Refresh data: last checked on this device 5h ago",
      "stale | Refresh data: river flows could not be reached",
      "fresh | Refresh data: up to date",
    ]);
    expect(syncVM({ feed: true, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: null, now: NOW }).hint)
      .toBe("Checks the river flows and the data release again");
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

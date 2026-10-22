/**
 * The More tab and the sync icon (more.ts), whole strings (AGENTS 33): the Data block every reader sees at the top of
 * More, its summary badge, the tab's sections, and the sync icon's three states as said aloud.
 */
import { describe, expect, it } from "vitest";
import { dataVM, menuVM, syncVM, type DataInput } from "./more";

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
    ]).toEqual(["In force to Mar 31, 2027", "Printed Mar 20, 2025", "Date not recorded"]);
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
      // DFO is on the details page only, but a stale DFO is still counted
      sum({ ...fresh, sources: { ...fresh.sources, dfo: { date: "2026-01-01" } } }),
    ]).toEqual([
      "stale: Newer data available",
      "stale: 2 likely out of date",
      "stale: 1 likely out of date",
      "unknown: Some dates not recorded",
      "fresh: All up to date",
      "stale: 1 likely out of date",
    ]);
    expect([dataVM({ ...LIVE, newRelease: "r-new" }).newRelease, dataVM(LIVE).newRelease])
      .toEqual(["A newer data release is available. Reopen the app to use it.", null]);
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
    const v = syncVM({ feed: true, feedFetchedAt: new Date(NOW - H).toISOString(), checking: false, newRelease: null,
                       now: NOW, ...s });
    return `${v.kind} | ${v.label}`;
  };
  it("each state, with the feed's own cadence (2 h) as the edge", () => {
    expect([
      sync({}),
      sync({ checking: true }),
      sync({ newRelease: "r-new" }),
      sync({ feedFetchedAt: new Date(NOW - 2 * H).toISOString() }),
      sync({ feedFetchedAt: new Date(NOW - 27 * H).toISOString() }),
      sync({ feedFetchedAt: null }),
      sync({ feed: false, feedFetchedAt: null }),
    ]).toEqual([
      "fresh | Refresh data: up to date",
      "checking | Refresh data: checking now",
      "stale | Refresh data: a newer data release is available",
      "fresh | Refresh data: up to date",
      "stale | Refresh data: river flows last checked 27h ago",
      "stale | Refresh data: river flows could not be checked",
      "fresh | Refresh data: up to date",
    ]);
    expect(syncVM({ feed: true, feedFetchedAt: null, checking: false, newRelease: null, now: NOW }).hint)
      .toBe("Checks the river flows and the data release again");
  });
});

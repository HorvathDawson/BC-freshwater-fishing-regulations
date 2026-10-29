/** (T235 / T236) the licences list and the legal lines, pinned as finished strings (AGENTS 33). */
import { describe, expect, it } from "vitest";
import { asOfLine, FIRST_RUN_VERSION, firstRunAccepted, LEGAL_WORDS, regsAsOfDay } from "./legal";
import { LICENCE_ROWS, LICENCES, licencesVM } from "./licences";
import { MAP_CREDITS } from "./sources";
import { MENU_WORDS } from "./words";

const NOW = Date.parse("2026-10-28T18:00:00Z");

describe("the regulations' as-of line (T236)", () => {
  it("says the release's day, the salmon feed's age and the in-season feed's age", () => {
    expect(asOfLine({ release: { released: "2026-10-27" }, salmonAt: "2026-10-28T15:00:00Z",
                      inSeasonAt: "2026-10-28T17:00:00Z", now: NOW }))
      .toBe("Regulations as of Oct 27, 2026 · salmon feed 3h ago · in-season 1h ago");
  });
  it("leaves the salmon part out on a page with no salmon, and says the release's own salmon file", () => {
    expect(asOfLine({ release: null, inSeasonAt: null, now: NOW }))
      .toBe("Regulations: release date not stated · in-season not checked");
    expect(asOfLine({ release: null, salmonAt: null, inSeasonAt: null, now: NOW }))
      .toBe("Regulations: release date not stated · salmon from the release · in-season not checked");
  });
  it("without its own date, the release's newest regulation input (synopsis or DFO), never another source", () => {
    expect(regsAsOfDay({ sources: { synopsis: { date: "2025-04-01" }, dfo: { date: "2026-08-28" } } })).toBe("2026-08-28");
    expect(regsAsOfDay({ sources: { synopsis: { date: "2025-04-01" }, dfo: { date: null } } })).toBe("2025-04-01");
    expect(regsAsOfDay({ released: "not a day", sources: null })).toBeNull();
  });
  it("the foot line and the first-run notice say what the owner asked for", () => {
    expect(LEGAL_WORDS.footer).toBe("Not legal advice — the B.C. synopsis and DFO notices are the authority.");
    expect(LEGAL_WORDS.firstRun.lines.join(" ")).toContain("not legal advice");
    expect(MENU_WORDS.about.notOfficial).toContain("Nothing in this app is legal advice.");
  });
  it("first run: accepted only by the current version", () => {
    expect(firstRunAccepted(FIRST_RUN_VERSION)).toBe(true);
    expect(firstRunAccepted(null)).toBe(false);
    expect(firstRunAccepted("disclaimer-0")).toBe(false);
  });
});

describe("Sources and licences (T235)", () => {
  it("lists every dataset once, each with a licence it names", () => {
    const keys = LICENCE_ROWS.map((r) => r.k);
    expect(new Set(keys).size).toBe(keys.length);
    for (const k of ["synopsis", "dfo", "fwa", "osm", "obstacles", "hydat", "forecast", "runs", "weather", "imagery"])
      expect(keys).toContain(k);
    for (const r of licencesVM().rows) expect(r.licenceLabel).toBe(LICENCES[r.licence].label);
  });
  it("carries each licence's required words: OGL-BC, OGL-Canada, ODbL, DFO's non-endorsement", () => {
    const w = Object.fromEntries(LICENCE_ROWS.map((r) => [r.k, r.wording]));
    expect(w.fwa).toBe("Contains information licensed under the Open Government Licence – British Columbia.");
    expect(w.hydat).toBe("Contains information licensed under the Open Government Licence – Canada.");
    expect(w.osm).toBe("© OpenStreetMap contributors");
    expect(w.dfo).toContain("not an official version");
  });
  it("the forecast's words are the Province's verbatim, the same as the Conditions credits", () => {
    expect(MAP_CREDITS).toContain(LICENCE_ROWS.find((r) => r.k === "forecast")!.wording);
  });
  it("lists the four city stream layers, each with its own licence and no ODbL offer line", () => {
    for (const k of ["abbotsford", "burnaby", "portmoody", "squamish"]) {
      const r = licencesVM().rows.find((x) => x.k === k)!;
      expect(r.licenceUrl).toMatch(/^https:/);
      expect(r.wording).toContain("Open Government Licence");
    }
    expect(licencesVM()).not.toHaveProperty("odbl");
    expect(MENU_WORDS.items.sources.label).toBe("Sources and licences");
  });
});

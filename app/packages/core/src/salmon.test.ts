/**
 * (T211) THE SALMON FILE AND THE SALMON CARD: `salmon/1` decoded (the pipeline's encoder's layout, `pipeline/deliver/
 * salmon.py`), each section's chain (and the UNLISTED chain for a section in no run), the day's lookups, and the card's
 * words — whole strings (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { decodeSalmon, salmonCard, salmonDayIndex, salmonDayProfiles, salmonLabel, salmonLegendSource,
         salmonMapStatusOn, SalmonError } from "./salmon";

const HANDLES = "147b20dce7d8576c";
const JUL20 = new Date(2026, 6, 20), SEP15 = new Date(2026, 8, 15);
const d = (date: Date) => salmonDayIndex(date);

/** The Kispiox, as the pipeline built it from the Region 6 page dated 2026-08-28 (abridged): coho 4/day Jul 15–Aug 23
 *  then closed (FN0897); chinook, sockeye, pink and chum closed; no natural bait all year. */
const PAYLOAD = {
  format: "salmon/1", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations",
  default: 0,
  regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/region6" },
             "1": { name: "Vancouver Island", asof: "2026-08-25", url: "https://example/region1" } },
  verdicts: [
    { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },          // 0
    { s: "closed", t: "No fishing for chinook (Apr 1–Mar 31)" },                       // 1
    { s: "open", t: "4 per day, only 2 over 50 cm (Jul 15–Aug 23)" },                  // 2
    { s: "closed", t: "No fishing for coho — FN0897 (Aug 24–Mar 31)", n: "FN0897" },  // 3
    { s: "closed", t: "No fishing for sockeye (Apr 1–Mar 31)" },                       // 4
    { s: "closed", t: "No fishing for chum (Apr 1–Mar 31)" },                          // 5
    { s: "release", t: "Catch and release (Sep 1–Oct 31)" },                           // 6
  ],
  gears: [["No natural bait allowed (Jan 1–Dec 31)"]],
  chains: [
    // 0: the UNLISTED water — closed every day, every species
    { r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] },
    // 1: the Kispiox — coho open Jul 15 (196) to Aug 23, then closed
    { r: ["6"], w: "Kispiox River", l: ["6:kispiox-river:whole", "6:b(i):section", "6:a:region"],
      sp: [[0, 1], [0, 3, 196, 2, 236, 3], [0, 4], [0, 4], [0, 5]], g: [0, 0], m: [0, 3, 196, 1, 236, 3] },
    // 2: a water whose coho are release-only in the fall, the rest not opened
    { r: ["1"], w: "Cowichan River", l: ["1:cowichan"], sp: [[0, 0], [0, 0, 244, 6, 305, 0], [0, 0], [0, 0], [0, 0]],
      g: [0, -1], m: [0, 3, 244, 2, 305, 3] },
  ],
  unmapped: [{ items: ["gnis:4674"], r: "6", w: "Kispiox River — above the resort signs", chain: 2 }],
};

/** sid 100..104 the Kispiox (chain 1), 200..201 the Cowichan (chain 2); everything else unlisted. */
function encode(payload: object, runs: [number, number, number][], handles = HANDLES): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(payload));
  const out: number[] = [0x42, 0x43, 0x53, 0x41, 1];
  for (let i = 0; i < 16; i += 2) out.push(parseInt(handles.slice(i, i + 2), 16));
  out.push(body.length & 255, (body.length >> 8) & 255, (body.length >> 16) & 255, (body.length >>> 24) & 255);
  out.push(...body);
  const v = (n: number) => { while (n >= 0x80) { out.push((n & 0x7f) | 0x80); n = Math.floor(n / 128); } out.push(n); };
  v(runs.length);
  let end = 0;
  for (const [sid, n, chain] of runs) { v(sid - end); v(n); v(chain); end = sid + n; }
  return new Uint8Array(out);
}
const FILE = encode(PAYLOAD, [[100, 5, 1], [200, 2, 2]]);
const facts = () => decodeSalmon(FILE, HANDLES);

describe("salmon/1", () => {
  it("decodes the runs: a placed section reads its chain; a section in no run reads the UNLISTED chain (closed)", () => {
    const f = facts();
    expect([f.chainOf(99), f.chainOf(100), f.chainOf(104), f.chainOf(105), f.chainOf(201), f.chainOf(5_000_000)])
      .toEqual([0, 1, 1, 0, 2, 0]);
    expect([salmonLabel(f, 102), salmonLabel(f, 7)]).toEqual(["s1", "s0"]);
  });

  it("refuses another file, another atlas's handles, and trailing bytes", () => {
    expect(() => decodeSalmon(new Uint8Array([1, 2, 3]), HANDLES)).toThrow(SalmonError);
    expect(() => decodeSalmon(FILE, "0".repeat(16))).toThrow(/built for handles 147b20dce7d8576c/);
    expect(() => decodeSalmon(new Uint8Array([...FILE, 0]), HANDLES)).toThrow(/trailing/);
  });

  it("the day index is the calendar day with Feb 29 kept: Jul 15 is 196 in every year", () => {
    expect([d(new Date(2026, 0, 1)), d(new Date(2026, 6, 15)), d(new Date(2025, 6, 15)), d(new Date(2026, 11, 31))])
      .toEqual([0, 196, 196, 365]);
  });
});

describe("the Salmon colouring's day", () => {
  it("chain labels by status for the day: the unlisted water is CLOSED, an answer — never `none`", () => {
    expect(salmonDayProfiles(facts(), JUL20)).toEqual({ open: ["s1"], release: [], closed: ["s0", "s2"], check: [], none: [] });
    expect(salmonDayProfiles(facts(), SEP15)).toEqual({ open: [], release: ["s2"], closed: ["s0", "s1"], check: [], none: [] });
  });

  it("the pushed-data map reads the same: an unlisted section paints closed", () => {
    const f = facts();
    expect([salmonMapStatusOn(f, 101, JUL20), salmonMapStatusOn(f, 7, JUL20), salmonMapStatusOn(null, 7, JUL20)])
      .toEqual(["open", "closed", null]);
  });

  it("the legend says whose rules, as of when, and the unlisted rule", () => {
    expect(salmonLegendSource(facts())).toBe("Salmon rules from Fisheries and Oceans Canada (DFO), pages dated up to "
      + "Aug 28, 2026. Waters DFO does not list are closed to salmon.");
  });
});

describe("the Salmon card", () => {
  it("species that answer alike share a row; the open one stands alone (Jul 20 on the Kispiox)", () => {
    const c = salmonCard(facts(), { sids: [101], item: null }, JUL20);
    expect([c.allClosed, c.headline, c.headlineStatus]).toEqual([false, "Open today", "open"]);
    expect(c.rows.map((r) => [r.label, r.statusLabel, r.notes])).toEqual([
      ["Chinook", "Closed", ["No fishing for chinook (Apr 1–Mar 31)"]],
      ["Coho", "Open", ["4 per day, only 2 over 50 cm (Jul 15–Aug 23)"]],
      ["Sockeye · pink", "Closed", ["No fishing for sockeye (Apr 1–Mar 31)"]],
      ["Chum", "Closed", ["No fishing for chum (Apr 1–Mar 31)"]],
    ]);
    expect(c.gear).toEqual(["No natural bait allowed (Jan 1–Dec 31)"]);
    expect([c.source, c.asOf, c.notListed]).toEqual(["Fisheries and Oceans Canada · Region 6 Skeena",
                                                      "DFO page dated Aug 28, 2026", null]);
    expect(c.credit).toBe("Source: Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations. "
      + "Check DFO's page before you fish.");
  });

  it("EVERY species closed collapses to the one 'Salmon — closed' row (Sep 15 on the Kispiox)", () => {
    const c = salmonCard(facts(), { sids: [101], item: null }, SEP15);
    expect([c.allClosed, c.headline, c.rows.length, c.rows[0]!.label, c.rows[0]!.status]).toEqual(
      [true, "Closed today", 1, "Salmon", "closed"]);
    expect(c.rows[0]!.notes).toContain("No fishing for coho — FN0897 (Aug 24–Mar 31)");
  });

  it("A WATER DFO DOES NOT LIST READS CLOSED (owner 2026-10-27): 'Salmon — closed', and says why", () => {
    const c = salmonCard(facts(), { sids: [7, 8], item: "gnis:1" }, JUL20);
    expect([c.allClosed, c.headline, c.rows.map((r) => [r.label, r.status, r.notes])]).toEqual(
      [true, "Closed today", [["Salmon", "closed", ["Not opened by DFO: closed to salmon fishing"]]]]);
    expect(c.notListed).toBe("DFO does not list this water for salmon. It is closed to salmon fishing.");
    // and with no section at all (a water with no placed section) the same: the unlisted chain, not nothing
    expect(salmonCard(facts(), { sids: [], item: null }, JUL20).allClosed).toBe(true);
  });

  it("a page across two chains says where they differ, never one chain's answer for both", () => {
    const c = salmonCard(facts(), { sids: [101, 200], item: null }, SEP15);
    const coho = c.rows.find((r) => r.label.includes("Coho"))!;
    expect([coho.label, coho.status, coho.notes]).toEqual(
      ["Coho", "varies", ["Differs along this water. Tap a stretch on the map for its own answer."]]);
    expect(c.source).toBe("Fisheries and Oceans Canada · Region 1 Vancouver Island · Region 6 Skeena");
  });

  it("a DFO place on this water not drawn yet is listed under 'Location not yet mapped' with its own rules", () => {
    const c = salmonCard(facts(), { sids: [101], item: "gnis:4674" }, SEP15);
    expect(c.unmapped.map((u) => [u.label, u.tag, u.block.rows.map((r) => [r.label, r.statusLabel])])).toEqual([
      ["Kispiox River — above the resort signs", "Location not yet mapped",
       [["Chinook · sockeye · pink · chum", "Closed"], ["Coho", "Release only"]]]]);
  });
});

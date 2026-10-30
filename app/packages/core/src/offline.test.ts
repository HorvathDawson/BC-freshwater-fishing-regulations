/**
 * (T238, owner 2026-10-29) The Offline maps page's words, whole (AGENTS 33): the regulations one fixed row for all of
 * B.C. (never an action), the map one province-wide row, the regions listed as part of it (no size of their own until
 * the tile split); the phone's release download and Wi-Fi only; the web's share of the regulations kept so far.
 */
import { describe, expect, it } from "vitest";
import { offlineVM, type OfflineInput } from "./offline";

const PHONE: OfflineInput = { platform: "phone", regions: ["2", "7a"], regs: { bytes: 41_000_000, held: null },
                              map: { tiles: 185_300_000, basemap: 274_500_000 }, wifiOnly: true, update: null };
const WEB: OfflineInput = { platform: "web", regions: ["2", "7a"], regs: { bytes: 41_000_000, held: 12_500_000 },
                            map: { tiles: 185_300_000, basemap: 274_500_000 } };

describe("the phone", () => {
  it("regulations first and always offline; the map one file; regions part of it; storage = map + regulations", () => {
    const v = offlineVM(PHONE);
    expect([v.regs.aloud, v.map.aloud, v.regions.map((r) => r.aloud), v.total!.value]).toEqual([
      "Regulations: all of B.C.: 41.0 MB. Always available offline.",
      "Map of B.C.: 459.8 MB. On this phone · one file for the whole province.",
      ["Region 2 · Lower Mainland: Part of the province map · on this phone.",
       "Region 7A · Omineca: Part of the province map · on this phone."],
      "500.8 MB"]);
    expect(v.wifi).toEqual({ label: "Download large updates on Wi-Fi only",
                             hint: "Map and regulation updates can be hundreds of megabytes", on: true, value: "On" });
  });
  it("a release download: progress; waiting for Wi-Fi: Resume now; ready: said", () => {
    expect(offlineVM({ ...PHONE, update: { phase: "downloading", bytes: 50, of: 200 } }).update)
      .toEqual({ line: "Downloading new map data · 25%", progress: 0.25, resume: null });
    expect(offlineVM({ ...PHONE, update: { phase: "waiting-wifi", bytes: 0, of: 0 } }).update)
      .toEqual({ line: "New map data is waiting for Wi-Fi", progress: null, resume: "Resume now" });
    expect(offlineVM({ ...PHONE, update: { phase: "current", bytes: 0, of: 0 } }).update).toBeNull();
  });
});

describe("the web", () => {
  it("the regulations' share kept so far; the map read online; no Wi-Fi setting; storage = what the browser holds", () => {
    const v = offlineVM(WEB);
    expect([v.regs.state, v.regs.kept, v.map.state, v.regions[0]!.state, v.total!.value, v.wifi]).toEqual([
      "Always available offline · 12.5 MB of 41.0 MB kept in this browser so far", false,
      "Read online as you move the map", "Part of the province map", "12.5 MB", null]);
    expect(offlineVM({ ...WEB, regs: { bytes: 41_000_000, held: 41_000_000 } }).regs.state)
      .toBe("Always available offline · All kept in this browser");
    expect(offlineVM({ ...WEB, regs: { bytes: 41_000_000, held: null } }).total).toBeNull();
  });
});

describe("the rule book (owner 2026-10-29: part of a phone's first download)", () => {
  it("phone: its own row, on the phone, opens offline, counted in storage; web: fetched when opened; none: no row", () => {
    const p = offlineVM({ ...PHONE, book: 25_041_728 });
    expect([p.book!.aloud, p.book!.kept, p.total!.value]).toEqual([
      "Rule book (synopsis PDF): 25.0 MB. On this phone · opens offline.", true, "525.8 MB"]);
    const w = offlineVM({ ...WEB, book: 25_041_728 });
    expect([w.book!.aloud, w.book!.kept, w.total!.value]).toEqual([
      "Rule book (synopsis PDF): 25.0 MB. Downloaded when you open it.", false, "12.5 MB"]);
    expect(offlineVM(PHONE).book).toBeNull();
  });
});

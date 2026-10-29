/**
 * (T238) The Offline maps page's words, whole (AGENTS 33): the phone (everything on it, sizes, the release download,
 * Wi-Fi only) and the web (saved / partly / not, Save / Remove / Resume, progress, storage used, remove everything).
 */
import { describe, expect, it } from "vitest";
import { offlineVM, type OfflineInput } from "./offline";

const REGIONS = [
  { region: "2", bytes: 1_200_000, held: 0, state: "none" as const },
  { region: "7a", bytes: 500_000, held: 0, state: "none" as const },
];
const PHONE: OfflineInput = { platform: "phone", regions: REGIONS, shared: { bytes: 30_000_000, held: 0 },
                              map: { tiles: 185_300_000, basemap: 274_500_000 }, wifiOnly: true, update: null };
const WEB: OfflineInput = { platform: "web", regions: REGIONS, shared: { bytes: 30_000_000, held: 0 },
                            map: { tiles: 185_300_000, basemap: 274_500_000 }, job: null, storable: true };

describe("the phone: everything installed", () => {
  it("every region on the phone, no actions; the map one province-wide file; storage = map + data", () => {
    const v = offlineVM(PHONE);
    expect(v.total).toEqual({ label: "Storage used", value: "491.5 MB", aloud: "Storage used: 491.5 MB" });
    expect(v.parts.map((p) => p.aloud)).toEqual([
      "Map of BC: 459.8 MB. On this phone · one file for the whole province.",
      "Province-wide data: 30.0 MB. On this phone."]);
    expect(v.regions.map((r) => [r.aloud, r.action])).toEqual([
      ["Region 2 · Lower Mainland: 1.2 MB. On this phone.", null], ["Region 7A · Omineca: 0.5 MB. On this phone.", null]]);
    expect([v.clear, v.update, v.wifi]).toEqual([null, null,
      { label: "Download large updates on Wi-Fi only", hint: "Map and regulation updates can be hundreds of megabytes",
        on: true, value: "On" }]);
  });
  it("a release download: progress; waiting for Wi-Fi: Resume now; ready: said", () => {
    expect(offlineVM({ ...PHONE, update: { phase: "downloading", bytes: 50, of: 200 } }).update)
      .toEqual({ line: "Downloading new map data · 25%", progress: 0.25, resume: null });
    expect(offlineVM({ ...PHONE, update: { phase: "waiting-wifi", bytes: 0, of: 0 } }).update)
      .toEqual({ line: "New map data is waiting for Wi-Fi", progress: null, resume: "Resume now" });
    expect(offlineVM({ ...PHONE, update: { phase: "ready", bytes: 0, of: 0 } }).update!.line)
      .toBe("New map data is downloaded; it is used the next time you open the map");
    expect(offlineVM({ ...PHONE, update: { phase: "current", bytes: 0, of: 0 } }).update).toBeNull();
  });
});

describe("the web: the release cache", () => {
  it("nothing saved: Save on each region; storage 0; no remove-everything", () => {
    const v = offlineVM(WEB);
    expect(v.total.value).toBe("0.0 MB");
    expect(v.parts.map((p) => p.state)).toEqual(["Read online as you move the map", "Saved with the first region you save"]);
    expect(v.regions.map((r) => [r.state, r.action?.label, r.action?.aloud]))
      .toEqual([["Not saved", "Save", "Save Region 2 · Lower Mainland"], ["Not saved", "Save", "Save Region 7A · Omineca"]]);
    expect([v.clear, v.wifi, v.update]).toEqual([null, null, null]);
  });
  it("Region 2 saved: Remove; storage counts shared + its own; remove everything offered", () => {
    const v = offlineVM({ ...WEB, shared: { bytes: 30_000_000, held: 30_000_000 },
                          regions: [{ ...REGIONS[0]!, held: 1_200_000, state: "kept" }, REGIONS[1]!] });
    expect([v.total.value, v.parts[1]!.state, v.regions[0]!.state, v.regions[0]!.action!.label, v.clear])
      .toEqual(["31.2 MB", "Saved", "Saved", "Remove", "Remove everything saved"]);
  });
  it("saving: the row's progress, every action busy, no remove-everything; a failure: Stopped, Resume, said why", () => {
    const s = offlineVM({ ...WEB, job: { region: "2", kind: "save", phase: "running", bytes: 3, of: 4, error: null } });
    expect([s.regions[0]!.state, s.regions[0]!.progress, s.regions[0]!.action, s.regions[1]!.action!.busy, s.clear])
      .toEqual(["Saving… 75%", 0.75, null, true, null]);
    const f = offlineVM({ ...WEB, regions: [{ ...REGIONS[0]!, held: 600_000, state: "partial" }, REGIONS[1]!],
                          job: { region: "2", kind: "save", phase: "failed", bytes: 1, of: 4, error: "HTTP 503" } });
    expect([f.regions[0]!.state, f.regions[0]!.action!.label, f.failed]).toEqual(["Stopped", "Resume",
      "The download stopped: HTTP 503. What was saved is kept; Resume fetches the rest."]);
  });
  it("the province-wide data partly kept (the background prefetch's share): Save on its row; saving it shows progress", () => {
    const v = offlineVM({ ...WEB, shared: { bytes: 30_000_000, held: 12_000_000 } });
    expect([v.parts[1]!.state, v.parts[1]!.action!.aloud]).toEqual(["Partly saved", "Save Province-wide data"]);
    const s = offlineVM({ ...WEB, job: { region: "shared", kind: "save", phase: "running", bytes: 1, of: 2, error: null } });
    expect([s.parts[1]!.state, s.parts[1]!.progress]).toEqual(["Saving… 50%", 0.5]);
  });
  it("a browser that cannot keep files offers nothing", () => {
    const v = offlineVM({ ...WEB, storable: false });
    expect(v.regions.map((r) => r.action)).toEqual([null, null]);
  });
});

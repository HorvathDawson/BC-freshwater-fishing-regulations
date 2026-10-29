/** (T231) The data updates in words, and the sync icon's states over them — whole strings pinned. */
import { describe, expect, it } from "vitest";
import { syncVM } from "./more";
import { bellLabel, failedSources, failureWords, syncErrorVM, updatedToast, updateSourcesVM,
         type UpdateSourceInput } from "./updates";

const NOW = Date.parse("2026-10-28T20:00:00Z");
const MIN = 60_000, H = 3_600_000;
const src = (k: UpdateSourceInput["k"], o: Partial<UpdateSourceInput> = {}): UpdateSourceInput =>
  ({ k, checkedAt: NOW - 3 * MIN, updatedAt: NOW - 2 * H, error: null, failedAt: null, offline: false, ...o });
const base = { feed: true, feedFetchedAt: new Date(NOW - 20 * MIN).toISOString(), checkedAt: (NOW - MIN) as number | null, checking: false,
               newRelease: null, now: NOW };
const idle: { downloading: number | null; ready: boolean; waitingWifi: boolean; failed: string[]; done: boolean } =
  { downloading: null, ready: false, waitingWifi: false, failed: [], done: false };

describe("syncVM: the update service's states", () => {
  it("checking > error > downloading > done > ready / waiting > the feed rules", () => {
    const at = (u: Partial<typeof idle>, s: Partial<typeof base> = {}) => syncVM({ ...base, ...s, update: { ...idle, ...u } });
    expect(at({ failed: ["the data release"] }, { checking: true }).kind).toBe("checking");
    expect(at({ failed: ["the live gauge readings", "the DFO salmon feed"], downloading: 0.5 })).toEqual({
      kind: "error", label: "Refresh data: could not update the live gauge readings and the DFO salmon feed",
      hint: "Shows what could not be updated, and lets you try again" });
    expect(at({ downloading: 0.42, done: true })).toMatchObject({ kind: "downloading", progress: 0.42,
                                                                 label: "Refresh data: downloading an update, 42%" });
    expect(at({ done: true, ready: true }).label).toBe("Refresh data: up to date, just checked");
    expect(at({ ready: true })).toMatchObject({ kind: "stale",
      label: "Refresh data: an update is downloaded and will be used at the next pause" });
    expect(at({ waitingWifi: true }).label).toBe("Refresh data: an update is waiting for Wi-Fi");
    expect(at({}).kind).toBe("fresh");
    expect(at({}, { checkedAt: null }).kind).toBe("stale");
  });
});

describe("the sources' words", () => {
  it("failures in the reader's words; offline shows red only after a manual check", () => {
    expect(failureWords("x: HTTP 503", false)).toBe("The server did not answer");
    expect(failureWords("core: sha256 0123…, the manifest says ab…", false)).toBe("A download did not match its checksum");
    expect(failureWords("x: Network request failed", true)).toBe("No connection");
    expect(failureWords("x: not a feed", false)).toBe("The download could not be read");
    const s = [src("gauges", { error: "offline", offline: true }), src("dfo", { error: "x: HTTP 500" }), src("inseason")];
    expect(failedSources(s, false).map((x) => x.k)).toEqual(["dfo"]);
    expect(failedSources(s, true).map((x) => x.k)).toEqual(["gauges", "dfo"]);
  });

  it("the More tab's Updates line and one line per source", () => {
    const vm = updateSourcesVM({ sources: [src("gauges"), src("release", { checkedAt: null, updatedAt: null })],
                                 release: { phase: "current", progress: null }, now: NOW });
    expect(vm.line).toBe("Checked 3 min ago");
    expect(vm.sources.map((s) => `${s.name}: ${s.line}`)).toEqual([
      "Live gauge readings: Checked 3 min ago · updated 2h ago",
      "Regulations, map and salmon data: Not checked yet · not updated on this device yet"]);
    expect(updateSourcesVM({ sources: [src("gauges")], release: { phase: "downloading", progress: { bytes: 1, of: 4 } },
                             now: NOW }).line).toBe("Downloading an update, 25%");
    expect(updateSourcesVM({ sources: [src("gauges", { error: "HTTP 500" })], release: null, now: NOW }).line)
      .toBe("1 source could not be updated");
  });

  it("the failure sheet: what failed, when it last succeeded, Retry", () => {
    const vm = syncErrorVM([src("dfo", { error: "HTTP 500" }), src("release", { error: "sha256 x", checkedAt: null })], NOW);
    expect(vm.title).toBe("Could not update");
    expect(vm.rows).toEqual([
      { k: "dfo", name: "DFO salmon", problem: "The server did not answer", last: "Last succeeded 3 min ago",
        label: "DFO salmon: The server did not answer. Last succeeded 3 min ago." },
      { k: "release", name: "Regulations, map and salmon data", problem: "A download did not match its checksum",
        last: "Not reached from this device yet",
        label: "Regulations, map and salmon data: A download did not match its checksum. Not reached from this device yet." }]);
    expect(vm.retry).toBe("Retry");
  });

  it("the toast and the bell", () => {
    expect(updatedToast("2026-10-27")).toBe("Updated to Oct 27, 2026");
    expect(updatedToast(null)).toBe("Data updated");
    expect(bellLabel("2 in-season changes not on the map yet", 1))
      .toBe("2 in-season changes not on the map yet; 1 update failed");
    expect(bellLabel(null, 2)).toBe("2 updates failed");
  });
});

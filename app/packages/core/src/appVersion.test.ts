/** (T251) the app's version: resolution from the build's stamp, semver order, the words, and the release gate. */
import { describe, expect, it } from "vitest";
import { compareSemver, releaseAllowed, resolveAppVersion, versionHeaders, versionLine, versionTag } from "./appVersion";
import { errorClassOf, updateLogStatus, updateLogText, updateLogVM } from "./updateLog";

describe("resolveAppVersion", () => {
  it("reads the stamp; the sha is cut to 7; unstamped is 0.0.0 with nothing else", () => {
    expect(resolveAppVersion({ version: "0.1.0", build: "3", sha: "A1B2C3D4E5F6", date: "2026-10-28" }))
      .toEqual({ version: "0.1.0", build: 3, sha: "a1b2c3d", date: "2026-10-28" });
    expect(resolveAppVersion({})).toEqual({ version: "0.0.0", build: null, sha: null, date: null });
  });
  it("a malformed part throws: a build must not compare wrongly against a release", () => {
    for (const bad of [{ version: "1.2" }, { build: "3a" }, { sha: "xyz" }, { date: "28/10/2026" }])
      expect(() => resolveAppVersion(bad)).toThrow();
  });
  it("the line, the tag and the headers", () => {
    const v = resolveAppVersion({ version: "0.1.0", build: "3", sha: "a1b2c3d", date: "2026-10-28" });
    expect(versionLine(v)).toBe("0.1.0 (build 3, a1b2c3d, 2026-10-28)");
    expect(versionLine(resolveAppVersion({ version: "0.1.0" }))).toBe("0.1.0");
    expect(versionTag(v)).toBe("0.1.0+3.a1b2c3d");
    expect(versionHeaders(v, { platform: "android 35", ua: true }))
      .toEqual({ "X-App-Version": "0.1.0+3.a1b2c3d", "User-Agent": "canifishthis/0.1.0+3.a1b2c3d (android 35)" });
    expect(versionHeaders(v, { platform: "web", ua: false })).toEqual({ "X-App-Version": "0.1.0+3.a1b2c3d" });
  });
});

describe("semver order and the release gate", () => {
  it("orders numbers, pre-releases below their release, build metadata ignored", () => {
    const sorted = ["1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta.2", "1.0.0-beta.11", "1.0.0", "1.0.1",
                    "1.2.0", "1.10.0"];
    for (let i = 1; i < sorted.length; i++) expect(compareSemver(sorted[i - 1]!, sorted[i]!)).toBe(-1);
    expect(compareSemver("1.0.0+5", "1.0.0+9")).toBe(0);
  });
  it("absent → allowed; older app → refused naming the need; equal or newer → allowed; garbage → refused", () => {
    const v = resolveAppVersion({ version: "0.2.0" });
    expect([undefined, null, ""].map((m) => releaseAllowed(m, v).ok)).toEqual([true, true, true]);
    expect(releaseAllowed("0.3.0", v)).toEqual({ ok: false, need: "0.3.0" });
    expect([releaseAllowed("0.2.0", v).ok, releaseAllowed("0.1.9", v).ok]).toEqual([true, true]);
    expect(releaseAllowed("soon", v)).toEqual({ ok: false, need: "soon" });
  });
});

describe("the Updates log's words", () => {
  it("classes a failure", () => {
    expect(["x: HTTP 404", "x: HTTP 503", "x: HTTP 403", "x: sha256 01…, the manifest says 02…",
            "x: 5 bytes, the manifest says 6", "manifest: format \"x\" is not app-manifest/2", "boom"]
      .map((e) => errorClassOf(e, false)))
      .toEqual(["http-404", "http-5xx", "http-4xx", "sha-mismatch", "size-mismatch", "parse", "other"]);
    expect(errorClassOf("x: HTTP 404", true)).toBe("offline");
  });
  it("rows newest first; the export is the build, the source, then each event oldest first", () => {
    const ev = [{ at: Date.UTC(2026, 9, 28, 12), source: "gauges" as const, kind: "unchanged" as const, status: 304, bytes: 0,
                  ms: 12, before: "a1", after: "a1" },
                { at: Date.UTC(2026, 9, 28, 12, 1), source: "release" as const, kind: "failed" as const, status: 404,
                  bytes: 0, error: "x: HTTP 404", errorClass: "http-404" as const, manual: true }];
    const rows = updateLogVM(ev);
    expect(rows.map((r) => [r.source, r.result, r.detail, r.change, r.problem])).toEqual([
      ["Release", "failed", "404 · 0 B · manual", null, "http-404: x: HTTP 404"],
      ["Gauges", "unchanged", "304 · 0 B · 12 ms", "a1 → a1", null],
    ]);
    expect(updateLogText(ev, { version: "0.1.0 (build 3)", platform: "web", env: "staging", origin: "https://d",
                               channel: "test", release: "r-1" }, Date.UTC(2026, 9, 28, 13)).split("\n")).toEqual([
      "Can I Fish This? updates log — exported 2026-10-28T13:00:00.000Z",
      "app 0.1.0 (build 3) · web",
      "data staging · d · channel test · release r-1",
      "",
      "2026-10-28T12:00:00.000Z | gauges | unchanged | 304 · 0 B · 12 ms | a1 → a1",
      "2026-10-28T12:01:00.000Z | release | failed | 404 · 0 B · manual | http-404: x: HTTP 404",
    ]);
  });
});

describe("the Updates log's status lines", () => {
  it("each source's state, the release's phase and what it asks for", () => {
    expect(updateLogStatus([
      { id: "gauges", checkedAt: 1, error: null, offline: false },
      { id: "dfo", checkedAt: null, error: "x", offline: true },
      { id: "inseason", checkedAt: null, error: null, offline: false },
      { id: "release", checkedAt: 1, error: null, offline: false, phase: "app-too-old", target: "r-2", needsApp: "0.3.0" },
    ])).toEqual(["Gauges: ok", "DFO salmon: failed (offline)", "In-season: not checked",
                 "Release: ok · app-too-old r-2 (needs 0.3.0)"]);
  });
});

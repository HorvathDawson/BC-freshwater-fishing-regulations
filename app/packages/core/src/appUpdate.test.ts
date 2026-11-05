/**
 * (T390) A newer app: the channel by build, the once-a-day ask, the staging page's comparison, and the bell's section —
 * gentle for a newer version, strong for a release refused by `minAppVersion`, nothing offline, failures said.
 */
import { describe, expect, it } from "vitest";
import { APP_UPDATE_WORDS as W, apkIsNewer, appUpdateChannel, appUpdateVM, readAppUpdateSaved, shouldCheckAppUpdate,
         type AppUpdateCheck } from "./appUpdate";
import { bellLabel } from "./updates";

const own = { version: "0.5.0", build: 12, sha: "abc1234", date: "2026-11-04" };

describe("appUpdateChannel", () => {
  it("Android: the build's word, else production is Play and staging the APK page", () => {
    expect(appUpdateChannel({ os: "android", env: "staging", distribution: "play" })).toBe("play");
    expect(appUpdateChannel({ os: "android", env: "staging", distribution: "apk-page", apkPage: "https://x/index.html" })).toBe("apk-page");
    expect(appUpdateChannel({ os: "android", env: "staging", distribution: "apk-page" })).toBe("none");
    expect(appUpdateChannel({ os: "android", env: "production" })).toBe("play");
    expect(appUpdateChannel({ os: "android", env: "staging", apkPage: "https://x/index.html" })).toBe("apk-page");
    expect(appUpdateChannel({ os: "android", env: "local" })).toBe("none");
    expect(() => appUpdateChannel({ os: "android", env: "production", distribution: "sideload" })).toThrow(/not play/);
  });
  it("iOS has a channel only once the listing is named (T88); the web never", () => {
    expect(appUpdateChannel({ os: "ios", env: "production" })).toBe("none");
    expect(appUpdateChannel({ os: "ios", env: "production", appStoreUrl: "https://apps.apple.com/x" })).toBe("app-store");
    expect(appUpdateChannel({ os: "web", env: "production", distribution: "play" })).toBe("none");
  });
});

describe("once a device day, never offline", () => {
  const saved = { day: "2026-11-04", available: false, version: null };
  it("asks when nothing is kept, or the kept ask was another day", () => {
    expect(shouldCheckAppUpdate({ channel: "play", saved: null, today: "2026-11-04", offline: false })).toBe(true);
    expect(shouldCheckAppUpdate({ channel: "play", saved, today: "2026-11-04", offline: false })).toBe(false);
    expect(shouldCheckAppUpdate({ channel: "play", saved, today: "2026-11-05", offline: false })).toBe(true);
  });
  it("offline or without a channel: never", () => {
    expect(shouldCheckAppUpdate({ channel: "play", saved: null, today: "2026-11-04", offline: true })).toBe(false);
    expect(shouldCheckAppUpdate({ channel: "none", saved: null, today: "2026-11-04", offline: false })).toBe(false);
  });
  it("the kept file: read whole, anything else is nothing kept", () => {
    expect(readAppUpdateSaved(JSON.stringify({ day: "2026-11-04", available: true, version: "0.6.0" })))
      .toEqual({ day: "2026-11-04", available: true, version: "0.6.0" });
    expect(readAppUpdateSaved("{")).toBeNull();
    expect(readAppUpdateSaved(JSON.stringify({ day: 3 }))).toBeNull();
    expect(readAppUpdateSaved(null)).toBeNull();
  });
});

describe("apkIsNewer (the staging page's latest.json)", () => {
  it("semver, then build, then another commit on the same or a later day", () => {
    expect(apkIsNewer({ version: "0.6.0", build: 1 }, own)).toBe(true);
    expect(apkIsNewer({ version: "0.4.9", build: 99 }, own)).toBe(false);
    expect(apkIsNewer({ version: "0.5.0", build: 13 }, own)).toBe(true);
    expect(apkIsNewer({ version: "0.5.0", build: 12, sha: "abc1234ff", date: "2026-11-05" }, own)).toBe(false);
    expect(apkIsNewer({ version: "0.5.0", build: 12, sha: "def5678", date: "2026-11-05" }, own)).toBe(true);
    expect(apkIsNewer({ version: "0.5.0", build: 12, sha: "def5678", date: "2026-11-03" }, own)).toBe(false);
  });
  it("a page that names no version is a defect", () => {
    expect(() => apkIsNewer({ build: 3 }, own)).toThrow(/no app version/);
    expect(() => apkIsNewer(null, own)).toThrow();
  });
});

describe("appUpdateVM: the bell's App update section", () => {
  const current: AppUpdateCheck = { state: "current" };
  const avail: AppUpdateCheck = { state: "available", version: "0.6.0" };
  it("a newer version: the gentle prompt, flexible, counted on the bell", () => {
    const vm = appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "idle" })!;
    expect(vm).toMatchObject({ strength: "gentle", text: "Version 0.6.0 of the app is available.", counts: true,
                               button: { label: "Update", immediate: false }, said: null, failed: false });
    // Play names a version CODE, not a semver: the plain words
    expect(appUpdateVM({ channel: "play", check: { state: "available", version: "57" }, tooOld: false, start: "idle" })!.text)
      .toBe(W.available);
  });
  it("min-app refusal: the strong prompt, immediate, whatever the check said", () => {
    for (const check of [current, avail, { state: "unknown" } as const]) {
      const vm = appUpdateVM({ channel: "play", check, tooOld: true, start: "idle" })!;
      expect(vm).toMatchObject({ strength: "strong", text: "New data needs the latest app.", counts: true,
                                 button: { label: "Update", immediate: true } });
    }
    // no channel (the web, an iOS build before T88): said, with no button
    expect(appUpdateVM({ channel: "none", check: current, tooOld: true, start: "idle" })!.button).toBeNull();
    // the staging APK: the test-build page
    expect(appUpdateVM({ channel: "apk-page", check: current, tooOld: true, start: "idle" })!.button!.label)
      .toBe("Get the new test build");
  });
  it("nothing to say: current, not checked yet, offline, no channel", () => {
    expect(appUpdateVM({ channel: "play", check: current, tooOld: false, start: "idle" })).toBeNull();
    expect(appUpdateVM({ channel: "play", check: { state: "unknown" }, tooOld: false, start: "idle" })).toBeNull();
    expect(appUpdateVM({ channel: "play", check: { state: "failed", offline: true }, tooOld: false, start: "idle" })).toBeNull();
    expect(appUpdateVM({ channel: "none", check: avail, tooOld: false, start: "idle" })).toBeNull();
  });
  it("failures are said: a check that failed (no button, not counted), a start that failed (button again)", () => {
    expect(appUpdateVM({ channel: "play", check: { state: "failed", offline: false }, tooOld: false, start: "idle" }))
      .toMatchObject({ text: W.checkFailed, button: null, counts: false, failed: true });
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "failed" }))
      .toMatchObject({ said: W.startFailed, failed: true, button: { label: "Update" } });
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: true, start: "offline" })!.said).toBe(W.offline);
  });
  it("after the button: under way (no button), store opened, nothing newer yet", () => {
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "downloading" }))
      .toMatchObject({ said: W.downloading, button: null, failed: false });
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "working" })!.button).toBeNull();
    expect(appUpdateVM({ channel: "apk-page", check: avail, tooOld: false, start: "opened" })!.said).toBe(W.opened);
    expect(appUpdateVM({ channel: "play", check: current, tooOld: true, start: "none" })!.said).toBe(W.notYet);
  });
  it("the bell says it first", () => {
    const vm = appUpdateVM({ channel: "play", check: current, tooOld: true, start: "idle" });
    expect(bellLabel("2 in-season changes", 1, null, vm)).toBe("New data needs the latest app; 2 in-season changes; 1 update failed");
  });
});

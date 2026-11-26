/**
 * (T390) A newer app: the channel by build, the once-a-day ask, the staging page's comparison, and the bell's section —
 * gentle for a newer version, strong for a release refused by `minAppVersion`, nothing offline, failures said.
 */
import { describe, expect, it } from "vitest";
import { APP_UPDATE_WORDS as W, apkIsNewer, appUpdateBackgroundNotice, appUpdateChannel, appUpdateOs, appUpdateVM, appVersionFileUrl, appVersionToTell, changelogVM, notesFor,
         readAppUpdateSaved, shouldCheckAppUpdate, storeUpdateCheck, storeVersionFileUrl, updatePopupVM, type AppUpdateChannel,
         type AppUpdateCheck } from "./appUpdate";
import { CHANGELOG, notesOn, type ChangelogEntry } from "./changelog";
import { compareSemver } from "./appVersion";
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

describe("every opening and return, never offline", () => {
  it("asks whenever there is a channel and a connection (no day cap: 0.6.0 missed 0.6.1 behind one)", () => {
    expect(shouldCheckAppUpdate({ channel: "play", offline: false })).toBe(true);
    expect(shouldCheckAppUpdate({ channel: "apk-page", offline: false })).toBe(true);
  });
  it("offline or without a channel: never", () => {
    expect(shouldCheckAppUpdate({ channel: "play", offline: true })).toBe(false);
    expect(shouldCheckAppUpdate({ channel: "none", offline: false })).toBe(false);
  });
  it("the kept file: read whole, anything else is nothing kept", () => {
    expect(readAppUpdateSaved(JSON.stringify({ day: "2026-11-04", available: true, version: "0.6.0" })))
      .toEqual({ day: "2026-11-04", available: true, version: "0.6.0", notes: null, popped: null });
    // (T888) the version whose popup was answered
    expect(readAppUpdateSaved(JSON.stringify({ day: "2026-11-04", available: true, version: "0.6.0", popped: "0.6.0" }))!.popped)
      .toBe("0.6.0");
    expect(readAppUpdateSaved(JSON.stringify({ day: "2026-11-04", available: true, version: "0.6.0", notes: ["a", 3] }))!.notes)
      .toEqual(["a"]);
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
  it("min-app refusal: the strong prompt, immediate, when the store offers the app (T1073: only then)", () => {
    const vm = appUpdateVM({ channel: "play", check: avail, tooOld: true, start: "idle" })!;
    expect(vm).toMatchObject({ strength: "strong", text: "New data needs the latest app.", counts: true, offered: true,
                               button: { label: "Update", immediate: true } });
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
  it("failures: a store check that failed shows nothing (T1073, the hook reports it); a start that failed gets its button again", () => {
    expect(appUpdateVM({ channel: "play", check: { state: "failed", offline: false }, tooOld: false, start: "idle" })).toBeNull();
    expect(appUpdateVM({ channel: "app-store", check: { state: "failed", offline: false }, tooOld: false, start: "idle" })).toBeNull();
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "failed" }))
      .toMatchObject({ said: W.startFailed, failed: true, button: { label: "Update" } });
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: true, start: "offline" })!.said).toBe(W.offline);
  });
  it("after the button: under way (no button), store opened, nothing newer yet", () => {
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "downloading" }))
      .toMatchObject({ said: W.downloading, button: null, failed: false });
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "working" })!.button).toBeNull();
    expect(appUpdateVM({ channel: "apk-page", check: avail, tooOld: false, start: "opened" })!.said).toBe(W.opened);
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: true, start: "none" })!.said).toBe(W.notYet);
    // (T390 11-05) our file names a newer build that Play does not offer yet: the listing opened, said so, button kept
    expect(appUpdateVM({ channel: "play", check: avail, tooOld: false, start: "listing" }))
      .toMatchObject({ said: W.playPreparing, failed: false, button: { label: "Update" } });
  });
  it("the bell says it first", () => {
    const vm = appUpdateVM({ channel: "play", check: avail, tooOld: true, start: "idle" });
    expect(bellLabel("2 in-season changes", 1, null, vm)).toBe("New data needs the latest app; 2 in-season changes; 1 update failed");
  });
});

describe("(T1073) the update shows only when the store says this install can update now", () => {
  // our version file says 0.9.2 is out; the store's answer is what decides
  const notOffered: AppUpdateCheck = { state: "current" };
  const offered: AppUpdateCheck = { state: "available", version: "0.9.2", notes: ["A fix"] };
  const failed: AppUpdateCheck = { state: "failed", offline: false };
  const surfaces = (channel: AppUpdateChannel, check: AppUpdateCheck, tooOld = false) => {
    const vm = appUpdateVM({ channel, check, tooOld, start: "idle" });
    const pop = updatePopupVM({ vm, channel, check, installed: "0.9.1", popped: null, dismissed: null, held: false });
    return { row: vm !== null, button: vm?.button ?? null, notice: pop !== null };
  };
  it("the store does not offer it (a newer version in our file, Play or the App Store says no): no notice, no row, no button", () => {
    for (const ch of ["play", "app-store"] as const) {
      expect(surfaces(ch, notOffered)).toEqual({ row: false, button: null, notice: false });
      expect(surfaces(ch, { state: "unknown" })).toEqual({ row: false, button: null, notice: false });
    }
  });
  it("the store offers it: the notice, the row and the button, all three", () => {
    for (const ch of ["play", "app-store"] as const) {
      expect(surfaces(ch, offered)).toEqual({ row: true, button: { label: "Update", immediate: false }, notice: true });
    }
  });
  it("the store check failed: nothing shown (the hook reports it once)", () => {
    for (const ch of ["play", "app-store"] as const) expect(surfaces(ch, failed)).toEqual({ row: false, button: null, notice: false });
  });
  it("the strong prompt while the store does not offer the app: whole words, no button, no promise of an install", () => {
    for (const check of [notOffered, { state: "unknown" } as const, failed]) {
      const vm = appUpdateVM({ channel: "play", check, tooOld: true, start: "idle" })!;
      expect(vm).toMatchObject({
        strength: "strong", title: "App update", offered: false, counts: true, button: null, failed: false,
        text: "New data needs a newer app. It is not available for your phone yet.",
        sub: "The app keeps using the data it has, and asks the store again each time it opens.",
        label: "New data needs a newer app, not available for your phone yet",
      });
      expect(updatePopupVM({ vm, channel: "play", check, installed: "0.9.1", popped: null, dismissed: null, held: false }))
        .toMatchObject({ key: "needed", strength: "strong", title: "New data needs a newer app", button: null,
                         sub: "The app keeps using the data it has, and asks the store again each time it opens." });
    }
    // the store offers it: the strong prompt promises the install, with the button
    expect(appUpdateVM({ channel: "app-store", check: offered, tooOld: true, start: "idle" }))
      .toMatchObject({ text: "New data needs the latest app.", offered: true, button: { label: "Update", immediate: true } });
  });
  it("the sideloaded APK's own file and the web keep their rules", () => {
    expect(surfaces("apk-page", offered)).toEqual({ row: true, button: { label: "Get the new test build", immediate: false }, notice: true });
    expect(appUpdateVM({ channel: "none", check: notOffered, tooOld: true, start: "idle" }))
      .toMatchObject({ text: "New data needs the latest app.", button: null });
  });
  it("only the sideloaded APK raises the background new-version notice; a store install is told by its store", () => {
    expect(["play", "app-store", "apk-page", "none"].map((c) => appUpdateBackgroundNotice(c as AppUpdateChannel)))
      .toEqual([false, false, true, false]);
  });
  it("a TestFlight install, or an iPhone with no listing, has no channel: no notice we cannot act on", () => {
    expect(appUpdateChannel({ os: "ios", env: "production", distribution: "testflight", appStoreUrl: "https://apps.apple.com/x" })).toBe("none");
    expect(appUpdateChannel({ os: "ios", env: "production", distribution: "testflight" })).toBe("none");
    expect(appUpdateChannel({ os: "ios", env: "production", appStoreUrl: "https://apps.apple.com/x" })).toBe("app-store");
  });
});

describe("(T234 v1.1) the changelog and the incoming version's bullets", () => {
  const log = [
    { version: "0.4.0", date: "2026-10-31", notes: ["Offline gauge charts"] },
    { version: "0.10.0", date: null, notes: [] },
    { version: "0.6.0", date: null, notes: ["Gauge alerts"] },
  ];
  it("the page lists every version newest first (by semver, not file order), the installed one marked", () => {
    const rows = changelogVM("0.6.0", log);
    expect(rows.map((r) => r.version)).toEqual(["0.10.0", "0.6.0", "0.4.0"]);
    expect(rows.map((r) => r.installed)).toEqual([false, true, false]);
    expect(rows[2]).toMatchObject({ title: "Version 0.4.0", when: "2026-10-31", notes: ["Offline gauge charts"], missing: null });
  });
  it("a version with no notes is said missing, never a blank row", () => {
    expect(changelogVM(null, log)[0]).toMatchObject({ when: "Coming next", notes: [], missing: "No notes for this version." });
  });
  it("the shipped changelog: one entry per version, each with notes, newest first; the installed version has one", () => {
    const rows = changelogVM(null);
    expect(new Set(rows.map((r) => r.version)).size).toBe(CHANGELOG.length);
    expect(rows.every((r) => r.notes.length >= 1 && r.notes.length <= 5)).toBe(true);
    // (T734) by semver: 0.7.10 is newer than 0.7.9 (a string sort put it last)
    expect(rows.map((r) => r.version)).toEqual([...rows.map((r) => r.version)].sort((a, b) => compareSemver(b, a)));
  });
  it("(T723 G11) each platform reads only its own notes; Android's list is the file's, byte for byte", () => {
    const tagged: ChangelogEntry[] = [
      { version: "0.2.0", date: null, notes: ["Both", "Only Play has it"], only: { "Only Play has it": "android" as const } },
      { version: "0.1.0", date: null, notes: ["Only Play has it too"], only: { "Only Play has it too": "android" as const } },
    ];
    expect(changelogVM(null, tagged, "ios").map((r) => [r.version, r.notes])).toEqual([["0.2.0", ["Both"]]]);
    expect(changelogVM(null, tagged, "android").map((r) => [r.version, r.notes])).toEqual(
      [["0.2.0", ["Both", "Only Play has it"]], ["0.1.0", ["Only Play has it too"]]]);
    // the shipped log: Android sees every note of every entry exactly as written; no tag names a note that is not there
    expect(changelogVM(null).map((r) => [r.version, r.notes])).toEqual(
      [...CHANGELOG].sort((a, b) => compareSemver(b.version, a.version)).map((e) => [e.version, e.notes]));
    for (const e of CHANGELOG) for (const k of Object.keys(e.only ?? {})) expect(e.notes).toContain(k);
  });
  it("(T723 G11) the shipped notes an iPhone reads never name Google Play, Android or the APK", () => {
    const text = changelogVM(null, CHANGELOG, "ios").flatMap((r) => r.notes).join("\n");
    expect(/google play|android|\bapk\b/i.test(text)).toBe(false);
  });
  it("the prompt shows the incoming version's bullets: the version file's first, else the bundled entry", () => {
    expect(notesFor("0.6.0", ["From the server"], log)).toEqual(["From the server"]);
    expect(notesFor("0.6.0", null, log)).toEqual(["Gauge alerts"]);
    expect(notesFor("0.9.0", [], log)).toBeNull();
    const vm = appUpdateVM({ channel: "apk-page", check: { state: "available", version: "0.6.0", notes: ["Alerts", "Changelog"] },
                             tooOld: false, start: "idle" })!;
    expect(vm.whatsNew).toEqual({ title: "What's new in 0.6.0", notes: ["Alerts", "Changelog"], missing: null });
  });
  it("a version with no notes anywhere is said missing in the prompt", () => {
    const vm = appUpdateVM({ channel: "apk-page", check: { state: "available", version: "9.9.9" }, tooOld: false, start: "idle" })!;
    expect(vm.whatsNew).toEqual({ title: "What's new in 9.9.9", notes: null, missing: "No notes were published for version 9.9.9." });
    // Play names a version code: no version, no bullets section
    expect(appUpdateVM({ channel: "play", check: { state: "available", version: "57" }, tooOld: false, start: "idle" })!.whatsNew)
      .toBeNull();
  });
});

describe("(T234 v1.1) the new-app-version notice: once per version", () => {
  const on = { on: true, appVersion: true, toldAppVersion: null };
  it("a newer version is told once; the same again is not; the next one is", () => {
    const k = appVersionToTell(on, { version: "0.6.0", build: 14 }, own);
    expect(k).toBe("0.6.0+14");
    expect(appVersionToTell({ ...on, toldAppVersion: k }, { version: "0.6.0", build: 14 }, own)).toBeNull();
    expect(appVersionToTell({ ...on, toldAppVersion: k }, { version: "0.6.1", build: 15 }, own)).toBe("0.6.1+15");
  });
  it("never for this version or an older one, nor with a switch off", () => {
    expect(appVersionToTell(on, { version: "0.5.0", build: 12, sha: "abc1234" }, own)).toBeNull();
    expect(appVersionToTell(on, { version: "0.4.0", build: 20 }, own)).toBeNull();
    expect(appVersionToTell({ ...on, appVersion: false }, { version: "0.6.0" }, own)).toBeNull();
    expect(appVersionToTell({ ...on, on: false }, { version: "0.6.0" }, own)).toBeNull();
  });
  it("a file naming no version is a defect (thrown, reported by the caller)", () => {
    expect(() => appVersionToTell(on, { build: 3 }, own)).toThrow(/no app version/);
  });
});

describe("(T888) the popup on opening", () => {
  const avail = (v: string | null, notes: readonly string[] | null = null): AppUpdateCheck => ({ state: "available", version: v, notes });
  const popup = (channel: AppUpdateChannel, check: AppUpdateCheck, o: { tooOld?: boolean; popped?: string | null;
                                                                       dismissed?: string | null; held?: boolean;
                                                                       installed?: string } = {}) =>
    updatePopupVM({ vm: appUpdateVM({ channel, check, tooOld: !!o.tooOld, start: "idle" }), channel, check,
                    installed: o.installed ?? "0.8.1", popped: o.popped ?? null, dismissed: o.dismissed ?? null, held: !!o.held });
  const notes = ["The map draws faster", "A swimming coho greets you"];
  it("a newer version: the whole popup, composed — the version, its bullets, Update, Not now, the bell", () => {
    expect(popup("play", avail("0.8.2", notes))).toEqual({
      key: "0.8.2", strength: "gentle", title: "Version 0.8.2 is out", sub: null,
      whatsNew: { title: "What's new", notes, missing: null },
      button: { label: "Update", immediate: false }, said: null, failed: false,
      notNow: "Not now", later: "If you wait, I keep it under the bell.",
    });
    // the staging APK page: the bell's own button word
    expect(popup("apk-page", avail("0.8.2", notes))!.button!.label).toBe("Get the new test build");
    // iOS: the App Store's version, the bundled notes when the app has them, else said missing
    expect(popup("app-store", avail("9.9.9"))!.whatsNew).toEqual(
      { title: "What's new", notes: null, missing: "No notes were published for version 9.9.9." });
  });
  it("once per version: answered (kept or this run) it stays shut; a newer version comes up again", () => {
    expect(popup("play", avail("0.8.2"), { popped: "0.8.2" })).toBeNull();
    expect(popup("play", avail("0.8.2"), { dismissed: "0.8.2" })).toBeNull();
    expect(popup("play", avail("0.8.3"), { popped: "0.8.2", dismissed: "0.8.2" })!.key).toBe("0.8.3");
  });
  it("never when current, unchecked, failed, offline (no vm), named by a code only, held, or on the web", () => {
    expect(popup("play", { state: "current" })).toBeNull();
    expect(popup("play", { state: "unknown" })).toBeNull();
    expect(popup("play", { state: "failed", offline: false })).toBeNull();
    expect(popup("play", { state: "failed", offline: false, noPlay: true })).toBeNull();
    expect(updatePopupVM({ vm: null, channel: "play", check: avail("0.8.2"), installed: "0.8.1", popped: null, dismissed: null,
                           held: false })).toBeNull();
    expect(popup("play", avail("57"))).toBeNull();   // Play's version code with no version file: the bell only
    expect(popup("play", avail("0.8.2"), { held: true })).toBeNull();
    expect(popup("none", avail("0.8.2"))).toBeNull();
    expect(popup("none", avail("0.8.2"), { tooOld: true })).toBeNull();   // the web: the site updates itself
    expect([appUpdateOs("play"), appUpdateOs("apk-page"), appUpdateOs("app-store"), appUpdateOs("none")])
      .toEqual(["android", "android", "ios", null]);
  });
  it("the strong prompt keeps its rule: every opening, Not now for this run only, whatever the check said", () => {
    for (const check of [avail("0.8.2")]) {
      expect(popup("play", check, { tooOld: true, popped: "0.8.2" })).toMatchObject({
        key: "needed", strength: "strong", title: "New data needs the latest app",
        sub: "The app keeps using the data it has until it is updated.", button: { immediate: true } });
    }
    expect(popup("play", avail("0.8.2"), { tooOld: true })!.whatsNew!.title).toBe("What's new in 0.8.2");
    expect(popup("play", { state: "current" }, { tooOld: true, dismissed: "needed" })).toBeNull();
    expect(popup("play", { state: "current" }, { tooOld: true, held: true })).toBeNull();
  });
  it("only a version ABOVE the installed one pops, titled and kept by that version (finding 2, 5)", () => {
    // a kept check for the version now installed (updated from the bell, or by the store): no "Version 0.8.2 is out" on 0.8.2
    expect(popup("play", avail("0.8.2", notes), { installed: "0.8.2" })).toBeNull();
    expect(popup("play", avail("0.8.2", notes), { installed: "0.8.3" })).toBeNull();
    // a newer build of the SAME version (the staging page counts it newer): the bell says it, the popup does not
    expect(popup("apk-page", avail("0.8.2", notes), { installed: "0.8.2" })).toBeNull();
    expect(appUpdateVM({ channel: "apk-page", check: avail("0.8.2", notes), tooOld: false, start: "idle" })!.text)
      .toBe("Version 0.8.2 of the app is available.");
    expect(popup("apk-page", avail("0.8.2", notes), { installed: "0.8.1" })!.title).toBe("Version 0.8.2 is out");
  });
  it("(finding 1) the incoming version's list is the version file's: the platform's own, never looked up in the bundled log", () => {
    // a newer version is NEVER in the bundled log: these logs do not hold 0.8.2
    const log: ChangelogEntry[] = [{ version: "0.8.1", date: null, notes: ["Older"] }];
    expect(log.some((e) => e.version === "0.8.2")).toBe(false);
    expect(notesFor("0.8.2", ["Both"], log, "ios")).toEqual(["Both"]);
    expect(notesFor("0.8.2", ["Both", "Play only"], log, "android")).toEqual(["Both", "Play only"]);
    expect(notesFor("0.8.2", null, log, "ios")).toBeNull();
    expect(notesFor("0.8.2", [], log, "android")).toBeNull();
    // the popup, iPhone and Android, on the shipped log (which never holds 9.9.9): the file's list, whole
    expect(CHANGELOG.some((e) => e.version === "9.9.9")).toBe(false);
    expect(popup("app-store", avail("9.9.9", ["Both"]))!.whatsNew).toEqual({ title: "What's new", notes: ["Both"], missing: null });
    expect(popup("play", avail("9.9.9", notes))!.whatsNew!.notes).toEqual(notes);
    expect(popup("app-store", avail("9.9.9"))!.whatsNew!.missing).toBe("No notes were published for version 9.9.9.");
  });
  it("(finding 1) the one platform filter the build scripts write the version files with (`notesOn`)", () => {
    const e: ChangelogEntry = { version: "0.8.2", date: null, notes: ["Both", "Play only", "iPhone only"],
                                only: { "Play only": "android", "iPhone only": "ios" } };
    expect(notesOn(e, "android")).toEqual(["Both", "Play only"]);
    expect(notesOn(e, "ios")).toEqual(["Both", "iPhone only"]);
    expect(notesOn(e, "web")).toEqual(["Both"]);
    // the bundled fallback (a version the app holds) filters the same way
    expect(notesFor("0.8.2", null, [e], "ios")).toEqual(["Both", "iPhone only"]);
    expect(notesFor("0.8.2", null, [{ ...e, notes: ["Play only"] }], "ios")).toBeNull();
    // the shipped log: every Android-only note stays out of the iPhone's list
    for (const x of CHANGELOG) for (const [n, o] of Object.entries(x.only ?? {})) expect(notesOn(x, o === "android" ? "ios" : "android")).not.toContain(n);
  });
  it("(finding 1) where each channel's version file is: the iPhone's beside Play's, on our own server", () => {
    const i = { apkPage: "https://data-staging.example/app/android/", dataBase: "https://data.example/" };
    expect([appVersionFileUrl("apk-page", i), appVersionFileUrl("play", i), appVersionFileUrl("app-store", i),
            appVersionFileUrl("none", i)]).toEqual(["https://data-staging.example/app/android/latest.json",
      "https://data.example/app/android/play.json", "https://data.example/app/ios/appstore.json", null]);
  });
});

describe("(T894) the store channels read the version file of the build the store OFFERS", () => {
  const BASE = "https://data.example/";
  // the bucket as the review found it: play.json / appstore.json name the LAST upload (0.8.3, closed testing / TestFlight)
  // while the store offers this phone 0.8.2 (production: versionCode 30, App Store 0.8.2)
  const BUCKET: Record<string, unknown> = {
    "https://data.example/app/android/play.json": { version: "0.8.3", build: 31, sha: "b", date: null, notes: ["Three"] },
    "https://data.example/app/android/play/30.json": { version: "0.8.2", build: 30, sha: "a", date: null, notes: ["Two"] },
    "https://data.example/app/android/play/31.json": { version: "0.8.3", build: 31, sha: "b", date: null, notes: ["Three"] },
    "https://data.example/app/ios/appstore.json": { version: "0.8.3", build: 40, sha: "b", date: null, notes: ["Three"] },
    "https://data.example/app/ios/0.8.2.json": { version: "0.8.2", build: 39, sha: "a", date: null, notes: ["Two"] },
  };
  const run = async (channel: "play" | "app-store", u: { updateAvailable: boolean; storeVersion?: string | null },
                     bucket: Record<string, unknown> = BUCKET) => {
    const read: string[] = [], reported: unknown[] = [];
    const r = await storeUpdateCheck(channel, u, { dataBase: BASE, reportFailure: (e) => reported.push(e),
      readJson: async (url) => { read.push(url); if (!(url in bucket)) throw new Error(`HTTP 404 for ${url}`); return bucket[url]; } });
    return { r, read, reported };
  };

  it("Play: offered versionCode 30 says 0.8.2 and its bullets, never play.json's 0.8.3 (the popup's once-key is 0.8.2)", async () => {
    const { r, read, reported } = await run("play", { updateAvailable: true, storeVersion: "30" });
    expect(r).toEqual({ available: true, version: "0.8.2", notes: ["Two"] });
    expect(read).toEqual(["https://data.example/app/android/play/30.json"]);
    expect(reported).toEqual([]);
    // the popup pops 0.8.2 (what Update installs), so a later 0.8.3 still pops
    const check: AppUpdateCheck = { state: "available", version: r.version, notes: r.notes };
    const p = updatePopupVM({ vm: appUpdateVM({ channel: "play", check, tooOld: false, start: "idle" }), channel: "play", check,
                              installed: "0.8.1", popped: null, dismissed: null, held: false });
    expect([p?.key, p?.title]).toEqual(["0.8.2", W.popup.title("0.8.2")]);
  });
  it("Play: a file naming another build than the offered code is reported; the notice stands with no version (bell only)", async () => {
    const { r, reported } = await run("play", { updateAvailable: true, storeVersion: "30" },
      { "https://data.example/app/android/play/30.json": { version: "0.8.3", build: 31, notes: ["Three"] } });
    expect(r).toEqual({ available: true, version: null });
    expect(String(reported[0])).toContain("names build 31 (version 0.8.3), not the offered 30");
  });
  it("Play: a missing file is reported, the notice stands; an update in progress (no code) reads nothing", async () => {
    const miss = await run("play", { updateAvailable: true, storeVersion: "32" });
    expect(miss.r).toEqual({ available: true, version: null });
    expect(String(miss.reported[0])).toContain("HTTP 404 for https://data.example/app/android/play/32.json");
    const busy = await run("play", { updateAvailable: true });
    expect(busy).toEqual({ r: { available: true, version: null }, read: [], reported: [] });
    expect((await run("play", { updateAvailable: false, storeVersion: "30" })).r).toEqual({ available: false, version: null });
  });
  it("App Store: the store's 0.8.2 reads app/ios/0.8.2.json, not appstore.json (a TestFlight 0.8.3 ahead of it)", async () => {
    const { r, read, reported } = await run("app-store", { updateAvailable: true, storeVersion: "0.8.2" });
    expect(r).toEqual({ available: true, version: "0.8.2", notes: ["Two"] });
    expect(read).toEqual(["https://data.example/app/ios/0.8.2.json"]);
    expect(reported).toEqual([]);
  });
  it("App Store: the version-equality guard — a file naming another version gives no bullets (reported), the store's version stands", async () => {
    const { r, reported } = await run("app-store", { updateAvailable: true, storeVersion: "0.8.2" },
      { "https://data.example/app/ios/0.8.2.json": { version: "0.8.3", build: 40, notes: ["Three"] } });
    expect(r).toEqual({ available: true, version: "0.8.2" });
    expect(String(reported[0])).toContain("names version 0.8.3, not the App Store's 0.8.2");
    const miss = await run("app-store", { updateAvailable: true, storeVersion: "0.9.0" });
    expect(miss.r).toEqual({ available: true, version: "0.9.0" });
    expect(miss.reported).toHaveLength(1);
  });
  it("the store's word becomes a path only when it is plain digits / x.y.z", () => {
    expect(storeVersionFileUrl("play", BASE, "30")).toBe("https://data.example/app/android/play/30.json");
    expect(storeVersionFileUrl("app-store", BASE, "0.8.2")).toBe("https://data.example/app/ios/0.8.2.json");
    for (const bad of [null, undefined, "", "0", "../x", "30/../31", "1.2.3-beta", "1.2.3/x"]) {
      expect(storeVersionFileUrl("play", BASE, bad)).toBeNull();
      expect(storeVersionFileUrl("app-store", BASE, bad)).toBeNull();
    }
  });
});

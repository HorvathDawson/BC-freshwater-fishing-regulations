/**
 * (T390, owner 2026-11-04) A NEWER APP, IN WORDS AND DECISIONS: the app notices a newer version of itself and offers ONE
 * Update button, in the bell's sheet. (T888, owner 2026-11-18: "When a user opens the app and a new app version is
 * available it should give a popup with the swimming fish and the new update list. That they need to dismiss (then it
 * goes to Bell) for both apple and android .") On a phone the version is ALSO said once in a popup (`updatePopupVM`): the
 * same view model, its own words (`APP_UPDATE_WORDS.popup`), once per version; the bell keeps it as before.
 *
 *   gentle   a newer version exists (Play's in-app update check, the App Store lookup, or the staging APK page's
 *            `latest.json`): "A new version of the app is available." — a flexible update on Play; it counts too
 *   strong   a data release was refused because it states a `minAppVersion` above this app (T251, the update service's
 *            `app-too-old`): "New data needs the latest app." — an immediate update on Play; it counts on the bell
 *
 * Where the button goes is the CHANNEL (`appUpdateChannel`): Play's in-app update for a Play install, the App Store page
 * on iOS (once the listing exists, T88), the staging APK page for a sideloaded test build; none on the web (the site is
 * always the latest) or a local build. The check runs on every opening and every return to the app (T390 2026-11-05: a
 * daily cap hid 0.6.1): the APK page's `latest.json`; on Play, Play's own check (owner 2026-11-06: only once Play offers
 * this phone the build — our `play.json`, written at upload, said 0.6.4 before Play had it; it gives the bullets), never
 * offline (`shouldCheckAppUpdate`); offline says nothing (`appUpdateVM`). A check or a start that fails is shown here and reported by the hook
 * (`@app/ui` `useAppUpdate`, T368).
 */
import { compareSemver, parseSemver, type AppVersion } from "./appVersion";
import { CHANGELOG, notesOn, type ChangelogEntry } from "./changelog";

export type AppUpdateChannel = "play" | "app-store" | "apk-page" | "none";

export const APP_UPDATE_WORDS = {
  title: "App update",
  /** the gentle prompt: a newer version exists */
  available: "A new version of the app is available.",
  availableVersion: (v: string) => `Version ${v} of the app is available.`,
  /** the strong prompt: a release was refused for an app older than it needs */
  needed: "New data needs the latest app.",
  neededSub: "The app keeps using the data it has until it is updated.",
  /** the one button, everywhere */
  update: "Update",
  /** a sideloaded staging build: the button opens the test-build page */
  getBuild: "Get the new test build",
  /** after the button, by what happened */
  downloading: "Downloading the update. The app restarts when it is ready.",
  opened: "The store page is open.",
  notYet: "No newer version is available for this phone yet.",
  /** our version file names a newer build that Play does not offer this phone yet: the button opened the listing */
  playPreparing: "Play is still preparing this version for your phone. The Play listing is open.",
  working: "Starting the update…",
  /** failures, said under the button (the hook reports them) */
  checkFailed: "Could not check for a newer version of the app.",
  /** (T595) a phone with no Google Play to answer (an emulator, a de-Googled phone): a known state, not a failure */
  noPlay: "This phone has no Google Play to ask, so the app cannot check for a newer version.",
  startFailed: "The update could not start.",
  offline: "No connection. Try again when you are online.",
  /** the bell's label aloud */
  label: { available: "A new version of the app is available", needed: "New data needs the latest app" },
  /** (T234 v1.1) the incoming version's bullets under the prompt */
  whatsNew: (v: string) => `What's new in ${v}`,
  whatsNewMissing: (v: string) => `No notes were published for version ${v}.`,
  /** (T888) the popup on opening: the version, its bullets, Update and Not now (then the bell keeps it) */
  popup: {
    title: (v: string) => `Version ${v} is out`,
    /** the strong prompt's title (a release this app is too old for), with or without a version named */
    neededTitle: "New data needs the latest app",
    whatsNew: "What's new",
    notNow: "Not now",
    later: "If you wait, I keep it under the bell.",
  },
} as const;

/** (T234 v1.1) The changelog page (More → About → What's new). */
export const CHANGELOG_WORDS = {
  title: "What's new",
  open: "What's new in each version",
  openHint: "Every version of the app, newest first",
  installed: "Installed",
  version: (v: string) => `Version ${v}`,
  date: (d: string) => d,
  notCut: "Coming next",
  noNotes: "No notes for this version.",
} as const;

/** (T234 v1.1) The new-app-version notification (opt-in, once per version). */
export const APP_VERSION_NOTE = {
  title: "A new version of the app is out",
  body: (v: string) => `Version ${v} is ready. Tap to see what's new and update.`,
} as const;

/** The bullets for a version: the version file's own (it travels with a version not installed yet), else the bundled
 *  changelog's; null when neither has any (shown as missing, never blank). (T888) The file's list is already its
 *  platform's own (the build scripts write it with `notesOn`: Android's in `latest.json` / `play.json`, the iPhone's in
 *  `appstore.json`), since a newer version is never in the bundled log; `os` filters the bundled fallback the same way. */
export function notesFor(version: string | null, fromFile: readonly string[] | null | undefined,
                         log: readonly ChangelogEntry[] = CHANGELOG, os?: "android" | "ios"): readonly string[] | null {
  const own = (fromFile ?? []).filter((n) => typeof n === "string" && n.trim());
  if (own.length) return own;
  const e = version ? log.find((x) => x.version === version) : undefined;
  const logged = e ? (os ? notesOn(e, os) : e.notes) : [];
  return logged.length ? logged : null;
}

/** (T888) The phone a channel serves: Play and the staging APK page are Android's, the App Store iOS's; none (the web, a
 *  local build) has no popup and no platform notes. */
export function appUpdateOs(channel: AppUpdateChannel): "android" | "ios" | null {
  return channel === "play" || channel === "apk-page" ? "android" : channel === "app-store" ? "ios" : null;
}

export interface ChangelogRowVM {
  readonly version: string;
  readonly title: string;
  /** the day it was cut, or "Coming next" */
  readonly when: string;
  readonly installed: boolean;
  readonly notes: readonly string[];
  /** said when a version has no notes (never a blank row) */
  readonly missing: string | null;
}

/** Every version, NEWEST FIRST (by semver, whatever order the file is in); the installed one marked. */
export function changelogVM(installed: string | null, log: readonly ChangelogEntry[] = CHANGELOG,
                            os: "android" | "ios" | "web" = "android"): readonly ChangelogRowVM[] {
  const W = CHANGELOG_WORDS;
  // (T723 G11) a note tagged for the other platform is not shown; a version whose every note was the other's is not listed
  const mine = (e: ChangelogEntry): readonly string[] => notesOn(e, os);
  return [...log].filter((e) => parseSemver(e.version) && (e.notes.length === 0 || mine(e).length > 0))
    .sort((a, b) => compareSemver(b.version, a.version)).map((e) => {
      const notes = mine(e);
      return {
        version: e.version, title: W.version(e.version), when: e.date ? W.date(e.date) : W.notCut,
        installed: installed !== null && compareSemver(e.version, installed) === 0,
        notes, missing: notes.length ? null : W.noNotes,
      };
    });
}

/**
 * (T234 v1.1) WHERE THIS BUILD LEARNS OF A NEWER VERSION OF ITSELF, ON OUR OWN SERVER (owner: self-host everything):
 *   apk-page  `latest.json` beside the staging APK page (build-apk.sh writes it with the APK)
 *   play      `<data origin>/app/android/play.json` (build-apk.sh --play writes it after the Play upload)
 *   app-store `<data origin>/app/ios/appstore.json` (T888: build-ios-testflight.sh writes it after the upload)
 *   none: no file (a local build, the web) — null.
 * The background check reads it for the new-version notice (the sideloaded APK only: the stores tell their own installs).
 * (T894) play.json / appstore.json name the LAST UPLOAD, on any track: a store build from 0.8.2 on reads the file of the
 * build the store offers instead (`storeVersionFileUrl`); the scripts still write these two for 0.8.1 / 0.8.2 installs.
 */
export function appVersionFileUrl(channel: AppUpdateChannel, i: { apkPage: string | null; dataBase: string }): string | null {
  if (channel === "apk-page" && i.apkPage) return new URL("latest.json", i.apkPage).toString();
  const base = i.dataBase.replace(/\/+$/, "");
  if (channel === "play") return `${base}/app/android/play.json`;
  if (channel === "app-store") return `${base}/app/ios/appstore.json`;
  return null;
}

/** The version file our server publishes beside the app (`latest.json` sideload / `play.json` Play, build-apk.sh;
 *  `appstore.json` iPhone, build-ios-testflight.sh). */
export interface AppVersionFile extends ApkLatest { readonly notes?: readonly string[] | null }

/**
 * (T894) THE STORE BUILD'S OWN VERSION FILE, named by what the store OFFERS this phone — never "the last upload":
 *   play       `<data origin>/app/android/play/<versionCode>.json` (Play's `availableVersionCode`; build-apk.sh --play
 *              writes one per upload). A build uploaded to internal and promoted in Play Console keeps its code, so the
 *              file follows it to every track; a per-track file would not (the owner promotes, the script never knows).
 *   app-store  `<data origin>/app/ios/<version>.json` (the App Store lookup's version; build-ios-testflight.sh writes one
 *              per upload), so a TestFlight upload ahead of the store's version never hides the store version's notes.
 * Null when the store names nothing usable (Play's in-progress update reports no code; a code or version that is not
 * plain digits / x.y.z never becomes a path).
 */
export function storeVersionFileUrl(channel: "play" | "app-store", dataBase: string, offered: string | null | undefined): string | null {
  const base = dataBase.replace(/\/+$/, "");
  const o = (offered ?? "").trim();
  if (channel === "play") return /^[1-9]\d{0,9}$/.test(o) ? `${base}/app/android/play/${o}.json` : null;
  return /^\d+\.\d+(\.\d+)?$/.test(o) ? `${base}/app/ios/${o}.json` : null;
}

/** What the store's own update check says (expo-in-app-updates `checkForUpdate`): on Android `storeVersion` is the
 *  versionCode Play offers, on iOS the App Store's version string. */
export interface StoreUpdateInfo { readonly updateAvailable: boolean; readonly storeVersion?: string | null }

/**
 * (T894) THE STORE CHANNELS' CHECK: the store decides whether a newer build is there; the version file of THE BUILD IT
 * OFFERS gives the version said (Play: the file's version, only when its `build` is the offered code; the App Store: its
 * own version) and the bullets (only when the file names that build / version). The popup's once-key is this version,
 * so it is always the version Update installs. A failed read, or a file naming another build, is reported and the
 * notice still stands: on Play without a version (the bell only, no popup), on the App Store with no notes.
 */
export async function storeUpdateCheck(channel: "play" | "app-store", u: StoreUpdateInfo,
                                       i: { dataBase: string; readJson: (url: string) => Promise<unknown>;
                                                /** core `reportFailure` (the app passes it; a test collects) */ reportFailure: (e: unknown) => void }):
  Promise<{ available: boolean; version: string | null; notes?: readonly string[] | null }> {
  if (!u.updateAvailable) return { available: false, version: null };
  const offered = (u.storeVersion ?? "").trim() || null;
  const said = channel === "app-store" ? offered : null;
  const url = storeVersionFileUrl(channel, i.dataBase, offered);
  if (!url) return { available: true, version: said };
  try {
    const l = await i.readJson(url) as Partial<AppVersionFile> | null;
    const v = l && typeof l.version === "string" && parseSemver(l.version) ? l.version : null;
    const notes = Array.isArray(l?.notes) ? l.notes : null;
    if (channel === "play") {
      if (v && typeof l?.build === "number" && String(l.build) === offered) return { available: true, version: v, notes };
      throw new Error(`${url} names build ${String(l?.build)} (version ${String(l?.version)}), not the offered ${offered}`);
    }
    if (v === offered) return { available: true, version: offered, notes };
    throw new Error(`${url} names version ${String(l?.version)}, not the App Store's ${offered}`);
  } catch (e) { i.reportFailure(e); }
  return { available: true, version: said };
}

/**
 * (T234 v1.1) THE NEW-APP-VERSION NOTICE: the key of a version to tell now, or null. Only with notifications and the
 * switch on, only a version newer than this install (`apkIsNewer`: semver, then build, then commit), once per version
 * (`told`). A file that names no version throws (the caller reports it).
 */
export function appVersionToTell(s: { on: boolean; appVersion: boolean; toldAppVersion: string | null },
                                 latest: unknown, own: AppVersion): string | null {
  if (!s.on || !s.appVersion) return null;
  if (!apkIsNewer(latest, own)) return null;
  const l = latest as AppVersionFile;
  const key = `${l.version}+${typeof l.build === "number" ? l.build : ""}`;
  return key === s.toldAppVersion ? null : key;
}

/**
 * Which way this build updates. `distribution` is the build's own word (`EXPO_PUBLIC_APP_DISTRIBUTION`: "play" for a
 * Play upload, "apk-page" for a sideloaded staging APK — `deploy/staging/build-apk.sh` sets it); a production Android
 * build without one is a Play build. iOS has a channel only once its App Store listing is named (T88).
 */
export function appUpdateChannel(i: { os: string; env: string; distribution?: string | null; apkPage?: string | null;
                                      appStoreUrl?: string | null }): AppUpdateChannel {
  const d = (i.distribution ?? "").trim();
  if (i.os === "android") {
    if (d === "play") return "play";
    if (d === "apk-page") return i.apkPage ? "apk-page" : "none";
    if (d) throw new Error(`EXPO_PUBLIC_APP_DISTRIBUTION "${d}" is not play or apk-page`);
    return i.env === "production" ? "play" : i.apkPage ? "apk-page" : "none";
  }
  if (i.os === "ios") return i.appStoreUrl ? "app-store" : "none";
  return "none";
}

/** What the last check found, as kept on the device: the device day it was asked, and the answer. */
export interface AppUpdateSaved {
  readonly day: string;
  readonly available: boolean;
  readonly version: string | null;
  /** (T234 v1.1) the incoming version's bullets, from the version file */
  readonly notes?: readonly string[] | null;
  /** (T888) the version whose popup was answered (Not now, or Update started): it does not pop up again */
  readonly popped?: string | null;
}

/** The kept check, or null for anything else (nothing kept, an old shape). */
export function readAppUpdateSaved(text: string | null): AppUpdateSaved | null {
  if (!text) return null;
  try {
    const o = JSON.parse(text) as Partial<AppUpdateSaved>;
    if (typeof o.day !== "string" || typeof o.available !== "boolean") return null;
    return { day: o.day, available: o.available, version: typeof o.version === "string" ? o.version : null,
             notes: Array.isArray(o.notes) ? o.notes.filter((n): n is string => typeof n === "string") : null,
             popped: typeof o.popped === "string" ? o.popped : null };
  } catch { return null; }
}

/** Check now? Never offline, never without a channel; otherwise on every opening and every return to the app (no day
 *  cap: the check is our own small version file, T390 2026-11-05). */
export function shouldCheckAppUpdate(i: { channel: AppUpdateChannel; offline: boolean }): boolean {
  return i.channel !== "none" && !i.offline;
}

/** The staging APK page's `latest.json` (written by `deploy/staging/build-apk.sh` beside `latest.apk`). */
export interface ApkLatest { readonly version: string; readonly build: number | null; readonly sha: string | null;
                             readonly date: string | null }

/**
 * Is the APK on the staging page newer than this build? A higher semver, else the same semver with a higher build
 * number, else the same both from another commit built on a later (or the same) day. A page that names no version is
 * a defect (it throws: shown and reported).
 */
export function apkIsNewer(latest: unknown, own: AppVersion): boolean {
  const l = latest as Partial<ApkLatest> | null;
  if (!l || typeof l.version !== "string" || !parseSemver(l.version)) throw new Error("latest.json names no app version");
  const c = compareSemver(l.version, own.version);
  if (c !== 0) return c > 0;
  const b = typeof l.build === "number" ? l.build : null;
  if (b !== null && own.build !== null && b !== own.build) return b > own.build;
  if (!l.sha || !own.sha || l.sha.slice(0, 7) === own.sha) return false;
  return !own.date || !l.date || l.date >= own.date;
}

/** The check's state as the hook holds it. */
export type AppUpdateCheck =
  | { readonly state: "unknown" }
  | { readonly state: "current" }
  | { readonly state: "available"; readonly version: string | null; readonly notes?: readonly string[] | null }
  | { readonly state: "failed"; readonly offline: boolean; readonly noPlay?: boolean };

/**
 * (T595) The Play check could not run because this phone has no Play service to bind ("Failed to bind to the service":
 * an emulator image without the Play Store, a phone without Google services). A STATE the app knows and says
 * (`APP_UPDATE_WORDS.noPlay`), never an error report.
 */
export function isNoPlayService(e: unknown): boolean {
  const x = e as { name?: unknown; message?: unknown } | null;
  return !!x && typeof x === "object"
    && (x.name === "NoPlayService" || (typeof x.message === "string" && /Failed to bind to the service/i.test(x.message)));
}

/** What the button last did. */
export type AppUpdateStart = "idle" | "working" | "downloading" | "opened" | "listing" | "none" | "failed" | "offline";

export interface AppUpdateVM {
  readonly strength: "gentle" | "strong";
  readonly title: string;
  readonly text: string;
  /** a second line (the strong prompt: the data in use is kept) */
  readonly sub: string | null;
  /** the bell's label aloud */
  readonly label: string;
  /** counts toward the bell's badge (both prompts do; a failed check is said only when the sheet is open) */
  readonly counts: boolean;
  /** the one button, or null (no channel; a start under way) */
  readonly button: { readonly label: string; readonly immediate: boolean } | null;
  /** what the button did, or a failure, under it; null: nothing to say */
  readonly said: string | null;
  /** `said` is a failure (drawn as one) */
  readonly failed: boolean;
  /** (T234 v1.1) the incoming version's bullets: "What's new in 0.6.0", or said missing; null: no version named */
  readonly whatsNew: { readonly title: string; readonly notes: readonly string[] | null; readonly missing: string | null } | null;
}

/**
 * The bell's "App update" section, or null when there is nothing to say: the app is current, the check has not run, or
 * the phone is offline. `tooOld`: a release was refused for this app's version (the strong prompt, whatever the check
 * said — Play may not have the build yet, and the reader is told so when they press).
 */
export function appUpdateVM(i: { channel: AppUpdateChannel; check: AppUpdateCheck; tooOld: boolean;
                                 start: AppUpdateStart }): AppUpdateVM | null {
  const vm = appUpdateVMBare(i);
  if (!vm) return null;
  const v = i.check.state === "available" && i.check.version && parseSemver(i.check.version) ? i.check.version : null;
  if (!v) return { ...vm, whatsNew: null };
  const notes = notesFor(v, i.check.state === "available" ? i.check.notes : null, CHANGELOG, appUpdateOs(i.channel) ?? undefined);
  return { ...vm, whatsNew: { title: APP_UPDATE_WORDS.whatsNew(v), notes, missing: notes ? null : APP_UPDATE_WORDS.whatsNewMissing(v) } };
}

function appUpdateVMBare(i: { channel: AppUpdateChannel; check: AppUpdateCheck; tooOld: boolean;
                              start: AppUpdateStart }): Omit<AppUpdateVM, "whatsNew"> | null {
  const W = APP_UPDATE_WORDS;
  const said = i.start === "working" ? W.working : i.start === "downloading" ? W.downloading
    : i.start === "opened" ? W.opened : i.start === "listing" ? W.playPreparing : i.start === "none" ? W.notYet : i.start === "failed" ? W.startFailed
    : i.start === "offline" ? W.offline : null;
  const startFailed = i.start === "failed" || i.start === "offline";
  const label = i.channel === "apk-page" ? W.getBuild : W.update;
  const button = (immediate: boolean) => i.channel === "none" || i.start === "working" || i.start === "downloading"
    ? null : { label, immediate };
  if (i.tooOld) {
    return { strength: "strong", title: W.title, text: W.needed, sub: W.neededSub, label: W.label.needed, counts: true,
             button: button(true), said, failed: startFailed };
  }
  if (i.channel === "none") return null;
  if (i.check.state === "available") {
    const v = i.check.version && parseSemver(i.check.version) ? i.check.version : null;
    return { strength: "gentle", title: W.title, text: v ? W.availableVersion(v) : W.available, sub: null,
             label: W.label.available, counts: true, button: button(false), said, failed: startFailed };
  }
  // (T595) no Play service on this phone: said as what it is, with no button
  if (i.check.state === "failed" && i.check.noPlay) {
    return { strength: "gentle", title: W.title, text: W.noPlay, sub: null, label: W.noPlay, counts: false,
             button: null, said: null, failed: false };
  }
  // a check that failed for a reason other than the connection: said, quietly, with no button
  if (i.check.state === "failed" && !i.check.offline) {
    return { strength: "gentle", title: W.title, text: W.checkFailed, sub: null, label: W.checkFailed, counts: false,
             button: null, said: null, failed: true };
  }
  return null;
}

/** (T888) The popup on opening. */
export interface UpdatePopupVM {
  /** what Not now records: the version (gentle); the strong prompt's is `NEEDED_KEY`, kept for this run only */
  readonly key: string;
  readonly strength: "gentle" | "strong";
  readonly title: string;
  /** the strong prompt's second line (the data in use is kept) */
  readonly sub: string | null;
  /** the version's bullets ("What's new"), or said missing; null: no version named */
  readonly whatsNew: { readonly title: string; readonly notes: readonly string[] | null; readonly missing: string | null } | null;
  /** the bell's one button and what it did, as the bell has them */
  readonly button: AppUpdateVM["button"];
  readonly said: string | null;
  readonly failed: boolean;
  readonly notNow: string;
  /** under Not now: the bell keeps it */
  readonly later: string;
}

/** (T888) the strong prompt's key: never written down, so it pops up again on the next opening */
export const NEEDED_KEY = "needed";

/**
 * (T888) THE POPUP ON OPENING, or null. Built from the bell's own view model (`appUpdateVM`), so the two say the same
 * version, bullets and button. Rules:
 *   - a phone only: a channel with a platform (`appUpdateOs`); never the web, which updates itself, or a local build;
 *   - `held`: never over the first-run notice, the walkthrough (due or under way) or another sheet that opened by itself;
 *     it waits, and comes up once they are done (the first download is before the app is up at all);
 *   - gentle (a newer version): only a version the check NAMES (a semver) ABOVE the `installed` one, once per version:
 *     `popped` (kept on the device) or `dismissed` (this run) equal to it hides it; a newer version later pops up again.
 *     The title and the once-key are that version, so what pops is what the title says: a newer build of the SAME version
 *     (the staging page, `apkIsNewer`) is the bell's only, and a kept check for the version now installed (updated from
 *     the bell, or by the store itself) never says "Version 0.8.2 is out" on 0.8.2. A check that failed, or one that names
 *     no version (Play's code with no version file): the bell only;
 *   - strong (a release this app is too old for, T251): its rule is unchanged — it stands until the app is updated — so
 *     it pops up on every opening; Not now hides it for this run (`dismissed` = `NEEDED_KEY`), never for good.
 */
export function updatePopupVM(i: { vm: AppUpdateVM | null; channel: AppUpdateChannel; check: AppUpdateCheck;
                                   /** this app's own version (`AppVersion.version`) */
                                   installed: string;
                                   popped: string | null; dismissed: string | null; held: boolean }): UpdatePopupVM | null {
  const P = APP_UPDATE_WORDS.popup;
  const vm = i.vm;
  if (!vm || i.held || appUpdateOs(i.channel) === null) return null;
  const v = i.check.state === "available" && i.check.version && parseSemver(i.check.version) ? i.check.version : null;
  const base = { sub: vm.sub, button: vm.button, said: vm.said, failed: vm.failed, notNow: P.notNow, later: P.later };
  if (vm.strength === "strong") {
    if (i.dismissed === NEEDED_KEY) return null;
    // the reason is the title; the version, when the check names one, heads its bullets ("What's new in 0.8.2")
    return { key: NEEDED_KEY, strength: "strong", title: P.neededTitle, whatsNew: vm.whatsNew, ...base };
  }
  if (i.check.state !== "available" || !v || i.popped === v || i.dismissed === v) return null;
  if (!parseSemver(i.installed) || compareSemver(v, i.installed) <= 0) return null;
  // the version is the title; its bullets are headed "What's new"
  return { key: v, strength: "gentle", title: P.title(v), whatsNew: vm.whatsNew ? { ...vm.whatsNew, title: P.whatsNew } : null,
           ...base };
}

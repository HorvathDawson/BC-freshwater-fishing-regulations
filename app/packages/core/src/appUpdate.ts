/**
 * (T390, owner 2026-11-04) A NEWER APP, IN WORDS AND DECISIONS: the app notices a newer version of itself and offers ONE
 * Update button, in the bell's sheet (never a modal on open).
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
import { CHANGELOG, type ChangelogEntry } from "./changelog";

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
  startFailed: "The update could not start.",
  offline: "No connection. Try again when you are online.",
  /** the bell's label aloud */
  label: { available: "A new version of the app is available", needed: "New data needs the latest app" },
  /** (T234 v1.1) the incoming version's bullets under the prompt */
  whatsNew: (v: string) => `What's new in ${v}`,
  whatsNewMissing: (v: string) => `No notes were published for version ${v}.`,
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
 *  changelog's; null when neither has any (shown as missing, never blank). */
export function notesFor(version: string | null, fromFile: readonly string[] | null | undefined,
                         log: readonly ChangelogEntry[] = CHANGELOG): readonly string[] | null {
  const own = (fromFile ?? []).filter((n) => typeof n === "string" && n.trim());
  if (own.length) return own;
  const e = version ? log.find((x) => x.version === version) : undefined;
  return e && e.notes.length ? e.notes : null;
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
export function changelogVM(installed: string | null, log: readonly ChangelogEntry[] = CHANGELOG): readonly ChangelogRowVM[] {
  const W = CHANGELOG_WORDS;
  return [...log].filter((e) => parseSemver(e.version)).sort((a, b) => compareSemver(b.version, a.version)).map((e) => ({
    version: e.version, title: W.version(e.version), when: e.date ? W.date(e.date) : W.notCut,
    installed: installed !== null && compareSemver(e.version, installed) === 0,
    notes: e.notes, missing: e.notes.length ? null : W.noNotes,
  }));
}

/**
 * (T234 v1.1) WHERE THIS BUILD LEARNS OF A NEWER VERSION OF ITSELF, ON OUR OWN SERVER (owner: self-host everything):
 *   apk-page  `latest.json` beside the staging APK page (build-apk.sh writes it with the APK)
 *   play      `<data origin>/app/android/play.json` (build-apk.sh --play writes it after the Play upload)
 *   app-store / none: no file (iOS waits for T88; a local build has no channel) — null.
 * The background check reads it for the new-version notice; the foreground check for Play reads it for the bullets.
 */
export function appVersionFileUrl(channel: AppUpdateChannel, i: { apkPage: string | null; dataBase: string }): string | null {
  if (channel === "apk-page" && i.apkPage) return new URL("latest.json", i.apkPage).toString();
  if (channel === "play") return `${i.dataBase.replace(/\/+$/, "")}/app/android/play.json`;
  return null;
}

/** The version file our server publishes beside the app (`latest.json` sideload / `play.json` Play, build-apk.sh). */
export interface AppVersionFile extends ApkLatest { readonly notes?: readonly string[] | null }

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
}

/** The kept check, or null for anything else (nothing kept, an old shape). */
export function readAppUpdateSaved(text: string | null): AppUpdateSaved | null {
  if (!text) return null;
  try {
    const o = JSON.parse(text) as Partial<AppUpdateSaved>;
    if (typeof o.day !== "string" || typeof o.available !== "boolean") return null;
    return { day: o.day, available: o.available, version: typeof o.version === "string" ? o.version : null,
             notes: Array.isArray(o.notes) ? o.notes.filter((n): n is string => typeof n === "string") : null };
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
  | { readonly state: "failed"; readonly offline: boolean };

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
  const notes = notesFor(v, i.check.state === "available" ? i.check.notes : null);
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
  // a check that failed for a reason other than the connection: said, quietly, with no button
  if (i.check.state === "failed" && !i.check.offline) {
    return { strength: "gentle", title: W.title, text: W.checkFailed, sub: null, label: W.checkFailed, counts: false,
             button: null, said: null, failed: true };
  }
  return null;
}

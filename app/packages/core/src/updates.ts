/**
 * (T231) THE DATA UPDATES IN WORDS: the update service (`@app/data` `createUpdateService`) checks four things on the
 * data origin — the live gauge readings, the DFO salmon feed, the in-season changes feed and the data release — and
 * these are its words, whole, for the three places that say them:
 *
 *   updateSourcesVM   one line per source (last checked, last updated, its failure), and one line for the More tab's
 *                     Data block ("Updates · checked 3 min ago"); a download under way or waiting for Wi-Fi is said
 *   syncErrorVM       the sheet the red-badged sync icon opens: what failed, when each last succeeded, Retry
 *   updatedToast      the line said when a downloaded release is put in use ("Updated to Oct 27")
 *
 * The sources' names are the Sources page's (`MENU_WORDS.sources.names`); the release is named for what it carries.
 * Times are THIS DEVICE's (when it reached the origin, when new data landed), never the publisher's.
 */
import { agoWords } from "./freshness";
import { isoDayWords, MENU_WORDS } from "./menu";

export type UpdateSourceKey = "gauges" | "dfo" | "inseason" | "release";

export const UPDATE_WORDS = {
  names: {
    gauges: MENU_WORDS.sources.names.feed,
    dfo: MENU_WORDS.sources.names.dfo,
    inseason: MENU_WORDS.sources.names.inseason,
    release: "Regulations, map and salmon data",
  } satisfies Record<UpdateSourceKey, string>,
  /** the names in a sentence, lower-cased after the first word ("live gauge readings") */
  inline: {
    gauges: "the live gauge readings", dfo: "the DFO salmon feed", inseason: "the in-season changes",
    release: "the data release",
  } satisfies Record<UpdateSourceKey, string>,
  row: {
    title: "Updates",
    checked: (ago: string) => `Checked ${ago}`,
    notChecked: "Not checked yet",
    updated: (ago: string) => `updated ${ago}`,
    notUpdated: "not updated on this device yet",
    downloading: (pct: number) => `Downloading an update, ${pct}%`,
    waitingWifi: "An update is waiting for Wi-Fi",
    ready: "An update is ready; it is used at the next pause",
    /** (T251) the release states a minAppVersion newer than this app: the data in use is kept */
    appTooOld: "Update the app to get new regulations",
    failed: (n: number) => (n === 1 ? "1 source could not be updated" : `${n} sources could not be updated`),
  },
  sheet: {
    title: "Could not update",
    sub: "The app keeps using the data it has.",
    offline: "No connection",
    server: "The server did not answer",
    bytes: "A download did not match its checksum",
    other: "The download could not be read",
    lastOk: (ago: string) => `Last succeeded ${ago}`,
    neverOk: "Not reached from this device yet",
    retry: "Retry",
    retrying: "Retrying…",
    aloud: (name: string, problem: string, last: string) => `${name}: ${problem}. ${last}.`,
    notOpen: "The release is not open yet",
  },
  cellular: {
    label: "Large updates on mobile data",
    on: "Allowed",
    off: "Wi-Fi only",
    hint: "Map and regulation updates can be hundreds of megabytes",
  },
  /** (T231, owner 2026-10-28) the top bar's bell: its sync section, and the bell's title when it holds both kinds */
  bell: {
    count: (n: number) => (n === 1 ? "1 update failed" : `${n} updates failed`),
    title: "Notices",
  },
  toast: (day: string) => `Updated to ${day}`,
  toastNoDate: "Data updated",
} as const;

export interface UpdateSourceInput {
  readonly k: UpdateSourceKey;
  /** when this device last reached the origin for it (epoch ms), or null */
  readonly checkedAt: number | null;
  /** when new data from it was last put in use on this device (epoch ms), or null */
  readonly updatedAt: number | null;
  readonly error: string | null;
  readonly failedAt: number | null;
  readonly offline: boolean;
}

export interface UpdateReleaseInput {
  readonly phase: "current" | "waiting-wifi" | "downloading" | "ready" | "app-too-old";
  readonly progress: { readonly bytes: number; readonly of: number } | null;
}

const ago = (ms: number, now: number) => agoWords(new Date(ms).toISOString(), now);

/**
 * A source's failure, in the reader's words: no connection, the server, a checksum, or the bytes. The raw message is
 * the report's, not the reader's.
 */
export function failureWords(error: string, offline: boolean): string {
  const S = UPDATE_WORDS.sheet;
  if (offline) return S.offline;
  if (/sha256|bytes, the manifest says|checksum/i.test(error)) return S.bytes;
  if (/HTTP \d{3}/.test(error)) return S.server;
  return S.other;
}

/**
 * Which sources the sync icon shows as FAILED: an origin or bytes failure always; offline only when the reader asked
 * (a manual check) — an offline phone is the ordinary case and is said by the dot's own rules, not a red badge.
 */
export function failedSources(sources: readonly UpdateSourceInput[], manualFailed: boolean): UpdateSourceInput[] {
  return sources.filter((s) => s.error !== null && (!s.offline || manualFailed));
}

export interface UpdateSourceRowVM {
  readonly k: UpdateSourceKey;
  readonly name: string;
  /** "Checked 3 min ago · updated 2h ago" */
  readonly line: string;
  /** the failure in words, or null */
  readonly problem: string | null;
  /** the row whole: "Live gauge readings: Checked … · updated …" and its failure after it */
  readonly shown: string;
}

export interface UpdatesVM {
  readonly title: string;
  /** the More tab's one line: the newest check, a download's state, or how many failed */
  readonly line: string;
  readonly label: string;
  readonly sources: readonly UpdateSourceRowVM[];
}

export function updateSourcesVM(i: { sources: readonly UpdateSourceInput[]; release: UpdateReleaseInput | null;
                                     now: number }): UpdatesVM {
  const R = UPDATE_WORDS.row;
  const sources = i.sources.map((s): UpdateSourceRowVM => {
    const name = UPDATE_WORDS.names[s.k];
    const line = `${s.checkedAt !== null ? R.checked(ago(s.checkedAt, i.now)) : R.notChecked} · `
      + (s.updatedAt !== null ? R.updated(ago(s.updatedAt, i.now)) : R.notUpdated);
    const problem = s.error !== null ? failureWords(s.error, s.offline) : null;
    return { k: s.k, name, line, problem, shown: `${name}: ${line}${problem ? ` · ${problem}` : ""}` };
  });
  const failed = i.sources.filter((s) => s.error !== null && !s.offline).length;
  const checked = i.sources.map((s) => s.checkedAt).filter((t): t is number => t !== null);
  const r = i.release;
  const line = r?.phase === "downloading"
    ? R.downloading(r.progress && r.progress.of > 0 ? Math.round((100 * r.progress.bytes) / r.progress.of) : 0)
    : r?.phase === "waiting-wifi" ? R.waitingWifi
    : r?.phase === "ready" ? R.ready
    : r?.phase === "app-too-old" ? R.appTooOld
    : failed > 0 ? R.failed(failed)
    : checked.length ? R.checked(ago(Math.max(...checked), i.now)) : R.notChecked;
  return { title: R.title, line, label: `${R.title}: ${line}.`, sources };
}

export interface SyncErrorVM {
  readonly title: string;
  readonly sub: string;
  readonly rows: readonly { readonly k: UpdateSourceKey; readonly name: string; readonly problem: string;
                            readonly last: string; readonly label: string }[];
  readonly retry: string;
  readonly retrying: string;
}

/** The sheet the red-badged sync icon opens: each failed source, what went wrong, when it last succeeded; Retry. */
export function syncErrorVM(failed: readonly UpdateSourceInput[], now: number): SyncErrorVM {
  const S = UPDATE_WORDS.sheet;
  return {
    title: S.title, sub: S.sub, retry: S.retry, retrying: S.retrying,
    rows: failed.map((s) => {
      const name = UPDATE_WORDS.names[s.k], problem = failureWords(s.error ?? "", s.offline);
      // the last check that SUCCEEDED (a failed one never moves `checkedAt`)
      const last = s.checkedAt !== null ? S.lastOk(ago(s.checkedAt, now)) : S.neverOk;
      return { k: s.k, name, problem, last, label: S.aloud(name, problem, last) };
    }),
  };
}

/** What is said when a downloaded release is put in use: the newest date it states, else that it was updated. */
export function updatedToast(day: string | null): string {
  const d = isoDayWords(day);
  return d ? UPDATE_WORDS.toast(d) : UPDATE_WORDS.toastNoDate;
}

/**
 * (T231, owner 2026-10-28: "the notification bell should also list sync failures") The bell's label aloud, over its two
 * sections: the in-season changes not on the map (`inseason`: T230's own words for them) and the sources that failed.
 */
export function bellLabel(inseason: string | null, failed: number): string {
  return [inseason, failed > 0 ? UPDATE_WORDS.bell.count(failed) : null].filter(Boolean).join("; ");
}

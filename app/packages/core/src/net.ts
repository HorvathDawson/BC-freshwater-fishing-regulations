/**
 * (T254) BAD CONNECTIONS, IN WORDS: the More → Settings setting, the banner a slow or missing line puts over the app, what a
 * page says while it waits ("still loading" is never "not there"), and the rule book's size before it is fetched on a
 * slow line. The policy that decides the mode is `@app/data` net (`netMode`); these are only its words.
 */
import { byteWords } from "./updateLog";

/** The reader's Data setting: the app decides, always low, never low. */
export type DataSetting = "auto" | "on" | "off";
/** Why the app is in low-bandwidth mode. */
export type LowWhy = "setting" | "save-data" | "measured" | "browser" | "cellular";

export const NET_WORDS = Object.freeze({
  setting: {
    label: "Low-bandwidth mode",
    hint: "On a slow connection: the regulations you ask for first, nothing downloaded ahead, large files only when you say",
    choices: { auto: "Automatic", on: "On", off: "Off" } as Record<DataSetting, string>,
  },
  /** the setting's state now, under it */
  now: {
    on: "On now",
    off: "Off now",
    why: {
      setting: "you turned it on",
      "save-data": "your device is saving data",
      measured: "slow connection",
      browser: "slow connection",
      cellular: "slow mobile connection",
    } as Record<LowWhy, string>,
    good: "connection is good",
    unknown: "connection not measured yet",
  },
  banner: {
    slow: "Slow connection — showing what we have",
    offline: "Offline — showing what’s saved on this device",
    a11ySettings: "Data settings",
    settingsLink: "Data settings ›",
  },
  /** a page waiting on the line */
  waiting: {
    regs: "Loading the regulations",
    offline: "Offline — this will load when you’re back online",
    retrying: (n: number) => `Connection dropped — trying again (${n})`,
  },
  /** a part of a page that is not there yet, or cannot come */
  part: {
    loading: (what: string) => `${what}: still loading (slow connection)`,
    offline: (what: string) => `${what}: not available offline`,
    failed: (what: string) => `${what}: couldn’t load`,
    retry: "Try again",
    salmon: "Salmon rules",
    /** (T368) a lazy part whose code could not be fetched */
    code: "Couldn’t load this part of the app. Check your connection.",
  },
  /** the rule book on a slow line: asked before it is fetched */
  pdf: {
    offer: (bytes: number, seconds: number | null) =>
      `The rule book is ${byteWords(bytes)}${seconds === null ? "" : ` — about ${duration(seconds)} on this connection`}.`,
    download: "Download it",
    cancel: "Not now",
  },
});

/** "40 s", "8 min", "1 h 20 min" */
export function duration(seconds: number): string {
  const s = Math.max(1, Math.round(seconds));
  if (s < 60) return `${s} s`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m} min`;
  return `${Math.floor(m / 60)} h${m % 60 ? ` ${m % 60} min` : ""}`;
}

/** A rate in words: "0.4 Mbit/s", "120 kbit/s". */
export function rateWords(bps: number): string {
  const kbit = (bps * 8) / 1000;
  return kbit >= 1000 ? `${(kbit / 1000).toFixed(1)} Mbit/s` : `${Math.round(kbit)} kbit/s`;
}

/**
 * The line under the setting in More → Settings: "On now — slow connection (0.4 Mbit/s)", "Off now — connection is good
 * (8.2 Mbit/s)", "Off now — connection not measured yet".
 */
export function dataModeLine(m: { low: boolean; why: LowWhy | null }, bps: number | null): string {
  const rate = bps !== null ? ` (${rateWords(bps)})` : "";
  if (m.low) return `${NET_WORDS.now.on} — ${NET_WORDS.now.why[m.why ?? "setting"]}${m.why === "measured" ? rate : ""}`;
  return `${NET_WORDS.now.off} — ${bps !== null ? NET_WORDS.now.good + rate : NET_WORDS.now.unknown}`;
}

/** The banner over the app, or null: offline first; then a slow line the app found itself (not the reader's own "On"). */
export function netBanner(m: { low: boolean; why: LowWhy | null; offline: boolean },
                          o: { live?: boolean } = {}): string | null {
  if (m.offline) return NET_WORDS.banner.offline;
  // (T298, owner 2026-10-30) on a phone the regulations are on the device: a slow line is said only while something
  // live is loading (gauges, forecasts, weather) — `live` false; absent (the web): whenever the mode is low
  if (m.low && m.why !== "setting" && o.live !== false) return NET_WORDS.banner.slow;
  return null;
}

/** What a page waiting on its files says: "Loading the regulations… 1.2 of 3.4 MB" (no total: the bytes so far). */
export function waitingLine(p: { loaded: number; total: number | null; offline: boolean; attempt?: number }): string {
  if (p.offline) return NET_WORDS.waiting.offline;
  if ((p.attempt ?? 1) > 1) return NET_WORDS.waiting.retrying(p.attempt! - 1);
  const so = progressWords(p.loaded, p.total);
  return `${NET_WORDS.waiting.regs}…${so ? ` ${so}` : ""}`;
}

/** How far a download has come: "0.3 of 0.7 MB", "120.4 kB" (no total). Empty: nothing yet. */
export function progressWords(loaded: number, total: number | null): string {
  if (total !== null && total > 0) return `${byteWords(loaded)} of ${byteWords(total)}`;
  return loaded > 0 ? byteWords(loaded) : "";
}

/** A part of a page in words while it is not there: still coming, cannot come offline, or failed. Null: it is there. */
export function partLine(what: string, s: "loading" | "failed" | "ready", offline: boolean): string | null {
  if (s === "ready") return null;
  if (s === "loading") return NET_WORDS.part.loading(what);
  return offline ? NET_WORDS.part.offline(what) : NET_WORDS.part.failed(what);
}

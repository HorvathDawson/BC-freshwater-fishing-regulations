/**
 * START-UP AND FIRST-RUN WORDS (P11): what the app says while it reads the manifest, installs a release (native, first
 * run) and decodes paint. Composed here so a test pins the finished string (AGENTS 33) and no screen types its own.
 */

export const INSTALL_WORDS = {
  failed: "The map data could not be loaded.",
  firstRunOffline: "The first start needs a connection: the map and regulations download once, then work offline.",
  wifi: "Wi-Fi recommended.",
  /** (T262) a first start that failed for any other reason (the exception itself is a developer's, never shown) */
  again: "Check the connection and try again. What has downloaded is kept.",
  retry: "Try again",
  /** (T180) under a failed start, for a developer: the local tile server serves the release */
  serve: "Run `pnpm tiles` to serve the release.",
} as const;

const STAGES: Readonly<Record<string, string>> = {
  manifest: "Checking for the latest regulations…",
  wifi: "Waiting for Wi-Fi",
  install: "Downloading the map and regulations for offline use",
  paint: "Opening the map…",
  fonts: "Loading type…",
};

/** The line for a start-up stage; an unknown stage says nothing rather than something made up. */
export function startStage(stage: string): string {
  return STAGES[stage] ?? "";
}

/** Megabytes to one decimal, thousands grouped — by hand: an engine's Intl is not something core may count on. */
const mb = (bytes: number) => {
  const [whole, tenth] = (bytes / 1_000_000).toFixed(1).split(".");
  return `${whole!.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}.${tenth}`;
};

/** (T238) a size in words: "1.2 MB" */
export const megabytes = (bytes: number): string => `${mb(bytes)} MB`;

/** (T272) The download's share done, 0..1 (the first bar). */
export function downloadFraction(p: { bytes: number; of: number }): number {
  return p.of > 0 ? Math.min(1, Math.max(0, p.bytes / p.of)) : 0;
}

/**
 * (T272) THE SECOND STAGE of a first install: after the last byte, the files are checked (sha256 of the map files),
 * the map's labels and symbols placed, and the set opened. Its own bar and words, so a bar never sits at 100 % with
 * nothing moving.
 */
export const SETUP_WORDS = {
  title: "Setting up on this device",
  verify: "Checking the downloaded files",
  assets: "Fetching the map's labels and symbols",
  open: "Opening the map",
} as const;

export interface SetupStep { step: "verify" | "assets" | "open"; done: number; of: number }

/** Where each step sits on the second bar (by its measured share of the wait on the emulator, T272: the map assets now load beside the download). */
const SETUP_SPAN: Readonly<Record<SetupStep["step"], readonly [number, number]>> = {
  verify: [0, 0.85], assets: [0.85, 0.95], open: [0.95, 1],
};

/** (T272) The second bar's share done, 0..1: the step's span, filled by its own count. */
export function setupFraction(s: SetupStep): number {
  const [a, b] = SETUP_SPAN[s.step];
  const f = s.of > 0 ? Math.min(1, Math.max(0, s.done / s.of)) : 1;
  return a + (b - a) * f;
}

/** (T272) "Checking the downloaded files · 62 %" */
export function setupLine(s: SetupStep): string {
  return `${SETUP_WORDS[s.step]} · ${Math.floor(setupFraction(s) * 100)} %`;
}

/** "12.5 of 1,461.2 MB downloaded"; nothing when nothing is fetched (an update that only copies held files). */
export function installLine(p: { bytes: number; of: number }): string {
  return p.of > 0 ? `${mb(p.bytes)} of ${mb(p.of)} MB downloaded` : "";
}

/**
 * Said on a phone when the newest set would not open at launch and the app went back to the set it replaced (boot's
 * `rolledBack`): which release was refused and which one is in use. A name it does not have is left out, not invented.
 */
export function rolledBackLine(r: { refused: string | null; using: string | null }): string {
  const refused = r.refused ? ` (${r.refused})` : "", using = r.using ? ` (${r.using})` : "";
  return `The newest map data${refused} would not open, so the app is using the data it had before${using}.`;
}

/**
 * (T298) THE SYSTEM DOWNLOAD'S NOTIFICATION while the first install's big files download (on with the app closed):
 * its title, and a line per file.
 */
export const DOWNLOAD_NOTE_WORDS = {
  title: "Downloading BC fishing rules",
  files: { tiles: "Map of the waters", mask: "Map outline", basemap: "Base map", synopsis: "Rule book (PDF)" },
  other: "Map and regulations",
  /** (Android 13+) said on the boot screen as the system asks to allow notifications, once, before the first download */
  why: "Allow notifications to see the download's progress after you close the app.",
  text: (what: string, bytes: number) => `${what} · ${megabytes(bytes)}`,
} as const;

/**
 * (T298, owner 2026-10-29) THE MOBILE-DATA SETTING: a large download (the first install, a big update) on a metered
 * line — always go, ask first, or wait for Wi-Fi. "ask" until the reader answers the prompt once; the answer is saved
 * (More → Data) and can be changed there any time.
 */
export type MobileDataSetting = "always" | "ask" | "wifi";

/** A download below this never asks (owner: "small updates (< ~5 MB) don't ask"). */
export const SMALL_DOWNLOAD_BYTES = 5_000_000;

export const MOBILE_DATA_WORDS = {
  label: "Download over mobile data",
  hint: "The first download is about 500 MB; map and regulation updates can be hundreds of megabytes",
  choices: { always: "Always", ask: "Ask", wifi: "Wi-Fi only" } as Record<MobileDataSetting, string>,
  /** the prompt before a large download on mobile data */
  prompt: (bytes: number) => `Download about ${megabytes(bytes)} using mobile data?`,
  now: "Download now",
  wait: "Wait for Wi-Fi",
  /** said once the answer is kept */
  saved: "Saved. Change this any time in More → Settings.",
  /** the first install, held for Wi-Fi */
  waiting: "Waiting for Wi-Fi — the download starts by itself when you connect.",
  closeOk: "You can close the app: the download carries on.",
} as const;

/** A stored setting read back: anything unknown is "ask". */
export function mobileDataFrom(text: string | null | undefined): MobileDataSetting {
  const t = (text ?? "").trim();
  return t === "always" || t === "wifi" ? t : "ask";
}

/**
 * What a download of `bytes` does now: "go", "ask" the reader first, or "wait" for an unmetered line. Off mobile data,
 * or a small download, always goes.
 */
export function mobileDataDecision(setting: MobileDataSetting, metered: boolean, bytes: number): "go" | "ask" | "wait" {
  if (!metered || bytes < SMALL_DOWNLOAD_BYTES || setting === "always") return "go";
  return setting === "ask" ? "ask" : "wait";
}

/**
 * (T298, owner 2026-10-29) THE FISH SAYINGS: short lines that take turns under the bars while the first install
 * downloads (`SAYING_MS` apart; with reduced motion they swap without a fade). The search placeholders' spirit; about
 * the waiting and the gear, never a promise of fish.
 */
export const DOWNLOAD_SAYINGS = [
  "Untangling the leader…",
  "Checking the tackle box…",
  "Waiting for the bite… of data",
  "Reading the water…",
  "Tying a better knot…",
  "Patience is half of fishing…",
  "Sorting the fly box…",
  "Letting the line drift…",
  "Checking which way the current runs…",
  "Waders on, almost there…",
] as const;

export const SAYING_MS = 4000;

/** The saying for turn `n` (any integer; wraps). */
export function sayingAt(n: number): string {
  const L = DOWNLOAD_SAYINGS.length;
  return DOWNLOAD_SAYINGS[((Math.floor(n) % L) + L) % L]!;
}

/**
 * (T254) THE BAD-CONNECTION POLICY, pure: whether the app is in LOW-BANDWIDTH mode, what is fetched first in it, and how
 * long a fetch may take before it is given up and tried again. Owner 2026-10-29: "make the app work well on BAD
 * connections (rural BC, 1–2 bars, flaky) … use a different loading strategy when the connection is poor".
 *
 *   MODE        the reader's Data setting (More → Data: Automatic / On / Off) decides first. Automatic is low when the
 *               reader asked the system to save data, when the app's own meter (bytes per second over its real fetches,
 *               `meter.ts`) is below `LOW_BPS`, or — nothing measured yet — when the platform says the link is slow
 *               (the browser's `effectiveType` / `downlink` / `rtt`, a phone's NetInfo cellular generation). A measured
 *               rate beats what the platform says: Chrome reports "4g" on a throttled link. Low is left only above
 *               `HIGH_BPS` (hysteresis: a link near the line does not flip the app back and forth).
 *   ORDER       in low mode a fetch waits for the ones before it (`gate.ts`): the ANSWER (the files a tapped water's
 *               regulations are built from) first, then the EXTRAS a page adds (salmon, the gentle access note, bait &
 *               fly, the map's modes, Conditions' history), then anything fetched AHEAD of need (prefetch). Off low, all
 *               run at once, as before.
 *   TIMEOUTS    per size (`timeoutsFor`): a first byte within `FIRST_BYTE_MS`, never `STALL_MS` without a byte, and the
 *               whole file within a time that assumes no worse than `FLOOR_BPS`. A download that is MOVING is never cut;
 *               a stalled one is, and tried again (`backoffMs`) — from where it stopped when the server allows it.
 */
import type { DataSetting, LowWhy } from "@app/core";
import type { FileName } from "../release/loader";

/** The reader's Data setting (More → Data: the app decides, always low, or never low) and why the app is low: core's. */
export type { DataSetting, LowWhy } from "@app/core";
export const DATA_SETTINGS: readonly DataSetting[] = ["auto", "on", "off"];

/** What the platform and the app's own meter say about the link now. Every field may be unknown (null). */
export interface NetHints {
  online: boolean;
  /** the reader asked the system to save data (`navigator.connection.saveData`; Android's Data Saver) */
  saveData: boolean;
  /** the browser's `navigator.connection.effectiveType` ("slow-2g" | "2g" | "3g" | "4g") */
  effectiveType: string | null;
  /** the browser's `navigator.connection.downlink`, Mbit/s */
  downlinkMbps: number | null;
  /** the browser's `navigator.connection.rtt`, ms */
  rttMs: number | null;
  /** a phone's NetInfo `details.cellularGeneration` ("2g" | "3g" | "4g" | "5g") on cellular; null on Wi-Fi / unknown */
  cellularGeneration: string | null;
  /** the app's own measured rate over its real fetches, bytes per second (`meter.ts`); null: nothing measured yet */
  measuredBps: number | null;
}

export const NO_HINTS: NetHints = {
  online: true, saveData: false, effectiveType: null, downlinkMbps: null, rttMs: null, cellularGeneration: null,
  measuredBps: null,
};

/** Below this measured rate the app goes low: 1 Mbit/s (the owner's "1–2 bars"; Slow 3G is 0.4). */
export const LOW_BPS = 125_000;
/** Above this measured rate a low app goes back: 2 Mbit/s. Between the two it stays as it was. */
export const HIGH_BPS = 250_000;
/** The browser's word for a slow link. */
const SLOW_TYPES: readonly string[] = ["slow-2g", "2g", "3g"];
/** A round trip this long is a poor link whatever its rate (a satellite hop, a far tower). */
export const SLOW_RTT_MS = 900;

export interface NetMode {
  /** load the minimum for the visible answer first (the ORDER above), skip prefetch, offer large files */
  readonly low: boolean;
  /** why it is low; null when it is not */
  readonly why: LowWhy | null;
  /** the device says it is offline: nothing new can be fetched; what is held is shown */
  readonly offline: boolean;
}

/**
 * THE DECISION. `wasLow`: the mode a moment ago (hysteresis on the measured rate). Offline is said beside the mode, not
 * as a mode: an offline app shows what it holds, and the mode it comes back to is decided again when it is online.
 */
export function netMode(setting: DataSetting, h: NetHints, wasLow = false): NetMode {
  const offline = !h.online;
  const mode = (why: LowWhy | null): NetMode => ({ low: why !== null, why, offline });
  if (setting === "on") return mode("setting");
  if (setting === "off") return mode(null);
  if (h.saveData) return mode("save-data");
  if (h.measuredBps !== null) {
    if (h.measuredBps < LOW_BPS) return mode("measured");
    if (wasLow && h.measuredBps < HIGH_BPS) return mode("measured");
    return mode(null);
  }
  if (h.cellularGeneration !== null && ["2g", "3g"].includes(h.cellularGeneration)) return mode("cellular");
  if (h.effectiveType !== null && SLOW_TYPES.includes(h.effectiveType)) return mode("browser");
  if (h.downlinkMbps !== null && h.downlinkMbps > 0 && h.downlinkMbps * 125_000 < LOW_BPS) return mode("browser");
  if (h.rttMs !== null && h.rttMs >= SLOW_RTT_MS) return mode("browser");
  return mode(null);
}

// ── the order ────────────────────────────────────────────────────────────────────────────────────────────────────────

/** 0 the answer, 1 an extra the page adds, 2 ahead of need (prefetch). Lower goes first in low mode. */
export type FetchPriority = 0 | 1 | 2;
export const PRIORITY = { answer: 0, extra: 1, ahead: 2 } as const;

/**
 * The release files a Regulations page is BUILT from (the store, the water, the region's rules, an unnamed reach's
 * key and its "Tributary of …"), and paint, which every map and every page's no-access banner reads. Everything else
 * the web fetches on its own is an extra: it adds to an answer already on screen.
 */
export function isAnswerFile(name: FileName): boolean {
  return name === "paint" || name === "core" || name === "water" || name === "regs-sid" || name === "down"
    || /^(regs|core|water)-[0-9a-z]+$/.test(name);            // (T256) + the answer slices core-<r>, water-<r>
}

/** A release file's place in the order when the page asks for it (prefetch is `PRIORITY.ahead`, whatever the file). */
export const priorityOf = (name: FileName): FetchPriority => (isAnswerFile(name) ? PRIORITY.answer : PRIORITY.extra);

/**
 * THE DEFERRAL ORDER, as one list (the tests read it; the gate implements it): what a tap on a water fetches, in the
 * order a low-bandwidth app lets it through.
 */
export const DEFERRAL_ORDER: readonly { readonly what: string; readonly priority: FetchPriority }[] = [
  { what: "the tapped water's regulations (paint, core, water or their region's slices, regs-<region>, regs-sid, down)",
    priority: 0 },
  { what: "what the page adds: salmon slices, access note, bait & fly, Conditions, history, place names", priority: 1 },
  { what: "anything ahead of need: the idle and background prefetch, the next region", priority: 2 },
];

// ── timeouts ─────────────────────────────────────────────────────────────────────────────────────────────────────────

/** A request with no response this long is dead (a far tower can take 3 s per round trip; DNS + TLS + request ≈ 4). */
export const FIRST_BYTE_MS = 20_000;
/** A body with no new byte this long is stalled: cut it and try again (resuming where the server allows). */
export const STALL_MS = 20_000;
/** The slowest link the whole-file limit allows for: 32 kbit/s (well under the 2G-ish profile's 100). */
export const FLOOR_BPS = 4_000;
/** Tries per file (the first and three more). */
export const MAX_ATTEMPTS = 4;

export interface Timeouts { readonly firstByteMs: number; readonly stallMs: number; readonly totalMs: number }

/** The limits for a file of about `bytes` on the wire (unknown: a small file's). */
export function timeoutsFor(bytes: number | null): Timeouts {
  const b = bytes !== null && bytes > 0 ? bytes : 64_000;
  return { firstByteMs: FIRST_BYTE_MS, stallMs: STALL_MS, totalMs: FIRST_BYTE_MS + Math.ceil((b / FLOOR_BPS) * 1000) };
}

/** The wait before try `attempt` (2, 3, …): 1 s, 2 s, 4 s … to 15 s, ± 20 % (`rand` in [0, 1)). */
export function backoffMs(attempt: number, rand: () => number = Math.random): number {
  const base = Math.min(15_000, 1000 * 2 ** Math.max(0, attempt - 2));
  return Math.round(base * (0.8 + 0.4 * rand()));
}

/** Seconds a download of `bytes` takes at `bps` (null: unknown rate). */
export const etaSeconds = (bytes: number, bps: number | null): number | null =>
  bps !== null && bps > 0 ? Math.ceil(bytes / bps) : null;

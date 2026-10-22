/**
 * @app/core — domain logic. Pure TypeScript, no React, no platform.
 *
 * Flow, gauge trust, the zoom ladder and the trace. The Regulations page's logic is
 * `./regs` (re-exported through `./regulations`) — pure functions from a hydrated `RegsInputs` to a `RegsPage`. The
 * day's STATUS of a section or a water is (`./statusIndex`, read through `./status`).
 *
 * `Freshness` below encodes a rule the plan calls a correctness requirement, not a nicety:
 * a stale answer must never be rendered as a live one (13-build-plan §5).
 */

import { localStamp } from "./day";

export * from "./flow";
export * from "./trace";
export * from "./ladder";
export * from "./trust";
export * from "./section";
export * from "./regulations";
export * from "./status";
export * from "./statusIndex";
export * from "./lakeFacts";
export * from "./paint";
export * from "./search";
export * from "./near";
export * from "./day";
export * from "./conditions";
export * from "./chartZoom";
export * from "./routeMap";
export * from "./temperature";
export * from "./install";
export * from "./pairing";
export * from "./menu";

export type Freshness =
  | { state: "live"; ageMs: number }
  | { state: "stale"; ageMs: number }
  | { state: "unknown" };

/** A feed reading older than this is stale: the publisher writes every 30 minutes, so two hours is four
 *  missed runs (P02). */
export const STALE_AFTER_MS = 2 * 60 * 60_000;

/** Classify a cached value's age. `unknown` is NOT the same as "nothing to report". */
export function freshness(
  fetchedAt: number | null,
  now: number,
  staleAfterMs: number = STALE_AFTER_MS,
): Freshness {
  if (fetchedAt === null || !Number.isFinite(fetchedAt)) return { state: "unknown" };
  const ageMs = Math.max(0, now - fetchedAt);
  return ageMs > staleAfterMs ? { state: "stale", ageMs } : { state: "live", ageMs };
}

const agoWords = (ms: number): string => {
  const min = Math.floor(ms / 60_000);
  if (min < 60) return `${min} min ago`;
  const h = Math.floor(min / 60);
  return h < 48 ? `${h} h ago` : `${Math.floor(h / 24)} days ago`;
};

/**
 * The line under a gauge reading (P02): when it was observed, and how old our copy is. An unknown age says so; it
 * is never shown as live. `observedAt` is the reading's own ISO time, left out when missing or malformed.
 */
export function readingAgeLine(f: Freshness, observedAt: string | null, zone?: string): string {
  const parts = ["This gauge's own reading"];
  const t = observedAt ? Date.parse(observedAt) : NaN;
  // LOCAL AND LABELLED (P16): the reader's clock, with its zone named — see `localStamp`.
  if (Number.isFinite(t)) parts.push(`observed ${localStamp(t, zone)}`);
  parts.push(f.state === "unknown" ? "age unknown"
    : f.state === "live" ? `checked ${agoWords(f.ageMs)}`
    : `last checked ${agoWords(f.ageMs)}, may be out of date`);
  return parts.join(" · ");
}
export * from "./gestures";
export * from "./waterViews";
export * from "./waterNav";
export * from "./mapRail";

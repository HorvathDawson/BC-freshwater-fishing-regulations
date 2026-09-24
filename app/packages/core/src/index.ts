/**
 * @app/core — domain logic. Pure TypeScript, no React, no platform.
 *
 * Flow, gauge trust, the zoom ladder and the trace. REGULATIONS ARE NOT HERE: they are not
 * integrated, and `./regulations` is the typed placeholder they plug into — read it first.
 *
 * `Freshness` below encodes a rule the plan calls a correctness requirement, not a nicety:
 * a stale answer must never be rendered as a live one (13-build-plan §5).
 */

export * from "./flow";
export * from "./trace";
export * from "./ladder";
export * from "./trust";
export * from "./section";
export * from "./regulations";

export type Freshness =
  | { state: "live"; ageMs: number }
  | { state: "stale"; ageMs: number }
  | { state: "unknown" };

/** Classify a cached value's age. `unknown` is NOT the same as "nothing to report". */
export function freshness(
  fetchedAt: number | null,
  now: number,
  staleAfterMs: number,
): Freshness {
  if (fetchedAt === null) return { state: "unknown" };
  const ageMs = Math.max(0, now - fetchedAt);
  return ageMs > staleAfterMs ? { state: "stale", ageMs } : { state: "live", ageMs };
}

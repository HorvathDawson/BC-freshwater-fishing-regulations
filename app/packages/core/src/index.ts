/**
 * @app/core — domain logic. Pure TypeScript, no React, no platform.
 *
 * DELIBERATELY ALMOST EMPTY. The real content (rule precedence, date evaluation,
 * species logic) is not written until the pipeline settles the content schema
 * (pipeline/docs/13-build-plan.md, steps 6-7). Guessing it now would bake in a
 * shape the data cannot fill.
 *
 * `Freshness` below is the one piece that is safe to write today, because it does
 * not depend on the schema at all — and it encodes a rule the plan calls a
 * correctness requirement, not a nicety: a stale answer must never be rendered as
 * a live one (13-build-plan §5).
 */

export * from "./dates";
export * from "./status";
export * from "./flow";
export * from "./trace";

export type Freshness =
  | { state: "live"; ageMs: number }
  | { state: "stale"; ageMs: number }
  | { state: "unknown" };

/** Classify a cached value's age. `unknown` is NOT the same as "no closure". */
export function freshness(
  fetchedAt: number | null,
  now: number,
  staleAfterMs: number,
): Freshness {
  if (fetchedAt === null) return { state: "unknown" };
  const ageMs = Math.max(0, now - fetchedAt);
  return ageMs > staleAfterMs ? { state: "stale", ageMs } : { state: "live", ageMs };
}

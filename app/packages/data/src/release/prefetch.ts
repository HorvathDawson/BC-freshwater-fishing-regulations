/**
 * (T186, Q164) THE WEB PREFETCH: what the web fetches after first paint, before anyone asks. A phone holds the whole
 * release offline, so this is the web's only.
 *
 *   always                       the regs file of the region the map is centred on (`regs-<region>`, bytes only: into
 *                                the release cache, not opened — so sql.js is not loaded for it), again for each new
 *                                region the view settles in
 *   idle, not Save-Data, not     `core`, opened — which loads the sql.js engine (sql-wasm) with it
 *   a slow connection
 *
 * Both go through the release loader (`ReleaseLoader.prefetch` / `db`), so the later tap reads the SAME cached fetch
 * (Cache Storage by immutable url, else memory): nothing is downloaded twice (the web request walk pins it). Every
 * prefetch is said by a performance mark (`PREFETCH_MARK`) before it starts, so the walk can tell what first paint
 * needed from what the prefetch took.
 */
import { REGIONS, type Region } from "../pack.generated";
import type { ReleaseLoader } from "./loader";

/** The connections too slow to spend on a file nobody asked for (`navigator.connection.effectiveType`). */
const SLOW_CONNECTIONS: readonly string[] = ["slow-2g", "2g", "3g"];

/** The performance mark set just before any prefetch starts (read by `tools/request-walk.mjs`). */
export const PREFETCH_MARK = "release-prefetch";

export interface PrefetchSignals {
  /** The region the map is centred on (null: outside every region, or not known yet). */
  region: Region | null;
  /** The browser said it is idle (`requestIdleCallback` fired). */
  idle: boolean;
  /** `navigator.connection.saveData` (false when the browser does not say). */
  saveData: boolean;
  /** `navigator.connection.effectiveType` (null when the browser does not say). */
  effectiveType: string | null;
  /** (T254) low-bandwidth mode: core's bytes are fetched AHEAD (behind anything the reader asks for), not opened */
  lowData?: boolean;
}

export interface PrefetchPlan {
  /** The region file to fetch (always, when the map is over a region). */
  regs: Region | null;
  /** Open `core` (and with it the sql.js engine). */
  core: boolean;
  /** (T254) Fetch `core`'s bytes ahead, at the back of the queue, without opening it (low-bandwidth mode). */
  coreAhead?: boolean;
}

/** THE DECISION, pure: what to prefetch given the signals. */
export function prefetchPlan(s: PrefetchSignals): PrefetchPlan {
  const slow = s.effectiveType !== null && SLOW_CONNECTIONS.includes(s.effectiveType);
  // (T254) on a poor line nothing is opened ahead (no sql.js engine pulled in unasked): core's bytes alone wait at the
  // back of the queue, so a tap that needs them finds them partly or wholly in — and a tap on anything else goes first
  if (s.lowData) return { regs: s.region, core: false, coreAhead: s.idle && !s.saveData };
  return { regs: s.region, core: s.idle && !s.saveData && !slow };
}

/** A tile's `region_id` ("1", "7A", …) as the release's region word ("1", "7a", …); null for anything else. */
export function regionOfTile(id: unknown): Region | null {
  if (typeof id !== "string" && typeof id !== "number") return null;
  const r = String(id).trim().toLowerCase();
  return (REGIONS as readonly string[]).includes(r) ? (r as Region) : null;
}

/** Carry a plan out through the loader. Failures are dropped: a prefetch that fails is a later tap's ordinary fetch. */
export function runPrefetch(loader: ReleaseLoader, plan: PrefetchPlan, mark: (name: string) => void = () => {}): void {
  if (plan.regs && !loader.prefetched(`regs-${plan.regs}`)) {
    mark(`${PREFETCH_MARK}:regs-${plan.regs}`);
    void loader.prefetch(`regs-${plan.regs}`).catch(() => {});
  }
  if (plan.coreAhead && !loader.prefetched("core")) {
    mark(`${PREFETCH_MARK}:core-ahead`);
    void loader.prefetch("core").catch(() => {});
  }
  if (plan.core && !loader.opened().includes("core")) {
    mark(`${PREFETCH_MARK}:core`);
    void loader.db("core").catch(() => {});
  }
}

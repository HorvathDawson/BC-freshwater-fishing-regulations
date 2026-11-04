/**
 * (T368) ONE WAY A FAILURE IS REPORTED, from every package (core, data, map, ui, ui-native, the app).
 *
 * The owner's rule (2026-11-03/04): no silent fallbacks; a failure the app catches is SHOWN to the reader where it
 * matters and REPORTED. The one exception is the ENVIRONMENT — the device offline, the line dropped or timed out, a
 * permission refused, a request the app itself cancelled: "we expect the app to be offline a lot", so those are shown
 * but never reported (the crash inbox would fill with offline). Everything else — a 500, a file that will not parse,
 * a query that throws — is a defect and is reported.
 *
 * The reporter itself lives in `@app/ui` (crashReport.ts: the Data switch, scrubbing, the per-run cap) and is handed
 * in with `setFailureSink` when it is installed; before that (tests, tools) nothing is sent. Pair a report with what
 * the reader sees; never use it to hide one.
 */

/** Where reports go: set by the crash reporter when it is installed (`installCrashHandlers`). */
let sink: ((e: unknown) => void) | null = null;

/** @public — the crash reporter's hook (ui/crashReport.ts); returns the undo. */
export function setFailureSink(s: ((e: unknown) => void) | null): () => void {
  const prev = sink;
  sink = s;
  return () => { sink = prev; };
}

/** Browser and React Native words for a fetch that never reached a server (no connection), by engine. */
const NO_LINE = /^(?:Network request failed|Failed to fetch|Load failed|NetworkError when attempting to fetch resource\.?)$/;

/**
 * Is this failure the ENVIRONMENT (offline, a dropped or timed-out line, a refused permission, a cancelled request)
 * rather than a defect? Marked by the thrower (`environment: true`, as `@app/data`'s NetError does for every kind but
 * `http`), or one of the platform's own words for "no connection", or an abort.
 */
export function isEnvironment(e: unknown): boolean {
  if (!e || typeof e !== "object") return false;
  const x = e as { environment?: unknown; name?: unknown; message?: unknown };
  if (x.environment === true) return true;
  if (x.name === "AbortError") return true;
  return typeof x.message === "string" && x.name === "TypeError" && NO_LINE.test(x.message);
}

/** Mark an error as the environment's (see `isEnvironment`): it is shown, never reported. Returns the same error. */
export function environmentError<E extends Error>(e: E): E {
  (e as E & { environment: boolean }).environment = true;
  return e;
}

/** Report a failure the app caught (non-fatal) — unless it is the environment's. */
export function reportFailure(e: unknown): void {
  if (isEnvironment(e)) return;
  sink?.(e);
}

const once = new Set<string>();
/** Report a failure at most once per run under `key` (a condition that holds for the whole run, not an event). */
export function reportOnce(key: string, e: unknown): void {
  if (once.has(key) || isEnvironment(e)) return;
  once.add(key);
  reportFailure(e);
}

/**
 * `p.catch(reported(v))`: a failure that must not stop what it is part of (a spot must save without its weather) is
 * REPORTED and answers `v` — said where it is read as "not known". Never for a failure the reader would take as an
 * answer; there the failure is thrown and shown.
 */
export const reported = <T>(v: T) => (e: unknown): T => { reportFailure(e); return v; };

/**
 * `p.catch(offlineAs(v))`: OFFLINE answers `v` (owner 2026-11-04: offline is the only state with an acceptable
 * fallback); any other failure is thrown on, to be shown and reported by whoever asked.
 */
export const offlineAs = <T>(v: T) => (e: unknown): T => { if (isEnvironment(e)) return v; throw e; };

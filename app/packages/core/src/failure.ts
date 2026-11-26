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
/** (T1016) the sink's own "every report handed to the line" (the crash reporter's `sent`), or null (none: nothing waits) */
let sinkSent: (() => Promise<unknown>) | null = null;

/** @public — the crash reporter's hook (ui/crashReport.ts); `sent`: resolves once every report it took so far is handed to
 *  the line (`failuresSent`). Returns the undo. */
export function setFailureSink(s: ((e: unknown) => void) | null, sent?: () => Promise<unknown>): () => void {
  const prev = sink, prevSent = sinkSent;
  sink = s;
  sinkSent = sent ?? null;
  return () => { sink = prev; sinkSent = prevSent; };
}

/** Browser and React Native words for a fetch that never reached a server (no connection), by engine. */
const NO_LINE = /^(?:Network request failed|Failed to fetch|Load failed|NetworkError when attempting to fetch resource\.?)$/;

/**
 * (T1093) The phone's own refusal when its Location setting is off (or not on high accuracy) though the app holds the
 * permission: expo-location's `E_LOCATION_SETTINGS_UNSATISFIED`, "Location request failed due to unsatisfied device
 * settings." A condition of the device, from any call that asks for a position (a fix, a watch): never a defect.
 */
export function isLocationSettingsOff(e: unknown): boolean {
  if (!e || typeof e !== "object") return false;
  const x = e as { code?: unknown; message?: unknown };
  return x.code === "E_LOCATION_SETTINGS_UNSATISFIED" || (typeof x.message === "string" && /unsatisfied device settings/i.test(x.message));
}

/**
 * Is this failure the ENVIRONMENT (offline, a dropped or timed-out line, a refused permission, a cancelled request)
 * rather than a defect? Marked by the thrower (`environment: true`, as `@app/data`'s NetError does for every kind but
 * `http`), or one of the platform's own words for "no connection", or an abort.
 */
export function isEnvironment(e: unknown): boolean {
  if (!e || typeof e !== "object") return false;
  const x = e as { environment?: unknown; name?: unknown; message?: unknown };
  if (x.environment === true || isLocationSettingsOff(e)) return true;
  if (x.name === "AbortError") return true;
  return typeof x.message === "string" && x.name === "TypeError" && NO_LINE.test(x.message);
}

/** Mark an error as the environment's (see `isEnvironment`): it is shown, never reported. Returns the same error. */
export function environmentError<E extends Error>(e: E): E {
  (e as E & { environment: boolean }).environment = true;
  return e;
}

/**
 * (T1016) A SECOND LOOK BEFORE A FAILURE IS REPORTED: asked of every failure that is not the environment's. `null` — the
 * failure is reported as it is, at once (every failure but the few the look is for); a promise — what it resolves to is
 * reported instead (a known state named so, `REPLACED_NAMES`), or nothing when it resolves to the environment's (a probe
 * that found the line down). A look that throws or rejects: the failure is reported as it is, AND the look's own failure.
 * Set by the web app (`apps/mobile` replaced.web.ts: a 404 of a release the pointer no longer names, a code chunk gone
 * after a deploy); none on a phone.
 */
let triage: ((e: unknown) => Promise<unknown> | null) | null = null;

/** @public — the web app's second look (`apps/mobile` replaced.web.ts); returns the undo. */
export function setFailureTriage(t: ((e: unknown) => Promise<unknown> | null) | null): () => void {
  const prev = triage;
  triage = t;
  return () => { triage = prev; };
}

/** (T1016) the second looks in flight: each settles once what it found is in the sink */
const looking = new Set<Promise<void>>();

/** Report a failure the app caught (non-fatal) — unless it is the environment's. */
export function reportFailure(e: unknown): void {
  if (isEnvironment(e)) return;
  let later: Promise<unknown> | null;
  try { later = triage?.(e) ?? null; } catch (x) { sink?.(e); sink?.(x); return; }
  if (!later) { sink?.(e); return; }
  const done = later.then((x) => { if (!isEnvironment(x)) sink?.(x); }, (x: unknown) => { sink?.(e); sink?.(x); });
  looking.add(done);
  // `done` holds no failure of its own (both its handlers only hand to the sink, which never throws)
  void done.finally(() => { looking.delete(done); });
}

/**
 * (T1016) EVERY FAILURE REPORTED SO FAR IS ON ITS WAY: each second look in flight has put what it found in the sink, and the
 * sink has handed every report it took to the line (the crash reporter's `sent`: its POST issued, `keepalive`, so it
 * outlives the page; never longer than its own wait, `SENT_WAIT_MS`). What a web page that refreshes itself waits for first (`@app/ui` `createReplacedState`): the
 * report of the very failure that asks for the refresh is not lost with the page. A look ends within its own wait
 * (`LOOK_MS`); no sink: nothing to wait for.
 */
export async function failuresSent(): Promise<void> {
  while (looking.size) await Promise.all([...looking]);
  await sinkSent?.();
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

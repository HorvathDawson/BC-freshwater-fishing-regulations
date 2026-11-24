/**
 * (T1016) A PAGE OLDER THAN WHAT THE SERVER SERVES — the hook half: the notice's state, the failure triage that raises it,
 * and the one action for each kind. Platform-free; the web's probes (the live pointer, the site's entry) and its reload
 * are handed in by `apps/mobile` `replaced.web.ts` (a phone installs no triage: its set is on the device whole, its code
 * in the binary).
 *
 *   triage   a failure the app reports (core `reportFailure`) gets a second look when it is a 404 with a URL, or a code
 *            chunk that would not load. A 404 on a file of a set the live pointer no longer names, or a chunk gone after
 *            a deploy: the notice is raised and the report is renamed (`REPLACED_NAMES`, class `expected` on the crash
 *            server). A 404 on a file the live release names, a chunk missing from the live deploy: reported as it is (a
 *            defect). A pointer that does not answer: the 404 as it is; a chunk asked again with no answer: reported
 *            as the line's (`network`). No look holds a report longer than `LOOK_MS`.
 *   refresh  (owner 2026-11-24, "Always auto refresh") a verdict refreshes the page BY ITSELF, at once, typing or not, once
 *            its report is on the line (core `failuresSent`): data → the live release put in use through the app's ONE
 *            in-session apply (`loadLiveRelease`: the update service stages it, the app's `apply` boots onto it — no
 *            second loader); code → the page reloaded (`reloadOnce`: the new site, and with it the live release).
 *            At most ONE automatic refresh per verdict (kind + release) per page life, and one reload per page code
 *            across reloads (a one-shot marker that survives the reload): no loop.
 *   notice   one line and one action (core `REPLACED_WORDS`), only when the automatic refresh could not be made (nothing
 *            staged, a reload that would loop, the marker unkept): the failure shown, its action the same refresh by
 *            hand, and reported as a defect (`REFRESH_NOT_MADE`).
 */
import { useSyncExternalStore } from "react";
import { isEnvironment, missingChunk, notFoundUrl, refreshNotMade, replacedError, reportFailure, REPLACED_WORDS,
         type ReplacedKind } from "@app/core";
import type { UpdateService } from "@app/data";
import type { ReplacedProbe } from "@app/data/release";

/** What is raised: the kind, and (data) the release this page had open when it was found replaced. */
export interface ReplacedRaised { readonly kind: ReplacedKind; readonly release: string | null }

export interface ReplacedState {
  /** the notice shown: a verdict whose automatic refresh could not be made (`ReplacedRefresh`), else null */
  get(): ReplacedRaised | null;
  /** a verdict (the triage's): the first for its kind and release is refreshed by itself; any later one is not */
  raise(r: ReplacedRaised): void;
  subscribe(on: () => void): () => void;
}

/** What the state needs from the app to refresh by itself. */
export interface ReplacedRefresh {
  /** make the automatic refresh of `r`: resolves null once it is made (data: the app's apply started — what follows is
   *  the apply's own, said and reported there, T555; code: the page reloading), else why it could not be made */
  refresh(r: ReplacedRaised): Promise<string | null>;
  /** every report so far on the line (core `failuresSent`): waited for first, so a reload does not lose the report */
  sent(): Promise<void>;
  /** the release in use now: a data verdict for another (a late read of a set the page has since left) is not refreshed */
  inUse(): string | null;
}

/**
 * (T1016, owner 2026-11-24 "Always auto refresh") The verdicts of one page life. The first verdict for a kind and release
 * waits for the reports to be on the line, then refreshes; a verdict met again (another 404 of the same set while the
 * apply runs) is never refreshed again, and a data verdict for a set the page has left (a late read made on it) not at
 * all (its report, `ReleaseReplaced`, is all it is). A refresh that could not be made raises the
 * notice (the code notice outranks the data one: a reload loads both) and is reported as a defect, once.
 */
export function createReplacedState(o: ReplacedRefresh): ReplacedState {
  let now: ReplacedRaised | null = null;
  const subs = new Set<() => void>();
  const tried = new Set<string>();
  const show = (r: ReplacedRaised, why: string) => {
    reportFailure(refreshNotMade(r.kind, r.release, why));
    if (now?.kind === "code" || (now?.kind === r.kind && now.release === r.release)) return;
    now = r;
    for (const s of [...subs]) s();
  };
  return {
    get: () => now,
    raise(r) {
      const key = `${r.kind}:${r.release ?? ""}`;
      if (tried.has(key) || (r.kind === "data" && r.release !== o.inUse())) return;
      tried.add(key);
      void o.sent()
        .then(() => o.refresh(r))
        .then((why) => { if (why !== null) show(r, why); },
              (x: unknown) => show(r, `it failed: ${x instanceof Error ? x.message : String(x)}`));
    },
    subscribe(on) { subs.add(on); return () => { subs.delete(on); }; },
  };
}

/** The web's session store, as `reloadOnce` reads it (the browser's `sessionStorage`). */
export interface SessionStore { getItem(k: string): string | null; setItem(k: string, v: string): void }
/** (T1016) the one-shot marker's key: the page code (entry script) a reload was made FROM */
export const RELOADED_FROM_KEY = "canifishthis.t1016.reloadedFrom";

/**
 * (T1016) THE CODE VERDICT'S AUTOMATIC RELOAD, AT MOST ONCE PER PAGE CODE. `entry`: this page's entry script. The session
 * store keeps the entry a reload was made from, across the reload: a page that comes back on the SAME code (a cached
 * page, a deploy that did not reach it) and meets a missing chunk again does not reload (why, returned); a page on new
 * code may reload once for a later deploy. A store that cannot be read or written (a private window, storage blocked):
 * no reload (it could loop), why returned. Null: reloading now.
 */
export function reloadOnce(o: { store: () => SessionStore; entry: string | null; reload: () => void }): string | null {
  if (!o.entry) return "this page's code is not known (no entry script), so a reload could not be told from a loop";
  try {
    const s = o.store();
    if (s.getItem(RELOADED_FROM_KEY) === o.entry)
      return `this page reloaded once already from this code (${o.entry}) and the chunk is still missing`;
    s.setItem(RELOADED_FROM_KEY, o.entry);
    if (s.getItem(RELOADED_FROM_KEY) !== o.entry) return "the marker that stops a reload loop did not keep";
  } catch (x) {
    return `the marker that stops a reload loop could not be kept (${x instanceof Error ? x.message : String(x)})`;
  }
  o.reload();
  return null;
}

/** The web's look at the site: is a chunk that would not load gone because the site was deployed since this page opened? */
export type SiteProbe = (chunkUrl: string | null) => Promise<"replaced" | "live" | "unknown">;

/**
 * The site probe over `f`: `entry`, this page's entry script's path (null: none known, a dev server — nothing can be
 * told); `site`, the site's own page (`/`). The chunk is asked for again, fresh: a 404, and the site's page (fresh) no
 * longer names this page's entry — `replaced` (the site was deployed since this page opened); a 404 while it still names
 * it — `live` (the chunk is missing from the live deploy: a defect); any other answer (it loads now) — `unknown`. No
 * answer at all: the fetch's own failure (the line's), thrown; (T1016 review) both asks are aborted after `timeoutMs`.
 */
export function siteProbe(f: typeof fetch, entry: string | null, site: string, timeoutMs = LOOK_MS): SiteProbe {
  return async (chunkUrl) => {
    if (!chunkUrl || !entry) return "unknown";
    const stop = new AbortController();
    const timer = setTimeout(() => stop.abort(), timeoutMs);
    try {
      const r = await f(chunkUrl, { cache: "no-store", signal: stop.signal });
      if (r.status !== 404) return "unknown";
      const page = await f(site, { cache: "no-store", signal: stop.signal });
      if (!page.ok) throw new Error(`the site's page could not be read to explain a missing chunk: ${site}: HTTP ${page.status}`);
      return (await page.text()).includes(entry) ? "live" : "replaced";
    } finally { clearTimeout(timer); }
  };
}

/**
 * (T1016 review) HOW LONG THE SECOND LOOK MAY HOLD A REPORT: past it the failure is reported as it is (as when the
 * pointer does not answer), whatever the probe is still doing — a stalled read never holds a report for good.
 */
export const LOOK_MS = 10_000;

/** `p`, or `late` once `ms` have passed without it */
function within<T>(p: Promise<T>, ms: number, late: T): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const out = Promise.race([p, new Promise<T>((ok) => { timer = setTimeout(() => ok(late), ms); })]);
  return out.finally(() => clearTimeout(timer));
}

/** (T1016 review) A chunk asked again with no answer: the failure, with that said first (the crash server's `network`
 *  words: the line, not this app), as a NEW error — the one the part shows is never written. */
function unanswered(e: unknown, x: unknown): Error {
  const was = e instanceof Error ? e : new Error(String(e));
  const why = x instanceof Error ? x.message : String(x);
  const out = new Error(`no answer when asked again (${why}): ${was.message}`, { cause: e });
  out.name = was.name;
  return out;
}

/**
 * The second look (core `setFailureTriage`). A probe's failure: a data 404 is reported as it is (the server answered it;
 * a pointer that does not answer leaves it unexplained); a chunk asked again with no answer is reported as the line's
 * (`unanswered`); any other probe failure is thrown, and core reports the failure as it is and the probe's beside it.
 * Each look ends within `timeoutMs` (`LOOK_MS`): late, the failure is reported as it is.
 */
export function replacedTriage(o: { state: ReplacedState; data: ReplacedProbe | null; site: SiteProbe | null;
                                    timeoutMs?: number }): (e: unknown) => Promise<unknown> | null {
  const ms = o.timeoutMs ?? LOOK_MS;
  return (e) => {
    const chunk = o.site ? missingChunk(e) : null;
    if (chunk) return within(o.site!(chunk.url).then((v): unknown => {
      if (v !== "replaced") return e;
      o.state.raise({ kind: "code", release: null });
      return replacedError("code", { opened: null, live: null, cause: e });
    }, (x: unknown) => { if (isEnvironment(x)) return unanswered(e, x); throw x; }), ms, e);
    const url = o.data ? notFoundUrl(e) : null;
    if (!url || !o.data!.covers(url)) return null;      // not a release file (a feed's 404): reported at once, as it is
    return within(o.data!(url).then(({ verdict, live, opened }): unknown => {
      if (verdict !== "replaced") return e;
      // raised for the release whose file it was: one the page has since moved off by an apply shows no notice
      o.state.raise({ kind: "data", release: opened });
      return replacedError("data", { opened, live, cause: e });
    }, (x: unknown) => {
      // the pointer did not answer (the line, or aborted after the wait): the 404 came from the server and stays
      // unexplained — reported as it is
      if (isEnvironment(x)) return e;
      throw x;
    }), ms, e);
  };
}

/** What the notice draws: its words and its one action, or null (nothing raised, or raised for a release no longer open). */
export interface ReplacedNotice { readonly kind: ReplacedKind; readonly line: string; readonly action: string;
                                  readonly a11y: string; readonly act: () => void }

/**
 * The notice for the release in use (`release`): a data notice raised for another release (the one just replaced by the
 * apply) is not shown. `act`: the kind's one action.
 */
export function useReplacedNotice(state: ReplacedState | null, release: string | null,
                                  act: Record<ReplacedKind, () => void>): ReplacedNotice | null {
  const raised = useSyncExternalStore(state?.subscribe ?? noSub, state?.get ?? none, state?.get ?? none);
  if (!raised || (raised.kind === "data" && raised.release !== release)) return null;
  const w = REPLACED_WORDS[raised.kind];
  return { kind: raised.kind, line: w.line, action: w.action, a11y: w.a11y, act: act[raised.kind] };
}
const noSub = () => () => {};
const none = () => null;

/**
 * THE DATA NOTICE'S ACTION: the live release put in use through the app's one in-session apply. Ready already (the
 * update service staged it): applied now. Else the service is asked (a manual check: its failure is shown on the sync
 * icon) and the apply follows the moment its download is ready; a check that stages nothing, or a download that fails,
 * ends the wait (the notice stays, its action can be pressed again). `ended`: null once applied, else why not (the
 * automatic refresh's answer, `ReplacedRefresh.refresh`).
 */
export function loadLiveRelease(u: Pick<UpdateService, "snapshot" | "subscribe" | "check">, apply: () => void,
                                ended: (why: string | null) => void = () => {}): void {
  const phase = () => u.snapshot().release?.phase ?? null;
  if (phase() === "ready") { apply(); ended(null); return; }
  let checked = false, done = false;
  /** once per press: applied when ready, else the wait ends once the check is in and nothing is downloading */
  const look = () => {
    if (done) return;
    const p = phase();
    if (p === "ready") { done = true; off(); apply(); ended(null); }
    else if (checked && p !== "downloading" && p !== "waiting-wifi") {
      done = true; off();
      ended(`the update service staged no newer release (its release phase: ${p ?? "none"})`);
    }
  };
  const off = u.subscribe(look);
  void u.check({ manual: true, feeds: false }).finally(() => { checked = true; look(); });
}

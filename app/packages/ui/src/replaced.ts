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
 *   notice   one line and one action (core `REPLACED_WORDS`): data → put the live release in use through the app's ONE
 *            in-session apply (`loadLiveRelease`: the update service stages it, the app's `apply` boots onto it — no
 *            second loader); code → reload the page (the new site, and with it the live release).
 */
import { useSyncExternalStore } from "react";
import { isEnvironment, missingChunk, notFoundUrl, replacedError, REPLACED_WORDS, type ReplacedKind } from "@app/core";
import type { UpdateService } from "@app/data";
import type { ReplacedProbe } from "@app/data/release";

/** What is raised: the kind, and (data) the release this page had open when it was found replaced. */
export interface ReplacedRaised { readonly kind: ReplacedKind; readonly release: string | null }

export interface ReplacedState {
  get(): ReplacedRaised | null;
  /** raise a notice; the code notice outranks the data one (a reload loads both) */
  raise(r: ReplacedRaised): void;
  subscribe(on: () => void): () => void;
}

export function createReplacedState(): ReplacedState {
  let now: ReplacedRaised | null = null;
  const subs = new Set<() => void>();
  return {
    get: () => now,
    raise(r) {
      if (now?.kind === "code" || (now?.kind === r.kind && now.release === r.release)) return;
      now = r;
      for (const s of [...subs]) s();
    },
    subscribe(on) { subs.add(on); return () => { subs.delete(on); }; },
  };
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
 * ends the wait (the notice stays, its action can be pressed again).
 */
export function loadLiveRelease(u: Pick<UpdateService, "snapshot" | "subscribe" | "check">, apply: () => void): void {
  const phase = () => u.snapshot().release?.phase ?? null;
  if (phase() === "ready") { apply(); return; }
  let checked = false, done = false;
  /** once per press: applied when ready, else the wait ends once the check is in and nothing is downloading */
  const look = () => {
    if (done) return;
    const p = phase();
    if (p === "ready") { done = true; off(); apply(); }
    else if (checked && p !== "downloading" && p !== "waiting-wifi") { done = true; off(); }
  };
  const off = u.subscribe(look);
  void u.check({ manual: true, feeds: false }).finally(() => { checked = true; look(); });
}

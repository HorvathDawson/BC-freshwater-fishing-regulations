/**
 * (T676 item 2, owner 2026-11-20: a per-feed minimum app version) A LIVE FEED NEWER THAN THE APP IS REFUSED, NOT MISREAD.
 *
 * Every feed sits at one unversioned key (`feeds/dfo/index.json`, `feeds/inseason/index.json`, `feeds/live/index.json`,
 * `runtiming/index.json`). Every writer stamps its index with `minAppVersion`, the oldest app that reads its format
 * (`FEED_MIN_APP`, generated from `pipeline/common/feed_formats.py`; a format bump raises it in the same edit). A reader
 * given this gate checks an index BEFORE it reads it: an app older than the index asks for refuses it — no report (a
 * known state, as the release gate's `app-too-old`), its LAST CAPTURE answers, and the update service's snapshot says
 * which version is needed (`needsApp`), which raises the release gate's own update prompt ("New data needs the latest
 * app"). A newer index that passes clears that feed's need. (T676 L3) A FILE under an index (a gauge station file,
 * `obs.json`) refused for its own stamp holds its need under its own name until that file reads: the index passing
 * never clears it, so the More row and the prompt do not flicker while the file is still refused.
 *
 * The same rule as the release (`releaseAllowed`): no `minAppVersion` passes; one that is not a semver refuses (the app
 * cannot check it, and keeping what it has is the safe answer). No app version (the web, whose code is deployed with
 * its data, as for the release gate): every index passes.
 */
import { compareSemver, parseSemver, releaseAllowed, type AppVersion } from "./appVersion";
import { reportFailure } from "./failure";

/** The feeds whose index carries `minAppVersion` (the update service's ids, plus run timing, read on its own). */
export type GatedFeed = "dfo" | "inseason" | "gauges" | "runtiming";

/** Thrown by `FeedGate.check`: this app is older than the index asks for. Readers catch it by `isFeedTooNew`. */
export class FeedNeedsNewerApp extends Error {
  readonly feed: GatedFeed;
  readonly need: string;
  constructor(feed: GatedFeed, need: string) {
    super(`${feed} feed needs app ${need} or newer`);
    this.name = "FeedNeedsNewerApp";
    this.feed = feed;
    this.need = need;
  }
}

/** Is `e` the gate's refusal? (by name too: an error that crossed a module copy is still one) */
export function isFeedTooNew(e: unknown): e is FeedNeedsNewerApp {
  return e instanceof FeedNeedsNewerApp || (e instanceof Error && e.name === "FeedNeedsNewerApp" && "need" in e);
}

export interface FeedGate {
  /** Throws `FeedNeedsNewerApp` when `index` asks for a newer app than this one; otherwise clears `feed`'s INDEX need
   *  (a file's own hold stands: see `checkFile`). */
  check(index: unknown, feed: GatedFeed): void;
  /**
   * A FILE UNDER a feed's index (a gauge station file, `obs.json`), named by `file` (its url): throws `FeedNeedsNewerApp`
   * when it asks for a newer app, and HOLDS that need under the file's name; a readable copy of the same file clears its
   * own hold. (T676 L3) A file's hold is the file's: the index passing, or a 304 of it (`note(feed, null)`), never
   * clears it, so the More row and the prompt stay steady while that file is still refused.
   */
  checkFile(doc: unknown, feed: GatedFeed, file?: string): void;
  /** (T676 L2) Forget a file's hold without reading it (the origin no longer has the file: a 404). */
  clearFile(feed: GatedFeed, file: string): void;
  /** Set a feed's INDEX need from outside a read (the update service: a kept refusal after a restart; a 304 of the
   *  index this app reads); null clears it. A file's hold is not touched. */
  note(feed: GatedFeed, need: string | null): void;
  /** The highest version a refused index or file asks for, or null (everything this app was offered, it reads). */
  needs(): string | null;
  /** Each refused feed and the highest version it asks for: its index's need and its files' holds together. */
  refused(): Readonly<Partial<Record<GatedFeed, string>>>;
  /** Told when `needs()` or `refused()` may have moved. */
  subscribe(on: () => void): () => void;
}

const minOf = (index: unknown): unknown =>
  index && typeof index === "object" && !Array.isArray(index) ? (index as { minAppVersion?: unknown }).minAppVersion : undefined;

/** The gate for this app's version; `null` (the web): every index passes. */
export function createFeedGate(app: AppVersion | null): FeedGate {
  /** each feed's INDEX need (set and cleared by the index: `check`, `note`) */
  const index: Partial<Record<GatedFeed, string>> = {};
  /** (T676 L3) each feed's FILE holds, by file name: set by a refused file, cleared only by that file */
  const files: Partial<Record<GatedFeed, Map<string, string>>> = {};
  const listeners = new Set<() => void>();
  const highest = (vs: readonly string[]): string | null => (vs.length ? vs.reduce((a, b) => (higher(a, b) ? a : b)) : null);
  const all = (): Partial<Record<GatedFeed, string>> => {
    const out: Partial<Record<GatedFeed, string>> = {};
    for (const f of new Set([...Object.keys(index), ...Object.keys(files)]) as Set<GatedFeed>) {
      const n = highest([...(index[f] ? [index[f]!] : []), ...(files[f]?.values() ?? [])].sort());
      if (n) out[f] = n;
    }
    return out;
  };
  /** run `change`; tell the listeners when what a reader sees (`refused()`) moved */
  const changing = (change: () => void) => {
    const before = JSON.stringify(all());
    change();
    if (JSON.stringify(all()) === before) return;
    // a listener that throws never breaks a read: it is reported (T368), the others still told
    for (const l of [...listeners]) { try { l(); } catch (e) { reportFailure(e); } }
  };
  const setIndex = (feed: GatedFeed, need: string | null) => changing(() => {
    if (need === null) delete index[feed];
    else index[feed] = need;
  });
  const setFile = (feed: GatedFeed, file: string, need: string | null) => changing(() => {
    const m = files[feed] ?? (files[feed] = new Map());
    if (need === null) m.delete(file);
    else m.set(file, need);
    if (!m.size) delete files[feed];
  });
  return {
    check(doc, feed) {
      const g = app ? releaseAllowed(minOf(doc), app) : ({ ok: true } as const);
      if (!g.ok) {
        setIndex(feed, g.need);
        throw new FeedNeedsNewerApp(feed, g.need);
      }
      setIndex(feed, null);
    },
    checkFile(doc, feed, file = "") {
      const g = app ? releaseAllowed(minOf(doc), app) : ({ ok: true } as const);
      if (!g.ok) {
        setFile(feed, file, g.need);
        throw new FeedNeedsNewerApp(feed, g.need);
      }
      setFile(feed, file, null);
    },
    clearFile(feed, file) { setFile(feed, file, null); },
    note(feed, need) { setIndex(feed, app ? need : null); },
    needs: () => highest(Object.values(all()).filter((v): v is string => !!v).sort()),
    refused: all,
    subscribe(on) { listeners.add(on); return () => { listeners.delete(on); }; },
  };
}

/** a ≥ b by semver; a value that is not a semver ranks above (it is said as asked: the app cannot read it either) */
function higher(a: string, b: string): boolean {
  if (!parseSemver(a)) return true;
  if (!parseSemver(b)) return false;
  return compareSemver(a, b) >= 0;
}

/**
 * (T676 item 2) IS THIS APP TOO OLD FOR WHAT THE ORIGIN HOLDS: a release refused for its version (`app-too-old`, T251) or
 * a live feed whose index asks for a newer app (the update service's `needsApp`). The one test the strong update prompt
 * ("New data needs the latest app") reads, whichever it was.
 */
export function appTooOld(u: { readonly release?: { readonly phase: string } | null; readonly needsApp?: string | null }
                             | null | undefined): boolean {
  return !!u && (u.release?.phase === "app-too-old" || !!u.needsApp);
}

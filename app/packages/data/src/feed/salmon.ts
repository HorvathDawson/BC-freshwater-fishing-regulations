/**
 * (T211 part 2) THE LIVE DFO SALMON FEED, over HTTP: `{base}/index.json` (`dfo-feed/1`, `pipeline/regs/dfo_salmon/
 * live.py`) — the local tile server's `/feeds/dfo/` in dev, R2's `feeds/dfo/` in production, the same bytes.
 *
 * THE LAST CAPTURE IS KEPT (owner 2026-10-27). Every feed this device reads whole is written to `store` (a small file on
 * a phone, a browser storage key on the web); offline, or when the fetch fails, the last capture answers — with its own
 * time on the card. Only a device that has never captured one reads the release's salmon file alone. A capture that is
 * not a whole `dfo-feed/1` is never kept and never read.
 *
 * Asked once per `ttlMs` (the publisher runs every 6 h in season); every answer resolves, never rejects.
 */
import type { Region } from "../pack.generated";
import { parseSalmonFeed, pickSalmon, type SalmonFacts, type SalmonFeed } from "@app/core";

export interface SalmonFeedStore { read(): string | null; write(text: string): void }
export interface SalmonFeedSource {
  /** The newest feed: fetched now, else the last captured, else null. */
  latest(): Promise<SalmonFeed | null>;
  /**
   * (T231) The update service fetched a changed feed whole: take it (kept as the capture, answered from now on without a
   * fetch). Throws when it is not a whole `dfo-feed/1`: nothing is changed.
   */
  offer?(text: string): void;
  /** (T231) Bumped by every feed taken (`offer`): what holds an answer built from the feed asks again when it moves. */
  version?(): number;
}

export const SALMON_FEED_TTL_MS = 60 * 60_000;

export function httpSalmonFeed(base: string, store?: SalmonFeedStore, fetchImpl: typeof fetch = fetch,
                               opts: { ttlMs?: number; now?: () => number } = {}): SalmonFeedSource {
  const ttl = opts.ttlMs ?? SALMON_FEED_TTL_MS, now = opts.now ?? Date.now;
  let held: { at: number; p: Promise<SalmonFeed | null> } | null = null;
  const captured = (): SalmonFeed | null => {
    try { const t = store?.read(); return t ? parseSalmonFeed(JSON.parse(t)) : null; } catch { return null; }
  };
  const get = async (): Promise<SalmonFeed | null> => {
    try {
      const r = await fetchImpl(`${base.replace(/\/$/, "")}/index.json`, { cache: "no-store" });
      if (r.ok) {
        const text = await r.text();
        const f = parseSalmonFeed(JSON.parse(text));
        const was = captured();
        if (f && (!was || f.at >= was.at)) {
          try { store?.write(text); } catch { /* a cache: a refused write only costs the next offline open */ }
          return f;
        }
      }
    } catch { /* offline, or not a feed: the capture answers */ }
    return captured();
  };
  let version = 0;
  return {
    latest() {
      if (!held || now() - held.at > ttl) held = { at: now(), p: get() };
      return held.p;
    },
    offer(text) {
      const f = parseSalmonFeed(JSON.parse(text));
      if (!f) throw new Error("not a dfo-feed/1 feed");
      try { store?.write(text); } catch { /* a cache: a refused write only costs the next offline open */ }
      held = { at: now(), p: Promise.resolve(f) };
      version += 1;
    },
    version: () => version,
  };
}

/**
 * The release loader with the feed laid over its salmon (`pickSalmon`), so the card (`loader.salmon()`) and the map
 * (`source.salmonFacts()`) read ONE answer. Asked once per loader: the Standard / Salmon toggle never re-fetches. Every
 * other read is the loader's own (delegated, not copied).
 */
export function withLiveSalmon<L extends { salmon(regions?: readonly Region[]): Promise<SalmonFacts | null> }>(
  loader: L, feed?: SalmonFeedSource): L {
  if (!feed) return loader;
  // per set of slices asked (a page's region; every one for the map): the feed is fetched once (`latest`) either way
  const live = new Map<string, Promise<SalmonFacts | null>>();
  const out = Object.create(loader) as L;
  out.salmon = (regions?: readonly Region[]) => {
    // (T231) keyed by the feed's version too: a feed the update service took is read at the next ask
    const key = `${feed.version?.() ?? 0}:${regions ? [...regions].sort().join(",") : "*"}`;
    let p = live.get(key);
    if (!p) {
      p = loader.salmon(regions).then(async (f) => (f ? pickSalmon(f, await feed.latest().catch(() => null)) : f));
      p.catch(() => live.delete(key));
      live.set(key, p);
    }
    return p;
  };
  return out;
}

/**
 * (T230) THE PROVINCIAL IN-SEASON CHANGES, over HTTP: `{base}/index.json` (the T225 worker's `inseason-feed/1`) and
 * `{base}/matches.json` (`inseason-links/1`, `pipeline.regs.inseason.publish`) — the local tile server's
 * `/feeds/inseason/` in dev, R2's `feeds/inseason/` in production, the same bytes.
 *
 * THE LAST CAPTURE IS KEPT, as for the DFO feed: every whole pair this device reads is written to `store`; offline, or
 * when a fetch fails, the last capture answers. A feed fetched without its links (links missing or unreadable) is
 * kept with the captured links, never with none: an outage of one file must not turn matched changes into the
 * "not on the map yet" notice. Asked once per `ttlMs`; every answer resolves, never rejects.
 */
import { parseInSeasonFeed, parseInSeasonLinks, type InSeasonFeed, type InSeasonLinks } from "@app/core";

export interface InSeasonStore { read(): string | null; write(text: string): void }
export interface InSeasonData { readonly feed: InSeasonFeed; readonly links: InSeasonLinks | null }
export interface InSeasonSource {
  /** The newest pair: fetched now, else the last captured, else null. */
  latest(): Promise<InSeasonData | null>;
  /** (T231) The update service saw the feed move: forget what is held, so the next `latest()` fetches the pair. */
  refresh?(): void;
}

export const INSEASON_TTL_MS = 60 * 60_000;

export function httpInSeason(base: string, store?: InSeasonStore, fetchImpl: typeof fetch = fetch,
                             opts: { ttlMs?: number; now?: () => number } = {}): InSeasonSource {
  const ttl = opts.ttlMs ?? INSEASON_TTL_MS, now = opts.now ?? Date.now;
  const root = base.replace(/\/$/, "");
  let held: { at: number; p: Promise<InSeasonData | null> } | null = null;
  const captured = (): InSeasonData | null => {
    try {
      const t = store?.read();
      if (!t) return null;
      const j = JSON.parse(t) as { feed?: unknown; links?: unknown };
      const feed = parseInSeasonFeed(j.feed);
      return feed ? { feed, links: parseInSeasonLinks(j.links) } : null;
    } catch { return null; }
  };
  const getJson = async (name: string): Promise<unknown> => {
    try {
      const r = await fetchImpl(`${root}/${name}`, { cache: "no-store" });
      return r.ok ? JSON.parse(await r.text()) : null;
    } catch { return null; }
  };
  const get = async (): Promise<InSeasonData | null> => {
    const [f, l] = await Promise.all([getJson("index.json"), getJson("matches.json")]);
    const feed = parseInSeasonFeed(f);
    const was = captured();
    if (!feed || (was && feed.fetchedAt < was.feed.fetchedAt)) return was;
    const links = parseInSeasonLinks(l) ?? was?.links ?? null;
    try { store?.write(JSON.stringify({ feed: f, links: links })); } catch { /* a cache: costs only the next offline open */ }
    return { feed, links };
  };
  return {
    latest() {
      if (!held || now() - held.at > ttl) held = { at: now(), p: get() };
      return held.p;
    },
    refresh() { held = null; },
  };
}

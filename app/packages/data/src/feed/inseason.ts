/**
 * (T230) THE PROVINCIAL IN-SEASON CHANGES, over HTTP: `{base}/index.json` (the T225 worker's `inseason-feed/1`) and
 * `{base}/matches.json` (`inseason-links/4`, ONE file for every release, T673; `pipeline.regs.inseason.publish`) — the local tile server's
 * `/feeds/inseason/` in dev, R2's `feeds/inseason/` in production, the same bytes.
 *
 * THE LAST CAPTURE IS KEPT, as for the DFO feed: every whole pair this device reads is written to `store`; offline, or
 * when a fetch fails, the last capture answers. A feed fetched without its links (links missing or unreadable) is
 * kept with the captured links, never with none: an outage of one file must not turn matched changes into the
 * "not on the map yet" notice. Asked once per `ttlMs`; every answer resolves, never rejects.
 */
import { INSEASON_FAILURES, INSEASON_FEED_FORMAT, parseInSeasonFeed, reported, parseInSeasonLinks, type InSeasonFeed,
         type InSeasonLinks } from "@app/core";

export interface InSeasonStore { read(): string | null; write(text: string): void }
export interface InSeasonData { readonly feed: InSeasonFeed; readonly links: InSeasonLinks | null }
export interface InSeasonSource {
  /** The newest pair: fetched now, else the last captured, else null. */
  latest(): Promise<InSeasonData | null>;
  /** (T231) The update service saw the feed move: forget what is held, so the next `latest()` fetches the pair. */
  refresh?(): void;
  /**
   * (T548) The update service's apply: the index it fetched, THROWING when it is not a readable `inseason-feed/1` (a
   * format bump, a row the app cannot read) — so the service shows and reports it and does not mark the feed seen —
   * else as `refresh`.
   */
  offer?(text: string): void;
  /**
   * (T700) Why the newest fetch could not be READ (an index in a format this app does not read), or null: the inbox
   * shows a line for it (`INBOX_WORDS.failure`) while the capture answers. Offline is not one. It is also reported.
   */
  failure?(): string | null;
}

export const INSEASON_TTL_MS = 60 * 60_000;

export function httpInSeason(base: string, store?: InSeasonStore, fetchImpl: typeof fetch = fetch,
                             opts: { ttlMs?: number; now?: () => number } = {}): InSeasonSource {
  const ttl = opts.ttlMs ?? INSEASON_TTL_MS, now = opts.now ?? Date.now;
  const root = base.replace(/\/$/, "");
  let held: { at: number; p: Promise<InSeasonData | null> } | null = null;
  let failure: string | null = null;
  const captured = (): InSeasonData | null => {
    try {
      const t = store?.read();
      if (!t) return null;
      const j = JSON.parse(t) as { feed?: unknown; links?: unknown };
      const feed = parseInSeasonFeed(j.feed);
      return feed ? { feed, links: parseInSeasonLinks(j.links) } : null;
    } catch { return null; }
  };
  // (T368) OFFLINE reads as "nothing new": the captured copy below answers, its age with it. An HTTP refusal or a body
  // that will not parse answers the same way but is REPORTED (and shown: the update service's own check of this feed
  // fails on the sync badge) — never a silent "no in-season changes".
  const getJson = async (name: string): Promise<unknown> => {
    try {
      const r = await fetchImpl(`${root}/${name}`, { cache: "no-store" });
      if (!r.ok) throw new Error(`in-season ${name}: HTTP ${r.status}`);
      return JSON.parse(await r.text());
    } catch (e) { return reported(null)(e); }
  };
  const get = async (): Promise<InSeasonData | null> => {
    const [f, l] = await Promise.all([getJson("index.json"), getJson("matches.json")]);
    const feed = parseInSeasonFeed(f);
    // (T548) a body that came back but is not a feed this app reads is REPORTED; the capture answers meanwhile.
    // (T700) and held as this source's `failure()`, for the inbox's line, until a readable one comes back
    if (f !== null && !feed) {
      failure = `in-season index.json: not a readable ${INSEASON_FEED_FORMAT} feed — refused`;
      reported(null)(new Error(failure));
    } else if (feed) failure = null;
    const was = captured();
    if (!feed || (was && feed.fetchedAt < was.feed.fetchedAt)) return was;
    // (T520) links that came back but will not read (another format, a notice it cannot place) are REPORTED, naming the
    // format seen (T673: an app older or newer than the file); the captured links answer meanwhile, so matched changes
    // do not turn into the "not on the map yet" notice — with none captured, every change is in that notice (the bell)
    const read = parseInSeasonLinks(l);
    if (l !== null && !read) reported(null)(new Error(INSEASON_FAILURES.unreadableLinks((l as { format?: unknown } | null)?.format)));
    const links = read ?? was?.links ?? null;
    try { store?.write(JSON.stringify({ feed: f, links: links })); } catch { /* a cache: costs only the next offline open */ }
    return { feed, links };
  };
  return {
    latest() {
      if (!held || now() - held.at > ttl) held = { at: now(), p: get() };
      return held.p;
    },
    refresh() { held = null; },
    failure: () => failure,
    offer(text) {
      if (!parseInSeasonFeed(JSON.parse(text))) throw new Error(`not a readable ${INSEASON_FEED_FORMAT} feed`);
      held = null;
    },
  };
}

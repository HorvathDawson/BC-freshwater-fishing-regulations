/**
 * (T211 part 2) THE LIVE DFO SALMON FEED, over HTTP: `{base}/index.json` (`dfo-feed/3`, `pipeline/regs/dfo_salmon/
 * live.py`) — the local tile server's `/feeds/dfo/` in dev, R2's `feeds/dfo/` in production, the same bytes.
 *
 * THE LAST CAPTURE IS KEPT (owner 2026-10-27). Every feed this device reads whole is written to `store` (a small file on
 * a phone, a browser storage key on the web); offline, or when the fetch fails, the last capture answers — with its own
 * time on the card. Only a device that has never captured one reads the release's salmon file alone. A capture that is
 * not a whole `dfo-feed/3` is never kept and never read.
 *
 * Asked once per `ttlMs` (the publisher runs every 6 h in season); every answer resolves, never rejects.
 *
 * (T521) ONE LOAD PATH for DFO's places no curated location knows: when the index counts any (`unplaced`), its
 * `unplaced.json` is read beside it, in the same fetch, and kept in the same capture (`unplacedRows`). A failed read
 * is reported (`reportFailure`) and the feed answers without the rows — the card's feed line says how many could not
 * be read (`salmonFeedLine`), never an empty list passed off as none.
 */
import type { Region } from "../pack.generated";
import { reportFailure, parseDfoUnplaced, parseSalmonFeed, pickSalmon, SALMON_FEED_FORMAT, type SalmonFacts, type SalmonFeed } from "@app/core";

export interface SalmonFeedStore { read(): string | null; write(text: string): void }
export interface SalmonFeedSource {
  /** The newest feed: fetched now, else the last captured, else null. */
  latest(): Promise<SalmonFeed | null>;
  /**
   * (T231) The update service fetched a changed feed whole: take it (kept as the capture, answered from now on without a
   * fetch). Throws when it is not a whole `dfo-feed/3`: nothing is changed.
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
  const root = base.replace(/\/$/, "");
  /** a capture, its kept unplaced rows checked as a fresh read is (`parseDfoUnplaced`) */
  const readFeed = (text: string): SalmonFeed | null => {
    const raw = JSON.parse(text) as { unplacedRows?: unknown };
    const f = parseSalmonFeed(raw);
    return f && raw.unplacedRows !== undefined ? { ...f, unplacedRows: parseDfoUnplaced(raw.unplacedRows) } : f;
  };
  const captured = (): SalmonFeed | null => {
    try { const t = store?.read(); return t ? readFeed(t) : null; } catch { return null; }
  };
  /** (T521) the index's unplaced places read beside it, and the whole kept as the capture */
  const complete = async (f: SalmonFeed): Promise<SalmonFeed> => {
    let out: SalmonFeed = f;
    if (f.unplaced !== 0) {
      try {
        const r = await fetchImpl(`${root}/unplaced.json`, { cache: "no-store" });
        if (!r.ok) throw new Error(`dfo unplaced.json: HTTP ${r.status}`);
        out = { ...f, unplacedRows: parseDfoUnplaced(JSON.parse(await r.text())) };
      } catch (e) { reportFailure(e); /* the feed answers without them; its line says how many were not read */ }
    } else out = { ...f, unplacedRows: [] };
    try { store?.write(JSON.stringify(out)); } catch { /* a cache: a refused write only costs the next offline open */ }
    return out;
  };
  const get = async (): Promise<SalmonFeed | null> => {
    try {
      const r = await fetchImpl(`${root}/index.json`, { cache: "no-store" });
      if (r.ok) {
        const f = parseSalmonFeed(JSON.parse(await r.text()));
        // (T556) a body that came back but is not a feed this app reads (another format: a bump this build predates) is
        // REPORTED, as in-season's is (T548); the capture — else the release's salmon file — answers meanwhile
        if (!f) throw new Error(`dfo index.json: not a readable ${SALMON_FEED_FORMAT} feed — refused`);
        const was = captured();
        if (!was || f.at >= was.at) return await complete(f);
      }
    } catch (e) { reportFailure(e); /* offline, or not a feed: the capture answers, its age said; (T368) not offline: reported */ }
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
      if (!f) throw new Error("not a dfo-feed/3 feed");
      held = { at: now(), p: complete(f) };   // (T521) its unplaced places read beside it, as a fetch does
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
/**
 * (T287) The same release facts under the same capture answer with the same object: an applied update that kept the
 * salmon files (`adoptDecoded`) hands the map the facts it is drawing, and nothing is relabelled.
 */
const PICKED = new WeakMap<SalmonFacts, { live: unknown; out: SalmonFacts }>();
function picked(f: SalmonFacts, live: Parameters<typeof pickSalmon>[1]): SalmonFacts {
  const was = PICKED.get(f);
  if (was && was.live === live) return was.out;
  const out = pickSalmon(f, live);
  PICKED.set(f, { live, out });
  return out;
}

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
      p = loader.salmon(regions).then(async (f) => (f ? picked(f, await feed.latest().catch(() => null)) : f));
      p.catch(() => live.delete(key));
      live.set(key, p);
    }
    return p;
  };
  return out;
}

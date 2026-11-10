/**
 * (T385, owner 2026-11-04) SHARE A WATER: the state behind the water header's Share control.
 *
 * THE LINK IS THE PAGE'S RELOAD ADDRESS: `linkSearch` — the one function the web's address bar calls (T369: the water,
 * its part as a place made from the release, the view, the day when not today, the Conditions horizon) — on the
 * production site (`@app/core` `shareUrl`). Nothing of the sender's own is in it (their angler, their theme): the query
 * is made from the route alone. It is made ahead, as the page changes, so a press shares at once (a browser's share
 * sheet must open inside the tap; a fetch of the release's `lines` first would lose it).
 *
 * WHAT A PRESS DOES is the platform's (`SharePort`, the app's): the phone's share sheet; on the web `navigator.share`
 * where the browser has one, else the link copied ("Link copied"). A dismissed sheet is the reader's choice, not a
 * failure: nothing said, nothing reported (T368). Any other failure is said under the control and reported.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { GAUGE_SHARE_WORDS, bcNow, SHARE_WORDS, WEEKDAYS, gaugePageStations, gaugeUrl, isEnvironment, reportFailure, servedItems, shareUrl } from "@app/core";
import { readGaugeRows, type ReleaseLoader } from "@app/data/release";
import { waterHasPage, type PageDay } from "@app/data/regs";
import { linkSearch, placeReads, type PlaceReads } from "./place";
import { loadRegsStore } from "./regs";
import type { Route } from "./route";
import { waterFrame } from "./route";
import type { AddressState } from "./startLink";

export type ShareOutcome = "sent" | "copied" | "dismissed";

/** The platform's share (`apps/mobile` `share.native.ts` / `share.web.ts`). */
export interface SharePort {
  /** Send the link: resolves how it went (a dismissed sheet resolves "dismissed"); rejects only on a real failure. */
  send(link: { url: string; title: string }): Promise<ShareOutcome>;
}

export interface ShareControl {
  label: string;
  /** "Link copied", or why it could not be shared; null: nothing to say */
  said: string | null;
  busy: boolean;
  press(): void;
}

/** How long "Link copied" stays under the control. */
const SAID_MS = 2500;

/**
 * The shared link for a route: the reload address on the production site. `places`: the release's reads a place is made
 * from (null: the address without one). Null when the route has no water in front, or nothing to address it by (an
 * unnamed reach with no place).
 */
export async function shareLink(places: PlaceReads | null, r: Route, extra: AddressState | null,
                                onMissing?: (e: Error) => void, now?: Date): Promise<string | null> {
  if (waterFrame(r) === null) return null;
  const search = await linkSearch(places, r, "", onMissing, extra, now);
  return search ? shareUrl(search) : null;
}

/** The release's place reads, once its regs store is open (the store the pages already share, `loadRegsStore`). */
async function placesOf(loader: ReleaseLoader | null): Promise<PlaceReads | null> {
  if (!loader) return null;
  return placeReads(await loadRegsStore(loader), loader);
}

/**
 * (T530) THE ONE PRESS, for every Share control (the water header's, a gauge card's): busy while the platform's sheet is
 * open, "Link copied" for a moment, a dismissed sheet silent, any other failure said under the control and reported.
 * `send(url | Promise<url|null>)`: a null link says there is nothing to share.
 */
export function useSendLink(port: SharePort | null) {
  const [said, setSaid] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const alive = useRef(true);
  useEffect(() => () => { alive.current = false; }, []);
  useEffect(() => {
    if (said !== SHARE_WORDS.copied) return;
    const t = setTimeout(() => { if (alive.current) setSaid(null); }, SAID_MS);
    return () => clearTimeout(t);
  }, [said]);
  const clear = useCallback(() => setSaid(null), []);
  const send = useCallback((link: string | null | Promise<string | null>) => {
    if (!port || busy) return;
    setSaid(null);
    const go = (url: string | null): Promise<void> => {
      if (url === null) { setSaid(SHARE_WORDS.none); return Promise.resolve(); }
      return port.send({ url, title: SHARE_WORDS.title }).then((how) => {
        if (alive.current && how === "copied") setSaid(SHARE_WORDS.copied);
      });
    };
    setBusy(true);
    (typeof link === "string" || link === null ? go(link) : link.then(go)).catch((e: unknown) => {
      // a dismissed sheet resolves "dismissed"; a browser that says so by rejecting (AbortError) is the same choice
      if (isEnvironment(e)) return;
      reportFailure(e);
      if (alive.current) setSaid(SHARE_WORDS.failed);
    }).finally(() => { if (alive.current) setBusy(false); });
  }, [port, busy]);
  return { said, busy, send, clear };
}

/** Whether the site has a page for a station: asked once per station (`useGaugePageCheck`). */
export type GaugePageCheck = (station: string) => Promise<boolean>;

/**
 * (T530) WHETHER A STATION HAS A PAGE, asked of the release the app holds with the SAME rule the site's build lists pages by:
 * the station's gauge-serves rows (`readGaugeRows`) and, for each water it serves, whether that water has a page
 * (`waterHasPage`), through core's `gaugePageStations`. Asked once per station; null while the app has no release.
 */
export function useGaugePageCheck(loader: ReleaseLoader | null): GaugePageCheck | null {
  const cache = useRef(new Map<string, Promise<boolean>>());
  useEffect(() => { cache.current = new Map(); }, [loader]);
  return useCallback<GaugePageCheck>((station) => {
    let p = cache.current.get(station);
    if (!p) {
      p = (async () => {
        const rows = await readGaugeRows(loader!, station);
        if (!rows) return false;
        const store = await loadRegsStore(loader!), on = pageDay();
        const pages = new Set<string>();
        for (const item of servedItems(rows, station)) if (await waterHasPage(store, item, on)) { pages.add(item); break; }   // one is enough
        return gaugePageStations(rows, (i) => pages.has(i)).has(station);
      })().catch((e: unknown) => { cache.current.delete(station); throw e; });   // a failure is not remembered; the caller reports it
      cache.current.set(station, p);
    }
    return p;
  }, [loader]);
}

const pageDay = (): PageDay => {
  const d = bcNow();
  return { day: { month: d.getMonth() + 1, day: d.getDate() }, weekday: WEEKDAYS[(d.getDay() + 6) % 7]!, year: d.getFullYear() };
};

/**
 * (T530) SHARE A GAUGE: the Conditions gauge card's Share. The link is the gauge's public page (`@app/core` `gaugeUrl`, the
 * function the site's build names the page with). `target` is the charted station and its name as sourced; `hasPage` is
 * `useGaugePageCheck`. No control until the station is known to have a page; a failed check is reported and shows none.
 */
export function useGaugeShare(port: SharePort | null, target: { station: string | null; name: string | null } | null,
                              hasPage: GaugePageCheck | null): ShareControl | null {
  const { said, busy, send, clear } = useSendLink(port);
  const [page, setPage] = useState<string | null>(null);
  const station = target?.station ?? null, name = target?.name ?? null;
  useEffect(() => {
    clear();
    setPage(null);
    if (!port || !hasPage || !station || !name) return;
    let live = true;
    hasPage(station).then((yes) => { if (live && yes) setPage(gaugeUrl(station, name)); }, reportFailure);
    return () => { live = false; };
  }, [port, hasPage, station, name, clear]);
  const press = useCallback(() => { if (page) send(page); }, [page, send]);
  return port && page ? { label: GAUGE_SHARE_WORDS.label, said, busy, press } : null;
}

export function useShare(port: SharePort | null, loader: ReleaseLoader | null, route: Route,
                         extra: AddressState): ShareControl | null {
  const { said, busy, send, clear } = useSendLink(port);
  const water = waterFrame(route) !== null;
  /** the link for what is on screen now, made ahead (a promise: a press before it lands waits for it) */
  const link = useRef<Promise<string | null> | null>(null);
  const ready = useRef<string | null | undefined>(undefined);
  const day = extra.day?.getTime() ?? null;
  useEffect(() => {
    clear();
    ready.current = undefined;
    if (!port || !water) { link.current = null; return; }
    // a place the release cannot give is reported (as the address bar does) and the link made without it
    const p = placesOf(loader).catch((e: unknown) => { reportFailure(e); return null; })
      .then((places) => shareLink(places, route, extra, reportFailure));
    link.current = p;
    p.then((u) => { if (link.current === p) ready.current = u; }, reportFailure);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the route and the address, by value
  }, [port, loader, water, route, day, extra.horizon]);

  const press = useCallback(() => {
    if (!port || !link.current) return;
    // made already: sent inside the tap; else once it lands
    send(ready.current !== undefined ? ready.current : link.current);
  }, [port, send]);

  return port && water ? { label: SHARE_WORDS.label, said, busy, press } : null;
}

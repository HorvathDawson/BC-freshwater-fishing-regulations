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
import { SHARE_WORDS, isEnvironment, reportFailure, shareUrl } from "@app/core";
import type { ReleaseLoader } from "@app/data/release";
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

export function useShare(port: SharePort | null, loader: ReleaseLoader | null, route: Route,
                         extra: AddressState): ShareControl | null {
  const [said, setSaid] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const water = waterFrame(route) !== null;
  /** the link for what is on screen now, made ahead (a promise: a press before it lands waits for it) */
  const link = useRef<Promise<string | null> | null>(null);
  const ready = useRef<string | null | undefined>(undefined);
  const alive = useRef(true);
  useEffect(() => () => { alive.current = false; }, []);
  const day = extra.day?.getTime() ?? null;
  useEffect(() => {
    setSaid(null);
    ready.current = undefined;
    if (!port || !water) { link.current = null; return; }
    // a place the release cannot give is reported (as the address bar does) and the link made without it
    const p = placesOf(loader).catch((e: unknown) => { reportFailure(e); return null; })
      .then((places) => shareLink(places, route, extra, reportFailure));
    link.current = p;
    p.then((u) => { if (link.current === p) ready.current = u; }, reportFailure);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the route and the address, by value
  }, [port, loader, water, route, day, extra.horizon]);

  useEffect(() => {
    if (said !== SHARE_WORDS.copied) return;
    const t = setTimeout(() => { if (alive.current) setSaid(null); }, SAID_MS);
    return () => clearTimeout(t);
  }, [said]);

  const press = useCallback(() => {
    if (!port || busy || !link.current) return;
    setSaid(null);
    const send = (url: string | null): Promise<void> => {
      if (url === null) { setSaid(SHARE_WORDS.none); return Promise.resolve(); }
      return port.send({ url, title: SHARE_WORDS.title }).then((how) => {
        if (alive.current && how === "copied") setSaid(SHARE_WORDS.copied);
      });
    };
    setBusy(true);
    // made already: sent inside the tap; else once it lands
    const go = ready.current !== undefined ? send(ready.current) : link.current.then(send);
    go.catch((e: unknown) => {
      // a dismissed sheet resolves "dismissed"; a browser that says so by rejecting (AbortError) is the same choice
      if (isEnvironment(e)) return;
      reportFailure(e);
      if (alive.current) setSaid(SHARE_WORDS.failed);
    }).finally(() => { if (alive.current) setBusy(false); });
  }, [port, busy]);

  return port && water ? { label: SHARE_WORDS.label, said, busy, press } : null;
}

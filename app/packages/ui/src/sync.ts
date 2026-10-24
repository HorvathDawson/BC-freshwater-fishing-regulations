/**
 * THE SYNC ICON'S STATE (owner 2026-10-22: "we do want the sync icon up there with a way to indicate if it needs to be
 * refreshed"): when the live flows were last published (the feed index's own `fetchedAt`), whether the server names a
 * newer data release than the one in use, and whether a check is under way — plus `refresh`, what the icon's tap does.
 *
 * WHAT A REFRESH DOES, and nothing heavier: the feed forgets its short-lived cache (`GaugeFeed.refresh`), so the index
 * is fetched again — the flows' age here, and the Conditions colouring re-asked through `epoch` — and the release
 * manifest is read again (`checkRelease`) to see whether a newer release exists. A newer release is SAID (the icon's
 * dot, More's Data block), never loaded under the reader: on a phone it installs in the background at launch (P11) and
 * on the web a reload takes it, so the state on screen is never swapped mid-use.
 *
 * WHAT THE DOT ASKS FOR (`syncVM`, core more.ts) is only what a tap or a reopen fixes: `checkedAt` — when THIS DEVICE
 * last reached the feed, by its own clock — past the feed's cadence, or null (not reached), or a newer release. The
 * feed's `fetchedAt` is the publisher's clock: when the server itself is behind, a tap cannot help, so it is said, not
 * dotted.
 *
 * Read again on its own every `SYNC_POLL_MS` (the feed's own cache lives as long), so the icon turns stale by itself.
 * Only the feed's index is asked, never the bundle: a cold start opens no core for the icon.
 *
 * THE SAME READ CARRIES HYDAT'S NOTICE (owner 2026-10-22 late, "similar to bundle changes where the app checks when in
 * service for updates"): `hydatNotice` off the index's `maintenance` (core `hydatNotice`) — for the Data sources page
 * only. It never reaches the sync dot: no tap or reopen fixes it, the owner's update does. (T68) And the newest
 * forecast run's issue time (`forecastIssuedAt`), for the More tab's Forecasts line: no second read.
 */
import { useCallback, useEffect, useState } from "react";
import { hydatNotice as noticeOf, type HydatNotice } from "@app/core";
import { FEED_TTL_MS, type GaugeFeed } from "@app/data";
import { usePoll } from "./async";

/**
 * (T68) The index's word on forecasts: the stamp, null when it says none is held, undefined when it does not say (an
 * index from before the field, or none read). An unexpected type is no word.
 */
function forecastOf(i: { forecastIssuedAt?: string | null } | null): string | null | undefined {
  const v = i?.forecastIssuedAt;
  return typeof v === "string" || v === null ? v : undefined;
}

/** How often the sync state is re-read while the app is open: the feed's own cache lifetime (`TTL_MS`, http.ts). */
export const SYNC_POLL_MS = FEED_TTL_MS;
/** A refresh the reader asked for shows "checking" at least this long, so the tap visibly did something. */
export const SYNC_MIN_CHECK_MS = 600;

export interface SyncState {
  /** the feed index's `fetchedAt`, or null (no feed, not reached, or not answered yet) */
  readonly feedFetchedAt: string | null;
  /** when this device last reached the feed (epoch ms, its clock), or null (never, or the last check failed) */
  readonly checkedAt: number | null;
  /** a newer data release the server names than `release`, or null */
  readonly newRelease: string | null;
  /** a check is under way */
  readonly checking: boolean;
  /** what the feed's index says about HYDAT ("due" / { current: the newest HYDAT, ISO day }), or null (no word) */
  readonly hydatNotice: HydatNotice | null;
  /**
   * (T68) the index's newest forecast run (`forecastIssuedAt`): its instant; null when the index says none is held;
   * undefined when it does not say (an index from before the field, or the feed not reached)
   */
  readonly forecastIssuedAt: string | null | undefined;
  /** bumped by every refresh the reader asks for: what else re-asks the feed keys on it */
  readonly epoch: number;
  readonly refresh: () => void;
}

export function useSync({ feed, release, checkRelease }: {
  feed?: GaugeFeed;
  /** the data release in use (the loader's), or null (no release open: nothing to compare) */
  release: string | null;
  /** reads the release id the server names now; absent: no release to check */
  checkRelease?: (() => Promise<string | null>) | null;
}): SyncState {
  const [epoch, setEpoch] = useState(0);
  const tick = usePoll(SYNC_POLL_MS, !!feed || !!checkRelease);
  const [st, setSt] = useState<{ feedFetchedAt: string | null; checkedAt: number | null; latest: string | null;
                                 hydatNotice: HydatNotice | null; forecastIssuedAt: string | null | undefined;
                                 checking: boolean }>(
    { feedFetchedAt: null, checkedAt: null, latest: null, hydatNotice: null, forecastIssuedAt: undefined,
      checking: !!feed || !!checkRelease });
  useEffect(() => {
    if (!feed && !checkRelease) return;
    let live = true;
    setSt((s) => ({ ...s, checking: true }));
    const asked = epoch > 0;
    Promise.all([
      feed ? feed.index().then((i) => ({ at: i?.fetchedAt ?? null, hydat: noticeOf(i), forecast: forecastOf(i) }),
                               () => ({ at: null, hydat: null, forecast: undefined }))
           : Promise.resolve({ at: null, hydat: null, forecast: undefined }),
      checkRelease ? checkRelease().catch(() => null) : Promise.resolve(null),
      asked ? new Promise((r) => setTimeout(r, SYNC_MIN_CHECK_MS)) : null,
    ]).then(([idx, latest]) => {
      if (live) setSt({ feedFetchedAt: idx.at, checkedAt: idx.at !== null ? Date.now() : null, latest,
                        hydatNotice: idx.hydat, forecastIssuedAt: idx.forecast, checking: false });
    });
    return () => { live = false; };
  }, [feed, checkRelease, epoch, tick]);
  const refresh = useCallback(() => { feed?.refresh?.(); setEpoch((e) => e + 1); }, [feed]);
  const newRelease = st.latest !== null && release !== null && st.latest !== release ? st.latest : null;
  return { feedFetchedAt: st.feedFetchedAt, checkedAt: st.checkedAt, newRelease, hydatNotice: st.hydatNotice,
           forecastIssuedAt: st.forecastIssuedAt, checking: st.checking, epoch, refresh };
}

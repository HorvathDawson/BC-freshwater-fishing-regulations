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
 * Read again on its own every `SYNC_POLL_MS` (the feed's own cache lives as long), so the icon turns stale by itself.
 * Only the feed's index is asked, never the bundle: a cold start opens no core for the icon.
 */
import { useCallback, useEffect, useState } from "react";
import type { GaugeFeed } from "@app/data";
import { usePoll } from "./async";

/** How often the sync state is re-read while the app is open: the feed's own cache lifetime (`TTL_MS`, http.ts). */
export const SYNC_POLL_MS = 5 * 60_000;
/** A refresh the reader asked for shows "checking" at least this long, so the tap visibly did something. */
export const SYNC_MIN_CHECK_MS = 600;

export interface SyncState {
  /** the feed index's `fetchedAt`, or null (no feed, not reached, or not answered yet) */
  readonly feedFetchedAt: string | null;
  /** a newer data release the server names than `release`, or null */
  readonly newRelease: string | null;
  /** a check is under way */
  readonly checking: boolean;
  /** bumped by every refresh the reader asks for: what else re-asks the feed keys on it */
  readonly epoch: number;
  readonly refresh: () => void;
}

export function useSync({ feed, release, checkRelease }: {
  feed?: GaugeFeed;
  /** the data release in use (the loader's), or null (the dev bundle: nothing to compare) */
  release: string | null;
  /** reads the release id the server names now; absent: no release to check (the dev bundle) */
  checkRelease?: (() => Promise<string | null>) | null;
}): SyncState {
  const [epoch, setEpoch] = useState(0);
  const tick = usePoll(SYNC_POLL_MS, !!feed || !!checkRelease);
  const [st, setSt] = useState<{ feedFetchedAt: string | null; latest: string | null; checking: boolean }>(
    { feedFetchedAt: null, latest: null, checking: !!feed || !!checkRelease });
  useEffect(() => {
    if (!feed && !checkRelease) return;
    let live = true;
    setSt((s) => ({ ...s, checking: true }));
    const asked = epoch > 0;
    Promise.all([
      feed ? feed.index().then((i) => i?.fetchedAt ?? null, () => null) : Promise.resolve(null),
      checkRelease ? checkRelease().catch(() => null) : Promise.resolve(null),
      asked ? new Promise((r) => setTimeout(r, SYNC_MIN_CHECK_MS)) : null,
    ]).then(([feedFetchedAt, latest]) => { if (live) setSt({ feedFetchedAt, latest, checking: false }); });
    return () => { live = false; };
  }, [feed, checkRelease, epoch, tick]);
  const refresh = useCallback(() => { feed?.refresh?.(); setEpoch((e) => e + 1); }, [feed]);
  const newRelease = st.latest !== null && release !== null && st.latest !== release ? st.latest : null;
  return { feedFetchedAt: st.feedFetchedAt, newRelease, checking: st.checking, epoch, refresh };
}

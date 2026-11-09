/**
 * (T521, owner 2026-11-09) THE BELL'S DFO SECTION: the live DFO feed's rules on a water no curated binding knows (a
 * completely new water), read through the SAME feed source as the Salmon card (`httpSalmonFeed`: one fetch, one
 * capture). Words and rows are core's (`dfoNoticeRows`); a list the feed could not read is a row of its own, counted.
 * Asked again when the feed moves (`version`: a feed the update service took).
 */
import { dfoNoticeRows, type DfoNoticeVM } from "@app/core";
import type { SalmonFeedSource } from "@app/data";
import { useAsync } from "./async";

const NONE: DfoNoticeVM = dfoNoticeRows(null);

export function useDfoNotice(source: SalmonFeedSource | null | undefined): DfoNoticeVM {
  const a = useAsync(() => source!.latest(), `dfo-notice:${source?.version?.() ?? 0}`, !!source);
  return a.state === "ready" ? dfoNoticeRows(a.value) : NONE;
}

/**
 * (T526, owner 2026-11-09) WHEN THE FEEDS WORKER LAST CHECKED DFO — the feed's `at`, for the More tab's Data line (stale
 * past a day, `STALE_AFTER.dfo`). Never DFO's content time. No feed read, or a stamp that is not a time, is a FAILURE
 * (thrown, so `useAsync` reports it) and the line reads unknown — never the changed time, never the release's date.
 */
export function useDfoCheckedAt(source: SalmonFeedSource | null | undefined): string | null {
  const a = useAsync(async () => {
    const f = await source!.latest();
    if (!f) throw new Error("dfo feed: no feed read, so no checked time");
    if (!Number.isFinite(Date.parse(f.at))) throw new Error(`dfo feed: checked time "${f.at}" is not a time`);
    return f.at;
  }, `dfo-checked:${source?.version?.() ?? 0}`, !!source);
  return a.state === "ready" ? a.value : null;
}

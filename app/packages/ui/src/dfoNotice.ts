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

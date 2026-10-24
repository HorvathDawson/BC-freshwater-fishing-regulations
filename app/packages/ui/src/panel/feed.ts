/**
 * THE LIVE FEED AS THE PANEL HOOKS READ IT: the day's index, a quiet gauge's last report from its own file, and the
 * Province's forecast attribution.
 */
import { useMemo } from "react";
import { answerFrom, type DonorRow, type PanelAnswer, type StationId } from "@app/data";
import { donorCounted } from "@app/core";
import { useAsync } from "../async";

type Index = Parameters<typeof answerFrom>[1];

/** The live feed as the panel hooks read it: one call for the day's index. */
export type PanelFeed = {
  index(): Promise<Index>;
  /** A station's last report from its own file (`GaugeFeed.lastReport`), for a quiet gauge the index has dropped. */
  lastReport?(station: StationId): Promise<string | null | undefined>;
};

/**
 * The quiet donors whose last report the index does not carry — the ones worth asking their own file about. A file
 * that was read and holds nothing (null) marks its row `noData`, so the silent line can say the station "has never
 * reported"; a file that could not be read (undefined: a 404, offline) leaves the row as it was — not known.
 */
export function lastReportWanted(rows: readonly DonorRow[]): string[] {
  return rows.filter((r) => !donorCounted(r) && !r.observedAt).map((r) => r.station);
}

/**
 * THE LAST REPORTS, onto the rows (owner 2026-10-21): only a quiet row with none of its own takes one; the index's word
 * always stands. Returns the same array when there is nothing to fill.
 */
export function withLastReports(rows: readonly DonorRow[],
                                got: ReadonlyMap<string, string | null | undefined>): DonorRow[] {
  const want = new Set(lastReportWanted(rows));
  const heard = (s: string) => want.has(s) && got.get(s) !== undefined;
  if (![...want].some(heard)) return rows as DonorRow[];
  return rows.map((r) => {
    if (!heard(r.station)) return r;
    const at = got.get(r.station);
    return at ? { ...r, observedAt: at } : { ...r, noData: true as const };
  });
}

/** `withLastReports`, fetched: one file per quiet, index-less donor, through the feed's own cache. */
export function useLastReports(feed: PanelFeed | undefined, panel: PanelAnswer): PanelAnswer {
  const want = lastReportWanted(panel.rows);
  const key = `last:${want.join(",")}`;
  const got = useAsync(
    async () => new Map(await Promise.all(
      want.map(async (s) => [s, await feed!.lastReport!(s as StationId).catch(() => undefined)] as const))),
    key, !!feed?.lastReport && want.length > 0);
  const map = got.state === "ready" ? got.value : null;
  return useMemo(() => {
    if (!map) return panel;
    const rows = withLastReports(panel.rows, map);
    return rows === panel.rows ? panel : { ...panel, rows };
  }, [panel, map]);
}

/** The Province's forecast attribution, as the live feed carries it (T52); null until it is in, or when absent. */
export function useForecastAttribution(feed: PanelFeed | undefined): string | null {
  const got = useAsync(async () => (feed ? (await feed.index())?.forecastAttribution ?? null : null),
                       "forecast-attribution", feed !== undefined);
  return got.state === "ready" ? got.value ?? null : null;
}

/**
 * (T1194) THE GAUGE PAGE'S DATA, finished: what the app's gauge screen and the site's gauge page (the one shared piece,
 * `ui-native/GaugeReading.tsx`) say of a station — hooks and a pure VM, so the words are decided here and drawn there (AGENTS 25).
 *
 *   `gaugeReadingVM`   the reading line, its standing and colour-bar position, from the feed's reading; or, with no CURRENT
 *                      reading (core `readingCurrent`, which this file only calls), the plain line saying when it last reported.
 *   `useGaugePage`     the release's facts for one station (`readGaugePage`): name, flags, the waters it serves.
 */
import { CONDITIONS_WORDS, GAUGE_PAGE_WORDS, donorLastReport, figureIn, freshness, now as clockNow, readingAgeLine,
         readingCurrent, readingStanding, type Standing } from "@app/core";
import type { Parameter } from "@app/data";
import { readGaugePage, type GaugePageFacts, type ReleaseLoader } from "@app/data/release";
import type { Conditions } from "./hooks";
import { readingQuantityOf } from "./panel/conditions";
import { useAsync, type Async } from "./async";

export interface GaugeReadingVM {
  /** "8.01 m³/s": the figure with its unit; null: no current reading */
  figure: string | null;
  quantity: Parameter | null;
  /** the standing in one line ("Far lower than normal · lower than …"); null where the reading has no record to rank by */
  standing: string | null;
  /** the reading's place on the colour bar, 0–1; null: no mark (no current reading, or not ranked) */
  percentile: number | null;
  /** the foot line: how old the reading is */
  age: string | null;
  /** no current reading: when it last reported, said plainly */
  noReading: string | null;
}

/** The reading as the page says it. `ref`: the instant it is judged against (the feed's own write time, `Conditions.fetchedAt`). */
export function gaugeReadingVM(c: Conditions | null, ref: number | null = c?.fetchedAt ?? null): GaugeReadingVM {
  const none = (last: string): GaugeReadingVM => ({ figure: null, quantity: null, standing: null, percentile: null, age: null,
                                                    noReading: GAUGE_PAGE_WORDS.noReading(last) });
  if (!c) return none("The live feed has no report from this gauge.");
  const rq = readingQuantityOf(c);
  if (!rq || !readingCurrent(c.observedAt, ref)) return none(donorLastReport(c.observedAt));
  const ranked = c.percentile !== null && c.percentile !== undefined;
  return {
    figure: `${figureIn(rq.figure, rq.quantity)} ${CONDITIONS_WORDS.unit[rq.quantity]}`,
    quantity: rq.quantity,
    standing: ranked ? readingStanding({ percentile: c.percentile, standing: c.standing as Standing | null,
                                        ...(c.rankedAt !== undefined ? { rankedAt: c.rankedAt } : {}), observedAt: c.observedAt }) : null,
    percentile: ranked ? c.percentile! : null,
    age: readingAgeLine(freshness(c.fetchedAt, clockNow()), c.observedAt, undefined, !!c.kept),
    noReading: null,
  };
}

/**
 * WHERE A GAUGE DOT IS TAPPABLE (acceptance 8): the Conditions tab only, and not while a spot is being placed or (wide) search is
 * open. The Map tab, the Runs tab and pick mode keep their taps as they were.
 */
export const gaugeTapOn = (o: { tab: string; picking: boolean; wideSearch: boolean }): boolean =>
  o.tab === "conditions" && !o.picking && !o.wideSearch;

/** The release's facts for one station; `value` null once read: the release holds no such station. */
export function useGaugePage(loader: ReleaseLoader | null, station: string): Async<GaugePageFacts | null> {
  return useAsync(async () => (loader ? readGaugePage(loader, station) : null), `gauge-page:${loader?.release ?? ""}:${station}`, loader !== null);
}

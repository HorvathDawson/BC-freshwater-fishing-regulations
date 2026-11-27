/**
 * (T1194) THE SHARED PIECE: one gauge's reading and chart — the figure and its standing in one line, the Conditions tab's
 * colour bar with this gauge marked where it stands now, and the chart with its Flow / Level and range switches. Drawn by the
 * app's gauge screen (`GaugeScreen`) and mounted by the site's gauge page (`tools/seo/gaugeChart.tsx`), so the reading and its
 * words exist once. It decides nothing: every word is `@app/ui` `gaugeReadingVM` / core `GAUGE_PAGE_WORDS`, the bar is the
 * map legend's own `LegendRamp`, the chart is `StationChart`.
 *
 * A reading that cannot be loaded is a shown failure (the hook reported it); a gauge with no current reading says when it last
 * reported, with no figure, standing or mark guessed. `theme`: the map's theme name (the colour bar's ramp).
 */
import { useState } from "react";
import { View, type LayoutChangeEvent } from "react-native";
import { FLOW_LEGEND, GAUGE_PAGE_WORDS } from "@app/core";
import type { StationId } from "@app/data";
import { gaugeReadingVM, noRecordLine, readingQuantityOf, useSeries, useStationChart, useStationReading, type ChartSource } from "@app/ui";
import type { WaterSource } from "@app/data";
import { LegendRamp } from "./Chrome";
import { ErrorState } from "./ErrorState";
import { LoadingState } from "./LoadingState";
import { StationChart } from "./StationChart";
import { Text } from "./Text";
import { TYPE } from "./type";
import { flowRampStops, SPACE, type Palette } from "./theme";

/** the trend word beside the figure */
const TREND_FACE = TYPE.title;   // same size as the figure (owner 2026-11-26: "big near the big units"); one step down read as a footnote

/** what the piece asks of its source: the chart's two questions and the station's reading */
export type GaugeReadingSource = ChartSource & Pick<WaterSource, "gaugeNow">;

export function GaugeReading({ source, station, palette, theme, feed, notes, tidal = false }: {
  source: GaugeReadingSource; station: StationId; palette: Palette; theme: string;
  /** the feed whose observations the chart draws before the envelope lands (the site's `httpFeed`, the app's feed) */
  feed?: Parameters<typeof useStationChart>[2]["feed"];
  /** the notes the page puts under the chart for the quantity it shows (the tide, on Level: `gaugeNotes`); the app's, from the release */
  /** the station is flagged tidal (`gauge.tidal`): its Level has no trend word, the tide swings it */
  tidal?: boolean;
  notes?: (quantity: "discharge" | "level" | null) => readonly string[];
}) {
  const [width, setWidth] = useState(0);
  const measure = (e: LayoutChangeEvent) => { const w = Math.round(e.nativeEvent.layout.width); if (w > 0 && w !== width) setWidth(w); };
  const got = useStationReading(source, station);
  const c = got.state === "ready" ? got.value : null;
  const rq = c ? readingQuantityOf(c) : null;
  const vm = useStationChart(source, station, { width, feed: feed ?? null, offRecord: c?.offRecord ?? null,
                                               ...(rq ? { reports: c!.reports ?? null, readingQuantity: rq.quantity } : {}) });
  // the last day's measured flow, for the trend word (the 72 h series, shared with the chart's own asks)
  const day = useSeries(source, station, "72h", rq?.quantity, undefined, rq !== null);
  const r = got.state === "ready" ? gaugeReadingVM(c, day.state === "ready" ? day.value ?? null : null, undefined, tidal) : null;
  const noRecord = noRecordLine(vm.shown, vm.banded, vm.known, rq ? c!.reports : null, c?.offRecord ?? null);
  const failed = got.state === "failed" || vm.chart.state === "failed" || vm.series.state === "failed";
  return (
    <View testID="gauge-reading" onLayout={measure} style={{ gap: SPACE.s3 }}>
      {got.state === "loading" && <LoadingState palette={palette} size={48} label={GAUGE_PAGE_WORDS.reading} style={{ alignItems: "center", paddingVertical: SPACE.s3 }} />}
      {got.state === "failed" && <ErrorState palette={palette} size="body" testID="gauge-feed-failed" message={GAUGE_PAGE_WORDS.feedFailed} />}
      {r?.noReading && <Text testID="gauge-no-reading" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.noReading}</Text>}
      {r?.figure && (
        <View style={{ gap: SPACE.s1 }}>
          <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "baseline", columnGap: SPACE.s2 }}>
            <Text testID="gauge-figure" style={{ ...TYPE.title, color: palette.ink }}>{r.figure}</Text>
            {r.trend && <Text testID="gauge-trend" style={{ ...TREND_FACE, color: palette.sub }}>{`· ${r.trend}`}</Text>}
          </View>
          {(noRecord ?? r.standing) && <Text testID={noRecord ? "reading-no-record" : "gauge-standing"} style={{ ...TYPE.bodyStrong, color: palette.ink }}>{noRecord ?? r.standing}</Text>}
        </View>
      )}
      {r?.ranked && !noRecord && (
        <LegendRamp palette={palette} stops={flowRampStops(theme)} marks={[r.percentile!]}
                    title={FLOW_LEGEND.title[r.quantity === "level" ? "level" : "discharge"]}
                    low={FLOW_LEGEND.low} mid={FLOW_LEGEND.mid} high={FLOW_LEGEND.high} counted={false}
                    // (review 2026-11-27) the mark is THIS GAUGE (not "1 reach"), and the bar says it in words, never by colour alone
                    tickLabel={(_n, at) => GAUGE_PAGE_WORDS.barMark(FLOW_LEGEND.near(at))}
                    describe={`${FLOW_LEGEND.scale(FLOW_LEGEND.title[r.quantity === "level" ? "level" : "discharge"], FLOW_LEGEND.low, FLOW_LEGEND.high, FLOW_LEGEND.mid)}. ${GAUGE_PAGE_WORDS.barMark(FLOW_LEGEND.near(r.percentile!))}`} />
      )}
      {r?.age && <Text testID="gauge-age" style={{ ...TYPE.meta, color: palette.sub }}>{r.age}</Text>}
      <StationChart vm={vm} palette={palette} colour={palette.live} />
      {notes?.(c ? vm.shown : null).map((n) => <Text key={n} testID="gauge-note" style={{ ...TYPE.meta, color: palette.sub }}>{n}</Text>)}
      {failed && got.state !== "failed" && <ErrorState palette={palette} size="body" testID="gauge-chart-failed" message={GAUGE_PAGE_WORDS.feedFailed} />}
    </View>
  );
}

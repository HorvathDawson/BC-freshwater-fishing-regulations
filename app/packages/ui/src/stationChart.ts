/**
 * (T1039) ONE GAUGE'S CHART, AS DATA: the quantity, span and model picks, the series and the drawn chart for one station —
 * what a water's Conditions card (`ConditionsPanel`) charts, and what the public gauge page charts (owner 2026-11-23: "the
 * gauge website … should also have the same chart as the website"). Both draw it with `@app/ui-native`'s `StationChart`, so
 * the page and the app cannot chart a gauge differently: one hook, one component, two callers.
 *
 * It asks its source two questions only (`gaugeSeries`, `gaugeParameters`): the app's source answers them from the release
 * and the feed; the gauge page's (`@app/data` `stationChartSource`) from the same feed file over the same release's
 * climatology, which the site build writes into the page.
 */
import { useMemo, useState } from "react";
import type { Horizon } from "@app/core";
import type { Forecast, GaugeFeed, Parameter, Series, StationId, WaterSource } from "@app/data";
import type { Async } from "./async";
import { cardParameters, cardQuantity, chartParameter } from "./answers";
import { useChartPrefs } from "./chartPrefs";
import { useChartView, type ChartDraft, type ChartView } from "./chart";
import { useGaugeParameters, useSeries } from "./hooks";
import { changeChartModel, forecastDrawn, type ModelPick } from "./panel/change";
import type { ChartSpan } from "@app/core";

/** what the chart asks of a source: a station's series and the quantities it has an envelope for */
export type ChartSource = Pick<WaterSource, "gaugeSeries" | "gaugeParameters">;

/** a day marked on the plot beside today's rule (`useChartView`'s `days`) */
export type ChartDay = { at: number; label: string; kind: "conditions" | "regs" };

export interface StationChartInput {
  /**
   * The quantity, when the CALLER owns it (a water's Conditions view: the card's own pick, opening on the map's Flow or
   * Level; T728) with its setter. Left undefined, the chart owns the choice itself.
   */
  readonly parameter?: Parameter;
  readonly onParameter?: (p: Parameter) => void;
  /** the quantities the station's reading says it reports (a level-only gauge reports no flow), and the one it ranked in */
  readonly reports?: readonly Parameter[] | null;
  readonly readingQuantity?: Parameter | null;
  /** (T817) the Flow / Level choices for the WATER; null: what the charted gauge measures */
  readonly offer?: readonly Parameter[] | null;
  /** the panel's horizon: under Change the chart draws the change's own lead model (`changeChartModel`) */
  readonly horizon?: Horizon;
  /** the width the chart is drawn at (0 until measured) */
  readonly width: number;
  /** (T256) the feed whose observations are drawn before the envelope lands; absent: none */
  readonly feed?: { observations?: GaugeFeed["observations"] } | null;
  /** (T567, T604) the days marked on the plot beside today's rule */
  readonly days?: readonly ChartDay[];
  /**
   * (T1047) the quantities the station carries live on another zero than its record (the reading's `offRecord`): charted as
   * the line alone, with no band, earlier years or year scale
   */
  readonly offRecord?: readonly Parameter[] | null;
}

export interface StationChartVM {
  /** the quantities the station has an envelope for, and the ones the card offers */
  readonly banded: readonly Parameter[];
  readonly available: readonly Parameter[];
  /** the quantity asked (the caller's or the chart's own), its setter, and what the card shows (T581) */
  readonly picked: Parameter | null;
  readonly pick: (p: Parameter) => void;
  readonly shown: Parameter;
  readonly silent: boolean;
  readonly options: readonly Parameter[];
  /** the station's quantities are known; the chart is drawn (known, and the station measures `shown`) */
  readonly known: boolean;
  readonly charts: boolean;
  readonly span: ChartSpan;
  readonly setSpan: (s: ChartSpan) => void;
  readonly pickState: ModelPick;
  readonly setPick: (f: (p: ModelPick) => ModelPick) => void;
  /** the chart (its shape and actions), the series it is drawn from, the runs it offers and the one drawn */
  readonly chart: Async<ChartView | null>;
  readonly series: Async<Series | null>;
  readonly runs: readonly Forecast[];
  readonly run: Forecast | null;
  readonly drawsForecast: boolean;
}

const NO_DAYS: readonly ChartDay[] = [];

export function useStationChart(source: ChartSource, station: StationId | null, i: StationChartInput): StationChartVM {
  // WHICH QUANTITIES THIS STATION CAN ANSWER IN, asked of the bundle rather than assumed: 237 BC stations measure stage and
  // never discharge. (T830) and any the publisher says it reports live with no band (Capilano's level): offered, unranked
  const params = useGaugeParameters(source, station);
  const banded = params.state === "ready" ? params.value : [];
  const available = cardParameters(banded, i.reports);
  const [own, setOwn] = useState<Parameter | null>(null);
  // (T584) the reader's span and model pick are kept for the session, so every way into a water reads them alike
  const { span, setSpan, pick: pickState, setPick } = useChartPrefs();
  const picked = i.parameter ?? own;
  const pick = i.onParameter ?? setOwn;
  const param = chartParameter(picked, available);
  // The series is not asked until the station's quantities are known: asked earlier it is the station's "auto" series
  // and then the picked one a moment later — two loads, and the flicker between them.
  const known = params.state !== "loading";
  // (T581, F1) the card is about the quantity ASKED, always; a station silent in it says so (`silent`), never draws the other
  const { shown, silent, options } = cardQuantity(picked, available, known, i.readingQuantity, i.offer ?? null);
  const charts = known && !silent;
  // WHICH MODEL. Left alone the freshest run is chosen — a UI default, not a judgement. (T377) and the other models drawn
  // beside it, and the forecast switch, as one pick (`pickModel`). (T398) WHETHER A FORECAST IS DRAWN: the reader's switch,
  // null until touched (`forecastDrawn` decides the default — off where ELF is the only model, ELF being opt-in).
  // UNDER CHANGE THE CHART DRAWS THE CHANGE'S OWN LEAD MODEL, not the freshest run (T144 review, `changeChartModel`). The
  // station's change rides every series of the station, so the freshest one (the same request the default makes, shared by
  // `askOnce`) says which.
  const anyRun = useSeries(source, station, span, param, undefined, charts);
  const drawn = changeChartModel(pickState.model, i.horizon ?? 0, anyRun.state === "ready" ? anyRun.value?.change : null);
  // (T256) on a poor line the chart draws the feed's observations and forecast first; the normal range joins them
  const feed = i.feed;
  const draft = useMemo<ChartDraft | null>(() => (feed?.observations ? { feed: { observations: feed.observations.bind(feed) },
                                                                        parameter: picked ?? undefined } : null), [feed, picked]);
  const allRuns = anyRun.state === "ready" ? anyRun.value?.forecasts ?? [] : null;
  const drawsForecast = forecastDrawn(pickState.on, allRuns);
  const chart = useChartView(source, station, span, param, drawn, i.width, charts, draft, drawsForecast, pickState.also,
                             i.days ?? NO_DAYS, !!i.offRecord?.includes(shown));
  const series = useSeries(source, station, span, param, drawn, charts);
  const runs = series.state === "ready" ? series.value?.forecasts ?? [] : [];
  // (T398) switched off, no run is drawn: nothing below speaks for one (the ELF note, About this forecast)
  const run = (drawsForecast && series.state === "ready" ? series.value?.forecast : null) ?? null;
  return { banded, available, picked, pick, shown, silent, options, known, charts, span, setSpan, pickState, setPick,
           chart, series, runs, run, drawsForecast };
}

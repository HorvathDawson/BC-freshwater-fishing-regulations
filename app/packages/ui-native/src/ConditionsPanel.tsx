/**
 * WHAT THE WATER IS DOING. One implementation, every surface.
 *
 * There were two. Tapping a river on the Map tab opened the water sheet's "conditions"
 * face; tapping the same river on the Conditions tab opened a different screen — different
 * layout, different wording, different controls, and one of them had the chart toggles and
 * the other did not. Two answers to one question, in one app, which is exactly the drift
 * AGENTS rule 23 exists to stop. This file is that answer; both surfaces render it.
 *
 * It decides nothing. The answer, the lead station, the reading and every sentence arrive
 * finished from `useConditionsView` in @app/ui (rule 25, P12a); the chart's words are
 * core's (`conditions.ts`). What is left here is drawing, and the chart's own controls.
 */
import { useContext, useMemo, useRef, useState, type ReactNode } from "react";
import { ScrollView, View, type LayoutChangeEvent } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { CHANGE_LEGEND, CHANGE_LOW_WATER, chartLabel, figure as figureText, CONDITIONS_WORDS, forecastChartLabel, forecastModelOption,
         forecastRunLine, noChartSentence, OTHER_QUANTITY_WORDS, standingRankedIn, waterReportWhere, dayLabel, regsDayLink, DAY_LABELS, bcNow, type ChartSpan } from "@app/core";
import type { Parameter, WaterSource, SectionId, StationId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { cardQuantity, changeChartModel, changeView, chartParameter, currentFigure, forecastDash, forecastDrawn, pickModel, realtimeOnly,
         useChartPrefs, useChartView, useConditionsView, useForecastAttribution, useGaugeParameters,
         useGaugeShare, usePrefetchGauges, useSeries, type ChangeVM, type ShareControl, type GaugeChoiceVM, type GaugeGroupVM, type Horizon,
         type PanelFeed } from "@app/ui";
import { GaugeGroups } from "./GaugeGroups";
import { WaterReportLink } from "./menu/ReportLink";

/** (T569) the gauge feed's version a report names: when the charted station's file was fetched (ISO), else nothing */
const feedStamp = (o: { fetchedAt: number | null } | null): string | null =>
  o?.fetchedAt != null ? new Date(o.fetchedAt).toISOString() : null;
import { GaugeShare } from "./GaugeShare";
import { SharePortContext } from "./SharePortContext";
import { AlertMe } from "./AlertMe";
import { Icon } from "./icons";
import Svg, { Circle, Path } from "react-native-svg";
import { ChartControls } from "./ChartControls";
import { Credits } from "./Credits";
import { LoadingState } from "./LoadingState";
import { usePaneReady } from "./shell/paneReveal";
import { DonorPanel, donorTone, rampAt } from "./DonorPanel";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { ViewSwipe, WaterViewsContext } from "./ViewSwipe";
import { TYPE } from "./type";
import { flowRampStops, GUTTER, SPACE, type Palette, round } from "./theme";
import { hitProps } from "./regs/hit";
import { NoneLine } from "./EmptyState";
import { CoachScrollContext, useCoachScroller, useCoachTarget } from "./CoachMarks";
import { useGuideControl } from "./guideControls";

/** (T584) the body's wrapper: fills the frame as the bare body did; dimmed while the next part's speaker is looked up */
const KEPT_STYLE = { flexGrow: 1, flexShrink: 1 } as const;

/** The units, from core's one table. */
export const UNIT: Readonly<Record<Parameter, string>> = CONDITIONS_WORDS.unit;

export type Span = ChartSpan;

const W = CONDITIONS_WORDS;

export function ConditionsPanel({ source, section, palette, tiles, theme, colour,
                                  parameter, onParameter, from, feed, scroll = true,
                                  footer, credits, horizon = 0, onHorizon, day = null, regsDay, waterLine, waterBox, alertWater, reportAbout = null }: {
  source: WaterSource; section: SectionId | null; palette: Palette;
  /** The live index, for the donor panel. Absent offline — it then says so. */
  feed?: PanelFeed;
  /** Present, the route panel draws the chain of reaches down to the station. */
  tiles?: TileEndpoints; theme?: string;
  /** The trace colour, so a sheet can tint the chart to its own outcome. */
  colour?: string;
  /**
   * The quantity, when the CALLER owns it — the chart has to be about the same thing the
   * map is painting. Left undefined, the panel owns the choice itself.
   */
  parameter?: Parameter; onParameter?: (p: Parameter) => void;
  /** Data credits for the foot of the screen, passed down rather than restated. */
  credits?: readonly string[];
  /** Where the reader tapped, so the route map can mark where they are standing. */
  from?: { lat: number; lon: number } | null;
  scroll?: boolean;
  footer?: React.ReactNode;
  /** How far ahead the estimate is for — 0 is now. The caller (Shell) holds it. */
  horizon?: Horizon;
  /** Set the horizon from the sheet — see DonorPanel. Absent, the chips are not offered. */
  onHorizon?: (d: Horizon) => void;
  /** The day the answer is for; null is today. */
  day?: Date | null;
  /**
   * (T582) THE REGULATIONS DAY (the map's day; null is today), for a water's Conditions view: the panel names its own day,
   * marks the regulations day on the chart when it is inside the forecast window, and says when it is past it. Absent (a
   * sheet): no day line. It never moves the panel's own horizon.
   */
  regsDay?: Date | null;
  /** (T175) The water's own sections, kept at every zoom on the route map (`MapProps.waterLine`). */
  waterLine?: readonly SectionId[];
  /** (T175) The water's box, framed with the gauges when there is no tap (`DonorViewOptions.place`). */
  waterBox?: readonly [number, number, number, number] | null;
  /** (T234) The water a gauge alert set here opens (its durable item, its name); absent: the alert names the gauge. */
  alertWater?: { item: string | null; name: string } | null;
  /** (T569) A water's sheet: the facts its "Report a problem" link sends with the gauge, quantity and span (its item id, name, part). */
  reportAbout?: { water: string | null; place: string | null; part: number | null; partLabel: string | null } | null;
}) {
  /*
   * THE SAME QUANTITY THE MAP IS PAINTING. `parameter` is undefined only under
   * Temperature, which is not a hydrograph, so on Flow or Level this is exactly what the
   * map was asked for.
   */
  const vm = useConditionsView(source, section, day, horizon,
                               { feed, quantity: parameter ?? "discharge" });
  const station = vm.lead;
  // (T530) Share on the card: the charted gauge's public page, offered once the site is known to have one for the station
  // (a title that is only the station number is no name: the address is not made from it)
  const sourced = vm.reading?.title ?? vm.quiet?.title ?? vm.gauges.find((g) => g.station === station)?.name ?? null;
  const ports = useContext(SharePortContext);
  const gaugeShare = useGaugeShare(ports.port, { station, name: sourced !== station ? sourced : null }, ports.hasGaugePage);
  // THE VIEW SWIPE, on this body in a water's views (Regulations · Conditions · Map): the text, the estimate and the
  // reading take it; the maps and the chart are shielded (`MiniMap`, `Hydrograph`), their drags are theirs. The
  // detector sits INSIDE the ScrollView — a browser reads `touch-action` only up to the nearest scroll container.
  const bodySwipe = useContext(WaterViewsContext)?.bodySwipe ?? null;

  // WHICH QUANTITIES THIS STATION CAN ANSWER IN, asked of the bundle rather than assumed:
  // 237 BC stations measure stage and never discharge.
  const params = useGaugeParameters(source, station);
  const available = params.state === "ready" ? params.value : [];
  const [own, setOwn] = useState<Parameter | null>(null);
  // (T584) the reader's span and model pick are kept for the session, so every way into a water reads them alike
  const { span, setSpan, pick: pickState, setPick } = useChartPrefs();
  const picked = parameter ?? own;
  const pick = onParameter ?? setOwn;
  const param = chartParameter(picked, available);
  // The series is not asked until the station's quantities are known: asked earlier it is the station's "auto" series
  // and then the picked one a moment later — two loads, and the flicker between them.
  const known = params.state !== "loading";
  // (T581, F1) the card is about the quantity ASKED, always; a station silent in it says so (`silent`), never draws the other
  const { shown, silent, options } = cardQuantity(picked, available, known, vm.reading?.quantity);
  const charts = known && !silent;
  // WHICH MODEL. Left alone the freshest run is chosen — a UI default, not a judgement.
  // (T377) and the other models drawn beside it, and the forecast switch, as one pick (`pickModel`)
  const model = pickState.model;
  // (T398) WHETHER A FORECAST IS DRAWN: the reader's switch, null until touched (`forecastDrawn` decides the default —
  // off where ELF is the only model, ELF being opt-in).
  const forecastOn = pickState.on;
  // THE CHART IS BUILT FOR THE WIDTH IT IS DRAWN AT (owner 2026-10-21: "the plot doesnt scale to fill page"):
  // measured here, the paths are made for exactly that box and the labels stay label-sized.
  const [chartW, setChartW] = useState(0);
  const measure = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== chartW) setChartW(w);
  };
  // UNDER CHANGE THE CHART DRAWS THE CHANGE'S OWN LEAD MODEL, not the freshest run (T144 review, `changeChartModel`).
  // The station's change rides every series of the station, so the freshest one (the same request the default makes,
  // shared by `askOnce`) says which.
  const link = regsDay === undefined ? null : regsDayLink(regsDay, bcNow());
  const anyRun = useSeries(source, station, span, param, undefined, charts);
  const drawn = changeChartModel(model, horizon,
                                 anyRun.state === "ready" ? anyRun.value?.change : null);
  // (T256) on a poor line the chart draws the feed's observations and forecast first; the normal range joins them
  const draft = useMemo(() => (feed?.observations ? { feed: { observations: feed.observations.bind(feed) },
                                                      parameter: picked ?? undefined } : null), [feed, picked]);
  const allRuns = anyRun.state === "ready" ? anyRun.value?.forecasts ?? [] : null;
  const drawsForecast = forecastDrawn(forecastOn, allRuns);
  const chart = useChartView(source, station, span, param, drawn, chartW, charts, draft, drawsForecast, pickState.also,
                            // (T582, T604) the horizon's own day AND the regulations day when it is inside the window: all drawn
                            // (a chosen Conditions day does not hide the regulations day); the same day is one mark
                            useMemo(() => [
                              ...(vm.ahead ? [{ at: vm.ahead.at, label: vm.ahead.label, kind: "conditions" as const }] : []),
                              ...(link?.kind === "inside" ? [{ at: link.at, label: link.mark, kind: "regs" as const }] : []),
                            ], [vm.ahead, link?.kind === "inside" ? link.at : null, link?.kind === "inside" ? link.mark : null]));
  const series = useSeries(source, station, span, param, drawn, charts);
  // EVERY DONOR'S CHART, ASKED NOW (owner 2026-10-21: "get all the data when the page opens rather than one gauge at a
  // time when clicked"), so the gauge switch draws from what is already here.
  usePrefetchGauges(source, vm.gauges.map((g) => g.station as StationId), span, picked, model ?? undefined);
  const tint = colour ?? palette.live;
  const forecast = chart.state === "ready" ? chart.value?.shape.forecast : null;
  const runs = series.state === "ready" ? series.value?.forecasts ?? [] : [];
  // (T398) switched off, no run is drawn: nothing below speaks for one (the ELF note, About this forecast)
  const run = (drawsForecast && series.state === "ready" ? series.value?.forecast : null) ?? null;
  // The Centre's disclaimer, verbatim, while its forecast is drawn (the same condition the chart draws it under).
  const disclaimer = forecast && run?.disclaimer ? run.disclaimer : null;
  // THE EXPECTED CHANGE (T52), under Change only: each model's own words for the charted gauge, its days as a line with
  // the peak marked, and the Province's attribution as the feed carries it — decided in @app/ui (`changeView`).
  const attribution = useForecastAttribution(feed);
  const change: ChangeVM | null = horizon === "change" && series.state === "ready"
    ? changeView(runs, vm.day, attribution, { width: Math.max(160, (chartW || 300) - CHANGE_AXIS - 2 * SPACE.s3), height: 120 },
                 series.value?.change ?? null) : null;

  // (T456) the walkthrough: its Flow / Level and 72 h / Year steps see (and Show me sets) the chart's two toggles, and the
  // page scrolls a step's control into view
  useGuideControl(options.length > 1 ? "cond:quantity" : null, shown, pick);
  useGuideControl("cond:span", span, setSpan);
  const coachScroll = useCoachScroller();
  const readingRing = useCoachTarget("cond:reading"), chartRing = useCoachTarget("cond:chart");

  // (T508) DRAWN WHOLE: every first answer in (who speaks, the panel and its routes, the reading), and the chart's —
  // its quantities known, its record drawn (not loading, not the last chart dimmed while the next loads). What a pane
  // held over the browse map waits for (`shell/paneReveal.ts`), beside the route map's own draw (`MiniMap`).
  usePaneReady(vm.settled && (station === null || (known && (silent || (chart.state !== "loading" && !(chart.state === "ready" && chart.value?.pending))))));

  // (T584, T580 F11) A PART STEP KEEPS THE LAST BODY, DIMMED, while who speaks for the new part is looked up: this panel
  // stays mounted across a step on one water, and its body (estimate, chart, day strip, scroll place) is not swapped for a
  // spinner and back. The first look at a water, with no body yet, is the spinner.
  const kept = useRef<ReactNode>(null);
  if (vm.loading && kept.current === null)
    return (
      <LoadingState palette={palette} size={110} label={W.readingWater} style={{ paddingVertical: 48, alignItems: "center" }} />
    );

  const reading = vm.reading;

  /*
   * THE HEADLINE'S DETAIL ROWS (P16): the lead gauge's own reading — or, where no gauge
   * may speak, the sentence that says so — and the single-station trace where the panel
   * could not answer (`vm.trace` is null otherwise). They used to stack under the donor
   * list as two more answer-shaped blocks; here they are the working under the answer.
   */
  // (T567, owner's phone 2026-11-09) THE CARD'S FIGURE IS IN THE QUANTITY THE CHART SHOWS, OR IT IS NOT A FIGURE:
  // Fraser at Mission under Flow headed a m³/s chart with "1.548 m · 66th percentile" (its level). A gauge that
  // reports the other quantity only says so, in the words its row and its dot say.
  const otherOnly = reading && reading.quantity !== shown && reading.figures[shown] == null;
  // (T567) the station reports the asked quantity too, but its standing was ranked in the other: the figure of the asked
  // one, with the standing said to be the other's — never the other quantity's figure under this chart
  const figure = reading ? reading.figures[shown] ?? reading.figure : 0;
  // (T581) the station's quantities not yet known and the asked one not among the reading's: nothing is said yet, rather than
  // a claim about the other quantity or its figure under this chart (the pane waits for `known` before it is shown)
  const undecided = reading && !known && reading.figures[shown] == null;
  const readingRows = undecided ? null : reading && otherOnly ? (
        <View ref={readingRing} collapsable={false} style={{ gap: SPACE.s1 }}>
          <Text testID="reading-other-quantity" style={{ ...TYPE.bodyStrong, color: palette.ink }}>
            {OTHER_QUANTITY_WORDS[reading.quantity].reads}
          </Text>
          {reading.forecast && <Text testID="reading-forecast" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{reading.forecast}</Text>}
        </View>
  ) : reading ? (
        <View ref={readingRing} collapsable={false} style={{ gap: SPACE.s1 }}>
          {/* (T567) on a forecast day this figure is TODAY'S, said so; the day asked has its own line below */}
          {reading.scope && <Text testID="reading-scope" style={{ ...TYPE.metaBold, color: palette.sub }}>{reading.scope}</Text>}
          {/* (T367) whose reading this is is the card's title, over it; when it was taken is the card's foot */}
          {/* (T367) THE FIGURE AS A STAT: large in the display face, its unit small beside it, the stage after a dot —
              the monospace figure in the chart's teal read as code beside the rest of the card */}
          <View style={{ flexDirection: "row", alignItems: "baseline", columnGap: SPACE.s2, flexWrap: "wrap" }}>
            <Text testID="reading-figure" style={{ ...TYPE.title, color: palette.ink }}>{figureText(figure)}</Text>
            <Text style={{ ...TYPE.bodyStrong, color: palette.sub }}>{UNIT[shown]}</Text>
            {/* BOTH NUMBERS WHERE THERE ARE BOTH. */}
            {reading.quantity === shown && reading.stage && (
              <Text style={{ ...TYPE.body, color: palette.sub }}>{`· ${reading.stage}`}</Text>
            )}
          </View>
          <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
            {reading.percentile != null && theme ? (
              <View testID="reading-dot" style={{ width: 10, height: 10, borderRadius: round(10),
                                                  backgroundColor: rampAt(flowRampStops(theme), reading.percentile) }} />
            ) : null}
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink, flexShrink: 1 }}>
              {reading.quantity === shown || reading.percentile == null ? reading.standing : standingRankedIn(reading.standing, reading.quantity)}
            </Text>
          </View>
          {reading.forecast && <Text testID="reading-forecast" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{reading.forecast}</Text>}
        </View>
  ) : vm.quiet ? (
        // A QUIET DONOR AT THE HEAD OF THE CHART (review 2026-10-21): whose it is, that it is silent today and when it
        // last spoke — in the reading's place, under the switch. Never the no-gauge refusal: the headline answered.
        <View style={{ gap: SPACE.s1 }}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{vm.quiet.reads}</Text>
        </View>
  ) : station && !vm.settled ? (
        // (T584) the reading's place is kept while it loads, so the chart does not jump down when it arrives
        <Text testID="reading-pending" style={{ ...TYPE.bodyStrong, color: palette.faint }}>{W.readingWater}</Text>
  ) : null;

  /*
   * THE DETAIL ROWS where no gauge leads the chart: the refusal, and the single-station trace where the panel could
   * not answer. A reading — or a quiet donor's last report — sits in the chart block instead, under the switch that
   * picks its gauge and over the plot of the same gauge (owner 2026-10-21).
   */
  const refuse = vm.refuse;
  const detail = refuse || vm.trace ? (
    <>
      {refuse && <NoneLine palette={palette} size="body">{vm.noGauge}</NoneLine>}
      {vm.trace && (
        <GaugeTrace trace={vm.trace} palette={palette} at={tiles} theme={theme} from={from} />
      )}
    </>
  ) : undefined;

  const view = charts && chart.state === "ready" ? chart.value : null;
  /*
   * (T367, owner 2026-11-04: "all gauge info … should be built into graph look … right now it isnt very cohesive") THE
   * GAUGE CARD: one bordered card that reads top to bottom, the same for the lead gauge, a donor and a lake's own gauge —
   * its title the gauge (a tap opens the gauges behind the estimate, in place of the wrap of station chips), the
   * reading and its standing, the two toggles on one row, the chart with its legend (the models are legend entries,
   * in place of the model selector), and in its foot when the reading was taken.
   */
  const title = reading?.title ?? vm.quiet?.title ?? vm.gauges.find((g) => g.on)?.name ?? station ?? "";
  const foot = reading?.age ?? vm.quiet?.lastReport ?? null;
  const graph = station ? (
    <View testID="gauge-card" onLayout={measure}
          style={{ gap: SPACE.s3, padding: SPACE.s3, borderWidth: 1, borderColor: palette.line2,
                   borderRadius: palette.r.box, backgroundColor: palette.card }}>
      <GaugeTitle palette={palette} title={title} gauges={vm.gauges} groups={vm.gaugeGroups} share={gaugeShare}
                  onPick={(s) => vm.select(s as never)}
                  tools={<>
                    <ChartControls<Parameter> palette={palette} value={shown} testID="chart-quantity" coach="cond:quantity"
                                              label={W.controls.quantity} onPick={pick}
                                              options={options.map((p) => [p, W.quantity[p]] as const)} />
                    <ChartControls<Span> palette={palette} value={span} label={W.controls.span} testID="chart-span" coach="cond:span"
                                         onPick={setSpan}
                                         options={[["72h", W.spanShort["72h"], W.span["72h"]],
                                                   ["year", W.spanShort.year, W.span.year]] as const} />
                  </>} />
      {readingRows}
      {vm.leadNote && <Text testID="lead-note" style={{ ...TYPE.meta, color: palette.sub }}>{vm.leadNote}</Text>}
      {change && <ChangeBlock palette={palette} change={change} colour={tint} />}
      {chart.state === "loading" && charts && (
        <LoadingState palette={palette} size={72} label={W.loadingRecord} style={{ alignItems: "center", paddingVertical: SPACE.s4 }} />
      )}
      {/* THE LAST CHART STAYS UP, DIMMED, while a gauge not yet in hand loads — never a spinner in its place. */}
      {/* (T367) under Change the Change chart above is the one chart: the flow chart under it said the same twice */}
      {view && !change && charts && (
        <View ref={chartRing} collapsable={false} style={{ opacity: view.pending ? 0.45 : 1 }} aria-busy={view.pending}>
          <Hydrograph shape={view.shape} palette={palette} colour={tint}
                      unit={UNIT[shown]} view={view}
                      forecastLabel={run && forecast ? forecastChartLabel(run) : undefined}
                      models={runs.length > 0 ? runs.map((f) => {
                        // (T377) several at once: each entry says whether its trace is on the chart, in the dash it wears
                        const also = view.shape.alsoForecasts.findIndex((a) => a.key === f.model);
                        return { key: f.model, label: forecastModelOption(f), on: f.model === run?.model || also >= 0,
                                 dash: forecastDash(f.model === run?.model ? 0 : also >= 0 ? also + 1 : 0),
                                 onPress: () => setPick((p) => pickModel(p, f.model, run?.model ?? null, drawsForecast)) };
                      }) : undefined}
                      realtime={runs.length > 0 ? { on: !drawsForecast, onPress: () => setPick(realtimeOnly) } : undefined}
                      label={chartLabel(span)} />
        </View>
      )}
      {view?.pending && (
        <Text style={{ ...TYPE.meta, color: palette.faint }} accessibilityLiveRegion="polite">{W.loadingRecord}</Text>
      )}
      {view && !view.pending && view.draft && (
        <Text style={{ ...TYPE.meta, color: palette.faint }} accessibilityLiveRegion="polite">{W.envelopeLoading}</Text>
      )}
      {chart.state === "ready" && !view && charts && (
        <Text style={{ ...TYPE.body, color: palette.sub }}>{noChartSentence(span)}</Text>
      )}
      {/* (T300) ONLY ELF: the one model drawn is the low-water one because no other has a forecast here — said, not hidden */}
      {run && forecast && run.model === "ELF" && runs.length === 1 && (
        <Text testID="only-elf" style={{ ...TYPE.meta, color: palette.sub }}>{W.onlyModel(run.model)}</Text>
      )}
      {/* (T300) THE MODEL'S OWN INFORMATION, shut by default: when the run was made (local, labelled — core), and for ELF
          what it is. The Centre's disclaimer and the credits are under Sources and notes at the foot of the page. */}
      {run?.series && run.issuedAt && (
        <About palette={palette} label={W.aboutForecast}>
          <Text style={{ ...TYPE.meta, color: palette.sub }}>
            {forecastRunLine({ model: run.model, issuedAt: run.issuedAt, horizonDays: run.horizonDays })}
          </Text>
          {run.model === "ELF" && <Text style={{ ...TYPE.meta, color: palette.sub }}>{CHANGE_LOW_WATER}</Text>}
        </About>
      )}
      {/* THE CARD'S FOOT: when the reading was taken (and how fresh), then the offline note */}
      {(foot || view?.asOf) && (
        <View testID="gauge-card-foot" style={{ borderTopWidth: 1, borderTopColor: palette.line, paddingTop: SPACE.s2, gap: 2 }}>
          {foot && <Text style={{ ...TYPE.meta, color: palette.sub }}>{foot}</Text>}
          {view?.asOf && <Text testID="chart-kept-as-of" style={{ ...TYPE.meta, color: palette.sub }}>{view.asOf}</Text>}
        </View>
      )}
      {/* (T234) ALERT ME: a line on this gauge's quantity, told by the phone's background check (none on the web) */}
      {available.includes(shown) && (
        <AlertMe palette={palette} source={source}
                 target={{ station, stationName: title, quantity: shown, quantities: available,
                           current: currentFigure(reading, shown),
                           water: alertWater ?? null }} />
      )}
    </View>
  ) : null;

  /* (T300) THE FOLD'S FOOT, under "Sources and notes" at the bottom of the page (owner 2026-10-30, who rules that the
     Centre's disclaimer and the credits may be folded): the Centre's disclaimer verbatim while its forecast is drawn, the
     Province's attribution, ELF's low-water statement under Change, then the data credits. */
  const notes: React.ReactNode[] = [];
  if (disclaimer) notes.push(<Text key="disclaimer" testID="forecast-disclaimer" style={{ ...TYPE.meta, color: palette.faint }}>{disclaimer}</Text>);
  if (change?.attribution) notes.push(<Text key="attribution" style={{ ...TYPE.meta, color: palette.faint }}>{change.attribution}</Text>);
  if (horizon === "change") notes.push(<Text key="lowwater" testID="change-legend-low-water" style={{ ...TYPE.meta, color: palette.faint }}>{CHANGE_LEGEND.lowWater}</Text>);
  if (credits?.length) notes.push(<Credits key="credits" palette={palette} lines={credits} />);
  const small = notes.length ? <View style={{ gap: SPACE.s3 }}>{notes}</View> : undefined;

  /* ONE READING ORDER, decided in `DonorPanel`: header → headline → detail → chart →
     donors → the day strip (`footer`, T506) → the fold, last (Sources and notes: the credits, the disclaimer). */
  const body = (
    <>
      {regsDay !== undefined && (
        <View testID="conditions-day" style={{ gap: SPACE.s1 }}>
          <Text testID="conditions-day-label" style={{ ...TYPE.metaBold, color: palette.ink }}>{DAY_LABELS.conditions(horizon, bcNow())}</Text>
          {link && link.kind !== "today" && (
            <Text testID="conditions-day-link" style={{ ...TYPE.meta, color: palette.sub }}>{link.line}</Text>
          )}
        </View>
      )}
      <DonorPanel palette={palette} value={vm.panel} day={vm.day} at={tiles} theme={theme}
                  from={from} selected={station} onSelect={vm.select} horizon={horizon}
                  chain={vm.chain} standing={vm.route.standing}
                  onHorizon={onHorizon} quantity={parameter ?? "discharge"}
                  detail={detail} chart={graph}
                  foot={small} after={footer} holdKey={`${section ?? ""}:${String(horizon)}:${vm.day.toDateString()}`} sides={vm.sides} count={vm.count} waterLine={waterLine} place={waterBox} />
      {/* (T569) the way to report a problem from here, with the gauge charted, its quantity and span filled in */}
      {reportAbout && (
        <WaterReportLink where={waterReportWhere({ view: "conditions", ...reportAbout, day: dayLabel(vm.day), station,
                                                   quantity: CONDITIONS_WORDS.quantity[shown], span: CONDITIONS_WORDS.span[span] })}
                         feed={feed?.observations && station ? async () => feedStamp(await feed.observations!(station as StationId)) : undefined} palette={palette} />
      )}
    </>
  );

  // ONE MEASURE ON A DESKTOP is the water frame's (T44 `WaterFrame`, `WATER_FRAME.maxWidth`): the panel fills the
  // frame's column, which stops and centres, so a 2,560 px window is not one line of text across the whole screen —
  // and the Conditions view's left edge is the Regulations view's. The donor panel goes to two columns inside it
  // (`WIDE_PANEL`). The body is also a water's VIEW SWIPE when it has one (Regulations · Conditions · Map); the plot
  // and minimap inside are shielded so their own drags stay theirs.
  const pad = { paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s4 * 3, gap: SPACE.s4 };
  const scrolled = <CoachScrollContext.Provider value={coachScroll.scroller}>{body}</CoachScrollContext.Provider>;
  const follow = { ref: coachScroll.ref, scrollEventThrottle: 16,
                   onScroll: (e: { nativeEvent: { contentOffset: { y: number } } }) => coachScroll.track(e.nativeEvent.contentOffset.y) };
  const page = scroll
    ? bodySwipe
      ? <ScrollView {...follow} contentContainerStyle={{ flexGrow: 1 }}>
          <ViewSwipe swipe={bodySwipe} testId="view-swipe-conditions" style={pad}>{scrolled}</ViewSwipe>
        </ScrollView>
      : <ScrollView {...follow} contentContainerStyle={pad}>{scrolled}</ScrollView>
    : <View style={{ gap: SPACE.s4 }}>{body}</View>;
  // the wrapper is the same node whether the body is fresh or kept, so the scroll place survives the step
  if (vm.loading)
    return <View style={{ ...KEPT_STYLE, opacity: 0.45 }} pointerEvents="none" aria-busy testID="conditions-kept" accessibilityElementsHidden>{kept.current}</View>;
  kept.current = page;
  return <View style={KEPT_STYLE}>{page}</View>;
}

/** (T300) A shut-by-default line of detail: one toggle, its text when opened. */
function About({ palette, label, children }: { palette: Palette; label: string; children: React.ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <View style={{ gap: SPACE.s2 }}>
      <Pressable {...hitProps({ w: 200, h: 36 })} onPress={() => setOpen(!open)} accessibilityRole="button" aria-expanded={open}
                 style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, alignSelf: "flex-start" }}>
        <Text style={{ ...TYPE.metaBold, color: palette.sub }}>{label}</Text>
        <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={14} />
      </Pressable>
      {open && <View testID="about-forecast" style={{ gap: SPACE.s1 }}>{children}</View>}
    </View>
  );
}

/**
 * (T367) THE GAUGE CARD'S TITLE: the gauge's name as sourced, and — where there is more than one gauge behind the
 * estimate — a chevron: a tap opens them as a list (status dot, name, station, share), grouped as `gaugeChoiceGroups`
 * groups them (own first, the not-reporting last); a pick charts that gauge and closes the list. One gauge: a plain title.
 */
function GaugeTitle({ palette, title, gauges, groups, onPick, tools, share }: {
  palette: Palette; title: string; gauges: readonly GaugeChoiceVM[]; onPick: (station: string) => void;
  /** (T367) the chart's toggles, on the title's row (they wrap under it on a narrow screen) */
  tools?: ReactNode;
  /** (T530) the gauge's Share (`useGaugeShare`); null: the station has no public page */
  share?: ShareControl | null;
  /** (T168) the gauges grouped, own first, the not-reporting folded (`gaugeChoiceGroups`); null: one run */
  groups?: readonly GaugeGroupVM<GaugeChoiceVM>[] | null;
}) {
  const [open, setOpen] = useState(false);
  const head = <Text testID="gauge-title" style={{ ...TYPE.bodyStrong, color: palette.ink, flexShrink: 1 }}>{title}</Text>;
  const bar = (name: ReactNode) => (
    <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: SPACE.s2 }}>
      <View style={{ flexGrow: 1, flexBasis: 220, flexDirection: "row" }}>{name}</View>
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>{tools}{share && <GaugeShare share={share} palette={palette} />}</View>
    </View>
  );
  if (gauges.length < 2) return bar(head);
  const row = (g: GaugeChoiceVM, first: boolean) => (
    <Pressable {...hitProps({ w: 120, h: 40 })} key={g.station} onPress={() => { setOpen(false); onPick(g.station); }}
               accessibilityRole="radio" aria-checked={g.on} accessibilityLabel={g.label}
               style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, minHeight: 44, paddingHorizontal: SPACE.s2,
                        borderTopWidth: first ? 0 : 1, borderTopColor: palette.line,
                        backgroundColor: g.on ? palette.tint : palette.card }}>
      <View style={{ width: 10, height: 10, borderRadius: round(10), backgroundColor: donorTone(palette, g.row, g.quiet) }} />
      <View style={{ flex: 1 }}>
        <Text numberOfLines={1} style={{ ...TYPE.body, color: palette.ink, fontWeight: g.on ? "700" : "400" }}>{g.name ?? g.station}</Text>
        {g.name && <Text style={{ ...TYPE.meta, color: palette.faint }}>{g.station}</Text>}
      </View>
      <Text style={{ ...TYPE.metaBold, color: g.quiet ? palette.faint : palette.sub }}>{g.share}</Text>
    </Pressable>
  );
  return (
    <View style={{ gap: SPACE.s2 }}>
      {bar(
        <Pressable testID="gauge-pick" onPress={() => setOpen((o) => !o)} accessibilityRole="button" aria-expanded={open}
                   accessibilityLabel={W.card.pick(title)} {...hitProps({ w: 120, h: 34 })}
                   style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, flexShrink: 1 }}>
          {head}
          <Icon name={open ? "collapse" : "expand"} colour={palette.accent} size={16} />
        </Pressable>
      )}
      {open && (
        <View testID="gauge-list" accessibilityRole="radiogroup" accessibilityLabel={W.card.list}
              style={{ borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.box }}>
          {groups ? <GaugeGroups palette={palette} groups={groups} keyOf={(g) => g.station} render={row}
                                 headings={false} testID="switch-groups" />
            : gauges.map((g, i) => row(g, i === 0))}
        </View>
      )}
    </View>
  );
}

/**
 * THE CHANGE BLOCK (T52): what each forecast model expects at the charted gauge over the next ten days — its line in
 * core's words ("CLEVER: rises to about 25× today's flow on Mon …"), its days as a small line with the peak marked, and
 * the Province's attribution. Draws the VM and decides nothing (AGENTS 25).
 */
function ChangeBlock({ palette, change, colour }: { palette: Palette; change: ChangeVM; colour: string }) {
  const c = change.chart;
  const H = 120;
  return (
    <View testID="change-block" style={{ gap: SPACE.s2 }}>
      {change.runs.map((r) => (
        <Text key={r.model} style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.line}</Text>
      ))}
      {/* (T367) the models behind it, one quiet line */}
      {change.models && <Text testID="change-models" style={{ ...TYPE.meta, color: palette.sub }}>{change.models}</Text>}
      {c && (
        <View testID="change-chart" aria-label={c.label} role="img">
          <View style={{ flexDirection: "row" }}>
            {/* the ×today axis: each gridline's ratio at its height */}
            <View style={{ width: CHANGE_AXIS, height: H }}>
              {c.grid.map((g) => (
                <Text key={g.label} style={{ ...TYPE.micro, fontSize: 11, color: palette.sub, position: "absolute",
                                             right: 6, top: g.y - 7 }}>{g.label}</Text>
              ))}
            </View>
            <View style={{ flex: 1 }}>
              <Svg width="100%" height={H} viewBox={`0 0 ${c.days[c.days.length - 1]!.x} ${H}`} preserveAspectRatio="none">
                {c.grid.map((g) => (
                  <Path key={g.label} d={`M0,${g.y} L${c.days[c.days.length - 1]!.x},${g.y}`} stroke={palette.line}
                        strokeWidth={g.label === "1.0×" ? 1.5 : 1} strokeDasharray={g.label === "1.0×" ? undefined : "3 3"} />
                ))}
                {/* (T157) where a joined model stops: a dashed tick in the gap the bold line leaves, never a fall */}
                {c.handovers.map((hv) => (
                  <Path key={hv.model} testID="change-handover" d={`M${hv.x},0 L${hv.x},${H}`} stroke={palette.faint}
                        strokeWidth={1} strokeDasharray="2 3" />
                ))}
                {c.series.filter((s) => !s.bold).map((s) => (
                  <Path key={s.model} d={s.path} fill="none" stroke={palette.sub} strokeWidth={1.2} strokeLinejoin="round" />
                ))}
                {c.series.filter((s) => s.bold).map((s) => (
                  <Path key={s.model} d={s.path} fill="none" stroke={colour} strokeWidth={2.4} strokeLinejoin="round" />
                ))}
                {c.mark && <Circle cx={c.mark.x} cy={c.mark.y} r={4} fill={colour} />}
              </Svg>
            </View>
          </View>
          {/* the days along the foot, and the marked day's words */}
          <View style={{ flexDirection: "row", justifyContent: "space-between", marginLeft: CHANGE_AXIS }}>
            {c.days.map((d) => <Text key={d.x} style={{ ...TYPE.meta, color: palette.faint }}>{d.label}</Text>)}
          </View>
          {(c.mark || c.series.length > 1) && (
            <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 14, marginLeft: CHANGE_AXIS, marginTop: 4 }}>
              {c.mark && <Text style={{ ...TYPE.meta, color: palette.ink }}>{`● ${c.mark.label}`}</Text>}
              {c.series.map((s) => (
                <View key={s.model} style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
                  <View style={{ width: 16, height: 0, borderTopWidth: s.bold ? 2.4 : 1.2, borderTopColor: s.bold ? colour : palette.sub }} />
                  <Text style={{ ...TYPE.meta, color: palette.sub }}>{s.model}</Text>
                </View>
              ))}
            </View>
          )}
        </View>
      )}
      {change.note && <Text style={{ ...TYPE.meta, color: palette.sub }}>{change.note}</Text>}
      {change.caveat && (
        <Text testID="change-caveat" style={{ ...TYPE.body, color: palette.ink }}>{change.caveat}</Text>
      )}
      {change.none && <NoneLine palette={palette} size="body">{change.none}</NoneLine>}
    </View>
  );
}

/** (T367) the Change chart's ×today axis, left of the plot */
const CHANGE_AXIS = 40;

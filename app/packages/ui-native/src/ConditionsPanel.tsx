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
import { useState } from "react";
import { Pressable, ScrollView, Text, View, type LayoutChangeEvent } from "react-native";
import { chartCaption, chartLabel, CONDITIONS_WORDS, forecastChartLabel, forecastModelOption,
         forecastRunLine, noChartSentence, type ChartSpan } from "@app/core";
import type { Parameter, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useChartView, useConditionsView, useGaugeParameters, useSeries,
         type GaugeChoiceVM, type Horizon, type PanelFeed } from "@app/ui";
import { ChartControls } from "./ChartControls";
import { Credits } from "./Credits";
import { FishSpinner } from "./FishSpinner";
import { DonorPanel, donorTone, Kicker } from "./DonorPanel";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";

/** The units, from core's one table. */
export const UNIT: Readonly<Record<Parameter, string>> = CONDITIONS_WORDS.unit;

export type Span = ChartSpan;

/** The Conditions view's widest measure (desktop), content and header alike. */
export const CONDITIONS_MAX = 1280;

const W = CONDITIONS_WORDS;

export function ConditionsPanel({ source, section, palette, tiles, theme, colour,
                                  parameter, onParameter, from, feed, scroll = true,
                                  footer, credits, horizon = 0, onHorizon, day = null }: {
  source: RegsSource; section: SectionId | null; palette: Palette;
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
}) {
  /*
   * THE SAME QUANTITY THE MAP IS PAINTING. `parameter` is undefined only under
   * Temperature, which is not a hydrograph, so on Flow or Level this is exactly what the
   * map was asked for.
   */
  const vm = useConditionsView(source, section, day, horizon,
                               { feed, quantity: parameter ?? "discharge" });
  const station = vm.lead;

  // WHICH QUANTITIES THIS STATION CAN ANSWER IN, asked of the bundle rather than assumed:
  // 237 BC stations measure stage and never discharge.
  const params = useGaugeParameters(source, station);
  const available = params.state === "ready" ? params.value : [];
  const [own, setOwn] = useState<Parameter | null>(null);
  const [span, setSpan] = useState<Span>("72h");
  const picked = parameter ?? own;
  const pick = onParameter ?? setOwn;
  const param: Parameter | undefined =
    picked && available.includes(picked) ? picked : undefined;
  const shown: Parameter = param ?? vm.reading?.quantity ?? "level";
  // WHICH MODEL. Left alone the freshest run is chosen — a UI default, not a judgement.
  const [model, setModel] = useState<string | null>(null);
  // THE CHART IS BUILT FOR THE WIDTH IT IS DRAWN AT (owner 2026-10-21: "the plot doesnt scale to fill page"):
  // measured here, the paths are made for exactly that box and the labels stay label-sized.
  const [chartW, setChartW] = useState(0);
  const measure = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== chartW) setChartW(w);
  };
  const chart = useChartView(source, station, span, param, model ?? undefined, chartW);
  const series = useSeries(source, station, span, param, model ?? undefined);
  const tint = colour ?? palette.live;
  const forecast = chart.state === "ready" ? chart.value?.shape.forecast : null;
  const runs = series.state === "ready" ? series.value?.forecasts ?? [] : [];
  const run = (series.state === "ready" ? series.value?.forecast : null) ?? null;
  // The Centre's disclaimer, verbatim, while its forecast is drawn (the same condition the chart draws it under).
  const disclaimer = forecast && run?.disclaimer ? run.disclaimer : null;

  if (vm.loading)
    return (
      <View style={{ paddingVertical: 48, alignItems: "center" }}>
        <FishSpinner palette={palette} size={110} label={W.readingWater} />
      </View>
    );

  const reading = vm.reading;

  /*
   * THE HEADLINE'S DETAIL ROWS (P16): the lead gauge's own reading — or, where no gauge
   * may speak, the sentence that says so — and the single-station trace where the panel
   * could not answer (`vm.trace` is null otherwise). They used to stack under the donor
   * list as two more answer-shaped blocks; here they are the working under the answer.
   */
  const readingRows = reading ? (
        <View style={{ gap: SPACE.s1 }}>
          {/* WHOSE READING THIS IS, before the number rather than under it. */}
          <Kicker palette={palette}>{reading.heading}</Kicker>
          {/* ONE NUMBER STYLE: the figure and its unit in the figure face, side by side. */}
          <View style={{ flexDirection: "row", alignItems: "baseline", columnGap: SPACE.s2,
                         flexWrap: "wrap" }}>
            <Text style={{ ...TYPE.figureBig, color: tint }}>{reading.figure}</Text>
            <Text style={{ ...TYPE.figure, color: palette.sub }}>{reading.unit}</Text>
            {/* BOTH NUMBERS WHERE THERE ARE BOTH. */}
            {reading.stage && (
              <Text style={{ ...TYPE.figure, color: palette.sub }}>{reading.stage}</Text>
            )}
          </View>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{reading.standing}</Text>
          <Text style={{ ...TYPE.meta, color: palette.faint }}>{reading.age}</Text>
        </View>
  ) : vm.quiet ? (
        // A QUIET DONOR AT THE HEAD OF THE CHART (review 2026-10-21): whose it is, that it is silent today and when it
        // last spoke — in the reading's place, under the switch. Never the no-gauge refusal: the headline answered.
        <View style={{ gap: SPACE.s1 }}>
          <Kicker palette={palette}>{vm.quiet.heading}</Kicker>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{vm.quiet.reads}</Text>
          <Text style={{ ...TYPE.meta, color: palette.faint }}>{vm.quiet.lastReport}</Text>
        </View>
  ) : null;

  /*
   * THE DETAIL ROWS where no gauge leads the chart: the refusal, and the single-station trace where the panel could
   * not answer. A reading — or a quiet donor's last report — sits in the chart block instead, under the switch that
   * picks its gauge and over the plot of the same gauge (owner 2026-10-21).
   */
  const refuse = vm.refuse;
  const detail = refuse || vm.trace ? (
    <>
      {refuse && <Text style={{ ...TYPE.body, color: palette.sub }}>{vm.noGauge}</Text>}
      {vm.trace && (
        <GaugeTrace trace={vm.trace} palette={palette} at={tiles} theme={theme} from={from} />
      )}
    </>
  ) : undefined;

  const view = chart.state === "ready" ? chart.value : null;
  const graph = station ? (
    <View style={{ gap: SPACE.s3 }} onLayout={measure}>
      {/* WHICH GAUGE, RIGHT BY THE PLOT IT CONTROLS (owner 2026-10-21): it was the donor list, a screen below. */}
      {vm.gauges.length > 0 && (
        <GaugeSwitch palette={palette} gauges={vm.gauges} onPick={(s) => vm.select(s as never)} />
      )}
      {readingRows}
      <View style={{ flexDirection: "row", flexWrap: "wrap", gap: SPACE.s2 }}>
        <ChartControls<Parameter> palette={palette} value={shown}
                                  label={W.controls.quantity} onPick={pick}
                                  options={available.map((p) =>
                                    [p, W.quantity[p]] as const)} />
        <ChartControls<Span> palette={palette} value={span} label={W.controls.span}
                             onPick={setSpan}
                             options={[["72h", W.span["72h"]],
                                       ["year", W.span.year]] as const} />
        {/* THE MODEL, when more than one is running: they answer different questions,
            and in the shoulder months both are in season and disagree. */}
        <ChartControls<string> palette={palette} label={W.controls.model}
                               value={run?.model ?? ""} onPick={setModel}
                               options={runs.map((f) =>
                                 [f.model, forecastModelOption(f)] as const)} />
      </View>
      {chart.state === "loading" && (
        <View style={{ alignItems: "center", paddingVertical: SPACE.s4 }}>
          <FishSpinner palette={palette} size={72} label={W.loadingRecord} />
        </View>
      )}
      {view && (
        <Hydrograph shape={view.shape} palette={palette} colour={tint}
                    unit={UNIT[shown]} view={view}
                    forecastLabel={run && forecast ? forecastChartLabel(run) : undefined}
                    label={chartLabel(span)}
                    caption={chartCaption(span)} />
      )}
      {chart.state === "ready" && !view && (
        <Text style={{ ...TYPE.body, color: palette.sub }}>{noChartSentence(span)}</Text>
      )}
      {/* WHEN the run was made, beside what it says — local and labelled (core). */}
      {run?.series && run.issuedAt && (
        <Text style={{ ...TYPE.meta, color: palette.faint }}>
          {forecastRunLine({ model: run.model, issuedAt: run.issuedAt,
                             horizonDays: run.horizonDays })}
        </Text>
      )}
      {/* THE CENTRE'S DISCLAIMER STAYS IN VIEW, under the forecast it is about: the Province requires it verbatim
          "wherever a forecast appears" (App.tsx attribution, pipeline/hydro/forecast.py). A fold would hide a required
          notice, so it is not folded unless the owner rules otherwise (p16/README). */}
      {disclaimer && <Text style={{ ...TYPE.meta, color: palette.faint }}>{disclaimer}</Text>}
    </View>
  ) : null;

  /* THE FOLD'S FOOT: the credits. (The Centre's disclaimer is under the chart, never folded — see above.) */
  const small = credits?.length ? <Credits palette={palette} lines={credits} /> : undefined;

  /* ONE READING ORDER, decided in `DonorPanel`: header → headline → detail → chart →
     donors → the fold (which holds the credits; the disclaimer stays under the chart). */
  const body = (
    <>
      <DonorPanel palette={palette} value={vm.panel} day={vm.day} at={tiles} theme={theme}
                  from={from} selected={station} onSelect={vm.select} horizon={horizon}
                  chain={vm.chain} data={vm.routeData} onHorizon={onHorizon}
                  detail={detail} chart={graph}
                  foot={small} />
      {footer}
    </>
  );

  // ONE MEASURE ON A DESKTOP: the content stops at `CONDITIONS_MAX` and centres, so a 2,560 px window is not one
  // line of text across the whole screen. The donor panel goes to two columns inside it (`WIDE_PANEL`).
  return scroll
    ? <ScrollView contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3,
                                           paddingBottom: SPACE.s4 * 3, gap: SPACE.s4,
                                           width: "100%", maxWidth: CONDITIONS_MAX, alignSelf: "center" }}>
        {body}
      </ScrollView>
    : <View style={{ gap: SPACE.s4 }}>{body}</View>;
}

/**
 * THE GAUGE SWITCH: one chip per donor in the panel's weight order — its pin's colour, its station id and its share of
 * the estimate — directly above the reading and the plot it changes. Chosen from `vm.gauges` (@app/ui `gaugeChoices`);
 * a quiet gauge is still offered and says it counts for nothing today; picked, it says when it last reported, and the
 * chart says so if it has no recent record to draw.
 */
function GaugeSwitch({ palette, gauges, onPick }: {
  palette: Palette; gauges: readonly GaugeChoiceVM[]; onPick: (station: string) => void;
}) {
  return (
    <View style={{ gap: SPACE.s2 }}>
      <Kicker palette={palette}>{W.chartGauge}</Kicker>
      <View style={{ flexDirection: "row", flexWrap: "wrap", gap: SPACE.s2 }}
            accessibilityRole="radiogroup" accessibilityLabel={W.controls.gauge}>
        {gauges.map((g) => (
          <Pressable key={g.station} onPress={() => onPick(g.station)} accessibilityRole="radio"
                     aria-checked={g.on} accessibilityLabel={g.label}
                     style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2,
                              paddingVertical: SPACE.s2, paddingHorizontal: SPACE.s3,
                              borderRadius: palette.r.pill, borderWidth: g.on ? 2 : 1,
                              borderColor: g.on ? donorTone(palette, g.row, g.quiet) : palette.line2,
                              backgroundColor: g.on ? palette.tint : palette.card }}>
            <View style={{ width: 9, height: 9, borderRadius: 5,
                           backgroundColor: donorTone(palette, g.row, g.quiet) }} />
            <Text style={{ ...TYPE.figure, color: palette.ink }}>{g.station}</Text>
            <Text style={{ ...TYPE.metaBold, color: g.quiet ? palette.faint : palette.sub }}>{g.share}</Text>
          </Pressable>
        ))}
      </View>
    </View>
  );
}

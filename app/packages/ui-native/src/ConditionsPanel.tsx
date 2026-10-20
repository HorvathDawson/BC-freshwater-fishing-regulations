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
import { ScrollView, Text, View } from "react-native";
import { chartCaption, chartLabel, CONDITIONS_WORDS, forecastChartLabel, forecastModelOption,
         forecastRunLine, noChartSentence, type ChartSpan } from "@app/core";
import type { Parameter, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useConditionsView, useGaugeParameters, useHydrograph, useSeries,
         type Horizon, type PanelFeed } from "@app/ui";
import { ChartControls } from "./ChartControls";
import { Credits } from "./Credits";
import { FishSpinner } from "./FishSpinner";
import { DonorPanel } from "./DonorPanel";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** The units, from core's one table. */
export const UNIT: Readonly<Record<Parameter, string>> = CONDITIONS_WORDS.unit;

export type Span = ChartSpan;

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
  /** How far ahead the estimate is for — 0 is now. Kept in step with the map's control. */
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
  const chart = useHydrograph(source, station, span, param, model ?? undefined);
  const series = useSeries(source, station, span, param, model ?? undefined);
  const tint = colour ?? palette.live;
  const forecast = chart.state === "ready" ? chart.value?.forecast : null;
  const runs = series.state === "ready" ? series.value?.forecasts ?? [] : [];
  const run = (series.state === "ready" ? series.value?.forecast : null) ?? null;

  if (vm.loading)
    return (
      <View style={{ paddingVertical: 48, alignItems: "center" }}>
        <FishSpinner palette={palette} size={110} label={W.readingWater} />
      </View>
    );

  const reading = vm.reading;
  const body = (
    <>
      {/* THE ANSWER FIRST, THE EVIDENCE UNDER IT: the estimate for the spot is the answer
          to the question asked; one gauge's reading below it is the working. */}
      <DonorPanel palette={palette} value={vm.panel} day={vm.day} at={tiles} theme={theme}
                  from={from} selected={station} onSelect={vm.select} horizon={horizon}
                  chain={vm.chain} data={vm.routeData} onHorizon={onHorizon} />

      {reading ? (
        <View style={{ gap: 6 }}>
          {/* WHOSE READING THIS IS, before the number rather than under it. */}
          <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                         color: palette.faint }}>
            {reading.heading}
          </Text>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                         flexWrap: "wrap" }}>
            <Text style={{ ...TYPE.figureBig, fontSize: 34, color: tint }}>
              {reading.figure}
            </Text>
            <Text style={{ ...TYPE.figure, color: palette.sub }}>{reading.unit}</Text>
            {/* BOTH NUMBERS WHERE THERE ARE BOTH. */}
            {reading.stage && (
              <Text style={{ ...TYPE.figure, fontSize: 13, color: palette.faint }}>
                {reading.stage}
              </Text>
            )}
          </View>
          <Text style={{ ...TYPE.body, color: palette.ink }}>{reading.standing}</Text>
          <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
            {reading.age}
          </Text>
        </View>
      ) : (
        <Text style={{ ...TYPE.body, color: palette.sub }}>{vm.noGauge}</Text>
      )}

      {station && (
        <View style={{ gap: 10 }}>
          <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 10 }}>
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
            <View style={{ alignItems: "center", paddingVertical: 22 }}>
              <FishSpinner palette={palette} size={72} label={W.loadingRecord} />
            </View>
          )}
          {chart.state === "ready" && chart.value && (
            <Hydrograph shape={chart.value} palette={palette} colour={tint}
                        unit={UNIT[shown]}
                        disclaimer={run?.disclaimer ?? null}
                        forecastLabel={run && forecast ? forecastChartLabel(run) : undefined}
                        label={chartLabel(span)}
                        caption={chartCaption(span)} />
          )}
          {chart.state === "ready" && !chart.value && (
            <Text style={{ ...TYPE.small, color: palette.sub }}>{noChartSentence(span)}</Text>
          )}
          {/* WHEN the run was made, beside what it says — in labelled UTC. */}
          {run?.series && run.issuedAt && (
            <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint }}>
              {forecastRunLine({ model: run.model, issuedAt: run.issuedAt,
                                 horizonDays: run.horizonDays })}
            </Text>
          )}
        </View>
      )}

      {/* THE SINGLE-STATION TRACE IS THE FALLBACK, shown only where the panel could not
          answer (`vm.trace` is null otherwise). */}
      {vm.trace && (
        <GaugeTrace trace={vm.trace} palette={palette} at={tiles} theme={theme} from={from} />
      )}
      {footer}
      <Credits palette={palette} lines={credits} />
    </>
  );

  return scroll
    ? <ScrollView contentContainerStyle={{ padding: 18, gap: 22, paddingBottom: 40 }}>
        {body}
      </ScrollView>
    : <View style={{ gap: 22 }}>{body}</View>;
}

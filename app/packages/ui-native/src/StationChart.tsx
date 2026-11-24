/**
 * (T1039) ONE GAUGE'S CHART: the chart's two toggles (Flow | Level, 72 h | Year), the hydrograph with its legend (each
 * forecast model a switch, and "Real-time only"), and what the run says about itself. Drawn from `@app/ui`'s
 * `useStationChart`; computes nothing.
 *
 * Two callers, one chart (owner 2026-11-23: "the gauge website … should also have the same chart as the website"): the gauge
 * card of a water's Conditions view (`ConditionsPanel`) and the public gauge page (tools/seo `gaugeChart.tsx`).
 */
import { useState } from "react";
import { View } from "react-native";
import { CHANGE_LOW_WATER, chartLabel, CONDITIONS_WORDS, forecastChartLabel, forecastModelOption, forecastRunLine, noChartSentence,
         type ChartSpan } from "@app/core";
import type { Parameter } from "@app/data";
import { pickModel, realtimeOnly, type StationChartVM } from "@app/ui";
import { ChartControls } from "./ChartControls";
import { useCoachTarget } from "./CoachMarks";
import { Hydrograph } from "./Hydrograph";
import { Icon } from "./icons";
import { LoadingState } from "./LoadingState";
import { Pressable } from "./Pressable";
import { hitProps } from "./regs/hit";
import { Text } from "./Text";
import { SPACE, type Palette } from "./theme";
import { TYPE } from "./type";

const W = CONDITIONS_WORDS;

export function StationChart({ vm, palette, colour, hideChart = false }: {
  vm: StationChartVM; palette: Palette;
  /** the trace colour, so a sheet can tint the chart to its own outcome */
  colour: string;
  /** (T367) under Change the Change chart above is the one chart: the toggles stay, the flow chart is not drawn */
  hideChart?: boolean;
}) {
  const chartRing = useCoachTarget("cond:chart");
  const { chart, charts, shown, options, span, runs, run, drawsForecast } = vm;
  const view = charts && chart.state === "ready" ? chart.value : null;
  const forecast = chart.state === "ready" ? chart.value?.shape.forecast : null;
  return (
    <>
      {/* (T920, owner 2026-11-19: "level/flow selector is messy") THE CHART'S TWO TOGGLES, the old segmented look, together
          on ONE row directly over the chart they change (Flow | Level only where both are offered). Drawn whether or not a
          chart is: a gauge silent in the quantity asked keeps the other one tap away (T817) */}
      <View testID="chart-toggles" style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: SPACE.s2 }}>
        <ChartControls<Parameter> palette={palette} value={shown} testID="chart-quantity" coach="cond:quantity"
                                  label={W.controls.quantity} onPick={vm.pick}
                                  options={options.map((p) => [p, W.quantity[p]] as const)} />
        <ChartControls<ChartSpan> palette={palette} value={span} label={W.controls.span} testID="chart-span" coach="cond:span"
                                  onPick={vm.setSpan}
                                  options={[["72h", W.spanShort["72h"], W.span["72h"]],
                                            ["year", W.spanShort.year, W.span.year]] as const} />
      </View>
      {chart.state === "loading" && charts && (
        <LoadingState palette={palette} size={72} label={W.loadingRecord} style={{ alignItems: "center", paddingVertical: SPACE.s4 }} />
      )}
      {/* THE LAST CHART STAYS UP, DIMMED, while a gauge not yet in hand loads — never a spinner in its place. */}
      {view && !hideChart && (
        <View ref={chartRing} collapsable={false} style={{ opacity: view.pending ? 0.45 : 1 }} aria-busy={view.pending}>
          <Hydrograph shape={view.shape} palette={palette} colour={colour}
                      unit={W.unit[shown]} view={view}
                      forecastLabel={run && forecast ? forecastChartLabel(run) : undefined}
                      models={runs.length > 0 ? runs.map((f) => {
                        // (T377) several at once: each entry says whether its trace is on the chart; (T895) its colour and
                        // dash are the model's own (`modelLook`, in the Hydrograph), whichever others are on
                        const also = view.shape.alsoForecasts.some((a) => a.key === f.model);
                        return { key: f.model, label: forecastModelOption(f), on: f.model === run?.model || also,
                                 onPress: () => vm.setPick((p) => pickModel(p, f.model, run?.model ?? null, drawsForecast)) };
                      }) : undefined}
                      realtime={runs.length > 0 ? { on: !drawsForecast, onPress: () => vm.setPick(realtimeOnly) } : undefined}
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
    </>
  );
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

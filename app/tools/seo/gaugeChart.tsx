/**
 * (T1039, owner 2026-11-23: "the gauge website … should also have the same chart as the website"; Q183: "App's chart, ~260 KB")
 * THE GAUGE PAGE'S CHART IS THE APP'S: the very `StationChart` a water's Conditions card draws (@app/ui-native), over the very
 * hook (@app/ui `useStationChart`), asking the very series code (@app/data `climSeries`) of the same live feed file
 * (`<data>/feeds/live/<station>.json`, read by the app's `httpFeed`) over the same release's climatology (written into the
 * page by the site build, gauges.ts `gaugeConfig`). Every window (72 h, the year), quantity, zoom, read-out and forecast model
 * (each a switch, "Real-time only") is the app's.
 *
 * Bundled by the site build into one self-hosted, deferred script (gauges.ts `chartScript`): the page's words, reading and
 * weather are drawn first by its small script (gaugeClient.ts); this one draws into #gauge-chart after. A failure is shown
 * where the chart goes AND reported through the page's reporter (W46; core `setFailureSink`, as the app's crash reporter is
 * handed in): a feed file the app's `httpFeed` could not read is said by the app's chart ("no record") and reported by the
 * feed itself, as in the app.
 */
import "./globals";
import { createElement as h, useState } from "react";
import { createRoot } from "react-dom/client";
import { View, useColorScheme, type LayoutChangeEvent } from "react-native";
import { reportFailure, setFailureSink } from "@app/core";
import { httpFeed, stationChartSource, type StationClim, type StationId } from "@app/data";
import { useStationChart, type ChartSource } from "@app/ui";
import { GetAppBlock } from "../../packages/ui-native/src/GetAppBlock";
import { StationChart } from "../../packages/ui-native/src/StationChart";
import { Text } from "../../packages/ui-native/src/Text";
import { THEMES } from "../../packages/ui-native/src/theme";
import { TYPE } from "../../packages/ui-native/src/type";
import { pageReporter, type GaugeConfig } from "./gaugeClient";

/** what the chart could not do, in the page's words (the reading and the weather say theirs the same way) */
export const CHART_FAILED = "The chart could not be drawn. Try again later.";

/** the config's climatology (base64 bands) as the rows the source reads */
export function climOf(cfg: Pick<GaugeConfig, "clim">): StationClim[] {
  return cfg.clim.map((c) => ({ parameter: c.parameter, bands: Uint8Array.from(atob(c.bands), (ch) => ch.charCodeAt(0)) }));
}

/** the page's chart: one station, the app's chart at the width of the page's column */
export function GaugeChart({ source, station }: { source: ChartSource; station: StationId }) {
  const palette = THEMES[useColorScheme() === "dark" ? "dark" : "light"];
  const [width, setWidth] = useState(0);
  const measure = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== width) setWidth(w);
  };
  const vm = useStationChart(source, station, { width });
  const failed = vm.chart.state === "failed" || vm.series.state === "failed";
  return h(View, { onLayout: measure, testID: "gauge-page-chart", style: { gap: 12 } },
    h(StationChart, { vm, palette, colour: palette.live }),
    failed ? h(Text, { testID: "gauge-chart-failed", style: { ...TYPE.body, color: palette.sub } }, CHART_FAILED) : null);
}

/** mounts the chart in #gauge-chart from the page's #gauge-config */
export function mountGaugeChart(doc: Document, fetchImpl: typeof fetch, now: () => number): void {
  const cfgEl = doc.getElementById("gauge-config"), box = doc.getElementById("gauge-chart"), getApp = doc.getElementById("gauge-get-app");
  if (!cfgEl?.textContent || !box || !getApp) throw new Error("gauge chart: no #gauge-config, #gauge-chart or #gauge-get-app on this page");
  const cfg = JSON.parse(cfgEl.textContent) as GaugeConfig;
  const report = pageReporter(cfg, fetchImpl, now);
  setFailureSink(report);
  try {
    if (cfg.fonts) { const s = doc.createElement("style"); s.textContent = cfg.fonts; doc.head.appendChild(s); }
    const source = stationChartSource(cfg.station as StationId, httpFeed(cfg.live, fetchImpl), climOf(cfg));
    createRoot(box).render(h(GaugeChart, { source, station: cfg.station as StationId }));
  } catch (e) {
    box.innerHTML = `<p class="warn">${CHART_FAILED}</p>`;
    reportFailure(e);
  }
  // (T1039, owner 2026-11-23: "chart should use same buttons as spots too for now") the web app's own "Get the app" block
  createRoot(getApp).render(h(GaugePageGetApp, { contact: cfg.contact }));
}

/** the Spots tab's "Get the app" block, in the page's theme */
export function GaugePageGetApp({ contact }: { contact: string | null }) {
  const palette = THEMES[useColorScheme() === "dark" ? "dark" : "light"];
  return h(GetAppBlock, { palette, contact });
}


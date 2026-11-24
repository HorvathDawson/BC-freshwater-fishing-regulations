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
import { createElement as h, useState, type ReactElement } from "react";
import { createRoot } from "react-dom/client";
import { View, useColorScheme, type LayoutChangeEvent } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { reportFailure, setFailureSink } from "@app/core";
import { httpFeed, stationChartSource, type GaugeFeed, type StationClim, type StationId } from "@app/data";
import { noRecordLine, readingQuantityOf, useStationChart, useStationReading, type ChartSource } from "@app/ui";
import type { WaterSource } from "@app/data";

/** what the page asks of its source: the chart's two questions and the station's reading */
export type PageSource = ChartSource & Pick<WaterSource, "gaugeNow">;
/** the page's live feed as the chart reads it (its observations: the year scale, the early draft) */
export type PageFeed = { observations?: GaugeFeed["observations"] };
import { GetAppBlock } from "../../packages/ui-native/src/GetAppBlock";
import { StationChart } from "../../packages/ui-native/src/StationChart";
import { Text } from "../../packages/ui-native/src/Text";
import { THEMES } from "../../packages/ui-native/src/theme";
import { TYPE } from "../../packages/ui-native/src/type";
import { CHART_FAILED, pageReporter, type GaugeConfig } from "./gaugeClient";

export { CHART_FAILED };


/** the config's climatology (base64 bands) as the rows the source reads */
export function climOf(cfg: Pick<GaugeConfig, "clim">): StationClim[] {
  return cfg.clim.map((c) => ({ parameter: c.parameter, bands: Uint8Array.from(atob(c.bands), (ch) => ch.charCodeAt(0)) }));
}

/** the page's chart: one station, the app's chart at the width of the page's column */
/**
 * (T1043, T1047) `feed`: the page's live feed file (the one its source reads), so the chart reads the year the app's does for a
 * station with no normal range (its year scale) and draws its observations before the envelope (T256). Absent: neither.
 */
export function GaugeChart({ source, station, feed }: { source: PageSource; station: StationId; feed?: PageFeed | null }) {
  const palette = THEMES[useColorScheme() === "dark" ? "dark" : "light"];
  const [width, setWidth] = useState(0);
  const measure = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== width) setWidth(w);
  };
  // (T1039 review) the station's reading tells the chart which quantities it reports live and which it ranked in, as the
  // Conditions card's does (`readingQuantityOf`): a level-only gauge with no climatology (T830) charts level, unranked
  const got = useStationReading(source, station);
  const c = got.state === "ready" ? got.value : null, rq = c ? readingQuantityOf(c) : null;
  const vm = useStationChart(source, station, { width, feed: feed ?? null, offRecord: c?.offRecord ?? null,
                                               ...(rq ? { reports: c!.reports ?? null, readingQuantity: rq.quantity } : {}) });
  const noRecord = noRecordLine(vm.shown, vm.banded, vm.known, rq ? c!.reports : null);
  const failed = vm.chart.state === "failed" || vm.series.state === "failed";
  return h(View, { onLayout: measure, testID: "gauge-page-chart", style: { gap: 12 } },
    noRecord ? h(Text, { testID: "reading-no-record", style: { ...TYPE.bodyStrong, color: palette.ink } }, noRecord) : null,
    h(StationChart, { vm, palette, colour: palette.live }),
    failed ? h(Text, { testID: "gauge-chart-failed", style: { ...TYPE.body, color: palette.sub } }, CHART_FAILED) : null);
}

/**
 * (T1039 review) THE PAGE'S ROOT: the app's chart under react-native-gesture-handler's root, as the app runs under one
 * (apps/mobile gestureRoot.tsx): without it the chart's pinch, drag and double-tap recognisers do not run.
 */
export function GaugeChartRoot(props: { source: PageSource; station: StationId; feed?: PageFeed | null }) {
  return h(GestureHandlerRootView, { style: { flex: 1 } }, h(GaugeChart, props));
}

/** mounts the chart in #gauge-chart from the page's #gauge-config */
export function mountGaugeChart(doc: Document, fetchImpl: typeof fetch, now: () => number): void {
  const cfgEl = doc.getElementById("gauge-config"), box = doc.getElementById("gauge-chart"), getApp = doc.getElementById("gauge-get-app");
  // (T1039 review) ran: the page's own script (gaugeClient.ts) says the chart failed when this mark is missing at load; from
  // here on a failure is this script's to show AND report
  box?.setAttribute("data-mounted", "");
  const shown = (where: Element | null) => { if (where) where.innerHTML = `<p class="warn">${CHART_FAILED}</p>`; };
  let cfg: GaugeConfig;
  try {
    if (!cfgEl?.textContent) throw new Error("gauge chart: no #gauge-config on this page");
    cfg = JSON.parse(cfgEl.textContent) as GaugeConfig;
  } catch (e) {
    // no config (or one that does not parse): said where the chart goes; there is no endpoint to report it to
    shown(box);
    throw e;
  }
  setFailureSink(pageReporter(cfg, fetchImpl, now));
  if (!box || !getApp) {
    // (T1039 re-review) the config parsed: a page built without its chart's or Get the app's place is shown AND reported
    shown(box);
    const e = new Error(`gauge chart: no ${box ? "#gauge-get-app" : "#gauge-chart"} on this page`);
    reportFailure(e);
    throw e;
  }
  try {
    if (cfg.fonts) { const s = doc.createElement("style"); s.textContent = cfg.fonts; doc.head.appendChild(s); }
    // ONE feed, read by the source AND handed to the chart (the year scale and the early draft read its observations)
    const feed = httpFeed(cfg.live, fetchImpl);
    const source = stationChartSource(cfg.station as StationId, feed, climOf(cfg));
    renderInto(box, h(GaugeChartRoot, { source, station: cfg.station as StationId, feed }));
  } catch (e) {
    shown(box);
    reportFailure(e);
  }
  // (T1039, owner 2026-11-23: "chart should use same buttons as spots too for now") the web app's own "Get the app" block
  renderInto(getApp, h(GaugePageGetApp, { contact: cfg.contact }));
}

/**
 * (T1039 re-review, W46) A REACT ROOT THAT SAYS ITS OWN FAILURE: a render error (React 19 empties the root) is said where it
 * was drawn (`CHART_FAILED`) and reported (core `reportFailure`, to the page's reporter), never a silently empty box.
 */
export function renderInto(container: Element, element: ReactElement): void {
  createRoot(container, {
    onUncaughtError: (e: unknown) => {
      container.innerHTML = `<p class="warn">${CHART_FAILED}</p>`;
      reportFailure(e);
    },
  }).render(element);
}

/** the Spots tab's "Get the app" block, in the page's theme */
export function GaugePageGetApp({ contact }: { contact: string | null }) {
  const palette = THEMES[useColorScheme() === "dark" ? "dark" : "light"];
  return h(GetAppBlock, { palette, contact });
}


/**
 * (T1039) THE PUBLIC GAUGE PAGE'S SOURCE: the questions the app's gauge card asks (`gaugeSeries`, `gaugeParameters`, @app/ui
 * `useStationChart`; `gaugeNow`, its reading), answered as the release source answers them — the live feed's observations over the station's
 * climatology (`climSeries`; its quantities as `gaugeParameters` lists them) — with that climatology handed in: the site
 * build reads it from the same release's `gauge_clim` rows and writes it into the page (tools/seo gauges.ts). A page that
 * opens the whole release would download a 1 MB file and an SQL engine for one station's 1 KB.
 */
import { shownParameter } from "@app/core";
import type { Parameter, StationId } from "../index";
import type { WaterSource } from "../source";
import { climSeries, parametersOf } from "../sql/shared";
import type { GaugeFeed } from "./http";

/** one row of the release's `gauge_clim` for the page's station: a quantity and its clim/3 bands */
export interface StationClim { readonly parameter: Parameter; readonly bands: Uint8Array }

export function stationChartSource(station: StationId, feed: Pick<GaugeFeed, "observations" | "now">, clim: readonly StationClim[]):
  Pick<WaterSource, "gaugeSeries" | "gaugeParameters" | "gaugeNow"> {
  // a page asked about another station is a defect in the page, never "no envelope"
  const mine = (s: StationId) => { if (s !== station) throw new Error(`gauge page source: asked for ${s}, it holds ${station}`); };
  return {
    async gaugeSeries(s, span, parameter, model) {
      mine(s);
      const obs = await feed.observations(s);
      const param = parameter ?? shownParameter(obs ?? {});
      return climSeries(clim.find((c) => c.parameter === param)?.bands ?? null, obs, span, param, model);
    },
    // (T1039 review) the station's reading, as the release source answers it (`feed.now`): the quantities it reports live
    // tell the chart which it may draw for a station with no climatology (T830)
    async gaugeNow(s) {
      mine(s);
      return feed.now(s);
    },
    async gaugeParameters(s) {
      mine(s);
      // the release's `SELECT DISTINCT parameter … ORDER BY parameter`, over the rows the build wrote
      return parametersOf([...new Set(clim.map((c) => c.parameter))].sort().map((parameter) => ({ parameter })));
    },
  };
}

import { useMemo } from "react";
import { type DonorRow, type Horizon, type PanelAnswer, type Quantity, type WaterSource, type SectionId,
         type StationId } from "@app/data";
import { changeLowWaterNote, CONDITIONS_WORDS, disagreeNote, donorBecause, donorCounted, donorFacts, donorLastReport,
         donorPinTitle, donorReads, donorReason, donorRowLabel, donorSilent, donorVotes, donorWeightLine,
         estimateHeadline, estimateRange, estimateSentence, gaugeChoiceLabel, HORIZON_LABELS, isAhead, metresApart,
         panelCamera, refusalIsAboutTheDay, regulatedNote, SAME_PLACE_M, shortRecordNote, silentLine, tidalNote,
         backwaterNote, unplacedNote, type SectionKey } from "@app/core";
import { donorMapKey, donorMapLabel, hereMapLabel, ROUTE_MAP_WORDS, routeLegend } from "@app/core";
import { gaugeSides, groupGauges, stateAt, type GaugeGroup, type GaugeSide } from "@app/core";
import { useAsync } from "../async";
import { horizonChips, type HorizonChipVM } from "./horizon";

/* ============================================================ CONDITIONS VIEW ====
 *
 * P12a: what `DonorPanel` and `ConditionsPanel` draw, finished. The sentences are core's
 * (`conditions.ts`, pinned whole); the CHOICES — which station leads, which notes appear,
 * which day the range is for — are made here, so the components only draw.
 */

/** One donor row, every line finished. */
export interface DonorRowVM {
  station: string;
  /**
   * The member's index in the panel's weight order (`PanelAnswer.rows`) — its TONE. The same index its pin, its route,
   * its key and its switch chip carry, so a row and its pin are one colour though silent members are not drawn.
   */
  row: number;
  name: string | null;
  /** Not reporting: drawn in the quiet tone, contributes nothing. */
  quiet: boolean;
  /** The row's chart is the one showing. */
  on: boolean;
  /** The weight bar's width, never below 1% so a real share stays visible. */
  bar: `${number}%`;
  weightLine: string;
  reads: string;
  /** A quiet gauge's last report, local and labelled — or that it is not known. Null for a reporting gauge. */
  lastReport: string | null;
  facts: string;
  /** Why it counts what it counts; null for a quiet gauge, which counts nothing. */
  because: string | null;
  /** The row's screen-reader name. */
  label: string;
}

/**
 * A mark on the route map: `row` is the donor's index, null for "you are here".
 *
 * `kind` is the SHAPE (owner 2026-10-21: the spot must not look like a gauge); `label` is
 * what is written beside it on the map — a gauge's share, the spot's words; `rank` orders
 * the labels, heaviest gauge first, so a crowded valley keeps the label that matters most.
 */
export interface DonorPinVM {
  lat: number; lon: number; row: number | null; title: string;
  kind: "here" | "gauge"; label: string | null; rank: number;
}
export interface DonorKeyVM { row: number | null; label: string }

/**
 * (T168) EACH STATION'S SIDE — on this water or a donor (core `gaugeSide`), by station. Null until the waters are read
 * (`useGaugeSides`), and on a saved spot, which froze none: the surfaces then draw one run, as before.
 */
export type GaugeSides = ReadonlyMap<string, GaugeSide> | null;

/** A surface's items grouped (core `groupGauges`): own first, each group's not-reporting members under its fold. */
export type GaugeGroupVM<T> = GaugeGroup<T>;

/** One donor's route, for the overlay: its chain of reaches, spot first, and both ends. */
export interface DonorRouteVM {
  row: number;
  /** Where the GAUGE sits: downstream is drawn solid, upstream dashed. */
  role: "up" | "down";
  path: readonly SectionKey[];
  from: { lat: number; lon: number } | null;
  to: { lat: number; lon: number };
}

export interface DonorMapVM {
  camera: { lon: number; lat: number; zoom: number };
  /** Remounts the map only when the frame genuinely changes (a map reads its camera once). */
  key: string;
  pins: readonly DonorPinVM[];
  keys: readonly DonorKeyVM[];
  unplaced: string | null;
  /** The reaches between here and the gauges, unioned. */
  chain: readonly SectionKey[];
  /** Each placed donor's route, in weight order — what the overlay copies out of the tiles. */
  routes: readonly DonorRouteVM[];
  /** (T168) The key's gauges grouped, own first, the not-reporting under each group's fold; null without sides. */
  keyGroups: readonly GaugeGroupVM<DonorKeyVM>[] | null;
  /** The key's line samples: only the roles drawn. */
  legend: readonly { role: "up" | "down"; label: string }[];
  /** Every mark's extent [w, s, e, n] — what "fit" returns to. */
  bbox: readonly [number, number, number, number];
  words: { fit: string; fitLabel: string; larger: string; smaller: string };
}

export type DonorVM =
  | {
      ok: false; heading: string; reason: string;
      /** The chips, kept when the refusal is about the CHOSEN DAY (`no-forecast`), so the
       *  reader can step back from it; null when another day would not change the answer. */
      horizons: readonly HorizonChipVM[] | null;
      /** Every member is silent (no-record, no-forecast): the line naming them. Null with no members. */
      silent: string | null;
    }
  | {
      ok: true;
      heading: string;
      headline: string;
      /** "by then", drawn quieter after the headline, on a forecast; null for now. */
      headlineSuffix: string | null;
      sentence: string;
      range: string;
      horizons: readonly HorizonChipVM[];
      /** Null until the routes are in, or when no donor can be placed. */
      map: DonorMapVM | null;
      tapToChart: string;
      listLabel: string;
      /** Only the members that speak at this horizon; the silent ones are in `silent`. */
      rows: readonly DonorRowVM[];
      /**
       * (T168) EVERY member's row, grouped: on this water first, then the donors; in each, the reporting ones listed and
       * the rest (silent and unranked) under a closed fold. Null without sides (a saved spot, or still reading): draw
       * `rows` and `silent`, as before. With groups, `silent` is not drawn — its members are the folds' rows.
       */
      groups: readonly GaugeGroupVM<DonorRowVM>[] | null;
      /** "Not reporting: …" — the members hidden from the table and the map, each with its last data time. */
      silent: string | null;
      shortRecord: string | null;
      regulated: string | null;
      /** The tide (T78): one sentence, Level only, where a reporting member's station is tidal; else null. */
      tidal: string | null;
      /** A lake's standing backwater (T160): one sentence, Level only, where a reporting member's station is backed
       *  up by a lake below it; else null. */
      backwater: string | null;
      disagree: string | null;
      /**
       * Under Change, where a counted gauge's change is ELF's alone (T157, core `changeLowWaterNote`): the low-water
       * caveat the charted gauge's Change block says, said for the spot's own answer; null otherwise.
       */
      lowWater: string | null;
      explainer: string;
    };

export interface DonorViewOptions {
  /** The CHOSEN day: today on a live sheet, the visit on a saved spot. Never the clock. */
  day: Date;
  horizon: Horizon;
  /** Where the reader is, for "you are here". */
  from: { lat: number; lon: number } | null;
  /** The route map's measured size; the camera fits each axis to its own dimension. */
  size: { width: number; height: number };
  selected: StationId | null;
  /** Rows are controls (the caller has a chart to steer). */
  pickable: boolean;
  /** The time zone times are said in — for tests; left out, the device's. */
  zone?: string;
  /**
   * The quantity this answer is in — the one the map is painting. Only the tide's sentence reads it (Level only,
   * T78); left out (a saved spot, which did not record it), the tide is not said.
   */
  quantity?: Quantity;
  /** (T168) Each member's side; null or absent: one run, as before. */
  sides?: GaugeSides;
  /**
   * (T175) THE WATER ITSELF, where no tap says where the reader is (a water opened from search or the list): its box
   * [w, s, e, n] from the release (`WaterSource.waterBox`). The route map frames it with the gauges, so the water being
   * read is on the map it opens with — a lone gauge 20 km down the valley used to fill the frame and the creek was off
   * it. Ignored when `from` is given: the tap is the place then.
   */
  place?: readonly [number, number, number, number] | null;
}

/** The donor panel's view model, from an answer. Pure: a saved spot builds it from frozen data. */
export function donorView(value: PanelAnswer, o: DonorViewOptions): DonorVM {
  const { answer, rows } = value;
  /*
   * SILENT MEMBERS ARE HIDDEN, AND NAMED (owner 2026-10-22). A member with no reading at this horizon is not a row and
   * not a pin; `silentLine` names it under the table with its last data time. Core decides which are silent (the same
   * rule `donorReads` words), against the CHOSEN day.
   */
  const ref = o.day.getTime();
  const silent = silentLine(rows, { ref, horizon: o.horizon, ...(o.zone ? { zone: o.zone } : {}) });
  if (!answer.ok)
    return { ok: false, heading: CONDITIONS_WORDS.noEstimate, reason: donorReason(answer.why, o.horizon),
             horizons: refusalIsAboutTheDay(answer.why) ? horizonChips(o.horizon, o.quantity) : null,
             // T143: under a flow-only forecast the members are not without one — "No forecast this far ahead: …" under
             // the reason that says the forecast is there, in flow, would contradict it
             silent: answer.why === "flow-forecast-only" ? null : silent };
  const e = answer.value;
  /*
   * A ROW'S TONE IS ITS INDEX IN THE PANEL'S WEIGHT ORDER, not its place among the rows shown. The gauge switch lists
   * every member by that index, so the switch's dot, the row and the pin are one colour; and silent members weigh 0
   * and sort last, so the speaking ones keep the first tones anyway — a hidden member never shifts a shown one's hue.
   */
  const tone = new Map(rows.map((r, i) => [r, i]));
  const shown = rows.filter((r) => !donorSilent(r, { ref, horizon: o.horizon }));
  const toneOf = (r: DonorRow) => tone.get(r)!;

  const placed = shown.filter((r) => r.route?.lat != null && r.route?.lon != null);
  // (T175) no tap: the water's own box stands for the place (both corners), so the water is in the frame
  const corners = !o.from && o.place
    ? [{ lat: o.place[1], lon: o.place[0] }, { lat: o.place[3], lon: o.place[2] }] : [];
  const camera = panelCamera([...placed.map((r) => ({ lat: r.route!.lat, lon: r.route!.lon })), ...corners],
                             o.from, o.size);
  // A gauge AT the tap absorbs "you are here" — two pins on one pixel read as one lost.
  const from = o.from;
  const atSpot = from
    ? placed.find((r) => metresApart(from, { lat: r.route!.lat!, lon: r.route!.lon! })
                         <= SAME_PLACE_M)
    : undefined;
  const here = CONDITIONS_WORDS.youAreHere;
  /*
   * THE SPOT IS ALWAYS MARKED NOW, gauge at the spot or not. It used to be absorbed into a
   * gauge within `SAME_PLACE_M`, because both were the same teardrop and two on one pixel
   * read as one lost. The spot is a pin STANDING on the place and a gauge a dot lying on it
   * (owner 2026-10-21: different icons), so both show: the dot at the pin's point. The
   * absorbing words stay on the gauge's own title — it IS the gauge you are standing at.
   */
  const pts = [...placed.map((r) => ({ lat: r.route!.lat!, lon: r.route!.lon! })),
               ...(from ? [from] : corners)];
  const drawn = placed.filter((r) => (r.route?.path.length ?? 0) > 0);
  // (T168) on this water first: the key reads own gauges first, and their labels win a crowded map (`rank`)
  const sides = o.sides ?? null;
  const sideOf = (r: DonorRow): GaugeSide => sides?.get(r.station) ?? "donor";
  const ask = { ref, horizon: o.horizon };
  // every key stays in sight: each names a pin drawn on the map (a silent member has neither), so none folds
  const placedGroups = sides
    ? groupGauges(placed, sideOf, (r) => stateAt(r, ask), { horizon: o.horizon, keep: () => true }) : null;
  const keyOrder = placedGroups ? placedGroups.flatMap((g) => g.shown) : placed;
  const rankOf = new Map(keyOrder.map((r, i) => [r, i]));
  const keyOf = (r: DonorRow): DonorKeyVM => ({ row: toneOf(r), label: donorMapKey(r, ask) });
  const map: DonorMapVM | null = value.routesReady && camera ? {
    camera,
    key: `${camera.lon.toFixed(3)},${camera.lat.toFixed(3)},${camera.zoom.toFixed(1)}`,
    pins: [
      ...placed.map((r): DonorPinVM => {
        const row = toneOf(r);
        return { lat: r.route!.lat!, lon: r.route!.lon!, row, kind: "gauge",
                 title: donorPinTitle(r, r === atSpot), label: donorMapLabel(r, ask), rank: sides ? rankOf.get(r) ?? row : row };
      }),
      // LAST, so it draws over a gauge at the same place.
      ...(from ? [{ lat: from.lat, lon: from.lon, row: null, title: here, kind: "here" as const,
                    label: hereMapLabel(), rank: -1 }] : []),
    ],
    keys: [
      ...(from ? [{ row: null, label: here }] : []),
      ...keyOrder.map(keyOf),
    ],
    keyGroups: placedGroups?.map((g) => ({ ...g, shown: g.shown.map(keyOf), folded: g.folded.map(keyOf) })) ?? null,
    unplaced: unplacedNote(shown.length - placed.length),
    chain: [...new Set(shown.flatMap((r) => r.route?.path ?? []))],
    routes: drawn.map((r) => ({ row: toneOf(r), role: r.role, path: r.route!.path,
                                from, to: { lat: r.route!.lat!, lon: r.route!.lon! } })),
    legend: routeLegend(drawn.map((r) => r.role)),
    bbox: [Math.min(...pts.map((p) => p.lon)), Math.min(...pts.map((p) => p.lat)),
           Math.max(...pts.map((p) => p.lon)), Math.max(...pts.map((p) => p.lat))],
    words: { fit: ROUTE_MAP_WORDS.fit, fitLabel: ROUTE_MAP_WORDS.fitLabel,
             larger: ROUTE_MAP_WORDS.larger, smaller: ROUTE_MAP_WORDS.smaller },
  } : null;

  function rowVM(r: DonorRow): DonorRowVM {
    const on = o.selected != null && r.station === o.selected;
    return {
      station: r.station, row: toneOf(r), name: r.route?.name ?? null, quiet: !donorCounted(r), on,
      bar: `${Math.max(1, Math.round(r.weight * 100))}%`,
      weightLine: donorWeightLine(r),
      reads: donorReads(r, { ref: o.day.getTime(), horizon: o.horizon }),
      lastReport: !donorCounted(r) ? donorLastReport(r.observedAt, o.zone) : null,
      facts: donorFacts(r),
      because: donorCounted(r) ? donorBecause(r) : null,
      label: donorRowLabel(r, o.pickable, on, { ref, horizon: o.horizon }),
    };
  }

  return {
    ok: true,
    heading: HORIZON_LABELS[o.horizon].heading,
    headline: estimateHeadline(e, o.horizon),
    // "by then" is about a DAY; Change spans the window and its heading says so
    headlineSuffix: isAhead(o.horizon) && o.horizon !== "change" ? CONDITIONS_WORDS.byThen : null,
    sentence: estimateSentence(e, o.horizon),
    range: estimateRange(e, o.day, o.horizon, value.areaKm2),
    horizons: horizonChips(o.horizon, o.quantity),
    map,
    tapToChart: CONDITIONS_WORDS.tapToChart,
    listLabel: CONDITIONS_WORDS.donorList,
    rows: shown.map(rowVM),
    groups: sides ? groupGauges(rows, sideOf, (r) => stateAt(r, ask), { horizon: o.horizon })
      .map((g) => ({ ...g, shown: g.shown.map(rowVM), folded: g.folded.map(rowVM) })) : null,
    silent,
    shortRecord: shortRecordNote(rows),
    regulated: regulatedNote(rows),
    tidal: o.quantity ? tidalNote(rows, o.quantity) : null,
    backwater: o.quantity ? backwaterNote(rows, o.quantity) : null,
    disagree: disagreeNote(e, o.horizon),
    lowWater: o.horizon === "change" ? lowWaterNote(shown) : null,
    explainer: o.horizon === "change" ? `${CONDITIONS_WORDS.changeExplainer} ${CONDITIONS_WORDS.explainer}`
      : CONDITIONS_WORDS.explainer,
  };
}

/**
 * The members voting in the answer whose change is ELF's alone (`DonorRow.lowWater`), said by core (T157). Voting, not
 * merely reading (core `donorVotes`): a reserve member that stood back is not part of "this change".
 */
function lowWaterNote(rows: readonly DonorRow[]): string | null {
  const counted = rows.filter(donorVotes);
  const low = counted.filter((r) => r.lowWater).map((r) => r.station);
  return changeLowWaterNote(low, low.length === counted.length);
}

/** One choice in the chart's gauge switcher (owner 2026-10-21: the switch sits by the plot it controls). */
export interface GaugeChoiceVM {
  station: string;
  name: string | null;
  /**
   * "35%", "<1%", or the member's state — "not reporting", "no forecast", "flow forecast only" — in the words its dot
   * on the route map and its key say (core `donorMapLabel`), at the day and horizon the sheet is on (T157).
   */
  share: string;
  quiet: boolean;
  on: boolean;
  /** The donor's index, so the switcher's dot is its pin's and its row's colour. */
  row: number;
  label: string;
}

/**
 * THE GAUGE SWITCHER, beside the chart: every donor in the panel's weight order, the one the chart shows marked. Empty
 * when there is nothing to switch between (one gauge, or none) — a single option is not a choice.
 *
 * AT THE DAY AND HORIZON ASKED (T157): a member's word is its state there, as its row and its dot say it. It said
 * "quiet" for every member that did not count, so on a flow-only day under Level the switcher called every gauge quiet
 * under a sheet saying they have a flow forecast.
 */
export function gaugeChoices(rows: readonly DonorRow[], lead: StationId | null,
                             ask: { ref?: number; horizon?: Horizon } = {}): GaugeChoiceVM[] {
  if (rows.length < 2) return [];
  return rows.map((r, i) => {
    const on = r.station === lead;
    return { station: r.station, name: r.route?.name ?? null, share: donorMapLabel(r, ask),
             quiet: !donorCounted(r), on, row: i, label: gaugeChoiceLabel(r, on, ask) };
  });
}

/**
 * (T168) THE SWITCH GROUPED: `gaugeChoices` own first, each group's not-reporting chips under its fold — except the chip
 * the chart shows, which stays in sight. Null without sides or without a choice to make.
 */
export function gaugeChoiceGroups(rows: readonly DonorRow[], lead: StationId | null,
                                  ask: { ref?: number; horizon?: Horizon }, sides: GaugeSides):
  GaugeGroupVM<GaugeChoiceVM>[] | null {
  const choices = gaugeChoices(rows, lead, ask);
  if (!sides || !choices.length) return null;
  const row = new Map(rows.map((r) => [r.station as string, r]));
  return groupGauges(choices, (c) => sides.get(c.station) ?? "donor", (c) => stateAt(row.get(c.station)!, ask),
                     { horizon: ask.horizon ?? 0, keep: (c) => c.on });
}

/**
 * (T168) WHICH SIDE EACH STATION IS ON for this section (core `gaugeSides` over `WaterSource.gaugeWaters`). Null while
 * reading, without a section or stations, or when the source cannot say (an old test double): the surfaces then draw
 * one run, as before — never a guess at "on this water".
 */
export function useGaugeSides(source: WaterSource, section: SectionId | null,
                              stations: readonly StationId[]): GaugeSides {
  const key = stations.join(",");
  const got = useAsync(async () => {
    if (section === null || !stations.length) return null;
    const w = await source.gaugeWaters(section, stations);
    return gaugeSides(stations, { waters: w.waters, section }, w.gauges);
  }, `gaugesides:${section ?? ""}:${key}`, section !== null && stations.length > 0);
  return got.state === "ready" ? got.value : null;
}

/** `donorView`, memoised for a component. */
export function useDonorView(value: PanelAnswer, o: DonorViewOptions): DonorVM {
  const { day, horizon, from, size, selected, pickable, quantity, sides, place } = o;
  const t = day.getTime();
  const lat = from?.lat;
  const lon = from?.lon;
  const { width, height } = size;
  const placeKey = place ? place.join(",") : "";
  return useMemo(
    () => donorView(value, { day: new Date(t), horizon,
                             from: lat != null && lon != null ? { lat, lon } : null,
                             size: { width, height }, selected, pickable,
                             ...(quantity ? { quantity } : {}), sides: sides ?? null,
                             place: placeKey ? place ?? null : null }),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the box by value
    [value, t, horizon, lat, lon, width, height, selected, pickable, quantity, sides, placeKey]);
}

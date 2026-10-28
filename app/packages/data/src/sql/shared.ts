/**
 * HOW AN ANSWER IS MADE FROM ROWS (P11), once: the name ranking, the gauge link, the series join, the downstream walks.
 * `makeReleaseSource` (`../release/source.ts`) asks the release's files and hands the rows here; a test may hand rows
 * from any SQLite with the pipeline bundle's columns. (Its second reader, the dev bundle's, was retired in T61.)
 */
import { bandAt, boxOfParts, calendarDayOfYear, describeBy, rankPointers, rankWaters, type Band, type Bbox, type NearPlace } from "@app/core";
import { forecastFor, type Observations } from "../feed/http";
import type { Aged, GaugeLink, GaugeWaters, ItemId, NameHit, NearHit, Parameter, PanelRoute, PointerHit, SectionId,
              Series, StationId } from "../index";
import { num, str, type Cell, type Row } from "./db";

/**
 * A section handle out of SQLite.
 *
 * ONE conversion, so nothing can decide for itself that a section is a string again. A
 * section is an integer handle into the atlas's section_handles.txt — see the SectionId
 * doc in ../index. A null handle is a build defect (every section-keyed row is keyed by a
 * NOT NULL column), so it fails here rather than becoming section 0, which exists and is a
 * real piece of river.
 */
export const sid = (v: Cell): SectionId => {
  const n = typeof v === "number" ? v : Number(v);
  if (v == null || !Number.isInteger(n))
    throw new Error(`section handle is not an integer: ${JSON.stringify(v)}`);
  return n as SectionId;
};

/**
 * Search candidates (rows of `SEARCH`) to the hits a reader sees: ONE row per ITEM — an item matched by both its name
 * and an alias is one result, and the better of the two matches is the one it keeps — in core's order (`rankWaters`).
 */
export function rankNameHits(query: string, rows: readonly Row[], limit: number): NameHit[] {
  const seen = new Map<string, NameHit>();
  for (const r of rows) {
    const item = str(r.item_id);
    const hit: NameHit = {
      item: item as ItemId, name: str(r.name), kind: str(r.kind),
      matchedAs: r.matched_as == null ? null : str(r.matched_as),
      pieces: Number(r.pieces ?? 0), size: num(r.size), areaHa: num(r.area_ha), near: null,
    };
    const had = seen.get(item);
    if (!had || rankWaters(query, [had, hit])[0] === hit) seen.set(item, hit);
  }
  return rankWaters(query, [...seen.values()]).slice(0, limit);
}

/**
 * `SEARCH_POINTERS` rows (one per named row) to pointer hits — one per pointer row, its named waters in the book's
 * order (`ix`) — in core's order (`rankPointers`), at most `limit`.
 */
export function pointerHits(query: string, rows: readonly Row[], limit: number): PointerHit[] {
  const by = new Map<string, { entry: string; name: string; see: { ix: number; name: string; item: ItemId | null }[] }>();
  for (const r of rows) {
    const e = str(r.entry_id);
    const h = by.get(e) ?? { entry: e, name: str(r.name), see: [] };
    h.see.push({ ix: Number(r.ix), name: str(r.target_name), item: r.item_id == null ? null : (str(r.item_id) as ItemId) });
    by.set(e, h);
  }
  const hits: PointerHit[] = [...by.values()].map((h) => ({
    entry: h.entry, name: h.name,
    see: [...h.see].sort((a, b) => a.ix - b.ix).map(({ name, item }) => ({ name, item })) }));
  return rankPointers(query, hits).slice(0, limit);
}

/**
 * (T224) Each hit's name sources, from `name_source` rows (item_id, name, label): the shown name's (`nameSource`) and
 * the matched alias's (`matchedSource`). A name with no row is official (or the release predates the table): null.
 */
export function withNameSources(hits: NameHit[], rows: readonly Row[]): NameHit[] {
  const by = new Map<string, string>();
  for (const r of rows) by.set(`${str(r.item_id)}\u0000${str(r.name)}`, str(r.label));
  for (const h of hits) {
    const o = h as { nameSource?: string | null; matchedSource?: string | null };
    o.nameSource = by.get(`${h.item}\u0000${h.name}`) ?? null;
    o.matchedSource = h.matchedAs !== null ? by.get(`${h.item}\u0000${h.matchedAs}`) ?? null : null;
  }
  return hits;
}

/** Each hit's nearest-town line, from its rings (rows with item_id, name, kind, lat, lon, km; nearest first). */
export function withNear(hits: NameHit[], rings: readonly Row[]): NameHit[] {
  const by = new Map<string, NearPlace[]>();
  for (const r of rings) {
    const list = by.get(str(r.item_id)) ?? [];
    list.push({ name: str(r.name), kind: str(r.kind), lat: Number(r.lat), lon: Number(r.lon), km: Number(r.km) });
    by.set(str(r.item_id), list);
  }
  for (const h of hits) (h as { near: NearPlace | null }).near = describeBy(by.get(h.item) ?? []);
  return hits;
}

/** One `WATERS_NEAR`-shaped row to a hit, with its size signals. */
export function nearHit(r: Row): NearHit {
  return {
    item: str(r.item_id) as ItemId, name: str(r.name), kind: str(r.kind),
    km: Number(r.km),
    signals: {
      mag: r.mag === null || r.mag === undefined ? null : Number(r.mag),
      areaHa: num(r.area_ha),
      pieces: Number(r.pieces), towns: Number(r.towns),
      gauged: Number(r.gauged) === 1, stocked: Number(r.stocked) === 1,
      listed: Number(r.listed) === 1,
    },
  } as NearHit;
}

/**
 * One gauge row -> one GaugeLink, or null.
 *
 * Both gauge accessors funnel through here so a reach and a whole water can never describe
 * the same station differently — the failure the shared-logic rule exists to prevent.
 *
 * Both magnitudes are real and stored: `reach_mag` is this section's, `mag` the gauge
 * node's. Their ratio is what the trust band was cut from, so a sheet can show its working
 * — "this station drains 40x your reach" — rather than asserting a word. A 0 here means
 * the bundler had no magnitude for that node, which the trace model renders as "not known".
 */
export function link(r: Row | undefined, section: SectionId | null,
                     live: ReadonlySet<string> | null): GaugeLink | null {
  if (!r) return null;
  return {
    station: str(r.station) as StationId,
    // The station's own name when the join found it; its id is a poor label but a true one.
    name: r.name ? str(r.name) : str(r.station),
    trust: str(r.trust) as GaugeLink["trust"],
    reachMagnitude: num(r.reach_mag) ?? 0,
    gaugeMagnitude: num(r.mag) ?? 0,
    // Purely the feed's answer. `null` when no index was available — genuinely unknown,
    // which the UI must not render as "stopped reporting".
    live: live === null ? null : live.has(str(r.station)),
    areaKm2: num(r.area_km2),
    lon: num(r.lon), lat: num(r.lat),
    section,
  };
}

const bytesOf = (raw: Uint8Array | ArrayBuffer) => (raw instanceof Uint8Array ? raw : new Uint8Array(raw));

/**
 * The envelope blob to 73 pentads. `clim/1` (the bundle) is per pentad `u8 pentad, 5 × float32 LE`; `clim/2` (the
 * release, MINIMAL-PACKAGING §4) the same with float16. A short or ragged blob is treated as no envelope rather than
 * half of one: a band built from a truncated read would be silently wrong at exactly one time of year.
 */
export function pentadsOf(raw: Uint8Array | ArrayBuffer | null | undefined, codec: "clim/1" | "clim/2"): (Band | null)[] {
  const pentads: (Band | null)[] = Array.from({ length: 73 }, () => null);
  if (!raw) return pentads;
  const bytes = bytesOf(raw);
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const w = codec === "clim/1" ? 4 : 2, rec = 1 + 5 * w;
  const at = (o: number) => (w === 4 ? dv.getFloat32(o, true) : f16(dv.getUint16(o, true)));
  for (let o = 0; o + rec <= bytes.byteLength; o += rec) {
    const i = dv.getUint8(o);
    if (i >= 73) continue;
    pentads[i] = [at(o + 1), at(o + 1 + w), at(o + 1 + 2 * w), at(o + 1 + 3 * w), at(o + 1 + 4 * w)] as Band;
  }
  return pentads;
}

/** IEEE 754 half precision to a number (`DataView.getFloat16` is not on every engine the app runs on). */
export function f16(h: number): number {
  const s = h & 0x8000 ? -1 : 1, e = (h >> 10) & 0x1f, m = h & 0x3ff;
  if (e === 0) return s * m * 2 ** -24;
  if (e === 31) return m ? NaN : s * Infinity;
  return s * (1 + m / 1024) * 2 ** (e - 15);
}

/**
 * THE JOIN. Observations from the feed, envelope from the bundle, one axis.
 *
 * They live apart because they change apart: a reading is thirty minutes old and a
 * climatology is a year old, and putting the envelope in the feed would mean
 * republishing 440 stations' history every half hour to carry numbers that did not
 * move. Neither half is a chart on its own — a line with no envelope cannot say
 * whether today is unusual, and an envelope with no line does not say where today is.
 *
 * TWO SPANS, TWO DIFFERENT AXES:
 *
 *   72h    the observations, at 30-minute steps, over the envelope for those days.
 *          What is the river doing right now.
 *   year   the whole envelope, 73 pentads, with today's reading marked on it. There
 *          are NO observations here and that is honest rather than missing: the daily
 *          record for the current year is not published anywhere we read. The shape of
 *          the year and where today sits in it is the question, and it is answered.
 */
export function seriesOf(pentads: (Band | null)[], obs: Observations | null, span: "72h" | "year",
                         param: Parameter, model?: string): Aged<Series> | null {
  const hasClim = pentads.some((b) => b !== null);

  // EVERY RUN, narrowed to this chart's quantity. CLEVER publishes discharge only, so
  // on a level chart it comes back with no ribbon and drops out; ELF publishes both.
  const runs = Object.entries(obs?.forecasts ?? {})
    .map(([, raw]) => forecastFor(raw, param))
    .filter((f): f is NonNullable<typeof f> => f !== null && f.series !== null)
    .sort((a, b) => a.model.localeCompare(b.model));
  const chosen = (model ? runs.find((f) => f.model === model) : null)
    // No preference: the freshest run. Which is a UI default, not a judgement about
    // which model is right — that is the reader's, and both are offered.
    ?? [...runs].sort((a, b) => (b.issuedAt ?? "").localeCompare(a.issuedAt ?? ""))[0]
    ?? null;

  if (span === "year") {
    if (!hasClim) return null;      // a year chart with no envelope has nothing to draw
    /**
     * YEAR TO DATE PLUS A MONTH, not the whole calendar year.
     *
     * A full year is mostly empty on the right: the record stops today, and the four
     * remaining months are a band with nothing in it. Ending a month past today keeps
     * the frame full of things that exist — the year so far, where today sits, and the
     * longest forecast (ELF, 30 days) reaching the edge rather than off it.
     *
     * ONE POINT PER DAY. The envelope is sampled every five days because percentiles
     * are noisy at daily resolution, but the lines across it are real daily records and
     * belong at their own: on 73 buckets a whole autumn is fourteen points. `bandAt`
     * interpolates between pentads, which is what it is for.
     */
    const at = obs?.at.length ? new Date(obs.at[obs.at.length - 1]!) : new Date();
    const year = at.getUTCFullYear();
    const today = calendarDayOfYear(at.toISOString().slice(0, 10));
    const days = Math.min(daysInYear(year), today + 31);
    const values: (number | null)[] = Array.from({ length: days }, () => null);
    const stamps: string[] = Array.from({ length: days }, (_, i) => dayStamp(year, i));
    // Last December rides along in January (the feed keeps it for a spot's lead-up, S1). It is LAST year's, so it
    // goes on last year's line below, never on this one.
    const lastYear: (number | null)[] = [];
    for (const [day, lv, q] of obs?.daily ?? []) {
      const i = calendarDayOfYear(day) - 1;
      if (day.startsWith(`${year - 1}-`)) {
        const v = param === "level" ? lv : q;
        if (i >= 0 && v !== null && Number.isFinite(v)) lastYear[i] = v;
        continue;
      }
      if (!day.startsWith(`${year}-`)) continue;
      if (i < 0 || i >= days) continue;
      const v = param === "level" ? lv : q;
      if (v !== null && Number.isFinite(v)) values[i] = v;
    }
    const yearBand = Array.from({ length: 366 }, (_, i) => bandAt(pentads, i + 1));
    const band = yearBand.slice(0, days);
    const reading = obs ? last(param === "level" ? obs.level : obs.discharge) : null;
    // Prior years land on the SAME day index, so 3 August is above 3 August whatever
    // year it was — which is the only comparison a seasonal chart is making.
    // Whole years: the chart cuts each to its frame (buildHydrograph); a lead-up reads last December out of it.
    const priorRaw: Record<string, (number | null)[]> = { ...((obs?.priorYears ?? {})[param] ?? {}) };
    if (lastYear.length) {
      // HYDAT's own copy of a day wins; the feed's fills only what HYDAT does not have.
      const had = priorRaw[String(year - 1)] ?? [];
      priorRaw[String(year - 1)] = Array.from({ length: Math.max(had.length, lastYear.length) },
        (_, i) => had[i] ?? lastYear[i] ?? null);
    }
    const prior = Object.entries(priorRaw)
      .map(([y, vals]) => ({ year: Number(y), values: vals }))
      .filter((p) => Number.isFinite(p.year))
      .sort((a, b) => b.year - a.year);
    return {
      fetchedAt: obs?.fetchedAt ?? null,         // P02: no observations, no age; never "now"
      value: {
        step: "1d", from: `${year}-01-01`, parameter: param,
        at: stamps, values, band, yearBand,
        now: reading === null ? null : { index: today - 1, value: reading },
        forecast: chosen, forecasts: runs, priorYears: prior, change: obs?.change ?? null,
      },
    };
  }

  if (!obs) return null;
  const values = param === "level" ? obs.level : obs.discharge;
  // Each observation takes the envelope for the day its own timestamp falls in,
  // interpolated between pentads, so a series spanning weeks bends rather than steps.
  const band = obs.at.map((t: string) => bandAt(pentads, calendarDayOfYear(t)));
  const idx = lastIndex(values);
  return {
    fetchedAt: obs.fetchedAt,
    value: {
      step: "1h", from: obs.from, parameter: param,
      at: obs.at, values, band,
      now: idx < 0 ? null : { index: idx, value: values[idx]! },
      forecast: chosen, forecasts: runs, priorYears: [], change: obs.change ?? null,
    },
  };
}

/** The stored downstream pointer from a section, or null at the edge of the pointers. */
export type DownOf = (from: SectionId) => Promise<SectionId | null>;

/**
 * Walk the stored pointers from `from`. They exist only inside a gauge's watershed, which is
 * exactly the path this walks — so running out of pointers IS the end of the trace.
 */
export async function traceDown(down: DownOf, from: SectionId): Promise<SectionId[]> {
  const path: SectionId[] = [from];
  const seen = new Set<SectionId>([from]);
  let at: SectionId = from;
  for (let i = 0; i < 5000; i++) {
    const next = await down(at);
    if (next === null) break;
    at = next;
    if (seen.has(at)) break;     // a braid that loops is a build defect, not a hang
    seen.add(at);
    path.push(at);
  }
  return path;
}

/**
 * Every donor in a panel, placed on the map with the water between.
 *
 * ONE WALK SERVES BOTH DIRECTIONS, because the stored pointers only go downstream.
 *
 * A donor UPSTREAM of the spot is reached by walking down FROM THE GAUGE until the
 * spot turns up; a donor downstream by walking down from the spot until the gauge
 * does. Same walk, ends swapped — so there is one implementation and no chance of the
 * two directions disagreeing about what a route is.
 *
 * The walk stops at `stop` or at the edge of the pointers, and returns null if it
 * never arrives: a chain that runs past its target is not a route to it, and drawing
 * it would light up water the gauge has nothing to do with.
 */
export async function routesOf(section: SectionId,
                               members: readonly { station: StationId; role: "up" | "down" }[],
                               places: ReadonlyMap<string, { name: string; section: SectionId | null;
                                                             lon: number | null; lat: number | null;
                                                             tidal?: boolean; backwater?: boolean }>,
                               down: DownOf): Promise<PanelRoute[]> {
  const walk = async (from: SectionId, stop: SectionId): Promise<SectionId[] | null> => {
    const path: SectionId[] = [from];
    const seen = new Set<SectionId>([from]);
    let at = from;
    for (let i = 0; i < 5000 && at !== stop; i++) {
      const next = await down(at);
      if (next === null) return null;
      at = next;
      if (seen.has(at)) return null;   // a looping braid is a build defect, not a hang
      seen.add(at);
      path.push(at);
    }
    return at === stop ? path : null;
  };

  const out: PanelRoute[] = [];
  for (const m of members) {
    const place = places.get(m.station);
    const gaugeSection = place?.section ?? null;
    let path: SectionId[] = [];
    if (gaugeSection === section) {
      path = [section];                // the gauge is ON this reach; the route is the spot
    } else if (gaugeSection) {
      // `role` is where the GAUGE sits, so "up" walks from the gauge down to here.
      const got = m.role === "up" ? (await walk(gaugeSection, section))?.slice().reverse()
                                  : await walk(section, gaugeSection);
      path = got ?? [];
    }
    out.push({
      station: m.station, name: place?.name ?? null,
      lon: place?.lon ?? null, lat: place?.lat ?? null,
      role: m.role, path,
      // `true` only (T78): a station the release flags tidal; absent otherwise, as an older release reads.
      ...(place?.tidal ? { tidal: true as const } : {}),
      // `true` only (T160): a station a lake below it backs up; absent otherwise, as an older release reads.
      ...(place?.backwater ? { backwater: true as const } : {}),
    });
  }
  return out;
}

/** `gaugePlaces` rows to the map `routesOf` reads. */
export function placesOf(rows: readonly Row[]): Map<string, { name: string; section: SectionId | null;
                                                              lon: number | null; lat: number | null;
                                                              tidal: boolean; backwater: boolean }> {
  const places = new Map<string, { name: string; section: SectionId | null; lon: number | null; lat: number | null;
                                   tidal: boolean; backwater: boolean }>();
  for (const r of rows)
    places.set(str(r.station), {
      name: str(r.name),
      section: r.sid == null ? null : sid(r.sid),
      lon: num(r.lon), lat: num(r.lat),
      // `gauge.tidal` (T78); a release built before it has no such column, and that reads as not flagged.
      tidal: Number(r.tidal ?? 0) === 1,
      // `gauge.backwater` (T160); absent on a release built before it, read as not flagged.
      backwater: Number(r.backwater ?? 0) === 1,
    });
  return places;
}

/** The station's own quantities, from `CLIM_PARAMETERS` rows: only discharge and level. */
export function parametersOf(rows: readonly Row[]): Parameter[] {
  return rows.map((r) => str(r.parameter) as Parameter)
    .filter((p): p is Parameter => p === "discharge" || p === "level");
}

function daysInYear(year: number): number {
  return (Date.UTC(year + 1, 0, 1) - Date.UTC(year, 0, 1)) / 86_400_000;
}

function dayStamp(year: number, index: number): string {
  return new Date(Date.UTC(year, 0, 1 + index)).toISOString().slice(0, 10);
}

/** The last value that is actually a number, or null. A trailing gap is not a reading. */
function last(values: readonly (number | null)[]): number | null {
  const i = lastIndex(values);
  return i < 0 ? null : values[i]!;
}

/**
 * A water's box from its `part_geo` rows (w, s, e, n, mid_lon, mid_lat), `perDegree` units to a degree: 1 in the
 * bundle (REAL degrees), 1e6 in the release (integer microdegrees, the pack's units). One conversion for both readers.
 */
export function boxOfGeoRows(rows: readonly Row[], perDegree: 1 | 1_000_000): Bbox | null {
  const deg = (v: Cell | undefined) => (v === null || v === undefined ? null : Number(v) / perDegree);
  return boxOfParts(rows.map((r) => ({ w: deg(r.w), s: deg(r.s), e: deg(r.e), n: deg(r.n),
                                       midLon: Number(r.mid_lon) / perDegree,
                                       midLat: Number(r.mid_lat) / perDegree })));
}

function lastIndex(values: readonly (number | null)[]): number {
  for (let i = values.length - 1; i >= 0; i--)
    if (values[i] !== null && Number.isFinite(values[i]!)) return i;
  return -1;
}

/**
 * THE WATERS JOINED TO THESE across `water_continue` (T168), both ways and transitively, as ords — one system, as
 * `build_panels.own_distances` reads a water's own gauges: a river that changes its name is one water (the Chilliwack
 * and the Vedder). `linked` answers the links touching a set of ords (`ord`, `next_ord` rows). Bounded: the province
 * has 17 links, and a cycle cannot loop because every ord is visited once.
 */
export async function continuedOrds(start: readonly number[],
                                    linked: (ords: number[]) => Promise<readonly Row[]>): Promise<number[]> {
  const seen = new Set<number>(start);
  let frontier = [...seen];
  for (let i = 0; i < 64 && frontier.length; i++) {
    const next: number[] = [];
    for (const r of await linked(frontier))
      for (const o of [Number(r.ord), Number(r.next_ord)])
        if (Number.isFinite(o) && !seen.has(o)) { seen.add(o); next.push(o); }
    frontier = next;
  }
  return [...seen].sort((a, b) => a - b);
}

/** `gauge` rows (`station`, `item_id`, `sid`) and a section's waters, as `WaterSource.gaugeWaters` answers them. */
export function gaugeWatersOf(waters: readonly string[], rows: readonly Row[]): GaugeWaters {
  const gauges = new Map<StationId, { item: ItemId | null; section: SectionId | null }>();
  for (const r of rows)
    gauges.set(str(r.station) as StationId, {
      item: r.item_id == null || str(r.item_id) === "" ? null : str(r.item_id) as ItemId,
      section: r.sid == null ? null : sid(r.sid),
    });
  return { waters: [...new Set(waters)].sort() as ItemId[], gauges };
}

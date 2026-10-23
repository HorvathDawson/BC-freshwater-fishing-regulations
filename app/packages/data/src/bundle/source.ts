/**
 * A RegsSource over the bundle — THE DEVELOPMENT PATH until P20 (`EXPO_PUBLIC_DATA_SOURCE=bundle`). The app reads the
 * release (`../release/source.ts`); both answer through the same helpers (`./shared.ts`) and the same conformance suite.
 *
 * The driver differs — `node:sqlite` here and in tests, sql.js on the web, expo-sqlite on the phone — but this file does
 * not, which is the entire point of the SQLite decision (`design/data-contract.html` §10).
 *
 * It decides nothing, and it reads no regulation table: regulations are not integrated (see
 * `regulations.ts` in @app/core for where they plug in).
 */
import { shownParameter, type RecordYears, type WaterFix } from "@app/core";
import type { Observations } from "../feed/http";
import type { BasinMember,
  Aged, BundleCounts, BundleInfo, GaugeLink, ItemId, LakeInfo, NameHit, NearHit, Parameter,
  Panel, PanelMember, PanelRoute, PlaceHit, PlaceId, PointerHit, Reading, RegsSource, Release,
  SectionId, Series,
  StationId, Water,
} from "../index";
import * as Q from "./queries";
import { num, str, type Db, type Row } from "./db";
import { buildStandingClasses, type StandingClasses } from "../standing";
import { boxOfGeoRows, link, nearHit, parametersOf, pentadsOf, placesOf, pointerHits, rankNameHits, routesOf, seriesOf, sid,
         traceDown, withNear, type DownOf } from "./shared";

/** A `panel_member` / `basin_member` row's record per quantity (T110), as the bundle stores it. */
const recordOf = (r: Row): RecordYears => ({ discharge: Number(r.discharge_years ?? 0), level: Number(r.level_years ?? 0) });

export interface BundleSourceOptions {
  /** The live feed. Absent means conditions render as "we could not check", never as a number. */
  feed?: {
    now(station: StationId): Promise<Aged<Reading> | null>;
    /** Raw observations, with no envelope — this source supplies the other half. */
    observations?(station: StationId): Promise<Observations | null>;
    /**
     * The stations transmitting right now — membership IS the answer.
     *
     * The bundle deliberately holds no liveness column: any boolean it baked in would be
     * right the week it was cut and wrong months later. This is the resident index the map
     * already loads to colour its dots, so asking it costs a set lookup.
     *
     * Absent means we could not check. Then nothing is preferred over anything else, which
     * is correct — an unknown must not read as "not transmitting".
     */
    live?(): Promise<ReadonlySet<string> | null>;
  };
}

export function makeBundleSource(db: Db, opts: BundleSourceOptions = {}): RegsSource {
  // Read once at open: it is five rows and every method wants them.
  let meta = new Map<string, string>();
  const ready = db.all(Q.META).then((rows) => {
    meta = new Map(rows.map((r) => [str(r.k), str(r.v)]));
  });
  /*
   * Whether this bundle carries lake area (`Q.withArea`), asked once. A FAILURE TO ASK IS A FAILURE (P11): it used to
   * become `false`, so a bundle that would not answer ranked every lake at size 0 and nothing said why. Now every read
   * that needs it rejects with the cause. The no-op catch only keeps an unasked rejection from being reported as
   * unhandled; whoever awaits `area` still gets it.
   */
  const area = db.get(Q.HAS_ITEM_AREA).then((r) => r !== undefined);
  area.catch(() => {});
  const down: DownOf = async (at) => {
    const r = await db.get(Q.DOWN_FROM, at);
    return r ? sid(r.down_sid) : null;
  };
  /** The Conditions map's classes, built once per source (`standingClasses`). */
  let classes: Promise<StandingClasses> | null = null;

  return {
    async info(): Promise<BundleInfo> {
      await ready;
      return { version: meta.get("version") ?? "unknown",
               validUntil: meta.get("valid_until") ?? null,
               sectionHandles: meta.get("section_handles") ?? null,
               // empty (the dev fixture: no reach run) reads as null — not held to one
               reachDigest: meta.get("reach_digest") || null };
    },

    async counts(): Promise<BundleCounts> {
      await ready;
      const r = await db.get(Q.COUNTS);
      // `surveyed` is 0 until the bathymetry matcher lands and the chart table has rows.
      // Reported as `null` rather than 0, because "we do not know yet" and "we checked and
      // there are none" are different claims and the sheet renders them differently.
      const surveyed = num(r?.surveyed);
      return { waters: num(r?.waters) ?? 0, reaches: num(r?.reaches) ?? 0,
               surveyed: surveyed ? surveyed : null, stations: num(r?.stations) ?? 0 };
    },

    async itemForSection(id) {
      const r = await db.get(Q.ITEM_FOR_SECTION, id);
      return r ? (str(r.item_id) as ItemId) : null;
    },

    async waterFor(id) {
      const r = await db.get(Q.ITEM_FOR_SECTION, id);
      if (!r) return null;
      const item = await db.get(Q.ITEM, str(r.item_id));
      return item
        ? { item: str(item.item_id) as ItemId, name: str(item.name), kind: str(item.kind) }
        : null;
    },

    async water(id): Promise<Water | null> {
      const item = await db.get(Q.ITEM, id);
      if (!item) return null;
      return {
        item: str(item.item_id) as ItemId, name: str(item.name), kind: str(item.kind),
        // The water's own order — see SECTIONS_FOR_ITEM for why that is `ORDER BY sid`.
        sections: (await db.all(Q.SECTIONS_FOR_ITEM, id)).map((r) => sid(r.sid)),
      };
    },

    async searchNames(q, limit): Promise<readonly NameHit[]> {
      const query = q.trim(), needle = Q.nameNeedle(query);
      if (needle === null || limit < 1) return [];
      // Candidates from SQL, ORDER from core — see `rankWaters`. Over-fetched so the rank
      // has room to promote a big water past a small one of the same match quality.
      const rows = await db.all(Q.withArea(Q.SEARCH, await area), needle,
                                Math.max(limit * 3, 60));
      const hits = rankNameHits(query, rows, limit);
      if (!hits.length) return hits;
      return withNear(hits, await db.all(Q.NEAR_FOR_ITEMS.replace("%IDS%", Q.placeholders(hits.length)),
                                         ...hits.map((h) => h.item)));
    },

    async searchPointers(q, limit): Promise<readonly PointerHit[]> {
      const query = q.trim();
      if (query.length < 2 || limit < 1) return [];
      return pointerHits(query, await db.all(Q.SEARCH_POINTERS, Q.likeEscape(query), Math.max(limit * 12, 60)), limit);
    },

    async searchPlaces(q, limit): Promise<readonly PlaceHit[]> {
      if (q.trim().length < 2 || limit < 1) return [];
      return (await db.all(Q.SEARCH_PLACES, Q.likeEscape(q.trim()), limit)).map((r) => ({
        place: str(r.place_id) as PlaceId, name: str(r.name), kind: str(r.kind),
        lat: Number(r.lat), lon: Number(r.lon), pop: num(r.pop),
      }));
    },

    async watersNear(place): Promise<readonly NearHit[]> {
      // `place` is the integer place_id from `searchPlaces`, carried as a string. Bound as
      // a NUMBER: the column is INTEGER and SQLite's affinity would convert a text
      // parameter anyway, but relying on that is relying on every driver doing it.
      // Every row joins to a real item — the precompute is keyed on item_id, so there is
      // nothing to invent when a name does not match.
      const id = /^\d+$/.test(place) ? Number(place) : place;
      // EVERY water within 25 km, not the first forty. It was capped at 40, and the list
      // under "Water near Chilliwack" then read as the whole answer while 162 of its 202
      // waters were missing — a count a screen shows has to be the count there is. The
      // busiest town in the province has 543; the cap is only a guard against a bad build.
      return (await db.all(Q.withArea(Q.WATERS_NEAR, await area), id, 2000)).map(nearHit);
    },

    async locate(item): Promise<WaterFix> {
      const pt = (r: Row) => ({ lat: Number(r.lat), lon: Number(r.lon) });
      const [gauges, stocked, rings] = await Promise.all([
        db.all(Q.GAUGES_ON_ITEM, item), db.all(Q.STOCKED_ON_ITEM, item),
        db.all(Q.RINGS_FOR_ITEM, item),
      ]);
      return {
        on: [...gauges, ...stocked].map(pt),
        near: rings.map((r) => ({ ...pt(r), km: Number(r.km) })),
      };
    },

    async waterBox(item) {
      return boxOfGeoRows(await db.all(Q.PART_GEO_FOR_ITEM, item), 1);
    },

    async gaugeForSection(id): Promise<GaugeLink | null> {
      const r = await db.get(Q.GAUGE_FOR_SECTION, id);
      return link(r, id, (await opts.feed?.live?.()) ?? null);
    },

    async panelsFor(sections) {
      const out = new Map<SectionId, Panel>();
      if (!sections.length) return out;
      for (let i = 0; i < sections.length; i += 500) {
        const chunk = sections.slice(i, i + 500);
        for (const r of await db.all(Q.panelsForSections(chunk.length), ...chunk)) {
          const sec = sid(r.sid);
          let panel = out.get(sec);
          if (!panel) {
            panel = { areaKm2: r.target_area === null ? null : Number(r.target_area),
                      members: [] };
            out.set(sec, panel);
          }
          // Not sorted here. `ord` is a stable member index and NOT a ranking — the order
          // depends on weights and weights depend on the target, so the caller sorts by
          // what it computes. See the Panel doc.
          (panel.members as PanelMember[]).push({
            station: str(r.station) as StationId,
            role: str(r.role) === "down" ? "down" : "up",
            areaKm2: Number(r.donor_area),
            years: recordOf(r),
            regulated: Number(r.regulated ?? 0) === 1,
            sameRiver: Number(r.same_river ?? 1) === 1,
            ...(Number(r.lead ?? 0) > 0 ? { lead: Number(r.lead) } : {}),
          });
        }
      }
      return out;
    },

    async lakeStationsFor(sections) {
      const out = new Map<SectionId, StationId>();
      if (!sections.length) return out;
      for (let i = 0; i < sections.length; i += 500) {
        const chunk = sections.slice(i, i + 500);
        for (const r of await db.all(Q.lakeStationsFor(chunk.length), ...chunk))
          out.set(sid(r.sid), str(r.station) as StationId);
      }
      return out;
    },

    async basinMembers() {
      const out = new Map<string, BasinMember[]>();
      for (const r of await db.all(Q.BASIN_MEMBERS)) {
        const id = str(r.basin_id);
        let rows = out.get(id);
        if (!rows) out.set(id, (rows = []));
        rows.push({ station: str(r.station) as StationId,
                    areaKm2: r.area_km2 == null ? null : Number(r.area_km2),
                    years: recordOf(r) });
      }
      return out;
    },

    // the bundle carries no lake facts (they are the release's `lakes`): the lakes stay plain
    async lakeFacts() { return null; },

    standingClasses() {
      // the whole tables, once: the same rows `panelsFor` and `lakeStationsFor` read a viewport of
      return (classes ??= (async () => {
        const members = new Map<number, PanelMember[]>();
        for (const r of await db.all(Q.ALL_PANEL_MEMBERS)) {
          const id = Number(r.panel_id);
          let list = members.get(id);
          if (!list) members.set(id, (list = []));
          list.push({ station: str(r.station) as StationId, role: str(r.role) === "down" ? "down" : "up",
                      areaKm2: Number(r.area_km2), years: recordOf(r),
                      regulated: Number(r.regulated ?? 0) === 1, sameRiver: Number(r.same_river ?? 1) === 1,
                      ...(Number(r.lead ?? 0) > 0 ? { lead: Number(r.lead) } : {}) });
        }
        const sections = (await db.all(Q.ALL_SECTION_PANELS)).map((r) => ({
          sid: Number(r.sid), panel: Number(r.panel_id), areaKm2: r.area_km2 === null ? null : Number(r.area_km2) }));
        const lakes = new Map((await db.all(Q.ALL_LAKE_SECTIONS))
          .map((r) => [Number(r.sid), str(r.station) as StationId] as const));
        const basins = new Map<string, BasinMember[]>();
        for (const r of await db.all(Q.BASIN_MEMBERS)) {
          const id = str(r.basin_id);
          let rows = basins.get(id);
          if (!rows) basins.set(id, (rows = []));
          rows.push({ station: str(r.station) as StationId, areaKm2: r.area_km2 == null ? null : Number(r.area_km2),
                      years: recordOf(r) });
        }
        await ready;
        return buildStandingClasses(meta.get("version") ?? "unknown", sections, members, lakes, basins);
      })());
    },

    async gaugePoints(): Promise<readonly { station: StationId; name: string;
                                            lon: number; lat: number;
                                            mag: number | null }[]> {
      return (await db.all(Q.GAUGE_POINTS)).map((r) => ({
        station: str(r.station) as StationId, name: str(r.name),
        lon: Number(r.lon), lat: Number(r.lat),
        // NULL stays null. A station whose node has no magnitude is unmeasured, not tiny,
        // and a zero would push it to the bottom of the ladder as if it were a ditch.
        mag: r.mag === null || r.mag === undefined ? null : Number(r.mag),
      }));
    },

    async gaugeNow(station) { return (await opts.feed?.now(station)) ?? null; },

    /** The feed's observations over the bundle's envelope (`seriesOf`; clim/1: float32 bands). */
    async gaugeSeries(station, span, parameter, model): Promise<Aged<Series> | null> {
      const obs = (await opts.feed?.observations?.(station)) ?? null;
      // Which quantity. The caller's choice wins; failing that, the station's own — never
      // a default, because "discharge" is wrong for the 237 stations that never measure it (core `shownParameter`).
      const param: Parameter = parameter ?? shownParameter(obs ?? {});
      const clim = await db.all(Q.CLIMATOLOGY, station, param);
      return seriesOf(pentadsOf(clim[0]?.bands as Uint8Array | null | undefined, "clim/1"), obs, span, param, model);
    },

    async gaugeParameters(station): Promise<readonly Parameter[]> {
      return parametersOf(await db.all(Q.CLIM_PARAMETERS, station));
    },

    async traceToGauge(from): Promise<readonly SectionId[]> {
      return traceDown(down, from);
    },

    async panelRoutes(section): Promise<readonly PanelRoute[]> {
      // The panel is re-read here rather than taken from `this.panelsFor` — a source is
      // routinely destructured, and a method that only works while it is still attached to
      // its object is a trap set for the next caller.
      const members = (await db.all(Q.panelsForSections(1), section)).map((r) => ({
        station: str(r.station) as StationId,
        role: (str(r.role) === "down" ? "down" : "up") as "up" | "down",
      }));
      if (!members.length) return [];
      const stations = members.map((m) => m.station);
      return routesOf(section, members, placesOf(await db.all(Q.gaugePlaces(stations.length), ...stations)), down);
    },

    async lakeInfo(id): Promise<LakeInfo | null> {
      const charts = await db.all(Q.CHARTS_FOR_ITEM, id);
      if (!charts.length) return null;
      return {
        item: id,
        charts: charts.map((c) => ({
          id: str(c.chart_id), title: str(c.title),
          kind: str(c.kind) === "digitised" ? "digitised" : "scan",
          drafted: c.drafted == null ? null : str(c.drafted),
          scale: num(c.scale), bytes: null,
        })),
        lastStocked: null,
      };
    },

    async stockingHistory(id): Promise<readonly Release[]> {
      const item = await db.get(Q.ITEM, id);
      if (!item) return [];
      return (await db.all(Q.RELEASES_FOR_NAME, str(item.name))).map((r) => ({
        date: str(r.date), species: str(r.species),
        count: num(r.count), stage: r.stage == null ? null : str(r.stage),
      }));
    },

    // the bundle holds no regulations store (the release's water + regs-<region> files do)
    async regulationKeys() { return []; },
  };
}

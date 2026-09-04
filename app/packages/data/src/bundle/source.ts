/**
 * A RegsSource over the bundle. One implementation for every platform.
 *
 * The driver differs — `node:sqlite` here and in tests, a wasm build range-reading R2 on
 * the web, expo-sqlite on the phone — but this file does not, which is the entire point of
 * the SQLite decision (`design/data-contract.html` §10). The conformance suite runs the
 * same assertions against this and against the hand-written fixture, so a divergence in
 * ANSWERS is a test failure rather than something a user finds.
 *
 * It decides nothing. Every outcome comes from `evaluate()` in core; this assembles the
 * rules that function needs and gets out of the way (AGENTS rule 23).
 */
import { bandAt, evaluate, type Band, type PlainDate, type Rule,
         type RuleKind, type SpeciesGroup, type Status, type Window } from "@app/core";
import { forecastFor, type Observations } from "../feed/http";
import type {
  Aged, BundleCounts, BundleInfo, GaugeLink, ItemId, ItemRegs, LakeInfo, NameHit, NearHit, Parameter,
  Panel, PanelMember, PanelRoute, PlaceHit, PlaceId, Reading, RegsSource, Release,
  SectionId, Series,
  StationId,
} from "../index";
import * as Q from "./queries";
import { json, num, str, type Db, type Row } from "./db";

/** Rule kinds core knows. Anything else is a build that added one without telling us. */
const KINDS = new Set<RuleKind>(["closure", "gear_restriction", "harvest",
                                 "vessel_restriction", "licensing", "note"]);

function toRule(r: Row, scope: Rule["scope"], group: SpeciesGroup): Rule {
  const kind = str(r.kind) as RuleKind;
  return {
    // Unique only within an entry (AGENTS rule 8) — 49 rule_ids collide corpus-wide, so
    // the id carried around is always the pair.
    id: `${str(r.entry_id)}.${str(r.rule_id)}`,
    kind: KINDS.has(kind) ? kind : "note",
    scope,
    group,
    windows: json<Window[]>(r.windows, `rule ${str(r.rule_id)} windows`),
    ...(r.subject == null ? {} : { subject: str(r.subject) }),
    // A rule nobody could place applies to NOTHING. It may only ever raise "unknown";
    // core enforces that, and this is where the flag crosses over from the build.
    ...(Number(r.uncertain) ? { uncertain: true as const } : {}),
  };
}

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

  /** Rules bound to a set of sections, grouped by section. One query, not one per section. */
  const rulesBySection = async (sections: readonly SectionId[], group: SpeciesGroup) => {
    const out = new Map<SectionId, Rule[]>();
    if (sections.length === 0) return out;
    for (const r of await db.all(Q.rulesForScopes("section", sections.length), ...sections)) {
      const id = str(r.scope_id) as SectionId;
      (out.get(id) ?? out.set(id, []).get(id)!).push(toRule(r, "section", group));
    }
    return out;
  };

  return {
    async info(): Promise<BundleInfo> {
      await ready;
      return { version: meta.get("version") ?? "unknown",
               validUntil: meta.get("valid_until") ?? null };
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

    async itemExists(id) { return (await db.get(Q.ITEM, id)) !== undefined; },

    async itemForSection(id) {
      const r = await db.get(Q.ITEM_FOR_SECTION, id);
      return r ? (str(r.item_id) as ItemId) : null;
    },

    async regsForItem(id, on, group): Promise<ItemRegs | null> {
      const item = await db.get(Q.ITEM, id);
      if (!item) return null;
      const sections = (await db.all(Q.SECTIONS_FOR_ITEM, id)).map((r) => str(r.section_id) as SectionId);
      const byScope = await rulesBySection(sections, group);
      const entry = await db.get(Q.ENTRY_FOR_ITEM, id);
      const rules = entry
        ? (await db.all(Q.RULES_FOR_ENTRY, str(entry.entry_id)))
            .map((r) => toRule(r, "section", group))
        : [];
      return {
        item: id,
        name: str(item.name),
        // Mouth -> source is the order the sheet draws them, and section ids sort that way
        // because the measure is distance up the blue line.
        reaches: sections.map((section, seq) => ({
          section, seq, lowerLabel: null, upperLabel: null,
          status: evaluate({ rules: byScope.get(section) ?? [], on, group }),
        })),
        rules,
        area: [],
        // The synopsis paragraph, quoted exactly. The clause offsets are a parser output
        // the bundle does not carry yet, so the panel highlights nothing rather than
        // highlighting the wrong words.
        verbatim: entry ? { text: str(entry.verbatim), clauseStart: 0, clauseLength: 0 } : null,
        // Rules named in this entry that nobody could bind to geometry. SHOWN, never
        // applied — a person should be told a rule exists here even when the app cannot
        // say where it reaches.
        unplaceable: rules.filter((r) => r.uncertain)
          .map((rule) => ({ rule, detail: "no boundary could be resolved for this rule" })),
      };
    },

    async statusFor(ids, on, group): Promise<ReadonlyMap<SectionId, Status>> {
      const byScope = await rulesBySection(ids, group);
      const out = new Map<SectionId, Status>();
      // EVERY id asked about gets an answer, including ones with no rule at all — that is
      // "open under the general rules", which is a real answer and not an absence.
      for (const id of ids)
        out.set(id, evaluate({ rules: byScope.get(id) ?? [], on, group }));
      return out;
    },

    async searchNames(q, limit): Promise<readonly NameHit[]> {
      if (q.trim().length < 2) return [];
      const rows = await db.all(Q.SEARCH, q.trim(), limit * 2);
      // One row per ITEM: an item matched by both its name and an alias is one result.
      const seen = new Map<string, NameHit>();
      for (const r of rows) {
        const item = str(r.item_id);
        if (seen.has(item)) continue;
        seen.set(item, { item: item as ItemId, name: str(r.name),
                         matchedAs: r.matched_as == null ? null : str(r.matched_as),
                         pieces: 0 });
        if (seen.size >= limit) break;
      }
      const hits = [...seen.values()];
      if (hits.length) {
        const counts = await db.all(Q.PIECES.replace("%IDS%", Q.placeholders(hits.length)),
                                    ...hits.map((h) => h.item));
        const n = new Map(counts.map((r) => [str(r.item_id), Number(r.n)]));
        for (const h of hits) (h as { pieces: number }).pieces = n.get(h.item) ?? 0;
      }
      return hits;
    },

    async searchPlaces(q, limit): Promise<readonly PlaceHit[]> {
      if (q.trim().length < 2) return [];
      return (await db.all(Q.SEARCH_PLACES, q.trim(), limit)).map((r) => ({
        place: str(r.place_id) as PlaceId, name: str(r.name), kind: str(r.kind),
      }));
    },

    async watersNear(place): Promise<readonly NearHit[]> {
      // `place` is the integer place_id from `searchPlaces`. Every row joins to a real
      // item — the precompute is keyed on item_id, so there is nothing to invent when a
      // name does not match.
      return (await db.all(Q.WATERS_NEAR, place, 40)).map((r) => ({
        item: str(r.item_id) as ItemId, name: str(r.name), km: Number(r.km),
      })) as NearHit[];
    },

    async gaugeForSection(id): Promise<GaugeLink | null> {
      const r = await db.get(Q.GAUGE_FOR_SECTION, id);
      return link(r, id, (await opts.feed?.live?.()) ?? null);
    },

    async gaugeForItem(id): Promise<GaugeLink | null> {
      // Several candidates, already ordered by how well each speaks for this water. The
      // FEED then breaks the tie: at equal trust a station you can read today beats one
      // that stopped in 2004. Doing it here rather than in SQL is what lets the bundle
      // hold no liveness column at all.
      const rows = await db.all(Q.GAUGE_FOR_ITEM, id);
      if (!rows.length) return null;
      const live = (await opts.feed?.live?.()) ?? null;
      const best = live
        ? rows.find((r) => live.has(str(r.station))) ?? rows[0]!
        : rows[0]!;
      return link(best, str(best.section_id) as SectionId, live);
    },

    async stationsFor(sections): Promise<ReadonlyMap<SectionId, StationId>> {
      const out = new Map<SectionId, StationId>();
      if (!sections.length) return out;
      // Chunked: SQLite's default parameter ceiling is 999, and a dense viewport can hold
      // more reaches than that.
      // 500, not 999: the query binds the section list TWICE — once for streams and once
      // for the lakes union — so the ceiling is halved. Getting this wrong is a runtime
      // "too many SQL variables" on a dense viewport and nowhere else.
      for (let i = 0; i < sections.length; i += 400) {
        const chunk = sections.slice(i, i + 400);
        for (const r of await db.all(Q.gaugesForSections(chunk.length), ...chunk, ...chunk))
          out.set(str(r.section_id) as SectionId, str(r.station) as StationId);
      }
      return out;
    },

    async panelsFor(sections) {
      const out = new Map<SectionId, Panel>();
      if (!sections.length) return out;
      for (let i = 0; i < sections.length; i += 500) {
        const chunk = sections.slice(i, i + 500);
        for (const r of await db.all(Q.panelsForSections(chunk.length), ...chunk)) {
          const sec = str(r.section_id) as SectionId;
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
            years: Number(r.years),
          });
        }
      }
      return out;
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
    async gaugeSeries(station, span, parameter, model): Promise<Aged<Series> | null> {
      const obs = (await opts.feed?.observations?.(station)) ?? null;
      // Which quantity. The caller's choice wins; failing that, the station's own — never
      // a default, because "discharge" is wrong for the 237 stations that never measure it.
      const param: Parameter = parameter ?? obs?.parameter ?? "discharge";
      const clim = await db.all(Q.CLIMATOLOGY, station, param);
      const pentads: (Band | null)[] = Array.from({ length: 73 }, () => null);
      for (const r of clim) {
        const i = Number(r.pentad);
        if (i >= 0 && i < 73 && r.p10 !== null)
          pentads[i] = [Number(r.p10), Number(r.p25), Number(r.p50),
                        Number(r.p75), Number(r.p90)] as Band;
      }
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
        const today = dayOfYear(at.toISOString().slice(0, 10));
        const days = Math.min(daysInYear(year), today + 31);
        const values: (number | null)[] = Array.from({ length: days }, () => null);
        const stamps: string[] = Array.from({ length: days }, (_, i) => dayStamp(year, i));
        for (const [day, lv, q] of obs?.daily ?? []) {
          const i = dayOfYear(day) - 1;
          if (i < 0 || i >= days) continue;
          const v = param === "level" ? lv : q;
          if (v !== null && Number.isFinite(v)) values[i] = v;
        }
        const band = Array.from({ length: days }, (_, i) => bandAt(pentads, i + 1));
        const reading = obs
          ? last(param === "level" ? obs.level : obs.discharge) : null;
        // Prior years land on the SAME day index, so 3 August is above 3 August whatever
        // year it was — which is the only comparison a seasonal chart is making.
        const prior = Object.entries((obs?.priorYears ?? {})[param] ?? {})
          .map(([y, vals]) => ({ year: Number(y), values: vals.slice(0, days) }))
          .filter((p) => Number.isFinite(p.year))
          .sort((a, b) => b.year - a.year);
        return {
          fetchedAt: obs?.fetchedAt ?? Date.now(),
          value: {
            step: "1d", from: `${year}-01-01`, parameter: param,
            at: stamps, values, band,
            now: reading === null ? null : { index: today - 1, value: reading },
            forecast: chosen, forecasts: runs, priorYears: prior,
          },
        };
      }

      if (!obs) return null;
      const values = param === "level" ? obs.level : obs.discharge;
      // Each observation takes the envelope for the day its own timestamp falls in,
      // interpolated between pentads, so a series spanning weeks bends rather than steps.
      const band = obs.at.map((t: string) => bandAt(pentads, dayOfYear(t)));
      const idx = lastIndex(values);
      return {
        fetchedAt: obs.fetchedAt,
        value: {
          step: "1h", from: obs.from, parameter: param,
          at: obs.at, values, band,
          now: idx < 0 ? null : { index: idx, value: values[idx]! },
          forecast: chosen, forecasts: runs, priorYears: [],
        },
      };
    },

    async gaugeParameters(station): Promise<readonly Parameter[]> {
      const rows = await db.all(Q.CLIM_PARAMETERS, station);
      return rows.map((r) => str(r.parameter) as Parameter)
        .filter((p): p is Parameter => p === "discharge" || p === "level");
    },

    async traceToGauge(from): Promise<readonly SectionId[]> {
      // Walk the stored pointers. They exist only inside a gauge's watershed, which is
      // exactly the path this walks — so running out of pointers IS the end of the trace.
      const path: SectionId[] = [from];
      const seen = new Set<string>([from]);
      let at: string = from;
      for (let i = 0; i < 5000; i++) {
        const r = await db.get(Q.DOWN_FROM, at);
        if (!r) break;
        at = str(r.down_id);
        if (seen.has(at)) break;     // a braid that loops is a build defect, not a hang
        seen.add(at);
        path.push(at as SectionId);
      }
      return path;
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
      const places = new Map<string, { name: string; section: string | null;
                                       lon: number | null; lat: number | null }>();
      for (const r of await db.all(Q.gaugePlaces(stations.length), ...stations))
        places.set(str(r.station), {
          name: str(r.name),
          section: r.section_id == null ? null : str(r.section_id),
          lon: num(r.lon), lat: num(r.lat),
        });

      /*
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
      const walk = async (from: string, stop: string): Promise<string[] | null> => {
        const path: string[] = [from];
        const seen = new Set<string>([from]);
        let at = from;
        for (let i = 0; i < 5000 && at !== stop; i++) {
          const r = await db.get(Q.DOWN_FROM, at);
          if (!r) return null;
          at = str(r.down_id);
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
        let path: string[] = [];
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
          role: m.role, path: path as SectionId[],
        });
      }
      return out;
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
  };
}

/**
 * One bundle row -> one GaugeLink, or null.
 *
 * Both gauge accessors funnel through here so a reach and a whole water can never describe
 * the same station differently — the failure the shared-logic rule exists to prevent.
 *
 * Both magnitudes are real and stored: `reach_mag` is this section's, `mag` the gauge
 * node's. Their ratio is what the trust band was cut from, so a sheet can show its working
 * — "this station drains 40x your reach" — rather than asserting a word. A 0 here means
 * the bundler had no magnitude for that node, which the trace model renders as "not known".
 */
function link(r: Row | undefined, section: SectionId | null,
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


/** 1..366 for an ISO date or timestamp. The axis of every seasonal question here. */
function dayOfYear(iso: string): number {
  const y = Number(iso.slice(0, 4));
  const m = Number(iso.slice(5, 7));
  const d = Number(iso.slice(8, 10));
  if (!y || !m || !d) return 1;
  return Math.round((Date.UTC(y, m - 1, d) - Date.UTC(y, 0, 1)) / 86_400_000) + 1;
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

function lastIndex(values: readonly (number | null)[]): number {
  for (let i = values.length - 1; i >= 0; i--)
    if (values[i] !== null && Number.isFinite(values[i]!)) return i;
  return -1;
}

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
import { evaluate, gaugeTrust, type PlainDate, type Rule, type RuleKind, type SpeciesGroup,
         type Status, type Window } from "@app/core";
import type {
  Aged, BundleInfo, GaugeLink, ItemId, ItemRegs, LakeInfo, NameHit, NearHit, PlaceHit,
  PlaceId, Reading, RegsSource, Release, SectionId, Series, StationId,
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
    series(station: StationId, span: "72h" | "year"): Promise<Aged<Series> | null>;
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
      for (let i = 0; i < sections.length; i += 500) {
        const chunk = sections.slice(i, i + 500);
        for (const r of await db.all(Q.gaugesForSections(chunk.length), ...chunk))
          out.set(str(r.section_id) as SectionId, str(r.station) as StationId);
      }
      return out;
    },

    async gaugePoints(): Promise<readonly { station: StationId; name: string;
                                            lon: number; lat: number }[]> {
      return (await db.all(Q.GAUGE_POINTS)).map((r) => ({
        station: str(r.station) as StationId, name: str(r.name),
        lon: Number(r.lon), lat: Number(r.lat),
      }));
    },

    async gaugeNow(station) { return (await opts.feed?.now(station)) ?? null; },
    async gaugeSeries(station, span) { return (await opts.feed?.series(station, span)) ?? null; },

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

export { gaugeTrust };

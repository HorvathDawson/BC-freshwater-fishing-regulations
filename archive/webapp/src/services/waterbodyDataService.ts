/**
 * Waterbody Data Service — tier0.json + /api/resolve
 *
 * Loads tier0.json (enriched search index + regulations) at startup and
 * provides on-demand resolution of tile fids/wbks via the /api/resolve
 * edge API.
 *
 * tier0.json sections:
 *   _shard_version     — "v8"
 *   regulations        — reg_id → {raw_regs, source, match_type?, parsed?, restriction?, ...}
 *   reg_sets           — ["reg1,reg2,...", ...]  dedup'd comma-joined strings
 *   search_index       — [{display_name, name_variants[], segments[], feature_type, regions[], min_zoom, bbox, waterbody_group, zones, management_units, total_length_km}]
 *     segments[]       — [{rid, display_name, name_variants[], feature_type, reg_set_index, watershed_code, min_zoom, regions[], bbox, length_km, waterbody_group, fids[], tributary_reg_ids[]}]
 */

import { DATA_BASE, API_BASE } from '../config/endpoints';

/** Name variant with source provenance (structurally identical to NameVariant in featureUtils) */
export interface NameVariantEntry {
  name: string;
  source: 'direct' | 'tributary' | 'admin' | 'alias' | 'stocking' | 'marker';
}

/** A bathymetry survey depth-map sheet. URL = <R2 base>/bathymetry/<pdf>. */
export interface BathymetrySurvey {
  pdf: string;
  title: string;
}

/** One FIDQ (BC gov Fish Inventories Data Queries) stocking release record. */
export interface StockingRelease {
  release_date: string;
  species_name: string;
  brood_year: string;
  strain_name: string;
  source_name: string;
  origin: string;
  life_stage: string;
  released_quantity: number;
  average_weight: number;
}

/** Static stocking match table (cron/stocking/stocking_matches.json): reach_id /
 * waterbody_key → [waterbody_id]. The frontend joins here, then fetches the
 * per-waterbody release history from cron/stocking/records/<waterbody_id>.json. */
export interface StockingMatches {
  reach: Record<string, string[]>;
  waterbody: Record<string, string[]>;
}

/** A hydrometric gauge station from cron/hydro/stations.json (ECCC Wateroffice
 * + BC River Forecast Centre). The gauge→reach/waterbody link is NOT stored on
 * the station anymore — it lives in the static cron/hydro/gauge_matches.json
 * match table, joined by reach_id/waterbody_key at lookup time. */
export interface GaugeStation {
  id: string;
  name: string;
  lat: number;
  lon: number;
  hyd_status?: string | null;
  drainage_area_km2?: number | null;
  has_climatology?: boolean;
  latest?: {
    discharge?: { value: number | null; ts: string } | null;
    level?: { value: number | null; ts: string } | null;
  };
  condition?: {
    class?: string | null;
    pct_low?: number | null;
    pct_high?: number | null;
    text?: string | null;
    latest_discharge?: number | null;
    latest_stage?: number | null;
  } | null;
}
export interface GaugeStationsDoc {
  generated_at?: string;
  attribution?: string | null;
  forecast_attribution?: string | null;
  count?: number;
  stations: GaugeStation[];
}

/** gofishbc.com "Where To Fish" map marker enrichment — amenity/access
 * whitelist only. Not yet surfaced in any UI — attached to the reach for
 * future display work. */
export interface MarkerInfo {
  more_info?: string;
  description?: string;
  photo_url?: string;
  road_access?: string;
  boat_launch?: string;
  fishing_dock?: string;
  campsite?: string;
  washroom?: string;
  wheelchair_access?: string;
  hike_in_required?: string;
}

// ── Raw JSON shapes ──────────────────────────────────────────────────

/** Raw synopsis regulation from regulation_index.json */
interface SynopsisReg {
  water: string;
  region: string;
  mu: string[];
  raw_regs: string;
  symbols: string[];
  source: 'synopsis';
  page: number;
  image: string;
  match_type?: 'direct' | 'admin' | 'unmatched';
  parsed?: {
    regs_verbatim: string;
    includes_tributaries: boolean;
    tributary_only?: boolean;
    entry_location_text?: string;
    rules: {
      rule_text: string;
      restriction_type: string;
      details: string;
      location_text?: string;
      dates?: string[];
      /** Verbatim "except ..." carve-out that qualifies this one rule.
       *  Empty/undefined when the rule has no exception. */
      exception?: string;
      /** Per-rule tributary override: null/undefined = inherit entry-level
       *  includes_tributaries; true/false = override for this rule only. */
      includes_tributaries?: boolean | null;
    }[];
  };
}

/** Raw base regulation (zone/provincial/municipal/land_access) from regulation_index.json */
interface BaseReg {
  raw_regs: string;
  source: 'zone' | 'provincial' | 'municipal' | 'land_access';
  restriction?: { type: string; details: string };
  zone_ids?: string[];
  dates?: string[];
  scope_location?: string;
  notes?: string;
  zone?: string;
}

type RawRegulation = SynopsisReg | BaseReg;

/** Reach (from reaches section / resolve endpoint) */
export interface Reach {
  display_name: string;
  name_variants: NameVariantEntry[];
  feature_type: string;
  reg_set_index: number;
  watershed_code: string;
  min_zoom: number;
  regions: string[];
  bbox: [number, number, number, number] | null;
  length_km: number;
  tributary_reg_ids?: string[];
  /** Bathymetry survey depth-map PDFs (polygon reaches only). */
  bathymetry?: BathymetrySurvey[];
  /** gofishbc map marker amenity/access info (polygon reaches only). */
  marker?: MarkerInfo;
}

/** Search index entry (tier0 enriched format with segments) */
export interface SearchEntry {
  display_name: string;
  name_variants: NameVariantEntry[];
  segments: Tier0Segment[];
  feature_type: string;
  regions: string[];
  min_zoom: number;
  bbox: [number, number, number, number] | null;
  waterbody_group: string;
  zones: string[];
  management_units: string[];
  total_length_km: number;
  /** Towns nearest this feature (search-index only). New builds carry
   *  ``{name, km}`` records; legacy/cached indexes may still be plain strings. */
  nearby_towns?: (string | { name: string; km?: number })[];
}

/** Enriched segment from tier0 search_index */
export interface Tier0Segment {
  rid: string;
  display_name: string;
  name_variants: NameVariantEntry[];
  feature_type: string;
  reg_set_index: number;
  watershed_code: string;
  min_zoom: number;
  regions: string[];
  bbox: [number, number, number, number] | null;
  length_km: number;
  waterbody_group: string;
  fids: string[];
  tributary_reg_ids?: string[];
}

// ── Output types (consumed by Map.tsx, InfoPanel, SearchBar) ─────────

/** Provenance label: how a regulation reached a specific reach */
export type RegulationProvenance = 'direct' | 'tributary' | 'zone' | 'provincial';

/** Flat regulation shape matching what InfoPanel expects */
export interface Regulation {
  regulation_id: string;
  waterbody_name: string;
  region: string | null;
  management_units: string[];
  rule_text: string;
  restriction_type: string;
  restriction_details: string;
  dates: string[] | string | { period: string } | null;
  scope_type: string;
  scope_location: string | null;
  /** Verbatim "except ..." carve-out for this rule, shown inline in the UI so
   *  each rule card is self-contained.  Empty/undefined when none. */
  restriction_exception?: string;
  source: 'synopsis' | 'provincial' | 'zone' | 'municipal' | 'land_access';
  zone_ids?: string[];
  feature_types?: string[] | null;
  iid?: string;
  source_image?: string | null;
  source_page?: number | null;
  /** Full verbatim official text of the synopsis entry (all rules), shown as
   *  the "Official text" source block.  Undefined for base regs. */
  raw_regs?: string | null;
  exclusions?: { lookup_name: string; direction?: string; landmark_verbatim?: string; includes_tributaries?: boolean }[] | null;
  /** How this regulation reached the current reach.
   *  Stamped by regulationsService when resolving for a specific reach. */
  provenance?: RegulationProvenance;
  /** Effective tributary scope of this rule, resolved at expansion time:
   *  the rule's own includes_tributaries if set, else the entry-level flag.
   *  Used by regulationsService to drop mainstem-only rules on tributary
   *  reaches.  Undefined for base regs (never tributary-filtered). */
  effective_includes_tributaries?: boolean;
}

// ── Admin visibility config (from admin_visibility.json) ─────────────

/** Per-layer visibility config from enrichment pipeline */
export interface AdminLayerVisibility {
  display: 'all' | 'regulated_only';
  regulated_ids?: string[];
}

/** Full admin visibility map: tile_layer_name → config */
export type AdminVisibility = Record<string, AdminLayerVisibility>;

// ── Layer manifest (from layer_manifest.json) ────────────────────────

/** Per-layer menu config from the tiles pipeline — the SOURCE OF TRUTH for which
 * layers are user-toggleable. `toggleable` marks a layer as controllable in the
 * layer menu; `visible` is its default visibility (the checkbox's initial state);
 * `label` is the menu row text. The frontend supplies only the style-layer
 * mapping (see LAYER_STYLE_MAP in Map.tsx). */
/** Flat appearance knobs the frontend applies on top of its base style-layer
 *  definitions (see LAYER_STYLE_MAP / applyManifestStyle in Map.tsx). Only FLAT
 *  values live here — data-driven (match/interpolate) appearance stays in code
 *  and is omitted from the manifest. Any omitted key leaves the code default. */
export interface LayerManifestStyle {
  fill_color?: string;
  line_color?: string;
  fill_opacity?: number;
  line_opacity?: number;
  line_width?: number;
  line_dash?: number[];
  /** Zoom the polygon border/line fades in (applied via setLayerZoomRange). */
  border_minzoom?: number;
}
/** One data-source attribution link shown in a layer's settings popup. */
export interface LayerManifestSource {
  name: string;
  url: string;
}
export interface LayerManifestEntry {
  label?: string;
  type?: string;
  visible?: boolean;
  toggleable?: boolean;
  style?: LayerManifestStyle;
  /** Tile-generation-only floor (consumed by the pipeline, not the frontend). */
  min_tile_zoom?: number;
  /** Plain-language blurb shown in the layer's settings popup (About). */
  description?: string;
  /** Ordered data-source attribution links (settings popup → Data sources). */
  sources?: LayerManifestSource[];
  /** List-view grouping + order key. Omitted → "Other". */
  group?: 'water' | 'boundaries' | 'land' | 'roads' | 'points';
}
export type LayerManifest = Record<string, LayerManifestEntry>;

// ── In-season changes (from in_season.json) ──────────────────────────

/** A single scraped in-season regulation change resolved to reach IDs */
export interface InSeasonChange {
  water: string;
  region: string;
  change: string;
  effective_date: string;
  reach_ids: string[];
  match_status: string;
}

/** Loaded + decoded tier0 data */
export interface RegulationData {
  /** Expanded per-rule regulations keyed by synthetic rule ID */
  regulations: Record<string, Regulation>;
  /** Deduplicated regulation-ID strings (expanded with rule suffixes) */
  reg_sets: string[];
  /** Reach metadata keyed by reach_id (built from tier0 segments, extended by /api/resolve) */
  reaches: Record<string, Reach>;
  /** reach_id → [fid, ...] (for highlighting; from tier0 segments, extended by /api/resolve) */
  reachSegments: Record<string, string[]>;
  /** reach_ids whose full metadata (incl. bathymetry) was loaded from a shard,
   *  not just the lightweight tier0 search_index segment. */
  hydratedReaches: Set<string>;
  /** Tier0-enriched search entries (with segments instead of reach IDs) */
  searchIndex: SearchEntry[];
  /** Original reg_set strings (before rule expansion) */
  rawRegSets: string[];
  /** Admin layer visibility config (which layers show all vs. regulated only) */
  adminVisibility: AdminVisibility;
  /** reach_id → in-season changes affecting that reach */
  inSeasonIndex: Map<string, InSeasonChange[]>;
  /** ISO timestamp of when in-season data was last scraped */
  inSeasonScrapedAt: string;
  /** Source URL for the in-season changes page */
  inSeasonSourceUrl: string;
}

// ── Regulation expansion ─────────────────────────────────────────────

/**
 * Expand V2 regulations into flat per-rule Regulation objects.
 *
 * Synopsis regs with parsed.rules[] are expanded: one Regulation per rule,
 * keyed as `{reg_id}_rule{N}`.  Synopsis regs without parsing keep raw_regs
 * as a single Regulation.
 *
 * Base regs (zone/provincial) map directly to one Regulation each.
 *
 * Also rewrites reg_sets to reference the expanded rule IDs.
 */
function expandRegulations(
  rawRegs: Record<string, RawRegulation>,
  rawRegSets: string[],
): { regulations: Record<string, Regulation>; regSets: string[] } {
  const regulations: Record<string, Regulation> = {};
  const expansionMap: Record<string, string[]> = {};

  for (const [regId, raw] of Object.entries(rawRegs)) {
    if (raw.source === 'synopsis') {
      const syn = raw as SynopsisReg;
      if (syn.parsed?.rules?.length) {
        const entryIncludesTribs = syn.parsed.includes_tributaries === true;
        const ruleIds: string[] = [];
        for (let i = 0; i < syn.parsed.rules.length; i++) {
          const rule = syn.parsed.rules[i];
          const ruleId = `${regId}_rule${i}`;
          ruleIds.push(ruleId);
          // Effective tributary scope = rule override if set, else entry flag.
          const effectiveIncludesTribs =
            rule.includes_tributaries == null
              ? entryIncludesTribs
              : rule.includes_tributaries === true;
          regulations[ruleId] = {
            regulation_id: ruleId,
            waterbody_name: syn.water || '',
            region: syn.region || null,
            management_units: syn.mu || [],
            rule_text: rule.rule_text,
            restriction_type: rule.restriction_type,
            restriction_details: rule.details || '',
            dates: rule.dates?.length ? rule.dates : null,
            scope_type: rule.location_text ? 'location' : 'waterbody',
            scope_location: rule.location_text || null,
            restriction_exception: rule.exception || undefined,
            source: 'synopsis',
            iid: regId,
            source_image: syn.image || null,
            source_page: syn.page ?? null,
            raw_regs: syn.raw_regs || null,
            exclusions: null,
            effective_includes_tributaries: effectiveIncludesTribs,
          };
        }
        expansionMap[regId] = ruleIds;
      } else {
        // Unparsed synopsis — show raw_regs as single entry.  With no parsed
        // rules there is no per-rule scope, so treat it as tributary-applicable
        // (inherit) and never filter it out on tributary reaches.
        regulations[regId] = {
          regulation_id: regId,
          waterbody_name: syn.water || '',
          region: syn.region || null,
          management_units: syn.mu || [],
          rule_text: syn.raw_regs || '',
          restriction_type: 'notice',
          restriction_details: syn.raw_regs || '',
          dates: null,
          scope_type: 'waterbody',
          scope_location: null,
          source: 'synopsis',
          iid: regId,
          source_image: syn.image || null,
          source_page: syn.page ?? null,
          exclusions: null,
          effective_includes_tributaries: true,
        };
        expansionMap[regId] = [regId];
      }
    } else {
      const base = raw as BaseReg;
      regulations[regId] = {
        regulation_id: regId,
        waterbody_name: '',
        region: null,
        management_units: [],
        rule_text: base.raw_regs || '',
        restriction_type: base.restriction?.type?.toLowerCase() || 'notice',
        restriction_details: base.restriction?.details || '',
        dates: base.dates?.length ? base.dates : null,
        scope_type: base.scope_location ? 'location' : 'waterbody',
        scope_location: base.scope_location || null,
        source: base.source,
        zone_ids: base.zone_ids,
        exclusions: null,
      };
      expansionMap[regId] = [regId];
    }
  }

  // Rewrite reg_sets to reference expanded rule IDs
  const regSets = rawRegSets.map(rsStr => {
    const origIds = rsStr.split(',').filter(Boolean);
    const expanded = origIds.flatMap(id => expansionMap[id] || [id]);
    return expanded.join(',');
  });

  return { regulations, regSets };
}


// ── In-season index builder ──────────────────────────────────────────

/** Build reach_id → InSeasonChange[] index from raw in_season.json data. */
function buildInSeasonIndex(
  raw: { scraped_at?: string; source_url?: string; changes?: any[] },
): { index: Map<string, InSeasonChange[]>; scrapedAt: string; sourceUrl: string } {
  const index = new Map<string, InSeasonChange[]>();
  const scrapedAt = raw.scraped_at || '';
  const sourceUrl = raw.source_url || '';

  for (const c of (raw.changes || [])) {
    if (c.match_status !== 'matched' || !c.reach_ids?.length) continue;
    const change: InSeasonChange = {
      water: c.water || '',
      region: c.region || '',
      change: c.change || '',
      effective_date: c.effective_date || '',
      reach_ids: c.reach_ids,
      match_status: c.match_status,
    };
    for (const rid of change.reach_ids) {
      const existing = index.get(rid);
      if (existing) {
        existing.push(change);
      } else {
        index.set(rid, [change]);
      }
    }
  }

  return { index, scrapedAt, sourceUrl };
}

// ── IndexedDB helpers ────────────────────────────────────────────────
const IDB_NAME = 'waterbody_cache';
const IDB_STORE = 'kv';
const IDB_VERSION = 6;  // bumped for tier0 format (no segmentIndex/polyIndex)

function openCacheDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, IDB_VERSION);
    req.onupgradeneeded = (event) => {
      const db = req.result;
      if (!db.objectStoreNames.contains(IDB_STORE)) {
        db.createObjectStore(IDB_STORE);
      } else if ((event.oldVersion || 0) < 6) {
        // Wipe stale v5 cache — schema changed (no segmentIndex/polyIndex)
        const tx = (event.target as IDBOpenDBRequest).transaction!;
        tx.objectStore(IDB_STORE).clear();
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function idbGet<T>(key: string): Promise<T | undefined> {
  const db = await openCacheDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, 'readonly');
    const req = tx.objectStore(IDB_STORE).get(key);
    req.onsuccess = () => resolve(req.result as T | undefined);
    req.onerror = () => reject(req.error);
  });
}

async function idbSet(key: string, value: unknown): Promise<void> {
  const db = await openCacheDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, 'readwrite');
    tx.objectStore(IDB_STORE).put(value, key);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}


// ── Resolve API types ────────────────────────────────────────────────

export interface ResolveResult {
  reach_id: string;
  reach: Omit<Reach, never>;
  fids: string[];
  matched_fids: string[];
  matched_wbks: string[];
}

// ── Service ──────────────────────────────────────────────────────────

class WaterbodyDataService {
  private data: RegulationData | null = null;
  private loadPromise: Promise<RegulationData> | null = null;
  private dataVersionPromise: Promise<string> | null = null;

  /** /data endpoint for file fetches (tier0.json, pmtiles, admin_visibility) */
  private static readonly DATA_BASE = DATA_BASE;
  /** API endpoint for /api/resolve, /api/version */
  private static readonly API_BASE = API_BASE;

  private static readonly ETAG_KEY = 'tier0_etag';
  private static readonly DATA_KEY = 'tier0_data';
  private static readonly VERSION_KEY = 'data_version';

  // Resolve cache: keyed by "f{fid}" or "w{wbk}" → ResolveResult
  private resolveCache = new Map<string, ResolveResult>();
  private static readonly CACHE_CAP = 5000;

  // Stocking: reach_id/waterbody_key → [waterbody_id] match table (small, static;
  // cron/stocking/stocking_matches.json, produced by the full pipeline). Per-waterbody
  // release history lives in cron/stocking/records/<id>.json, fetched on demand and
  // cached. Replaces the old ~22 MB whole-file stocking.json load.
  private stockingMatches: StockingMatches | null = null;
  private stockingMatchesPromise: Promise<StockingMatches> | null = null;
  private stockingRecordCache = new Map<string, StockingRelease[]>();

  // Gauge station indexes. stations.json is the roster (id → GaugeStation); the
  // gauge→reach/waterbody link comes from the static gauge_matches.json match table
  // (reach_id/waterbody_key → [station_id]). Both are small (~450 stations) and lazily
  // loaded on the first Gauges-tab open / gauge interaction, degrading to empty if
  // absent (hydro seed may not have shipped).
  private gaugeStations: GaugeStation[] | null = null;
  private gaugeById: Map<string, GaugeStation> | null = null;
  private gaugeByReach: Map<string, GaugeStation[]> | null = null;
  private gaugeByWbk: Map<string, GaugeStation[]> | null = null;
  private gaugeDoc: GaugeStationsDoc | null = null;
  private gaugeIndexPromise: Promise<void> | null = null;
  // Per-station artifact caches (recent/history/climatology), fetched on demand.
  private gaugeArtifactCache = new Map<string, unknown>();

  // Layer menu config (which layers are toggleable + label + default visibility),
  // fetched once from layer_manifest.json.
  private layerManifest: LayerManifest | null = null;
  private layerManifestPromise: Promise<LayerManifest> | null = null;

  async load(): Promise<RegulationData> {
    if (this.data) return this.data;
    if (this.loadPromise) return this.loadPromise;
    this.loadPromise = this._load();
    return this.loadPromise;
  }

  private async _load(): Promise<RegulationData> {
    const url = `${WaterbodyDataService.DATA_BASE}/tier0.json`;

    try {
      const cachedEtag = await idbGet<string>(WaterbodyDataService.ETAG_KEY).catch(() => undefined);
      const cachedData = cachedEtag
        ? await idbGet<RegulationData>(WaterbodyDataService.DATA_KEY).catch(() => undefined)
        : undefined;

      let response: Response;

      if (cachedEtag && cachedData) {
        response = await fetch(url, { headers: { 'If-None-Match': cachedEtag } });
      } else {
        const earlyFetch = (window as any).__earlyFetch as Promise<Response | null> | undefined;
        if (earlyFetch) {
          delete (window as any).__earlyFetch;
          const earlyResp = await earlyFetch;
          response = earlyResp || await fetch(url);
        } else {
          response = await fetch(url);
        }
      }

      // Always fetch admin visibility config — it's small and not cached
      // by etag, so fetch it fresh on every load to stay in sync.
      const adminVisUrl = `${WaterbodyDataService.DATA_BASE}/admin_visibility.json`;
      const adminVis: AdminVisibility = await fetch(adminVisUrl)
        .then(r => {
          if (!r.ok) {
            console.warn(`⚠️ admin_visibility.json returned ${r.status} — admin layers unfiltered`);
            return {};
          }
          return r.json();
        })
        .catch(() => {
          console.warn('⚠️ Failed to fetch admin_visibility.json — admin layers unfiltered');
          return {};
        });

      // Fetch in-season changes (fresh every load, like admin_visibility).
      // Gracefully degrades to empty if file doesn't exist or fetch fails.
      const inSeasonUrl = `${WaterbodyDataService.DATA_BASE}/cron/in-season/in_season.json`;
      const inSeasonRaw: { scraped_at?: string; source_url?: string; changes?: any[] } = await fetch(inSeasonUrl)
        .then(r => {
          if (!r.ok) {
            console.warn(`⚠️ in_season.json returned ${r.status} — in-season notices unavailable`);
            return { changes: [] };
          }
          return r.json();
        })
        .catch(() => {
          console.warn('⚠️ Failed to fetch in_season.json — in-season notices unavailable');
          return { changes: [] };
        });

      if (response.status === 304 && cachedData) {
        cachedData.adminVisibility = adminVis;
        const { index, scrapedAt, sourceUrl } = buildInSeasonIndex(inSeasonRaw);
        cachedData.inSeasonIndex = index;
        cachedData.inSeasonScrapedAt = scrapedAt;
        cachedData.inSeasonSourceUrl = sourceUrl;
        // Hydration state is per-session and never persisted — reaches loaded
        // from cache start un-hydrated (no bathymetry) until a shard fetch runs.
        cachedData.hydratedReaches = new Set<string>();
        this.data = cachedData;
        this.loadPromise = null;
        console.log(`✅ tier0 loaded from cache (304)`);
        return cachedData;
      }

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
      }

      const raw = await response.json();
      const data = this._decode(raw, adminVis, inSeasonRaw);
      this.data = data;
      this.loadPromise = null;

      const etag = response.headers.get('ETag') || '';
      if (etag) {
        try {
          await idbSet(WaterbodyDataService.ETAG_KEY, etag);
          await idbSet(WaterbodyDataService.DATA_KEY, data);
        } catch {
          console.warn('IndexedDB write failed, cache disabled for this session');
        }
      }

      console.log(
        `✅ tier0 loaded: ${Object.keys(data.regulations).length} regulations, ` +
        `${Object.keys(data.reaches).length} reaches, ` +
        `${data.searchIndex.length} search entries`
      );
      return data;

    } catch (error) {
      this.loadPromise = null;
      console.error('❌ Failed to load tier0.json:', error);
      throw error;
    }
  }

  private _decode(
    raw: any,
    adminVisibility: AdminVisibility = {},
    inSeasonRaw: { scraped_at?: string; source_url?: string; changes?: any[] } = {},
  ): RegulationData {
    const rawRegSets: string[] = raw.reg_sets || [];
    const rawRegs: Record<string, RawRegulation> = raw.regulations || {};
    const searchEntries: SearchEntry[] = raw.search_index || [];

    const { regulations, regSets } = expandRegulations(rawRegs, rawRegSets);

    // Build reaches and reachSegments from enriched search_index segments.
    // These are lightweight (no bathymetry) — see hydratedReaches below.
    const reaches: Record<string, Reach> = {};
    const reachSegments: Record<string, string[]> = {};
    // reach_ids whose full shard metadata (incl. bathymetry) has been loaded.
    const hydratedReaches = new Set<string>();

    for (const entry of searchEntries) {
      for (const seg of (entry.segments || [])) {
        reaches[seg.rid] = {
          display_name: seg.display_name,
          name_variants: seg.name_variants || [],
          feature_type: seg.feature_type || 'stream',
          reg_set_index: seg.reg_set_index,
          watershed_code: seg.watershed_code || '',
          min_zoom: seg.min_zoom || 11,
          regions: seg.regions || [],
          bbox: seg.bbox,
          length_km: seg.length_km || 0,
          tributary_reg_ids: seg.tributary_reg_ids || [],
        };
        if (seg.fids?.length) {
          reachSegments[seg.rid] = seg.fids;
        }
      }
    }

    const { index: inSeasonIndex, scrapedAt: inSeasonScrapedAt, sourceUrl: inSeasonSourceUrl } =
      buildInSeasonIndex(inSeasonRaw);

    return {
      regulations,
      reg_sets: regSets,
      reaches,
      reachSegments,
      hydratedReaches,
      searchIndex: searchEntries,
      rawRegSets,
      adminVisibility,
      inSeasonIndex,
      inSeasonScrapedAt,
      inSeasonSourceUrl,
    };
  }

  /**
   * Resolve tile fids and/or waterbody keys to reach data via /api/resolve.
   * Uses a local cache to make repeat clicks instant.
   * Populates data.reaches and data.reachSegments for unnamed feature fallback.
   */
  async resolve(fids: string[], wbks: string[]): Promise<ResolveResult[]> {
    const results: ResolveResult[] = [];
    const missedFids: string[] = [];
    const missedWbks: string[] = [];

    for (const fid of fids) {
      const hit = this.resolveCache.get(`f${fid}`);
      if (hit) results.push(hit);
      else missedFids.push(fid);
    }
    for (const wbk of wbks) {
      const hit = this.resolveCache.get(`w${wbk}`);
      if (hit) results.push(hit);
      else missedWbks.push(wbk);
    }

    if (!missedFids.length && !missedWbks.length) return results;

    // Worker limits to 20 fids/wbks per request — batch to stay under.
    const BATCH = 20;
    const fetches: Promise<ResolveResult[]>[] = [];

    for (let i = 0; i < missedFids.length || i < missedWbks.length; i += BATCH) {
      const fidSlice = missedFids.slice(i, i + BATCH);
      const wbkSlice = missedWbks.slice(i, i + BATCH);
      if (!fidSlice.length && !wbkSlice.length) break;

      const params = new URLSearchParams();
      if (fidSlice.length) params.set('fids', fidSlice.join(','));
      if (wbkSlice.length) params.set('wbks', wbkSlice.join(','));

      fetches.push(
        fetch(`${WaterbodyDataService.API_BASE}/api/resolve?${params}`).then(async resp => {
          if (!resp.ok && resp.status !== 404) {
            throw new Error(`Resolve API error: ${resp.status}`);
          }
          const json = await resp.json() as { results: ResolveResult[] };
          return json.results || [];
        })
      );
    }

    const batches = await Promise.all(fetches);
    const apiResults = batches.flat();

    // Deduplicate by reach_id
    const seen = new Set(results.map(r => r.reach_id));

    for (const r of apiResults) {
      if (seen.has(r.reach_id)) continue;
      seen.add(r.reach_id);
      results.push(r);

      // Cache by matched input IDs
      for (const fid of (r.matched_fids || [])) {
        this.resolveCache.set(`f${fid}`, r);
      }
      for (const wbk of (r.matched_wbks || [])) {
        this.resolveCache.set(`w${wbk}`, r);
      }

      // Inject into data for use by click handler fallback paths
      if (this.data) {
        this.data.reaches[r.reach_id] = r.reach as Reach;
        this.data.hydratedReaches.add(r.reach_id);
        if (r.fids?.length) {
          this.data.reachSegments[r.reach_id] = r.fids;
        }
      }
    }

    // Prune cache if oversized
    if (this.resolveCache.size > WaterbodyDataService.CACHE_CAP) {
      const keep = [...this.resolveCache.entries()].slice(-3000);
      this.resolveCache = new Map(keep);
    }

    return results;
  }

  /**
   * Resolve a reach directly by its reach_id via /api/resolve?rids=.
   * Used for deep link restoration when the reach isn't in the search index.
   * Returns the ResolveResult or null if not found.
   */
  async resolveByReachId(reachId: string): Promise<ResolveResult | null> {
    // Return the cached reach only if it was hydrated from a shard (so it
    // carries bathymetry etc.). Lightweight tier0 search_index reaches are not
    // hydrated, so fall through to the /api/resolve?rids= fetch for those.
    if (this.data?.reaches[reachId] && this.data.hydratedReaches.has(reachId)) {
      return {
        reach_id: reachId,
        reach: this.data.reaches[reachId],
        fids: this.data.reachSegments[reachId] || [],
        matched_fids: [],
        matched_wbks: [],
      };
    }

    try {
      const params = new URLSearchParams();
      params.set('rids', reachId);
      const resp = await fetch(`${WaterbodyDataService.API_BASE}/api/resolve?${params}`);
      if (!resp.ok) return null;
      const json = await resp.json() as { results: ResolveResult[] };
      const results = json.results || [];
      if (results.length === 0) return null;

      const r = results[0];
      // Inject into data for reuse
      if (this.data) {
        this.data.reaches[r.reach_id] = r.reach as Reach;
        this.data.hydratedReaches.add(r.reach_id);
        if (r.fids?.length) {
          this.data.reachSegments[r.reach_id] = r.fids;
        }
      }
      return r;
    } catch {
      return null;
    }
  }

  async getRegulations(): Promise<Record<string, Regulation>> {
    const data = await this.load();
    return data.regulations;
  }

  /** Get in-season changes for a specific reach (synchronous — data must be loaded). */
  getInSeasonChanges(reachId: string): InSeasonChange[] {
    return this.data?.inSeasonIndex.get(reachId) || [];
  }

  /** Get FIDQ stocking release history for a reach (and/or waterbody_key).
   *  Loads the small static match table on first call, then fetches only the
   *  per-waterbody record files the reach maps to (cached). */
  async getStockingReleases(reachId: string, waterbodyKey?: string | null): Promise<StockingRelease[]> {
    const matches = await this._ensureStockingMatches();
    const ids = new Set<string>();
    for (const id of matches.reach[reachId] || []) ids.add(id);
    if (waterbodyKey) for (const id of matches.waterbody[waterbodyKey] || []) ids.add(id);
    if (!ids.size) return [];
    const out: StockingRelease[] = [];
    await Promise.all([...ids].map(async (id) => {
      out.push(...await this._stockingRecord(id));
    }));
    return out;
  }

  private async _ensureStockingMatches(): Promise<StockingMatches> {
    if (this.stockingMatches) return this.stockingMatches;
    if (!this.stockingMatchesPromise) this.stockingMatchesPromise = this._loadStockingMatches();
    this.stockingMatches = await this.stockingMatchesPromise;
    return this.stockingMatches;
  }

  private async _loadStockingMatches(): Promise<StockingMatches> {
    const url = `${WaterbodyDataService.DATA_BASE}/cron/stocking/stocking_matches.json`;
    try {
      const resp = await fetch(url);
      if (!resp.ok) {
        console.warn(`⚠️ stocking_matches.json returned ${resp.status} — stocking info unavailable`);
        return { reach: {}, waterbody: {} };
      }
      const doc = (await resp.json()) as Partial<StockingMatches>;
      return { reach: doc.reach || {}, waterbody: doc.waterbody || {} };
    } catch {
      console.warn('⚠️ Failed to fetch stocking_matches.json — stocking info unavailable');
      return { reach: {}, waterbody: {} };
    }
  }

  private async _stockingRecord(waterbodyId: string): Promise<StockingRelease[]> {
    const cached = this.stockingRecordCache.get(waterbodyId);
    if (cached) return cached;
    const url = `${WaterbodyDataService.DATA_BASE}/cron/stocking/records/${waterbodyId}.json`;
    let releases: StockingRelease[] = [];
    try {
      const resp = await fetch(url);
      if (resp.ok) {
        const doc = (await resp.json()) as { releases?: StockingRelease[] };
        releases = doc.releases || [];
      }
    } catch {
      // network error — degrade to no releases for this waterbody
    }
    this.stockingRecordCache.set(waterbodyId, releases);
    return releases;
  }

  // ── Hydro gauge stations ────────────────────────────────────────────────

  private async _loadGaugeIndex(): Promise<void> {
    const base = WaterbodyDataService.DATA_BASE;
    try {
      // Roster (id → station) + the static reach/waterbody → station_id match table.
      const [rosterResp, matchResp] = await Promise.all([
        fetch(`${base}/cron/hydro/stations.json`),
        fetch(`${base}/cron/hydro/gauge_matches.json`),
      ]);
      if (!rosterResp.ok) {
        console.warn(`⚠️ stations.json returned ${rosterResp.status} — gauge data unavailable`);
        this._emptyGaugeIndex();
        return;
      }
      const doc: GaugeStationsDoc = await rosterResp.json();
      const byId = new Map<string, GaugeStation>();
      for (const st of doc.stations || []) byId.set(st.id, st);

      const matches: { reach?: Record<string, string[]>; waterbody?: Record<string, string[]> } =
        matchResp.ok ? await matchResp.json() : {};
      const toStations = (ids?: string[]) =>
        (ids || []).map((id) => byId.get(id)).filter((s): s is GaugeStation => !!s);
      const byReach = new Map<string, GaugeStation[]>();
      for (const [rid, ids] of Object.entries(matches.reach || {})) byReach.set(rid, toStations(ids));
      const byWbk = new Map<string, GaugeStation[]>();
      for (const [wbk, ids] of Object.entries(matches.waterbody || {})) byWbk.set(wbk, toStations(ids));

      this.gaugeDoc = doc;
      this.gaugeStations = doc.stations || [];
      this.gaugeById = byId;
      this.gaugeByReach = byReach;
      this.gaugeByWbk = byWbk;
    } catch {
      console.warn('⚠️ Failed to fetch gauge index — gauge data unavailable');
      this._emptyGaugeIndex();
    }
  }

  private _emptyGaugeIndex(): void {
    this.gaugeStations = [];
    this.gaugeById = new Map();
    this.gaugeByReach = new Map();
    this.gaugeByWbk = new Map();
  }

  private async _ensureGaugeIndex(): Promise<void> {
    if (this.gaugeStations) return;
    if (!this.gaugeIndexPromise) this.gaugeIndexPromise = this._loadGaugeIndex();
    await this.gaugeIndexPromise;
  }

  /** Gauge station(s) whose FWA link matches this reach and/or waterbody. Lazily
   *  loads stations.json on first call; returns [] if the file is unavailable. */
  async getStationsForReach(
    reachId?: string | null,
    waterbodyKey?: string | null,
  ): Promise<GaugeStation[]> {
    await this._ensureGaugeIndex();
    const out: GaugeStation[] = [];
    const seen = new Set<string>();
    const push = (arr?: GaugeStation[]) => {
      for (const s of arr || []) {
        if (!seen.has(s.id)) { seen.add(s.id); out.push(s); }
      }
    };
    if (reachId) push(this.gaugeByReach?.get(reachId));
    if (waterbodyKey) push(this.gaugeByWbk?.get(waterbodyKey));
    return out;
  }

  /** All gauge stations (for the map layer). Lazily loaded; [] if unavailable. */
  async getAllStations(): Promise<GaugeStation[]> {
    await this._ensureGaugeIndex();
    return this.gaugeStations || [];
  }

  /** Layer menu config keyed by tile layer (which layers are toggleable, their
   *  label + default visibility). Lazily fetched from layer_manifest.json; {} if
   *  unavailable. The pipeline is the source of truth; the frontend maps each
   *  toggleable key → its MapLibre style layers. */
  async getLayerManifest(): Promise<LayerManifest> {
    if (this.layerManifest) return this.layerManifest;
    if (!this.layerManifestPromise) {
      const url = `${WaterbodyDataService.DATA_BASE}/layer_manifest.json`;
      this.layerManifestPromise = fetch(url)
        .then((r) => (r.ok ? (r.json() as Promise<LayerManifest>) : {}))
        .catch(() => {
          console.warn('⚠️ Failed to fetch layer_manifest.json — no layer toggles');
          return {} as LayerManifest;
        });
    }
    this.layerManifest = await this.layerManifestPromise;
    return this.layerManifest;
  }

  /** A single gauge station by id (e.g. a gauge map-icon click). Lazily loads the
   *  index; null if unknown. Map points come from cron/hydro/gauges.geojson. */
  async getStationById(id: string): Promise<GaugeStation | null> {
    await this._ensureGaugeIndex();
    return this.gaugeById?.get(id) ?? null;
  }

  /** Gauge map points enriched with their reach_id/waterbody_key (resolved from
   *  the static match table), for the map's gauge-points source. A gauge can link
   *  to multiple reaches; the first is used for the click→reach association. */
  async getGaugePoints(): Promise<Array<{
    id: string; name: string; lon: number; lat: number;
    reach_id: string; waterbody_key: string;
  }>> {
    await this._ensureGaugeIndex();
    const linkByStation = new Map<string, { reach_id?: string; waterbody_key?: string }>();
    for (const [rid, arr] of this.gaugeByReach || []) {
      for (const s of arr) {
        const e = linkByStation.get(s.id) || {};
        if (!e.reach_id) e.reach_id = rid;
        linkByStation.set(s.id, e);
      }
    }
    for (const [wbk, arr] of this.gaugeByWbk || []) {
      for (const s of arr) {
        const e = linkByStation.get(s.id) || {};
        if (!e.waterbody_key) e.waterbody_key = wbk;
        linkByStation.set(s.id, e);
      }
    }
    return (this.gaugeStations || [])
      .filter((s) => Number.isFinite(s.lon) && Number.isFinite(s.lat))
      .map((s) => ({
        id: s.id,
        name: s.name || '',
        lon: s.lon,
        lat: s.lat,
        reach_id: linkByStation.get(s.id)?.reach_id || '',
        waterbody_key: linkByStation.get(s.id)?.waterbody_key || '',
      }));
  }

  /** Required attribution strings for gauge data (ECCC + BC RFC). */
  getGaugeAttribution(): { data: string | null; forecast: string | null } {
    return {
      data: this.gaugeDoc?.attribution ?? null,
      forecast: this.gaugeDoc?.forecast_attribution ?? null,
    };
  }

  private async _gaugeArtifact<T>(kind: 'recent' | 'history' | 'climatology', id: string): Promise<T | null> {
    const cacheKey = `${kind}/${id}`;
    if (this.gaugeArtifactCache.has(cacheKey)) {
      return this.gaugeArtifactCache.get(cacheKey) as T | null;
    }
    const url = `${WaterbodyDataService.DATA_BASE}/cron/hydro/${kind}/${id}.json`;
    try {
      const resp = await fetch(url);
      if (!resp.ok) {
        // climatology is legitimately absent for short-record stations (404)
        this.gaugeArtifactCache.set(cacheKey, null);
        return null;
      }
      const doc = (await resp.json()) as T;
      this.gaugeArtifactCache.set(cacheKey, doc);
      return doc;
    } catch {
      this.gaugeArtifactCache.set(cacheKey, null);
      return null;
    }
  }

  getStationRecent<T = unknown>(id: string): Promise<T | null> {
    return this._gaugeArtifact<T>('recent', id);
  }
  getStationHistory<T = unknown>(id: string): Promise<T | null> {
    return this._gaugeArtifact<T>('history', id);
  }
  getStationClimatology<T = unknown>(id: string): Promise<T | null> {
    return this._gaugeArtifact<T>('climatology', id);
  }

  /** Get in-season metadata (synchronous — data must be loaded). */
  getInSeasonMeta(): { scrapedAt: string; sourceUrl: string } {
    return {
      scrapedAt: this.data?.inSeasonScrapedAt || '',
      sourceUrl: this.data?.inSeasonSourceUrl || '',
    };
  }

  /** Absolute URL for a bathymetry survey PDF served from the data bucket. */
  bathymetryUrl(pdf: string): string {
    return `${WaterbodyDataService.DATA_BASE}/bathymetry/${pdf}`;
  }

  getDataVersion(): Promise<string> {
    if (this.dataVersionPromise) return this.dataVersionPromise;
    this.dataVersionPromise = this._fetchDataVersion();
    return this.dataVersionPromise;
  }

  private async _fetchDataVersion(): Promise<string> {
    const url = `${WaterbodyDataService.DATA_BASE}/data_version.json`;
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json() as { v?: string };
      const version = String(json.v ?? '');
      if (version) {
        localStorage.setItem(WaterbodyDataService.VERSION_KEY, version);
      }
      return version;
    } catch {
      return localStorage.getItem(WaterbodyDataService.VERSION_KEY) ?? '';
    }
  }
}

export const waterbodyDataService = new WaterbodyDataService();

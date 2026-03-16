/**
 * Waterbody Data Service — V2 regulation_index.json loader
 *
 * Loads the unified regulation_index.json (v2 format) and provides:
 * - regulations: expanded per-rule Regulation objects
 * - reaches: reach_id → reach metadata
 * - segmentIndex: fid → reach_id (inverted from reach_segments)
 * - polyIndex: waterbody_key → reach_id (from poly_reaches)
 * - searchIndex: SearchableFeature[] for Fuse.js
 * - reg_sets: dedup'd regulation ID strings
 *
 * V2 regulation_index.json sections:
 *   regulations    — reg_id → {raw_regs, source, parsed?, restriction?, ...}
 *   reg_sets       — ["reg1,reg2,...", ...]  dedup'd comma-joined strings
 *   reaches        — reach_id → {dn, nv[], ft, ri, wsc, mz, rg[], bbox, lkm}
 *   reach_segments — reach_id → [fid, ...] (streams only)
 *   poly_reaches   — waterbody_key → reach_id
 *   search_index   — [{dn, nv[], reaches[], ft, rg[], mz, bbox, wbg, z, mu, tlkm}]
 */

import type { NameVariant } from '../utils/featureUtils';

// ── V2 Raw JSON shapes ───────────────────────────────────────────────

/** Raw synopsis regulation from regulation_index.json */
interface V2SynopsisReg {
  water: string;
  region: string;
  mu: string[];
  raw_regs: string;
  symbols: string[];
  source: 'synopsis';
  page: number;
  image: string;
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
    }[];
  };
}

/** Raw base regulation (zone/provincial) from regulation_index.json */
interface V2BaseReg {
  raw_regs: string;
  source: 'zone' | 'provincial';
  restriction?: { type: string; details: string };
  zone_ids?: string[];
  dates?: string[];
  scope_location?: string;
  notes?: string;
  zone?: string;
}

type V2Regulation = V2SynopsisReg | V2BaseReg;

/** V2 reach (from reaches section) */
export interface Reach {
  dn: string;       // display_name
  nv: string[];     // name_variants
  ft: string;       // feature_type
  ri: number;       // reg_set_index
  wsc: string;      // fwa_watershed_code or waterbody_key
  mz: number;       // minzoom
  rg: string[];     // regions
  bbox: [number, number, number, number] | null;
  lkm: number;      // length_km
}

/** V2 search index entry */
export interface V2SearchEntry {
  dn: string;
  nv: string[];
  reaches: string[];
  ft: string;
  rg: string[];
  mz: number;
  bbox: [number, number, number, number] | null;
  wbg: string;
  z: string[];
  mu: string[];
  tlkm: number;
}

// ── Output types (consumed by Map.tsx, InfoPanel, SearchBar) ─────────

/** Flat regulation shape matching what InfoPanel expects */
export interface Regulation {
  regulation_id: string;
  waterbody_name: string;
  region: string | null;
  management_units: string[];
  rule_text: string;
  restriction_type: string;
  restriction_details: string;
  dates: string[] | string | null;
  scope_type: string;
  scope_location: string | null;
  source: 'synopsis' | 'provincial' | 'zone';
  zone_ids?: string[];
  feature_types?: string[] | null;
  iid?: string;
  source_image?: string | null;
  exclusions?: null;
}

// ── Admin visibility config (from admin_visibility.json) ─────────────

/** Per-layer visibility config from enrichment pipeline */
export interface AdminLayerVisibility {
  display: 'all' | 'regulated_only';
  regulated_ids?: string[];
}

/** Full admin visibility map: tile_layer_name → config */
export type AdminVisibility = Record<string, AdminLayerVisibility>;

/** Loaded + decoded V2 data */
export interface RegulationData {
  /** Expanded per-rule regulations keyed by synthetic rule ID */
  regulations: Record<string, Regulation>;
  /** Deduplicated regulation-ID strings (expanded with rule suffixes) */
  reg_sets: string[];
  /** Reach metadata keyed by reach_id */
  reaches: Record<string, Reach>;
  /** fid → reach_id (inverted from reach_segments) */
  segmentIndex: Map<string, string>;
  /** waterbody_key → reach_id */
  polyIndex: Record<string, string>;
  /** reach_id → [fid, ...] (for highlighting) */
  reachSegments: Record<string, string[]>;
  /** Fuse.js-compatible search entries */
  searchIndex: V2SearchEntry[];
  /** Original reg_set strings (before rule expansion) */
  rawRegSets: string[];
  /** Admin layer visibility config (which layers show all vs. regulated only) */
  adminVisibility: AdminVisibility;
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
  rawRegs: Record<string, V2Regulation>,
  rawRegSets: string[],
): { regulations: Record<string, Regulation>; regSets: string[] } {
  const regulations: Record<string, Regulation> = {};
  const expansionMap: Record<string, string[]> = {};

  for (const [regId, raw] of Object.entries(rawRegs)) {
    if (raw.source === 'synopsis') {
      const syn = raw as V2SynopsisReg;
      if (syn.parsed?.rules?.length) {
        const ruleIds: string[] = [];
        for (let i = 0; i < syn.parsed.rules.length; i++) {
          const rule = syn.parsed.rules[i];
          const ruleId = `${regId}_rule${i}`;
          ruleIds.push(ruleId);
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
            source: 'synopsis',
            iid: regId,
            source_image: syn.image || null,
            exclusions: null,
          };
        }
        expansionMap[regId] = ruleIds;
      } else {
        // Unparsed synopsis — show raw_regs as single entry
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
          exclusions: null,
        };
        expansionMap[regId] = [regId];
      }
    } else {
      const base = raw as V2BaseReg;
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


// ── IndexedDB helpers ────────────────────────────────────────────────
const IDB_NAME = 'waterbody_cache';
const IDB_STORE = 'kv';
const IDB_VERSION = 5;  // bumped for v2 format

function openCacheDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, IDB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(IDB_STORE)) {
        db.createObjectStore(IDB_STORE);
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


// ── Service ──────────────────────────────────────────────────────────

class WaterbodyDataService {
  private data: RegulationData | null = null;
  private loadPromise: Promise<RegulationData> | null = null;
  private dataVersionPromise: Promise<string> | null = null;

  private static readonly DATA_BASE = import.meta.env.VITE_TILE_BASE_URL || '/data';
  private static readonly ETAG_KEY = 'regindex_etag';
  private static readonly DATA_KEY = 'regindex_data';
  private static readonly VERSION_KEY = 'data_version';

  async load(): Promise<RegulationData> {
    if (this.data) return this.data;
    if (this.loadPromise) return this.loadPromise;
    this.loadPromise = this._load();
    return this.loadPromise;
  }

  private async _load(): Promise<RegulationData> {
    const url = `${WaterbodyDataService.DATA_BASE}/regulation_index.json`;

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

      if (response.status === 304 && cachedData) {
        // Rebuild Map from cached plain object
        cachedData.segmentIndex = new Map(Object.entries(
          (cachedData as any)._segmentIndexObj || {}
        ));
        cachedData.adminVisibility = adminVis;
        this.data = cachedData;
        this.loadPromise = null;
        console.log(`✅ Regulation index loaded from cache (304)`);
        return cachedData;
      }

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
      }

      const raw = await response.json();
      const data = this._decode(raw, adminVis);
      this.data = data;
      this.loadPromise = null;

      const etag = response.headers.get('ETag') || '';
      if (etag) {
        try {
          const toCache = {
            ...data,
            segmentIndex: undefined,
            _segmentIndexObj: Object.fromEntries(data.segmentIndex),
          };
          await idbSet(WaterbodyDataService.ETAG_KEY, etag);
          await idbSet(WaterbodyDataService.DATA_KEY, toCache);
        } catch {
          console.warn('IndexedDB write failed, cache disabled for this session');
        }
      }

      console.log(
        `✅ Regulation index loaded: ${Object.keys(data.regulations).length} regulations, ` +
        `${Object.keys(data.reaches).length} reaches, ` +
        `${data.segmentIndex.size} segment mappings, ` +
        `${data.searchIndex.length} search entries`
      );
      return data;

    } catch (error) {
      this.loadPromise = null;
      console.error('❌ Failed to load regulation_index.json:', error);
      throw error;
    }
  }

  private _decode(raw: any, adminVisibility: AdminVisibility = {}): RegulationData {
    const rawRegSets: string[] = raw.reg_sets || [];
    const rawRegs: Record<string, V2Regulation> = raw.regulations || {};
    const reachesRaw: Record<string, Reach> = raw.reaches || {};
    const reachSegments: Record<string, string[]> = raw.reach_segments || {};
    const polyReaches: Record<string, string> = raw.poly_reaches || {};
    const searchEntries: V2SearchEntry[] = raw.search_index || [];

    const { regulations, regSets } = expandRegulations(rawRegs, rawRegSets);

    // Build inverted segment index: fid → reach_id
    const segmentIndex = new Map<string, string>();
    for (const [reachId, fids] of Object.entries(reachSegments)) {
      for (const fid of fids) {
        segmentIndex.set(fid, reachId);
      }
    }

    return {
      regulations,
      reg_sets: regSets,
      reaches: reachesRaw,
      segmentIndex,
      polyIndex: polyReaches,
      reachSegments,
      searchIndex: searchEntries,
      rawRegSets,
      adminVisibility,
    };
  }

  async getRegulations(): Promise<Record<string, Regulation>> {
    const data = await this.load();
    return data.regulations;
  }

  async getRegSets(): Promise<string[]> {
    const data = await this.load();
    return data.reg_sets;
  }

  async getReach(reachId: string): Promise<Reach | undefined> {
    const data = await this.load();
    return data.reaches[reachId];
  }

  async getReaches(): Promise<Record<string, Reach>> {
    const data = await this.load();
    return data.reaches;
  }

  /** Resolve a stream fid to its reach_id */
  async resolveStreamFid(fid: string): Promise<string | undefined> {
    const data = await this.load();
    return data.segmentIndex.get(fid);
  }

  /** Resolve a polygon waterbody_key to its reach_id */
  async resolvePolyWbk(wbk: string): Promise<string | undefined> {
    const data = await this.load();
    return data.polyIndex[wbk];
  }

  /** Get fid list for a stream reach (for highlighting) */
  async getReachFids(reachId: string): Promise<string[]> {
    const data = await this.load();
    return data.reachSegments[reachId] || [];
  }

  async getSearchIndex(): Promise<V2SearchEntry[]> {
    const data = await this.load();
    return data.searchIndex;
  }

  /** Get expanded regulation IDs for a reach */
  async getReachRegulationIds(reachId: string): Promise<string[]> {
    const data = await this.load();
    const reach = data.reaches[reachId];
    if (!reach) return [];
    const regSetStr = data.reg_sets[reach.ri] || '';
    return regSetStr.split(',').filter(Boolean);
  }

  preload(): void {
    this.load().catch((err) => {
      console.warn('Regulation index preload failed:', err);
    });
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

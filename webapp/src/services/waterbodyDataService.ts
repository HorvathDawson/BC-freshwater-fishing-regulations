/**
 * Waterbody Data Service
 *
 * Loads the unified waterbody_data.json and provides access to both:
 * - waterbodies: Search/map data with regulation segments
 * - regulations: Full regulation details keyed by regulation_id
 * - reg_sets + compact: Compact regulation lookup tables
 *
 * The JSON uses short keys for compactness.  This service decodes them
 * into the canonical long-key interfaces that the rest of the frontend
 * expects (WaterbodyItem, RegulationSegment) so consumers never see
 * the compressed format.
 *
 * Unnamed zone-only features are stored as compact entries
 * (frontend_group_id → reg_set_index).  The frontend resolves their
 * regulations at click-time via compact[fgid] → reg_sets[ri].
 */

import type { Regulation, WireRegulation, IdentityMeta } from './regulationsService';
import type { NameVariant } from '../utils/featureUtils';

/** Shape of a single waterbody entry after decoding (long keys). */
export interface WaterbodyItem {
  id: string;
  gnis_name?: string;
  display_name?: string;
  frontend_group_ids?: string[];
  type: string;
  zones?: string;
  mgmt_units?: string;
  region_name?: string;
  regulation_ids?: string;
  name_variants?: NameVariant[];
  bbox?: [number, number, number, number];
  min_zoom?: number;
  total_length_km?: number;
  properties?: Record<string, string | number | boolean | null>;
  regulation_segments?: {
    frontend_group_id?: string;
    group_id?: string;
    regulation_ids?: string;
    display_name?: string;
    name_variants?: NameVariant[];
    length_km?: number;
    bbox?: [number, number, number, number] | null;
    waterbody_group?: string;
  }[];
}

/** Decoded data ready for consumption by Map.tsx and other components. */
export interface WaterbodyData {
  waterbodies: WaterbodyItem[];
  regulations: Record<string, Regulation>;
  /** Deduplicated regulation-ID strings (shared across waterbodies). */
  reg_sets: string[];
  /** frontend_group_id → reg_sets index for unnamed zone-only features. */
  compact: Record<string, number>;
}

// ── Short-key → long-key decoding ────────────────────────────────────

/** Expand a compact (short-key) waterbody entry from the JSON wire format
 *  into the canonical WaterbodyItem shape used by the rest of the app.  */
function decodeWaterbody(raw: Record<string, any>, regSets: string[]): WaterbodyItem {
  const decodeVariants = (nvs: any[] | undefined): NameVariant[] =>
    (nvs || []).map((v: any) => ({ name: v.name, from_tributary: v.ft ?? v.from_tributary ?? false }));

  const regIds = regSets[raw.ri] ?? '';
  const zones = (raw.z as string[] | undefined) ?? [];
  const mus = (raw.mu as string[] | undefined) ?? [];
  const rns = (raw.rn as string[] | undefined) ?? [];

  const segments = ((raw.rs ?? []) as any[]).map((s: any) => ({
    frontend_group_id: s.fgid ?? s.frontend_group_id ?? '',
    group_id: s.gid ?? s.group_id ?? '',
    regulation_ids: regSets[s.ri] ?? s.regulation_ids ?? '',
    display_name: s.dn ?? s.display_name ?? '',
    name_variants: decodeVariants(s.nv ?? s.name_variants),
    length_km: s.lkm ?? s.length_km ?? 0,
    bbox: s.bbox ?? null,
    waterbody_group: s.wbg ?? s.waterbody_group ?? '',
  }));

  return {
    id: raw.id,
    gnis_name: raw.gn ?? raw.gnis_name ?? '',
    display_name: raw.dn ?? raw.display_name ?? '',
    frontend_group_ids: raw.fgids ?? raw.frontend_group_ids ?? [],
    type: raw.type,
    zones: zones.join(','),
    mgmt_units: mus.join(','),
    region_name: rns.join(','),
    regulation_ids: regIds,
    name_variants: decodeVariants(raw.nv ?? raw.name_variants),
    bbox: raw.bbox,
    min_zoom: raw.mz ?? raw.min_zoom,
    total_length_km: raw.tlkm ?? raw.total_length_km ?? 0,
    properties: {
      group_id: raw.props?.gid ?? raw.properties?.group_id ?? '',
      waterbody_key: raw.props?.wk ?? raw.properties?.waterbody_key ?? '',
      fwa_watershed_code: raw.props?.fwc ?? raw.properties?.fwa_watershed_code ?? '',
      regulation_count: raw.props?.rc ?? raw.properties?.regulation_count ?? 0,
      waterbody_group: raw.props?.wbg ?? raw.properties?.waterbody_group ?? '',
    },
    regulation_segments: segments,
  };
}

// ── IndexedDB helpers (no dependencies) ──────────────────────────────
const IDB_NAME = 'waterbody_cache';
const IDB_STORE = 'kv';
const IDB_VERSION = 4;  // bumped: identity_meta deduplication

function openCacheDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, IDB_VERSION);
    req.onupgradeneeded = () => req.result.createObjectStore(IDB_STORE);
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
  private data: WaterbodyData | null = null;
  private loadPromise: Promise<WaterbodyData> | null = null;
  private dataVersionPromise: Promise<string> | null = null;

  private static readonly DATA_BASE = import.meta.env.VITE_TILE_BASE_URL || '/data';
  private static readonly ETAG_KEY = 'waterbody_etag';
  private static readonly DATA_KEY = 'waterbody_data';
  private static readonly VERSION_KEY = 'data_version';

  async load(): Promise<WaterbodyData> {
    if (this.data) return this.data;
    if (this.loadPromise) return this.loadPromise;

    this.loadPromise = this._load();
    return this.loadPromise;
  }

  private async _load(): Promise<WaterbodyData> {
    const url = `${WaterbodyDataService.DATA_BASE}/waterbody_data.json`;

    try {
      // ── Single conditional GET using ETag ──────────────────────────
      // If we have a cached ETag, send If-None-Match.  The R2 worker
      // returns 304 (no body) when the file hasn't changed — saving the
      // full download.  This replaces the old HEAD + GET two-step and
      // eliminates the custom X-Data-Mtime header entirely.
      const cachedEtag = await idbGet<string>(WaterbodyDataService.ETAG_KEY).catch(() => undefined);
      const cachedData = cachedEtag
        ? await idbGet<WaterbodyData>(WaterbodyDataService.DATA_KEY).catch(() => undefined)
        : undefined;

      let response: Response;

      if (cachedEtag && cachedData) {
        // Returning visitor: conditional request with ETag
        response = await fetch(url, { headers: { 'If-None-Match': cachedEtag } });
      } else {
        // First visit: use the early-fetched response if the Vite-injected
        // inline script started the request before the JS bundle loaded.
        const earlyFetch = (window as any).__earlyFetch as Promise<Response | null> | undefined;
        if (earlyFetch) {
          delete (window as any).__earlyFetch;  // consume once
          const earlyResp = await earlyFetch;
          if (earlyResp) {
            response = earlyResp;
          } else {
            response = await fetch(url);
          }
        } else {
          response = await fetch(url);
        }
      }

      // 304 Not Modified → use cached data
      if (response.status === 304 && cachedData) {
        this.data = cachedData;
        this.loadPromise = null;
        console.log(
          `✅ Waterbody data loaded from cache (304): ${cachedData.waterbodies?.length || 0} waterbodies, ` +
          `${Object.keys(cachedData.regulations || {}).length} regulations, ` +
          `${cachedData.reg_sets?.length || 0} reg_sets, ` +
          `${Object.keys(cachedData.compact || {}).length} compact`
        );
        return cachedData;
      }

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
      }

      const raw = await response.json();

      // Decode compact format: expand short keys + dereference reg_set indices
      const regSets: string[] = raw.reg_sets || [];
      const compact: Record<string, number> = raw.compact || {};
      const waterbodies: WaterbodyItem[] = (raw.waterbodies || []).map(
        (w: Record<string, any>) => decodeWaterbody(w, regSets)
      );
      const wireRegulations: Record<string, WireRegulation> = raw.regulations || {};

      // ── Hydrate identity_meta onto synopsis regulations ────────────
      // Synopsis regs arrive without waterbody_name / region / management_units /
      // source_image / exclusions — those live in identity_meta keyed by base-ID.
      // Zone/provincial regs carry their fields directly and have no iid.
      const identityMeta: Record<string, IdentityMeta> | undefined = raw.identity_meta;
      const regulations: Record<string, Regulation> = {};

      for (const [regId, wire] of Object.entries(wireRegulations)) {
        if (wire.iid) {
          // Synopsis regulation: hydrate from identity_meta.
          if (!identityMeta || !(wire.iid in identityMeta)) {
            throw new Error(
              `Orphaned iid "${wire.iid}" on regulation "${regId}" — identity_meta is missing or incomplete.`
            );
          }
          const meta = identityMeta[wire.iid];
          regulations[regId] = {
            ...wire,
            regulation_id: regId,
            waterbody_name: meta.wn ?? '',
            region: meta.rg ?? null,
            management_units: meta.mu ?? [],
            source_image: meta.img ?? null,
            exclusions: meta.ex ?? null,
          } as Regulation;
        } else {
          // Zone/provincial: fields already present on the wire regulation.
          regulations[regId] = {
            ...wire,
            regulation_id: regId,
            waterbody_name: wire.waterbody_name ?? '',
            region: wire.region ?? null,
            management_units: wire.management_units ?? [],
          } as Regulation;
        }
      }

      const data: WaterbodyData = { waterbodies, regulations, reg_sets: regSets, compact };
      this.data = data;
      this.loadPromise = null;

      // Persist decoded data + ETag to IndexedDB for next visit
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
        `✅ Waterbody data loaded: ${data.waterbodies?.length || 0} waterbodies, ` +
        `${Object.keys(data.regulations || {}).length} regulations, ` +
        `${data.reg_sets?.length || 0} reg_sets, ` +
        `${Object.keys(data.compact || {}).length} compact`
      );
      return data;
    } catch (error) {
      this.loadPromise = null;
      console.error('❌ Failed to load waterbody_data.json:', error);
      throw error;
    }
  }

  async getWaterbodies(): Promise<WaterbodyItem[]> {
    const data = await this.load();
    return data.waterbodies || [];
  }

  async getRegulations(): Promise<Record<string, Regulation>> {
    const data = await this.load();
    return data.regulations || {};
  }

  /** Deduplicated regulation-ID strings, indexed by integer. */
  async getRegSets(): Promise<string[]> {
    const data = await this.load();
    return data.reg_sets || [];
  }

  /** frontend_group_id → reg_sets index for unnamed zone-only features. */
  async getCompact(): Promise<Record<string, number>> {
    const data = await this.load();
    return data.compact || {};
  }

  preload(): void {
    this.load().catch((err) => {
      console.warn('Waterbody data preload failed:', err);
    });
  }

  /**
   * Fetch the deployed data version string, used to cache-bust PMTiles URLs.
   *
   * On every call:
   *  - Fetches `data_version.json` with `cache: 'no-store'` (always fresh, ~50 bytes).
   *  - If the version changed vs localStorage, updates localStorage and returns the new version.
   *  - If unchanged (or fetch fails), returns the stored version (or '' as fallback).
   *
   * Result is memoized within the session so only one network round-trip occurs
   * regardless of how many callers await it.
   */
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
      // Network error or file missing (e.g. local dev without the file).
      // Fall back to whatever was stored last — PMTiles URL uses '?' param only
      // if version is non-empty, so '' means no cache-busting (backward-compatible).
      return localStorage.getItem(WaterbodyDataService.VERSION_KEY) ?? '';
    }
  }
}

export const waterbodyDataService = new WaterbodyDataService();

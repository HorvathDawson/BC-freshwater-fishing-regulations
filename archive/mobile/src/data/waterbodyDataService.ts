/**
 * Waterbody data service (mobile) — the on-device analogue of the web app's
 * `waterbodyDataService` + `/api/resolve` edge endpoint.
 *
 * The web app loads tier0.json and calls `/api/resolve` for tile fids/wbks.
 * On mobile there is no network: every lookup is a local SQLite query against
 * the bundled/downloaded `regulations.sqlite`, which mirrors the R2 shards:
 *
 *   meta(key,value)            reg_sets(idx,reg_ids)
 *   regulations(reg_id,json)   reaches(reach_id,json)
 *   fids(fid,reach_id)         polys(wbk,reach_id)
 *   search(...) + search_fts   (see searchService)
 *
 * Raw regulations are expanded once (same logic as the web app) so consumers
 * receive identical flat {@link Regulation} objects.
 */
import type { SQLiteDatabase } from 'expo-sqlite';

import type { Reach, Regulation } from '../types/regulations';
import { getDatabase, type DownloadProgress } from './database';
import { expandRegulations, type RawRegulation } from './regulationExpansion';

class WaterbodyDataService {
  private db: SQLiteDatabase | null = null;
  private regulations: Record<string, Regulation> = {};
  private regSets: string[] = [];
  private reachCache = new Map<string, Reach | null>();
  private initPromise: Promise<void> | null = null;

  /** Open the DB and load + expand all regulations. Idempotent. */
  async init(onProgress?: DownloadProgress): Promise<void> {
    if (this.initPromise) return this.initPromise;
    this.initPromise = (async () => {
      this.db = await getDatabase(onProgress);

      const rawRegs: Record<string, RawRegulation> = {};
      const regRows = await this.db.getAllAsync<{ reg_id: string; json: string }>(
        'SELECT reg_id, json FROM regulations',
      );
      for (const row of regRows) {
        rawRegs[row.reg_id] = JSON.parse(row.json) as RawRegulation;
      }

      const setRows = await this.db.getAllAsync<{ idx: number; reg_ids: string }>(
        'SELECT idx, reg_ids FROM reg_sets ORDER BY idx',
      );
      const rawRegSets = setRows.map((r) => r.reg_ids);

      const { regulations, regSets } = expandRegulations(rawRegs, rawRegSets);
      this.regulations = regulations;
      this.regSets = regSets;
    })();
    return this.initPromise;
  }

  private requireDb(): SQLiteDatabase {
    if (!this.db) throw new Error('WaterbodyDataService not initialised — call init() first.');
    return this.db;
  }

  // ── Tile → reach resolution (replaces /api/resolve) ────────────────

  /** Resolve a line-tile feature id to its reach id. */
  async reachIdForFid(fid: string): Promise<string | null> {
    const row = await this.requireDb().getFirstAsync<{ reach_id: string }>(
      'SELECT reach_id FROM fids WHERE fid = ?',
      [fid],
    );
    return row?.reach_id ?? null;
  }

  /** Resolve a polygon-tile waterbody key to its reach id. */
  async reachIdForWbk(wbk: string): Promise<string | null> {
    const row = await this.requireDb().getFirstAsync<{ reach_id: string }>(
      'SELECT reach_id FROM polys WHERE wbk = ?',
      [wbk],
    );
    return row?.reach_id ?? null;
  }

  /** Load full reach metadata by id (cached). */
  async getReach(reachId: string): Promise<Reach | null> {
    if (this.reachCache.has(reachId)) return this.reachCache.get(reachId) ?? null;
    const row = await this.requireDb().getFirstAsync<{ json: string }>(
      'SELECT json FROM reaches WHERE reach_id = ?',
      [reachId],
    );
    const reach = row ? (JSON.parse(row.json) as Reach) : null;
    this.reachCache.set(reachId, reach);
    return reach;
  }

  // ── Regulation accessors ───────────────────────────────────────────

  /** Expanded regulation IDs for a reg_set index. */
  regIdsForSet(index: number): string[] {
    const s = this.regSets[index];
    return s ? s.split(',').filter(Boolean) : [];
  }

  getRegulation(id: string): Regulation | undefined {
    return this.regulations[id];
  }

  getRegulations(ids: string[]): Regulation[] {
    return ids.map((id) => this.regulations[id]).filter((r): r is Regulation => Boolean(r));
  }

  /** All expanded regulations (used by regulationsService for provenance). */
  get allRegulations(): Record<string, Regulation> {
    return this.regulations;
  }
}

/** Shared singleton — mirrors the web app's module-level service instance. */
export const waterbodyDataService = new WaterbodyDataService();

/**
 * Search service (mobile) — on-device replacement for the web app's search,
 * which queries the R2 search index. Here we query the bundled SQLite:
 *
 *   - Primary: SQLite FTS5 (`search_fts` over display_name, name_variants,
 *     nearby_towns) for fast prefix/token matching.
 *   - Fallback/rerank: fuse.js over the FTS candidate rows for typo-tolerant
 *     fuzzy ranking, matching the web app's Fuse-based feel.
 *
 * Rows are hydrated back into {@link SearchEntry} objects (identical shape to
 * the web search index) so SearchBar / DisambiguationMenu are a straight port.
 */
import Fuse from 'fuse.js';
import type { SQLiteDatabase } from 'expo-sqlite';

import type { SearchEntry } from '../types/regulations';
import { getDatabase } from './database';

interface SearchRow {
  id: number;
  display_name: string;
  feature_type: string;
  waterbody_group: string;
  min_zoom: number;
  bbox: string | null;
  regions: string | null;
  zones: string | null;
  management_units: string | null;
  total_length_km: number | null;
  name_variants: string | null;
  nearby_towns: string | null;
  segments: string | null;
}

const parseJson = <T,>(value: string | null, fallback: T): T => {
  if (!value) return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
};

function rowToEntry(row: SearchRow): SearchEntry {
  return {
    display_name: row.display_name,
    feature_type: row.feature_type,
    waterbody_group: row.waterbody_group,
    min_zoom: row.min_zoom,
    bbox: parseJson<SearchEntry['bbox']>(row.bbox, null),
    regions: parseJson<string[]>(row.regions, []),
    zones: parseJson<string[]>(row.zones, []),
    management_units: parseJson<string[]>(row.management_units, []),
    total_length_km: row.total_length_km ?? 0,
    name_variants: parseJson<SearchEntry['name_variants']>(row.name_variants, []),
    nearby_towns: parseJson<SearchEntry['nearby_towns']>(row.nearby_towns, []),
    segments: parseJson<SearchEntry['segments']>(row.segments, []),
  };
}

/** Escape a user query for an FTS5 prefix MATCH (token* per word). */
function toFtsQuery(raw: string): string {
  const tokens = raw
    .toLowerCase()
    .replace(/["*]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
  if (!tokens.length) return '';
  return tokens.map((t) => `"${t}"*`).join(' AND ');
}

class SearchService {
  private db: SQLiteDatabase | null = null;

  async init(): Promise<void> {
    this.db = await getDatabase();
  }

  private requireDb(): SQLiteDatabase {
    if (!this.db) throw new Error('SearchService not initialised — call init() first.');
    return this.db;
  }

  /**
   * Full-text search with fuzzy rerank.
   * @param query   user text
   * @param limit   max results returned
   */
  async search(query: string, limit = 20): Promise<SearchEntry[]> {
    const trimmed = query.trim();
    if (trimmed.length < 2) return [];

    const match = toFtsQuery(trimmed);
    if (!match) return [];

    // Pull a generous candidate set from FTS, then fuzzy-rank for the final cut.
    const candidates = await this.requireDb().getAllAsync<SearchRow>(
      `SELECT s.* FROM search_fts f
         JOIN search s ON s.id = f.rowid
        WHERE search_fts MATCH ?
        LIMIT ?`,
      [match, Math.max(limit * 5, 50)],
    );

    const entries = candidates.map(rowToEntry);
    if (entries.length <= limit) return entries;

    const fuse = new Fuse(entries, {
      keys: [
        { name: 'display_name', weight: 0.6 },
        { name: 'name_variants.name', weight: 0.3 },
        { name: 'waterbody_group', weight: 0.1 },
      ],
      threshold: 0.4,
      ignoreLocation: true,
    });

    return fuse
      .search(trimmed)
      .slice(0, limit)
      .map((r) => r.item);
  }
}

export const searchService = new SearchService();

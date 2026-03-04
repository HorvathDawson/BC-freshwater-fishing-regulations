/**
 * Waterbody Data Service
 *
 * Loads the unified waterbody_data.json and provides access to both:
 * - waterbodies: Search/map data with regulation segments
 * - regulations: Full regulation details keyed by regulation_id
 *
 * This is the single source of truth for all frontend data.
 */

import type { Regulation } from './regulationsService';
import type { NameVariant } from '../utils/featureUtils';

/** Shape of a single waterbody entry from waterbody_data.json */
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
  segment_count?: number;
  properties?: Record<string, string | number | boolean | null>;
  regulation_segments?: {
    frontend_group_id?: string;
    group_id?: string;
    group_ids?: string[];
    regulation_ids?: string;
    display_name?: string;
    name_variants?: NameVariant[];
    length_km?: number;
    bbox?: [number, number, number, number] | null;
  }[];
}

export interface WaterbodyData {
  waterbodies: WaterbodyItem[];
  regulations: Record<string, Regulation>;
}

class WaterbodyDataService {
  private data: WaterbodyData | null = null;
  private loadPromise: Promise<WaterbodyData> | null = null;

  private static readonly DATA_BASE = import.meta.env.VITE_TILE_BASE_URL || '/data';

  async load(): Promise<WaterbodyData> {
    if (this.data) return this.data;
    if (this.loadPromise) return this.loadPromise;

    this.loadPromise = fetch(`${WaterbodyDataService.DATA_BASE}/waterbody_data.json`)
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
        }
        return response.json();
      })
      .then(data => {
        this.data = data;
        this.loadPromise = null;
        console.log(
          `✅ Waterbody data loaded: ${data.waterbodies?.length || 0} waterbodies, ` +
          `${Object.keys(data.regulations || {}).length} regulations`
        );
        return data;
      })
      .catch(error => {
        this.loadPromise = null;
        console.error('❌ Failed to load waterbody_data.json:', error);
        throw error;
      });

    return this.loadPromise;
  }

  async getWaterbodies(): Promise<WaterbodyItem[]> {
    const data = await this.load();
    return data.waterbodies || [];
  }

  async getRegulations(): Promise<Record<string, Regulation>> {
    const data = await this.load();
    return data.regulations || {};
  }

  preload(): void {
    this.load().catch((err) => {
      console.warn('Waterbody data preload failed:', err);
    });
  }
}

export const waterbodyDataService = new WaterbodyDataService();

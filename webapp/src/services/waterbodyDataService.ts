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

export interface WaterbodyData {
  waterbodies: any[];
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

  async getWaterbodies(): Promise<any[]> {
    const data = await this.load();
    return data.waterbodies || [];
  }

  async getRegulations(): Promise<Record<string, Regulation>> {
    const data = await this.load();
    return data.regulations || {};
  }

  preload(): void {
    this.load().catch(() => {});
  }
}

export const waterbodyDataService = new WaterbodyDataService();

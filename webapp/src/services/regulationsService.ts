/**
 * Regulations Service
 *
 * Provides regulation lookup by ID. Data is loaded from the unified
 * regulation_index.json via waterbodyDataService.
 *
 * V2: Regulations are expanded at load time from v2 format (synopsis
 * parsed.rules[] → flat per-rule entries, base regs → direct).
 * The Regulation type is re-exported from waterbodyDataService.
 */

import { waterbodyDataService } from './waterbodyDataService';
import type { Regulation } from './waterbodyDataService';

// Re-export the Regulation type for consumers
export type { Regulation };

// Legacy types kept for backward compatibility with InfoPanel
export interface ExclusionEntry {
  type: string;
  lookup_name: string;
  location_verbatim?: string | null;
  landmark_verbatim?: string | null;
  direction?: string | null;
  includes_tributaries?: boolean | null;
}

type RegulationsLookup = Record<string, Regulation>;

class RegulationsService {
  private regulations: RegulationsLookup | null = null;
  private loadPromise: Promise<RegulationsLookup> | null = null;

  async loadRegulations(): Promise<RegulationsLookup> {
    if (this.regulations) return this.regulations;
    if (this.loadPromise) return this.loadPromise;

    this.loadPromise = waterbodyDataService.getRegulations()
      .then(data => {
        this.regulations = data;
        this.loadPromise = null;
        console.log("✅ Regulations loaded. Total keys:", Object.keys(data).length);
        return data;
      })
      .catch(error => {
        this.loadPromise = null;
        console.error('❌ Failed to load regulations:', error);
        throw error;
      });

    return this.loadPromise;
  }

  async getRegulations(regulationIds: string | string[] | null | undefined): Promise<Regulation[]> {
    if (!regulationIds || regulationIds === "" || regulationIds === "null") {
      return [];
    }

    try {
      const regulations = await this.loadRegulations();

      let ids: string[] = [];
      if (Array.isArray(regulationIds)) {
        ids = regulationIds.map(id => String(id).trim());
      } else {
        ids = String(regulationIds)
          .replace(/[\[\]"\s]/g, '')
          .split(',')
          .filter(Boolean);
      }

      const results = ids
        .map(id => {
          const match = regulations[id];
          if (!match) console.warn(`⚠️ No match found for regulation ID: "${id}"`);
          return match ? { ...match, regulation_id: id } : null;
        })
        .filter(Boolean) as Regulation[];

      return results;
    } catch (error) {
      console.error('❌ Service Error:', error);
      throw error;
    }
  }

  async getRegulation(regulationId: string): Promise<Regulation | null> {
    if (!regulationId) return null;
    const regs = await this.loadRegulations();
    return regs[regulationId] || null;
  }

  preload(): void {
    this.loadRegulations().catch((err) => {
      console.warn('Regulations preload failed:', err);
    });
  }
}

export const regulationsService = new RegulationsService();
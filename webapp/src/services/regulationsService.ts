/**
 * Regulations Service
 * 
 * Provides regulation lookup by ID. Data is loaded from the unified
 * waterbody_data.json via waterbodyDataService.
 */

import { waterbodyDataService } from './waterbodyDataService';

export interface Regulation {
  regulation_id: string;
  waterbody_name: string;
  waterbody_key: string | null;
  region: string | null;
  management_units: string[];
  rule_text: string;
  restriction_type: string;
  restriction_details: string;
  dates: string[] | string | { period?: string; type?: string } | null;  // Legacy zone format had {period, type}; new format is string[]
  scope_type: string;
  scope_location: string | null;
  includes_tributaries: boolean | null;
  source?: 'synopsis' | 'provincial' | 'zone';
  source_image?: string | null;
  zone_ids?: string[];
  feature_types?: string[] | null;
  is_direct_match?: boolean;
}

type RegulationsLookup = Record<string, Regulation>;

class RegulationsService {
  private regulations: RegulationsLookup | null = null;
  private loadPromise: Promise<RegulationsLookup> | null = null;
  private provincialRuleTexts: Set<string> = new Set();

  async loadRegulations(): Promise<RegulationsLookup> {
    if (this.regulations) return this.regulations;
    if (this.loadPromise) return this.loadPromise;

    // Load from unified waterbody_data.json via shared service
    this.loadPromise = waterbodyDataService.getRegulations()
      .then(data => {
        this.regulations = data;
        this.loadPromise = null;

        // Build set of provincial/zone rule texts for filtering display names
        this.provincialRuleTexts = new Set(
          Object.values(data as RegulationsLookup)
            .filter(reg => (reg.source === 'provincial' || reg.source === 'zone') && reg.rule_text)
            .map(reg => reg.rule_text)
        );

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
    // If no IDs are provided, return empty array immediately
    if (!regulationIds || regulationIds === "" || regulationIds === "null") {
      return [];
    }
    
    try {
      const regulations = await this.loadRegulations();
      
      // Normalize IDs into an array of clean strings
      let ids: string[] = [];
      if (Array.isArray(regulationIds)) {
        ids = regulationIds.map(id => String(id).trim());
      } else {
        // Remove brackets, quotes, and split by comma
        ids = String(regulationIds)
          .replace(/[\[\]"\s]/g, '') 
          .split(',')
          .filter(Boolean);
      }

      console.log("🔍 Looking up IDs:", ids);

      const results = ids
        .map(id => {
          const match = regulations[id];
          if (!match) console.warn(`⚠️ No match found in JSON for ID: "${id}"`);
          return match ? { ...match, regulation_id: id } : null;
        })
        .filter(Boolean) as Regulation[];

      return results;
    } catch (error) {
      console.error('❌ Service Error:', error);
      // Re-throw so the UI knows to show the "Failed to load" state
      throw error;
    }
  }

  async getRegulation(regulationId: string): Promise<Regulation | null> {
    if (!regulationId) return null;
    const regs = await this.loadRegulations();
    return regs[regulationId] || null;
  }

  /**
   * Filter out provincial regulation names (rule_text) from a list of regulation names.
   * Provincial names are long rule texts that shouldn't appear in "Listed as" or as waterbody name fallbacks.
   * Arrow function to preserve `this` binding when passed as a callback.
   */
  filterOutProvincialNames = (names: string[]): string[] => {
    if (this.provincialRuleTexts.size === 0) return names;
    return names.filter(name => !this.provincialRuleTexts.has(name));
  };

  preload(): void {
    this.loadRegulations().catch((err) => {
      console.warn('Regulations preload failed:', err);
    });
  }

  /**
   * Get waterbody names for a set of regulation IDs.
   * Used to filter name_variants to only show names relevant to specific regulations.
   */
  getWaterbodyNamesForIds(regulationIds: string | string[] | null | undefined): Set<string> {
    const names = new Set<string>();
    if (!this.regulations || !regulationIds) return names;

    let ids: string[] = [];
    if (Array.isArray(regulationIds)) {
      ids = regulationIds;
    } else {
      ids = String(regulationIds).replace(/[\[\]"\s]/g, '').split(',').filter(Boolean);
    }

    for (const id of ids) {
      const reg = this.regulations[id];
      if (reg?.waterbody_name) {
        names.add(reg.waterbody_name);
      }
    }
    return names;
  }
}

export const regulationsService = new RegulationsService();
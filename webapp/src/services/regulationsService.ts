/**
 * Regulations Service
 * 
 * Provides regulation lookup by ID. Data is loaded from the unified
 * waterbody_data.json via waterbodyDataService.
 *
 * Type hierarchy:
 *   WireRegulation  – raw JSON shape (synopsis regs lack identity fields)
 *   Regulation      – hydrated shape used by all consumers
 *   IdentityMeta    – per-synopsis-entry metadata (short keys from JSON)
 */

import { waterbodyDataService } from './waterbodyDataService';

// ── Shared sub-types ─────────────────────────────────────────────────

export interface ExclusionEntry {
  type: string;
  lookup_name: string;
  location_verbatim?: string | null;
  landmark_verbatim?: string | null;
  direction?: string | null;
  includes_tributaries?: boolean | null;
}

// ── Wire format (pre-hydration) ──────────────────────────────────────

/** Raw regulation as stored in waterbody_data.json before hydration.
 *  Synopsis regs carry `iid` but lack identity fields (waterbody_name,
 *  region, management_units, source_image, exclusions).
 *  Zone/provincial regs carry those fields directly. */
export interface WireRegulation {
  rule_text: string;
  restriction_type: string;
  restriction_details: string;
  dates: string[] | string | { period?: string; type?: string } | null;
  scope_type: string;
  scope_location: string | null;
  source?: 'synopsis' | 'provincial' | 'zone';
  zone_ids?: string[];
  feature_types?: string[] | null;
  /** Synopsis only: back-reference to identity_meta entry. */
  iid?: string;
  // Zone/provincial carry these directly; absent on synopsis wire regs.
  waterbody_name?: string;
  region?: string | null;
  management_units?: string[];
  source_image?: string | null;
  exclusions?: ExclusionEntry[] | null;
}

// ── Identity metadata (short keys from JSON) ─────────────────────────

/** Per-synopsis-entry metadata shared across sibling _ruleN regulations. */
export interface IdentityMeta {
  /** waterbody_name */
  wn: string;
  /** region */
  rg?: string;
  /** management_units */
  mu?: string[];
  /** source_image */
  img?: string;
  /** exclusions */
  ex?: ExclusionEntry[];
}

// ── Hydrated regulation (post-hydration) ─────────────────────────────

/** Fully hydrated regulation used throughout the app.
 *  After hydration, identity fields are guaranteed to be present
 *  regardless of source type. */
export interface Regulation {
  regulation_id: string;
  waterbody_name: string;
  region: string | null;
  management_units: string[];
  rule_text: string;
  restriction_type: string;
  restriction_details: string;
  dates: string[] | string | { period?: string; type?: string } | null;
  scope_type: string;
  scope_location: string | null;
  source?: 'synopsis' | 'provincial' | 'zone';
  zone_ids?: string[];
  feature_types?: string[] | null;
  /** Back-reference to identity_meta entry (synopsis regs only). */
  iid?: string;
  source_image?: string | null;
  exclusions?: ExclusionEntry[] | null;
}

type RegulationsLookup = Record<string, Regulation>;

class RegulationsService {
  private regulations: RegulationsLookup | null = null;
  private loadPromise: Promise<RegulationsLookup> | null = null;

  async loadRegulations(): Promise<RegulationsLookup> {
    if (this.regulations) return this.regulations;
    if (this.loadPromise) return this.loadPromise;

    // Load from unified waterbody_data.json via shared service
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

  preload(): void {
    this.loadRegulations().catch((err) => {
      console.warn('Regulations preload failed:', err);
    });
  }
}

export const regulationsService = new RegulationsService();
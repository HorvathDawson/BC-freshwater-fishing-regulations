/**
 * Regulations Service
 * * Loads and caches regulation data from the static JSON file.
 */

export interface Regulation {
  waterbody_name: string;
  waterbody_key: string | null;
  region: string | null;
  management_units: string[];
  rule_text: string;
  restriction_type: string;
  restriction_details: string;
  dates: string[] | string | null;
  scope_type: string;
  scope_location: string | null;
  includes_tributaries: boolean | null;
}

type RegulationsLookup = Record<string, Regulation>;

class RegulationsService {
  private regulations: RegulationsLookup | null = null;
  private loadPromise: Promise<RegulationsLookup> | null = null;

  async loadRegulations(): Promise<RegulationsLookup> {
    if (this.regulations) return this.regulations;
    if (this.loadPromise) return this.loadPromise;

    // IMPORTANT: Ensure this path matches exactly where your file is in /public
    this.loadPromise = fetch('/data/regulations.json')
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
        }
        return response.json();
      })
      .then(data => {
        this.regulations = data;
        this.loadPromise = null;
        console.log("✅ Regulations JSON Loaded. Total keys:", Object.keys(data).length);
        return data;
      })
      .catch(error => {
        this.loadPromise = null;
        console.error('❌ Failed to load regulations.json:', error);
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
          return match;
        })
        .filter(Boolean);

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
    this.loadRegulations().catch(() => {});
  }
}

export const regulationsService = new RegulationsService();
/**
 * Regulations service (mobile) — faithful port of
 * `webapp/src/services/regulationsService.ts`.
 *
 * The only difference from the web version is the data source: instead of
 * `waterbodyDataService.getRegulations()` returning a preloaded map, the
 * mobile `waterbodyDataService` exposes the already-expanded regulation map
 * via `allRegulations` after `init()`. Provenance/tributary-filtering logic is
 * copied verbatim so InfoPanel renders identical rule cards.
 */
import type { Regulation, RegulationProvenance } from '../types/regulations';
import { waterbodyDataService } from './waterbodyDataService';

export type { Regulation, RegulationProvenance };

class RegulationsService {
  private get regulations(): Record<string, Regulation> {
    return waterbodyDataService.allRegulations;
  }

  getRegulations(
    regulationIds: string | string[] | null | undefined,
  ): Regulation[] {
    if (!regulationIds || regulationIds === '' || regulationIds === 'null') {
      return [];
    }

    let ids: string[] = [];
    if (Array.isArray(regulationIds)) {
      ids = regulationIds.map((id) => String(id).trim());
    } else {
      ids = String(regulationIds)
        .replace(/[[\]"\s]/g, '')
        .split(',')
        .filter(Boolean);
    }

    return ids
      .map((id) => {
        const match = this.regulations[id];
        if (!match) console.warn(`⚠️ No match found for regulation ID: "${id}"`);
        return match ? { ...match, regulation_id: id } : null;
      })
      .filter((r): r is Regulation => Boolean(r));
  }

  getRegulation(regulationId: string): Regulation | null {
    if (!regulationId) return null;
    return this.regulations[regulationId] || null;
  }

  /**
   * Look up regulations for a specific reach and stamp provenance.
   * Verbatim port of the web logic (tributary filtering + provenance labels).
   */
  getRegulationsForReach(
    regulationIds: string | string[] | null | undefined,
    tributaryRegIds: string[] = [],
  ): Regulation[] {
    const regs = this.getRegulations(regulationIds);
    if (!regs.length) return regs;

    const tribSet = new Set(tributaryRegIds);

    return regs
      .filter((reg) => {
        if (reg.source !== 'synopsis') return true;
        const isTribSourced =
          tribSet.size > 0 && tribSet.has(reg.iid || reg.regulation_id);
        if (!isTribSourced) return true;
        return reg.effective_includes_tributaries !== false;
      })
      .map((reg) => {
        let provenance: RegulationProvenance;
        if (reg.source === 'zone') {
          provenance = 'zone';
        } else if (reg.source === 'provincial') {
          provenance = 'provincial';
        } else if (tribSet.size > 0 && tribSet.has(reg.iid || reg.regulation_id)) {
          provenance = 'tributary';
        } else {
          provenance = 'direct';
        }
        return { ...reg, provenance };
      });
  }
}

export const regulationsService = new RegulationsService();

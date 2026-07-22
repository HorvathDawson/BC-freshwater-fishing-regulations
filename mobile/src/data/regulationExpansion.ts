/**
 * Regulation expansion — ported verbatim from
 * `webapp/src/services/waterbodyDataService.ts` (expandRegulations).
 *
 * The mobile `regulations` SQLite table stores each reg in the SAME raw shape
 * as tier0.json's `regulations` section, and `reg_sets` stores the raw
 * comma-joined ID strings. So the identical expansion runs on device to
 * produce the flat per-rule {@link Regulation} objects the UI consumes.
 */
import type { Regulation } from '../types/regulations';

// ── Raw JSON shapes (from regulation_index.json / tier0) ─────────────

interface SynopsisReg {
  water: string;
  region: string;
  mu: string[];
  raw_regs: string;
  symbols: string[];
  source: 'synopsis';
  page: number;
  image: string;
  match_type?: 'direct' | 'admin' | 'unmatched';
  parsed?: {
    regs_verbatim: string;
    includes_tributaries: boolean;
    tributary_only?: boolean;
    entry_location_text?: string;
    rules: {
      rule_text: string;
      restriction_type: string;
      details: string;
      location_text?: string;
      dates?: string[];
      exception?: string;
      includes_tributaries?: boolean | null;
    }[];
  };
}

interface BaseReg {
  raw_regs: string;
  source: 'zone' | 'provincial' | 'municipal' | 'land_access';
  restriction?: { type: string; details: string };
  zone_ids?: string[];
  dates?: string[];
  scope_location?: string;
  notes?: string;
  zone?: string;
}

export type RawRegulation = SynopsisReg | BaseReg;

/**
 * Expand raw regulations into flat per-rule Regulation objects and rewrite
 * reg_sets to reference the expanded rule IDs. See the web original for the
 * full contract; this is a faithful port.
 */
export function expandRegulations(
  rawRegs: Record<string, RawRegulation>,
  rawRegSets: string[],
): { regulations: Record<string, Regulation>; regSets: string[] } {
  const regulations: Record<string, Regulation> = {};
  const expansionMap: Record<string, string[]> = {};

  for (const [regId, raw] of Object.entries(rawRegs)) {
    if (raw.source === 'synopsis') {
      const syn = raw as SynopsisReg;
      if (syn.parsed?.rules?.length) {
        const entryIncludesTribs = syn.parsed.includes_tributaries === true;
        const ruleIds: string[] = [];
        for (let i = 0; i < syn.parsed.rules.length; i++) {
          const rule = syn.parsed.rules[i];
          const ruleId = `${regId}_rule${i}`;
          ruleIds.push(ruleId);
          const effectiveIncludesTribs =
            rule.includes_tributaries == null
              ? entryIncludesTribs
              : rule.includes_tributaries === true;
          regulations[ruleId] = {
            regulation_id: ruleId,
            waterbody_name: syn.water || '',
            region: syn.region || null,
            management_units: syn.mu || [],
            rule_text: rule.rule_text,
            restriction_type: rule.restriction_type,
            restriction_details: rule.details || '',
            dates: rule.dates?.length ? rule.dates : null,
            scope_type: rule.location_text ? 'location' : 'waterbody',
            scope_location: rule.location_text || null,
            restriction_exception: rule.exception || undefined,
            source: 'synopsis',
            iid: regId,
            source_image: syn.image || null,
            source_page: syn.page ?? null,
            exclusions: null,
            effective_includes_tributaries: effectiveIncludesTribs,
          };
        }
        expansionMap[regId] = ruleIds;
      } else {
        regulations[regId] = {
          regulation_id: regId,
          waterbody_name: syn.water || '',
          region: syn.region || null,
          management_units: syn.mu || [],
          rule_text: syn.raw_regs || '',
          restriction_type: 'notice',
          restriction_details: syn.raw_regs || '',
          dates: null,
          scope_type: 'waterbody',
          scope_location: null,
          source: 'synopsis',
          iid: regId,
          source_image: syn.image || null,
          source_page: syn.page ?? null,
          exclusions: null,
          effective_includes_tributaries: true,
        };
        expansionMap[regId] = [regId];
      }
    } else {
      const base = raw as BaseReg;
      regulations[regId] = {
        regulation_id: regId,
        waterbody_name: '',
        region: null,
        management_units: [],
        rule_text: base.raw_regs || '',
        restriction_type: base.restriction?.type?.toLowerCase() || 'notice',
        restriction_details: base.restriction?.details || '',
        dates: base.dates?.length ? base.dates : null,
        scope_type: base.scope_location ? 'location' : 'waterbody',
        scope_location: base.scope_location || null,
        source: base.source,
        zone_ids: base.zone_ids,
        exclusions: null,
      };
      expansionMap[regId] = [regId];
    }
  }

  const regSets = rawRegSets.map((rsStr) => {
    const origIds = rsStr.split(',').filter(Boolean);
    const expanded = origIds.flatMap((id) => expansionMap[id] || [id]);
    return expanded.join(',');
  });

  return { regulations, regSets };
}

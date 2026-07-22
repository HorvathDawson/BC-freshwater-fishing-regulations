/**
 * Data types — ported verbatim from
 * `webapp/src/services/waterbodyDataService.ts` so the mobile data layer emits
 * exactly the shapes the ported UI components expect.
 *
 * These are the on-device analogues of the tier0.json / resolve shapes; the
 * SQLite services in `src/data` reconstruct them from the bundled database.
 */

/** Name variant with source provenance. */
export interface NameVariantEntry {
  name: string;
  source: 'direct' | 'tributary' | 'admin' | 'bathymetry' | 'stocking' | 'marker';
}

/** A bathymetry survey depth-map sheet. URL = <R2 base>/bathymetry/<pdf>. */
export interface BathymetrySurvey {
  pdf: string;
  title: string;
}

/** gofishbc.com "Where To Fish" map marker enrichment — amenity/access
 * whitelist only (see pipeline/recurring/waterbody_db/export.py). Not yet
 * surfaced in any UI — attached to the reach for future display work. */
export interface MarkerInfo {
  more_info?: string;
  description?: string;
  photo_url?: string;
  road_access?: string;
  boat_launch?: string;
  fishing_dock?: string;
  campsite?: string;
  washroom?: string;
  wheelchair_access?: string;
  hike_in_required?: string;
}

/** Reach metadata (from reaches table / resolve). */
export interface Reach {
  display_name: string;
  name_variants: NameVariantEntry[];
  feature_type: string;
  reg_set_index: number;
  watershed_code: string;
  min_zoom: number;
  regions: string[];
  bbox: [number, number, number, number] | null;
  length_km: number;
  tributary_reg_ids?: string[];
  bathymetry?: BathymetrySurvey[];
  marker?: MarkerInfo;
}

/** Enriched segment from the tier0/search row. */
export interface Tier0Segment {
  rid: string;
  display_name: string;
  name_variants: NameVariantEntry[];
  feature_type: string;
  reg_set_index: number;
  watershed_code: string;
  min_zoom: number;
  regions: string[];
  bbox: [number, number, number, number] | null;
  length_km: number;
  waterbody_group: string;
  fids: string[];
  tributary_reg_ids?: string[];
}

/** A town near a feature (search only). */
export interface NearbyTown {
  name: string;
  km?: number;
}

/** Search index entry (tier0 enriched format with segments). */
export interface SearchEntry {
  display_name: string;
  name_variants: NameVariantEntry[];
  segments: Tier0Segment[];
  feature_type: string;
  regions: string[];
  min_zoom: number;
  bbox: [number, number, number, number] | null;
  waterbody_group: string;
  zones: string[];
  management_units: string[];
  total_length_km: number;
  nearby_towns?: (string | NearbyTown)[];
}

/** Provenance label: how a regulation reached a specific reach. */
export type RegulationProvenance = 'direct' | 'tributary' | 'zone' | 'provincial';

/** Flat regulation shape matching what InfoPanel expects. */
export interface Regulation {
  regulation_id: string;
  waterbody_name: string;
  region: string | null;
  management_units: string[];
  rule_text: string;
  restriction_type: string;
  restriction_details: string;
  dates: string[] | string | { period: string } | null;
  scope_type: string;
  scope_location: string | null;
  restriction_exception?: string;
  source: 'synopsis' | 'provincial' | 'zone' | 'municipal' | 'land_access';
  zone_ids?: string[];
  feature_types?: string[] | null;
  iid?: string;
  source_image?: string | null;
  source_page?: number | null;
  exclusions?: {
    lookup_name: string;
    direction?: string;
    landmark_verbatim?: string;
    includes_tributaries?: boolean;
  }[] | null;
  provenance?: RegulationProvenance;
  effective_includes_tributaries?: boolean;
}

/** Per-layer admin visibility config (from admin_visibility.json). */
export interface AdminLayerVisibility {
  display: 'all' | 'regulated_only';
  regulated_ids?: string[];
}
export type AdminVisibility = Record<string, AdminLayerVisibility>;

/** A single in-season regulation change resolved to reach IDs. */
export interface InSeasonChange {
  water: string;
  region: string;
  change: string;
  effective_date: string;
  reach_ids: string[];
  match_status: string;
}

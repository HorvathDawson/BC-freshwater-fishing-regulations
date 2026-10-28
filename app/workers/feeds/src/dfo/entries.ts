// T225 port of the read side of pipeline/regs/dfo_salmon/entries.py: the curated entry file as the live run reads it
// (`load`, `by_fingerprint`, `reconcile`). Nothing here writes a curated file — the Python `queue` / `seed` stay human.
import { ratio } from "./difflib";
import { PAGES } from "./fetch";
import type { Location, Structure } from "./locations";
import { normalize } from "./locations";
import { key } from "./names";
import { PyFloat } from "./pyjson";
import { formatPercent, pyRound, reprList } from "./py";

export interface WaterBinding {
  water_id: string;
  name: string;
  aliases: string[];
  item_ids: string[];
}
export interface EntryLocation {
  location_id: string;
  kind: string;
  precedence: number;
  water: string;
  water_id?: string | null;
  section?: string | null;
  fingerprints: string[];
  source_text: { specific_area?: string; landmarks?: string[] } & Record<string, unknown>;
  status: string;
}
export interface EntryFile {
  region: string;
  region_number: number;
  region_name: string;
  structure: Partial<Structure> & Record<string, unknown>;
  waters: WaterBinding[];
  locations: EntryLocation[];
}

/** entries.load from the file's JSON text (null: no file — an empty entry file, as the Python makes). */
export function loadEntryFile(slug: string, text: string | null): EntryFile {
  const pg = PAGES[slug]!;
  if (text === null) return { region: slug, region_number: pg.region, region_name: pg.name, structure: {}, waters: [], locations: [] };
  const d = JSON.parse(text);
  return {
    region: d.region,
    region_number: d.region_number,
    region_name: d.region_name,
    structure: d.structure || {},
    waters: (d.waters || []).map((w: WaterBinding) => ({ ...w, aliases: w.aliases || [], item_ids: w.item_ids || [] })),
    locations: (d.locations || []).map((l: EntryLocation) => ({
      ...l,
      fingerprints: l.fingerprints || [],
      source_text: l.source_text || {},
      status: l.status ?? "active",
      section: l.section ?? null,
    })),
  };
}

export function byFingerprint(ef: EntryFile): Map<string, EntryLocation> {
  const m = new Map<string, EntryLocation>();
  for (const loc of ef.locations) for (const fp of loc.fingerprints) m.set(fp, loc);
  return m;
}

export interface Candidate {
  location_id: string;
  score: PyFloat;
  text_similarity: PyFloat;
  landmark_overlap: PyFloat;
  specific_area: string;
}
export interface Outcome {
  status: string;
  fingerprint: string;
  location_id: string | null;
  water: string;
  specific_area: string;
  detail: string;
  candidates: Candidate[];
}
export interface Report {
  region: string;
  outcomes: Outcome[];
  structural: string[];
}

const STRUCTURAL_UNBOUND = 0.25;
const STRUCTURAL_COUNT = 0.2;

function candidates(loc: Location, ef: EntryFile, limit = 3): Candidate[] {
  const out: { c: Candidate; s: number }[] = [];
  const want = new Set(loc.landmarks);
  const locKey = key(loc.water);
  const locNorm = normalize(loc.specific_area);
  for (const cand of ef.locations) {
    if (key(cand.water) !== locKey) continue;
    const text = ratio(normalize(cand.source_text.specific_area ?? ""), locNorm);
    const have = new Set(cand.source_text.landmarks || []);
    const union = new Set([...want, ...have]);
    const inter = [...want].filter((w) => have.has(w)).length;
    const overlap = union.size ? inter / union.size : 0.0;
    const score = Math.max(text, overlap);
    if (score >= 0.5)
      out.push({
        s: pyRound(score, 3),
        c: {
          location_id: cand.location_id,
          score: new PyFloat(pyRound(score, 3)),
          text_similarity: new PyFloat(pyRound(text, 3)),
          landmark_overlap: new PyFloat(pyRound(overlap, 3)),
          specific_area: cand.source_text.specific_area ?? "",
        },
      });
  }
  // sorted(key=-score): stable
  return out
    .map((x, i) => ({ ...x, i }))
    .sort((a, b) => b.s - a.s || a.i - b.i)
    .slice(0, limit)
    .map((x) => x.c);
}

/** entries.reconcile: diff a fresh scrape against the curated file. Never mutates `ef`. */
export function reconcile(ef: EntryFile, locations: Location[], structure: Structure): Report {
  const rep: Report = { region: ef.region, outcomes: [], structural: [] };
  const index = byFingerprint(ef);
  const matched = new Set<string>();
  if (Object.keys(ef.structure).length) {
    const oldSecs = (ef.structure.sections as string[] | undefined) || [];
    const newSecs = structure.sections || [];
    if (oldSecs.length !== newSecs.length || oldSecs.some((s, i) => s !== newSecs[i]))
      rep.structural.push(`section set changed: ${reprList(oldSecs)} -> ${reprList(newSecs)}`);
    const oldN = (ef.structure.n_locations as number | undefined) || 0;
    const newN = structure.n_locations || 0;
    if (oldN && Math.abs(newN - oldN) / oldN > STRUCTURAL_COUNT)
      rep.structural.push(
        `location count moved ${oldN} -> ${newN} (${formatPercent((newN - oldN) / oldN, true)}, threshold ±${formatPercent(STRUCTURAL_COUNT)})`,
      );
  }
  for (const loc of locations) {
    const hit = index.get(loc.fingerprint);
    if (hit) {
      matched.add(hit.location_id);
      if ((hit.section || "") !== (loc.section || ""))
        rep.outcomes.push({
          status: "section_moved",
          fingerprint: loc.fingerprint,
          location_id: hit.location_id,
          water: loc.water,
          specific_area: loc.specific_area,
          detail: `section ${pyReprOpt(hit.section)} -> ${pyReprOpt(loc.section)}; re-scopes everything bound under it`,
          candidates: [],
        });
      else if (hit.status === "dormant")
        rep.outcomes.push({ status: "revived", fingerprint: loc.fingerprint, location_id: hit.location_id, water: loc.water, specific_area: loc.specific_area, detail: "dormant location is published again", candidates: [] });
      else
        rep.outcomes.push({ status: "ok", fingerprint: loc.fingerprint, location_id: hit.location_id, water: loc.water, specific_area: loc.specific_area, detail: "", candidates: [] });
      continue;
    }
    const cands = candidates(loc, ef);
    rep.outcomes.push({
      status: cands.length ? "drift" : "new",
      fingerprint: loc.fingerprint,
      location_id: null,
      water: loc.water,
      specific_area: loc.specific_area,
      detail: cands.length ? "wording changed; confirm the rebind" : "no known location on this water",
      candidates: cands,
    });
  }
  for (const cand of ef.locations)
    if (!matched.has(cand.location_id) && cand.status === "active")
      rep.outcomes.push({
        status: "dormant",
        fingerprint: cand.fingerprints.length ? cand.fingerprints[cand.fingerprints.length - 1]! : "",
        location_id: cand.location_id,
        water: cand.water,
        specific_area: cand.source_text.specific_area ?? "",
        detail: "absent from the page; binding kept",
        candidates: [],
      });
  const unbound = rep.outcomes.filter((o) => o.status === "drift" || o.status === "new").length;
  if (locations.length && unbound / locations.length > STRUCTURAL_UNBOUND)
    rep.structural.push(
      `${unbound}/${locations.length} locations unbound (${formatPercent(unbound / locations.length)} > ${formatPercent(STRUCTURAL_UNBOUND)}) — the table was probably rebuilt; hold the region`,
    );
  return rep;
}

const pyReprOpt = (s: string | null | undefined): string => reprList([s ?? null]).slice(1, -1);

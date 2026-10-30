/**
 * Reading a spot written by an older app.
 *
 * OWNER DECISION 2026-10-20 (AGENTS 5): a saved spot holds a coordinate and the water's `item_id`, and NO section id.
 * Spots saved before that carry three: `section` itself, a trace `path` of section handles, and (should a panel ever
 * have been frozen with its routes resolved) a `route.path` on each donor row. They are people's records and must
 * still load, so the handles are dropped here, on every load, and the next save writes the spot back without them.
 * The trace keeps what it said: its reach COUNT stands in for the handles.
 *
 * Only what is present is changed. A key that was not there is not added, so a partial record stays as it was.
 */
import type { PanelAnswer } from "../panel";
import type { Spot } from "./model";

/** A panel answer with no donor route on any row: routes are a walk over section handles. */
export function stripRoutes(p: PanelAnswer): PanelAnswer {
  if (!p.rows.some((r) => "route" in r)) return p;
  return { ...p, rows: p.rows.map(({ route: _route, ...row }) => row) };
}

type Raw = Record<string, unknown>;
const isObj = (v: unknown): v is Raw => typeof v === "object" && v !== null && !Array.isArray(v);

/**
 * One stored spot as the current model has it: no section id anywhere.
 *
 * T294: a spot is its first visit's record plus `visits` (the later ones), so a spot saved before visits existed loads
 * as a spot of ONE visit with nothing moved or rewritten (`./visits`). Each later visit is cleaned the same way the
 * first is (no section handles); a `visits` that is not a list, or an entry that is not a visit, is not one.
 */
export function fromDisk(raw: unknown): Spot {
  if (!isObj(raw)) return raw as Spot;
  const spot = clean(raw);
  if ("visits" in spot) {
    if (!Array.isArray(spot.visits)) delete spot.visits;
    else spot.visits = spot.visits.filter((v) => isObj(v) && typeof v.id === "string" && typeof v.visitedAt === "number")
      .map((v) => clean(v as Raw));
  }
  return spot as unknown as Spot;
}

/** One visit's record (the spot's own first visit, or a later one) with its section handles dropped. */
function clean(raw: Raw): Raw {
  const { section: _section, ...spot } = raw;
  const trace = spot.trace;
  if (isObj(trace) && "path" in trace) {
    const { path, ...facts } = trace;
    spot.trace = { ...facts, reaches: Array.isArray(path) ? path.length : 0 };
  }
  const panel = spot.panel;
  if (isObj(panel) && Array.isArray(panel.rows)) spot.panel = stripRoutes(panel as unknown as PanelAnswer);
  // S1: `photos` is read as a list everywhere (the store deletes the ones a save drops), so a record without one reads
  // as having none. `leadUp` is NOT added: absent reads as "not recorded", which is what it is, and a refresh fills it.
  if ("photos" in spot && !Array.isArray(spot.photos)) spot.photos = [];
  return spot;
}

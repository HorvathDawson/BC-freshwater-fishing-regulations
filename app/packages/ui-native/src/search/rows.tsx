/** The rows search's sheet draws (`sheetRows`), from what the reader picked and what search answered. Pure (a .tsx so the
 * T60 copy scan reads its words with the sheet's). */
import { SEARCH_BAR, nearCount, watersNear } from "@app/core";
import type { ItemId, NameHit, NearHit, PlaceHit } from "@app/data";
import { resultGroups, type SearchPick, type SearchSession } from "@app/ui";

export type Row =
  | { t: "head"; key: string; label: string; right?: string; back?: boolean; place?: boolean;
      /** Drawn on the places band — the heading of the places group. */
      band?: boolean }
  | { t: "place"; key: string; place: PlaceHit }
  | { t: "water"; key: string; hit: NameHit; dup: boolean }
  | { t: "near"; key: string; near: NearHit }
  /** A group name that only points: its line (`seeLine`) and a way to open each water it names. */
  | { t: "see"; key: string; line: string; see: readonly { name: string; item: ItemId | null }[] }
  | { t: "note"; key: string; text: string };

/** The rows the sheet draws, from what the reader picked and what search answered. Pure: the render test reads it. */
export function sheetRows(pick: SearchPick, s: SearchSession): Row[] {
  const rows: Row[] = [];
  const place = pick.town;
  if (place) {
    const near = s.town.state === "ready" && s.town.value ? s.town.value.near : [];
    rows.push({ t: "head", key: "back", label: "All results", back: true,
                right: s.town.state === "ready" ? nearCount(near.length) : "" });
    rows.push({ t: "head", key: "near-h", label: `Water near ${place.name}`, place: true });
    if (s.town.state === "failed")
      rows.push({ t: "note", key: "fail", text: "Could not read what is near this town." });
    else if (s.town.state === "ready" && !near.length)
      rows.push({ t: "note", key: "none", text: watersNear(0, place.name) });
    for (const n of near) rows.push({ t: "near", key: `n:${n.item}`, near: n });
  } else if (s.typed) {
    for (const g of resultGroups(s.results)) {
      if (g.kind === "places") {
        rows.push({ t: "head", key: "ph", label: g.title, right: g.note, place: true, band: true });
        for (const p of g.places) rows.push({ t: "place", key: `p:${p.place}`, place: p });
      } else if (g.kind === "see") {
        rows.push({ t: "head", key: "sh", label: g.title });
        for (const p of g.pointers) rows.push({ t: "see", key: `s:${p.hit.entry}`, line: p.line, see: p.hit.see });
      } else {
        rows.push({ t: "head", key: "wh", label: g.title });
        for (const w of g.waters) rows.push({ t: "water", key: `w:${w.hit.item}`, hit: w.hit, dup: w.dup });
      }
    }
    if (s.none) rows.push({ t: "note", key: "none", text: SEARCH_BAR.none });
    if (s.failed) rows.push({ t: "note", key: "fail", text: SEARCH_BAR.failed });
  } else {
    rows.push({ t: "note", key: "start", text: SEARCH_BAR.start });
  }
  return rows;
}

/**
 * (T858) THE KEYBOARD'S STOPS IN SEARCH'S LIST, on the web: what ↓ / ↑ move the highlight through from the field, in
 * the order the eye reads them, and Enter presses (the stop's own click, `listKeys.web.ts`). Pure: the sheet's rows
 * (`sheetRows`) and the pick in, the stops out. Every control the list draws is a stop — "← All results", each result
 * row, the selected water's open action (›) after its row, a pointer's waters, a recent search — so a keyboard reaches
 * everything a click does, by the same press. A heading, a note and the recents' Clear are not stops.
 */
import { isSelected, type SearchPick } from "@app/ui";
import type { Row } from "./rows";

/** The stops, in reading order. The open action follows its row, as it sits beside it. */
export function keyStops(rows: readonly Row[], pick: SearchPick): string[] {
  return stopsAt(rows, pick).map((s) => s.stop);
}

/** Which row of the list draws a stop (its index in `rows`), or -1: what the sheet scrolls to when it is not drawn yet. */
export function stopRow(rows: readonly Row[], pick: SearchPick, stop: string): number {
  return stopsAt(rows, pick).find((s) => s.stop === stop)?.row ?? -1;
}

function stopsAt(rows: readonly Row[], pick: SearchPick): { stop: string; row: number }[] {
  const stops: { stop: string; row: number }[] = [];
  rows.forEach((r, row) => {
    const add = (stop: string) => stops.push({ stop, row });
    if (r.t === "head") { if (r.back) add(r.key); }
    else if (r.t === "place") add(r.key);
    else if (r.t === "water" || r.t === "near") {
      add(r.key);
      const item = r.t === "water" ? r.hit.item : r.near.item;
      const name = r.t === "water" ? r.hit.name : r.near.name;
      if (isSelected(pick, { kind: "water", item, name })) add(goStop(r.key));
    } else if (r.t === "see") {
      for (const s of r.see) if (s.item !== null) add(seeStop(r.key, s.item));
    } else if (r.t === "recents") {
      for (const q of r.list) add(recentStop(q));
    }
  });
  return stops;
}

export const goStop = (rowKey: string): string => `go:${rowKey}`;
export const seeStop = (rowKey: string, item: string): string => `see:${rowKey}:${item}`;
export const recentStop = (q: string): string => `recent:${q}`;
/** A stop that opens a water: the page moves on, so the focus is not handed back to the field after it. */
export const leavesSearch = (stop: string): boolean => stop.startsWith("go:") || stop.startsWith("see:");

/** A stop's element id on the page (no spaces: `aria-activedescendant` is one id). */
export const stopDomId = (stop: string): string => `search-stop-${encodeURIComponent(stop)}`;

/**
 * One arrow: ↓ from the field (nothing highlighted) goes to the first stop, then down the list, and stays on the last;
 * ↑ goes back up, and from the first stop back to the field (null). A highlight no longer in the list (the results
 * changed) starts again from the field.
 */
export function stepStop(stops: readonly string[], active: string | null, dir: "down" | "up"): string | null {
  const at = active === null ? -1 : stops.indexOf(active);
  if (dir === "down") return stops.length === 0 ? null : stops[Math.min(at + 1, stops.length - 1)]!;
  return at <= 0 ? null : stops[at - 1]!;
}

/** (listKeys.web.ts) the field's combobox attributes on the web; none on a phone. */
export interface FieldA11y {
  role?: "combobox";
  "aria-expanded"?: boolean;
  "aria-controls"?: string;
  "aria-activedescendant"?: string;
  "aria-autocomplete"?: "list";
}
/** (listKeys.web.ts) the field's and the list's keyboard, as the Shell is handed it. */
export interface ListKeys {
  /** The highlighted stop (`keyStops`), or null: the field itself. Always null on a phone. */
  active: string | null;
  /** A key pressed in the field while search is open: true when it was the list's (the field then leaves it alone). */
  onKey: (key: string) => boolean;
  field: FieldA11y;
}

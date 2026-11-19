import { useCallback, useEffect, useRef, useState, type RefObject } from "react";
import { leavesSearch, stepStop, stopDomId, type FieldA11y, type KeyMods, type ListKeys } from "./keys";


/** The results panel's id: what the field's `aria-controls` names. */
const RESULTS_ID = "search-results";

/**
 * (T858, owner 2026-11-18: "keyboard navigation like in search bar using arrows to move highlight") THE KEYBOARD IN
 * SEARCH'S LIST, on the web. The focus never leaves the field: ↓ / ↑ move a highlight from it through the list's stops
 * (`keyStops`) and, from the first, back to it; Enter presses the highlighted stop — its own element's click, the very
 * path a mouse takes (a row selects and frames, a town shows its waters, the › opens the water); Escape stays the
 * field's (it closes search). The field is a combobox and names the highlighted stop (`aria-activedescendant`), so a
 * screen reader says it as the highlight moves. Typing, a new list, or search closing forgets the highlight.
 *
 * A press that keeps search open (a row, a town, "← All results", a recent search) hands the focus back to the field —
 * a pick blurs it (`searchAct`, for a phone's keyboard) — so the next ↓ still works; one that opens a water does not.
 */
export function useListKeys({ open, stops, query, field }: {
  open: boolean; stops: readonly string[]; query: string; field: RefObject<{ focus(): void } | null>;
}): ListKeys {
  const [active, setActive] = useState<string | null>(null);
  useEffect(() => { setActive(null); }, [query, open]);
  // a highlight whose stop is gone (the list changed under it) is no highlight — and is forgotten, so the stop coming
  // back (a town picked, then "← All results") does not bring the old highlight back with it
  const live = open && active !== null && stops.includes(active) ? active : null;
  useEffect(() => { if (active !== null && live === null) setActive(null); }, [active, live]);
  const now = useRef({ stops, live });
  now.current = { stops, live };
  const onKey = useCallback((key: string, mods?: KeyMods): boolean => {
    // with a modifier the key is the field's own (Shift+↑ selects to the start, Cmd+↓ jumps to the end)
    if (!open || mods?.shiftKey || mods?.metaKey || mods?.ctrlKey || mods?.altKey) return false;
    if (key === "ArrowDown" || key === "ArrowUp") {
      setActive(stepStop(now.current.stops, now.current.live, key === "ArrowDown" ? "down" : "up"));
      return true;                              // the caret stays where it is
    }
    const stop = now.current.live;
    if (key !== "Enter" || stop === null) return false;
    const el = document.getElementById(stopDomId(stop));
    if (!el) return false;
    el.click();
    if (!leavesSearch(stop)) field.current?.focus();
    return true;
  }, [open, field]);
  const a11y: FieldA11y = open
    ? { role: "combobox", "aria-expanded": true, "aria-controls": RESULTS_ID, "aria-autocomplete": "list",
        ...(live !== null ? { "aria-activedescendant": stopDomId(live) } : null) }
    : { role: "combobox", "aria-expanded": false, "aria-autocomplete": "list" };
  return { active: live, onKey, field: a11y };
}

export function stopProps(stop: string): { nativeID?: string } { return { nativeID: stopDomId(stop) }; }
export function resultsProps(): { nativeID?: string } { return { nativeID: RESULTS_ID }; }
export function revealStop(stop: string): boolean {
  const el = document.getElementById(stopDomId(stop));
  if (!el) return false;
  el.scrollIntoView?.({ block: "nearest" });
  return true;
}

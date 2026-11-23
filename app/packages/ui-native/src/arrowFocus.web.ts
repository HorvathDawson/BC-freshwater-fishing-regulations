/**
 * (T858) ARROW KEYS THROUGH A LIST OF CONTROLS, on the web: the water's parts list, Spots, the menu pages. Put on the
 * list's container, ↓ / ↑ move the focus to the next / previous control inside it (a button, a radio, a link: whatever
 * Tab stops at), Home / End to the first / last; Enter and Space press the focused control as they always did. The focus
 * only moves, nothing is pressed. A key meant for a text field (typing, its own arrows) is left alone, as is any key
 * with a modifier held. The scroll follows the focus (the browser's own).
 */
const STOPS = 'a[href], button, [role="button"], [role="radio"], [role="link"], [role="menuitem"], [tabindex="0"]';

export function arrowFocusProps(): { onKeyDown: (e: unknown) => void } {
  return { onKeyDown: (e) => {
    const k = e as { key: string; target: EventTarget | null; currentTarget: Element; shiftKey?: boolean; metaKey?: boolean;
                     ctrlKey?: boolean; altKey?: boolean; preventDefault(): void };
    if (k.shiftKey || k.metaKey || k.ctrlKey || k.altKey) return;
    if (k.key !== "ArrowDown" && k.key !== "ArrowUp" && k.key !== "Home" && k.key !== "End") return;
    const t = k.target as HTMLElement | null;
    if (!t || t.closest?.('input, textarea, select, [contenteditable="true"]')) return;
    const stops = [...k.currentTarget.querySelectorAll<HTMLElement>(STOPS)].filter(
      (el) => el.getAttribute("aria-disabled") !== "true" && el.getAttribute("aria-hidden") !== "true" && !el.closest('[aria-hidden="true"]'));
    if (stops.length === 0) return;
    const at = stops.findIndex((el) => el === t || el.contains(t));
    const to = k.key === "Home" ? 0 : k.key === "End" ? stops.length - 1
      : k.key === "ArrowDown" ? Math.min(at + 1, stops.length - 1) : Math.max(at - 1, 0);
    if (at === -1 && (k.key === "ArrowUp")) return;
    k.preventDefault();                  // the page does not scroll under the arrow
    stops[to]?.focus();
  } };
}

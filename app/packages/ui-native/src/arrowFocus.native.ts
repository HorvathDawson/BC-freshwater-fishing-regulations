/** A phone has no keyboard focus to move: no props. The web twin (arrowFocus.web.ts) moves it. */
export function arrowFocusProps(): { onKeyDown?: (e: unknown) => void } { return {}; }

/** A focused field holds the on-screen keyboard up: closing search drops the focus (the web twin keeps it, T69). */
export const CLOSE_KEEPS_FOCUS: boolean = false;

/** No focused-but-closed field on a phone: its focus always opens search, so a tap needs nothing of its own. */
export function closedFieldClick(_open: () => void): object {
  return {};
}

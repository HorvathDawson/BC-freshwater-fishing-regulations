/** The browser window's size; null where there is no window (a server render). The phone twin is windowSize.native.ts. */
export function windowSize(): { w: number; h: number } | null {
  return typeof window === "undefined" ? null : { w: window.innerWidth, h: window.innerHeight };
}

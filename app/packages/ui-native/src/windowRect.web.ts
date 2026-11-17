import type { Rect } from "./coachScroll";

/** The browser's box for a react-native-web view (a DOM element), at once; not an element (a test's stand-in): undefined. */
export function windowRect(node: unknown): Rect | null | undefined {
  const el = node as { getBoundingClientRect?: () => DOMRect; isConnected?: boolean } | null;
  if (!el || typeof el.getBoundingClientRect !== "function") return undefined;
  if (el.isConnected === false) return null;
  const b = el.getBoundingClientRect();
  return b.width > 0 && b.height > 0 ? { x: b.left, y: b.top, w: b.width, h: b.height } : null;
}

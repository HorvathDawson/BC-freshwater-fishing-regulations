import type { Rect } from "./coachScroll";

/** A phone's views are measured on the native side (`measureInWindow`): no answer here. */
export function windowRect(_node: unknown): Rect | null | undefined { return undefined; }

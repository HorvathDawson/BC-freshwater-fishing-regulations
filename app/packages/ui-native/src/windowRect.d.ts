import type { Rect } from "./coachScroll";
/**
 * (T824, owner on iPhone: "the highlighting following the items with a delay") A VIEW'S BOX IN THE WINDOW, READ NOW where
 * the platform can: react-native-web's `measureInWindow` answers a task later (a `setTimeout`), so a highlight measured
 * through it every frame still trailed a scrolling page by several frames; the browser's own `getBoundingClientRect` is
 * the same box at once (`windowRect.web.ts`). `undefined`: no answer now, ask `measureInWindow` (`windowRect.native.ts`,
 * whose answer comes from the native side). `null`: nothing laid out.
 */
export declare function windowRect(node: unknown): Rect | null | undefined;

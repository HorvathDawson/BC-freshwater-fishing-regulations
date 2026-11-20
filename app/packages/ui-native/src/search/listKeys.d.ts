/**
 * (T858) The keyboard in search's list on the web (listKeys.web.ts): ↓ / ↑ move a highlight from the field through the
 * list's stops (`keyStops`) and back, Enter presses the highlighted one. A phone has none of it (listKeys.native.ts).
 */
import type { RefObject } from "react";

import type { ListKeys } from "./keys";
export type { FieldA11y, ListKeys } from "./keys";

export declare function useListKeys(o: {
  open: boolean; stops: readonly string[]; query: string; field: RefObject<{ focus(): void } | null>;
}): ListKeys;
/** A stop's element id (web), so the field can name it and Enter can press it; nothing on a phone. */
export declare function stopProps(stop: string): { nativeID?: string };
/** The results panel's id (web), the one the field's `aria-controls` names; nothing on a phone. */
export declare function resultsProps(): { nativeID?: string };
/** Scroll a stop into view (web): false when it is not drawn (the list has not rendered that far yet). */
export declare function revealStop(stop: string): boolean;

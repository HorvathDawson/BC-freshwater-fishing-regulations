/** A phone has no arrow keys in search's list: the web twin (listKeys.web.ts) is the whole behaviour. */
import type { RefObject } from "react";
import type { ListKeys } from "./keys";

const NONE: ListKeys = { active: null, onKey: () => false, field: {} };
export function useListKeys(_o: { open: boolean; stops: readonly string[]; query: string;
                                  field: RefObject<{ focus(): void } | null> }): ListKeys { return NONE; }
export function stopProps(_stop: string): { nativeID?: string } { return {}; }
export function resultsProps(): { nativeID?: string } { return {}; }
export function revealStop(_stop: string): boolean { return false; }

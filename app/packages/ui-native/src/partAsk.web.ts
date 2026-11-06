import { unguarded } from "./unguarded";

/** (T282) The web fetches a part's chunk when it is first drawn or the browser is idle (`preloadParts`), not at once. */
export const ASK_AT_ONCE = false;
/** (T391) asked without `ErrorUtils`, so Metro's chunk miss throws and the chunk is fetched (`unguarded`) */
export function askPart<T>(load: () => Promise<T>): Promise<T> { return unguarded(load); }

/**
 * (T247) RECENT SEARCHES, as a hook: the list kept on this device (`@app/data` `openRecentSearchStore`), the way to put one
 * at the front (a search the reader picked a result from) and the way to clear them. The rules are core's
 * (`parseRecents`, `pushRecent`); a store that cannot be read or written is a list that does not persist, never an error.
 * Absent store (a test, a screen alone): an in-memory list for the session.
 */
import { useCallback, useState } from "react";
import { parseRecents, pushRecent, serializeRecents } from "@app/core";

export interface RecentStore { read(): string | null; write(text: string): void }

export interface Recents { readonly list: readonly string[]; push(q: string): void; clear(): void }

export function useRecentSearches(store: RecentStore | null | undefined): Recents {
  const [list, setList] = useState<readonly string[]>(() => {
    try { return parseRecents(store?.read()); } catch { return []; }
  });
  const keep = useCallback((next: readonly string[]) => {
    setList(next);
    try { store?.write(serializeRecents(next)); } catch { /* a refused write: the list lives for this run */ }
  }, [store]);
  const push = useCallback((q: string) => keep(pushRecent(list, q)), [list, keep]);
  const clear = useCallback(() => keep([]), [keep]);
  return { list, push, clear };
}

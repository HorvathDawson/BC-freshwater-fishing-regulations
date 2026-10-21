/**
 * A spot's photographs, as hooks (S1): adding and removing them while a spot is drafted or edited, and resolving their
 * references to something an `<Image>` can draw.
 *
 * A PHOTO IS A FILE THE MOMENT IT IS PICKED — the picker's copy does not last (`@app/data/spots` photos.ts) — but it
 * belongs to the spot only once the spot is saved. So:
 *   - a photo added and then removed, or added in an edit that is discarded, is deleted at once;
 *   - a photo the saved spot already had is never deleted here: the store deletes it when the spot is saved without
 *     it (`openSpotsWith`), so discarding the edit brings it back.
 *
 * The picker itself is the app's (`apps/mobile/src/pickPhoto.native.ts` / `.web.ts`): this package names no platform.
 */
import { useEffect, useRef, useState } from "react";
import { isPhotoRef, photoFailed, type PhotoSource, type PhotoStore, type PickedPhoto } from "@app/data/spots";

/** What the app hands the screens: the photo store, its platform's picker, and the sources that picker offers. */
export interface SpotPhotos {
  store: PhotoStore;
  pick(from: PhotoSource): Promise<PickedPhoto | null>;
  sources: readonly PhotoSource[];
}

export interface PhotoEdit {
  /** The photos the spot will be saved with. */
  refs: readonly string[];
  /** The sources the add buttons offer; empty when the app wired no photos. */
  sources: readonly PhotoSource[];
  /** Start editing this spot's photos, from what it has saved. */
  begin(spotId: string, saved: readonly string[]): void;
  add(from: PhotoSource): Promise<void>;
  remove(ref: string): Promise<void>;
  /** Back to the saved photos; the files this edit added are deleted. */
  discard(): Promise<void>;
  /** The spot was saved with `refs`: they are its photos now. */
  committed(): void;
  busy: boolean;
  /** A photo that could not be added, said whole (`photoFailed`). */
  error: string | null;
}

export function usePhotoEdit(photos: SpotPhotos | undefined): PhotoEdit {
  const [refs, setRefsState] = useState<readonly string[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Mirrors, so an add that resolves after another change reads the list as it is now, not as it was when it began.
  const cur = useRef<readonly string[]>([]);
  const session = useRef<{ id: string; saved: readonly string[]; added: Set<string> }>(
    { id: "", saved: [], added: new Set() });
  const setRefs = (next: readonly string[]) => { cur.current = next; setRefsState(next); };
  const drop = async (list: Iterable<string>) => {
    for (const r of [...list]) { try { await photos?.store.remove(r); } catch { /* an orphan, not a lost record */ } }
  };

  return {
    refs, busy, error,
    sources: photos?.sources ?? [],
    begin: (id, saved) => {
      session.current = { id, saved: [...saved], added: new Set() };
      setError(null);
      setRefs([...saved]);
    },
    add: async (from) => {
      if (!photos) return;
      setError(null);
      setBusy(true);
      try {
        const picked = await photos.pick(from);
        if (!picked) return;                                       // closed without a choice
        const s = session.current;
        // `taken` holds the saved photos too, so a number removed in this edit is never reused for a new file
        const ref = await photos.store.add(s.id, picked, [...new Set([...s.saved, ...cur.current, ...s.added])]);
        s.added.add(ref);
        setRefs([...cur.current, ref]);
      } catch (e) {
        setError(photoFailed(e));
      } finally {
        setBusy(false);
      }
    },
    remove: async (ref) => {
      setRefs(cur.current.filter((r) => r !== ref));
      const s = session.current;
      if (s.added.delete(ref)) await drop([ref]);                  // never saved: nothing else holds it
    },
    discard: async () => {
      const s = session.current;
      const added = [...s.added];
      s.added.clear();
      setError(null);
      setRefs(s.saved);
      await drop(added);
    },
    committed: () => {
      const s = session.current;
      session.current = { id: s.id, saved: [...cur.current], added: new Set() };
    },
  };
}

/**
 * Each reference as something an `<Image>` can draw: absent while it is being looked up, null when the photo is no
 * longer on this device. An entry from before S1 (not a reference) is drawn as it is; a reference with no store to ask
 * is null.
 */
export function usePhotoUris(store: Pick<PhotoStore, "uri"> | undefined,
                             refs: readonly string[]): ReadonlyMap<string, string | null> {
  const key = refs.join("\n");
  const [got, setGot] = useState<ReadonlyMap<string, string | null>>(() => new Map());
  useEffect(() => {
    let live = true;
    void (async () => {
      const next = new Map<string, string | null>();
      for (const r of refs) {
        if (!isPhotoRef(r)) { next.set(r, r); continue; }
        try { next.set(r, store ? await store.uri(r) : null); } catch { next.set(r, null); }
      }
      if (live) setGot(next);
    })();
    return () => { live = false; };
    // `key` stands for `refs`: a new array with the same references is the same question
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [store, key]);
  return got;
}

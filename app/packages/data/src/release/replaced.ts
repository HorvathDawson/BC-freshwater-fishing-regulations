/**
 * (T1016) A 404 ON A RELEASE FILE: IS IT A SET THE POINTER NO LONGER NAMES? Platform-free (the fetch is handed in).
 *
 * Every file of a release lives under the release root (`<origin>/release/`): its data files (`r-<id>/…`), its tile
 * folder (`tiles/<id>/…`: the atlas, the outside mask, the map assets — sprites and glyphs — and their pack), the basemap
 * (`basemap/<id>/…`) and the rule book. The files are immutable: a path is never served with other bytes, and production
 * keeps only the live release and the previous one. So a 404 on a path is one of three things, asked of the LIVE pointer
 * (`release/manifest.json`, read fresh, never from a cache):
 *
 *   live       the live manifest names this path (or the folder it is in): the file is missing from the release the
 *              server serves now — a DEFECT, reported as it is (W46: never quieted)
 *   replaced   the manifest this page opened names it and the live one does not: the page is older than what is served
 *              (a KNOWN state: said to the reader with one action, reported as `expected`)
 *   not-ours   neither names it, or it is not under the release root: reported as it is
 *
 * A pointer that cannot be read leaves the 404 unexplained: the probe throws, and the caller reports the 404 as it is.
 */
import type { Manifest } from "../manifest";

export type ReplacedVerdict = "live" | "replaced" | "not-ours";

type Named = Pick<Manifest, "data_release" | "files" | "tiles" | "basemap"> & { synopsis?: { url?: string } | null };

/** The paths (relative to the release root) a manifest names, and the folders it names (ending "/"). */
function namedBy(m: Named): { paths: Set<string>; folders: string[] } {
  const paths = new Set<string>(), folders: string[] = [];
  const add = (u: string | null | undefined) => { if (!u) return; if (u.endsWith("/")) folders.push(u); else paths.add(u); };
  for (const f of Object.values(m.files ?? {})) add(f?.url);
  add(m.tiles?.current?.url);
  add(m.tiles?.mask?.url);
  add(m.tiles?.assets?.url);
  add(m.tiles?.assets?.pack?.url);
  add(m.basemap?.url);
  add(m.synopsis?.url);
  return { paths, folders };
}

const names = (m: Named, path: string): boolean => {
  const n = namedBy(m);
  return n.paths.has(path) || n.folders.some((f) => path.startsWith(f));
};

/** The path of `url` under the release root `base` (no query, no fragment), or null when it is not under it. */
export function releasePath(url: string, base: string): string | null {
  if (!url.startsWith(base)) return null;
  return url.slice(base.length).replace(/[?#].*$/, "") || null;
}

/**
 * Which of the three a 404 on `url` is, given every manifest this page has opened (`opened`: a read made on a set the
 * page has since replaced by an apply is judged against that set, T1016 review) and the live one (see the head).
 */
export function replacedVerdict(url: string, base: string, opened: readonly Named[], live: Named): ReplacedVerdict {
  const path = releasePath(url, base);
  if (path === null) return "not-ours";
  if (names(live, path)) return "live";
  return opened.some((m) => names(m, path)) ? "replaced" : "not-ours";
}

/** (T1016 review) How long the pointer read may take: past it the read is aborted, and the 404 is reported as it is. */
export const POINTER_MS = 10_000;

export interface ReplacedProbe {
  /** The verdict on a 404 for `url`, the release the live pointer names and the opened release that names `url` (null:
   *  none). Throws when the pointer cannot be read (an abort after `POINTER_MS` included). */
  (url: string): Promise<{ verdict: ReplacedVerdict; live: string; opened: string | null }>;
  /** Is `url` under the release root at all (asked at once: a 404 elsewhere — a feed's — is not this probe's)? */
  covers(url: string): boolean;
}

/**
 * The probe over the live pointer. `opened()`: every manifest this page has opened, the one in use last (none yet:
 * every 404 is `not-ours`). A 404 not under the release root is answered without a request. One pointer read at a time
 * is shared by every 404 that asks meanwhile (a page whose slices are gone asks for several at once); it is aborted
 * after `timeoutMs` (a stalled read would hold every report waiting on it), so the next 404 reads afresh.
 */
export function createReplacedProbe(o: { manifestUrl: string; releaseBase: string; opened: () => readonly Named[];
                                         fetch?: typeof fetch; timeoutMs?: number }): ReplacedProbe {
  const f = o.fetch ?? ((u: RequestInfo | URL, i?: RequestInit) => fetch(u, i));
  let reading: Promise<Named> | null = null;
  const pointer = (): Promise<Named> => (reading ??= (async () => {
    const stop = new AbortController();
    const timer = setTimeout(() => stop.abort(), o.timeoutMs ?? POINTER_MS);
    try {
      // fresh: a cached pointer would name the very release this page opened
      const r = await f(o.manifestUrl, { cache: "no-store", signal: stop.signal });
      if (!r.ok) throw new Error(`the release pointer could not be read: ${o.manifestUrl}: HTTP ${r.status}`);
      const m = JSON.parse(await r.text()) as Named;
      if (!m || typeof m.data_release !== "string" || !m.files)
        throw new Error(`the release pointer could not be read: ${o.manifestUrl}: not a manifest`);
      return m;
    } finally { clearTimeout(timer); }
  })().finally(() => { reading = null; }));
  const covers = (url: string) => releasePath(url, o.releaseBase) !== null;
  return Object.assign(async (url: string) => {
    const opened = o.opened();
    const inUse = opened[opened.length - 1]?.data_release ?? "";
    if (!opened.length || !covers(url)) return { verdict: "not-ours" as const, live: inUse, opened: null };
    const live = await pointer();
    const path = releasePath(url, o.releaseBase)!;
    return { verdict: replacedVerdict(url, o.releaseBase, opened, live), live: live.data_release,
             opened: opened.find((m) => names(m, path))?.data_release ?? null };
  }, { covers });
}

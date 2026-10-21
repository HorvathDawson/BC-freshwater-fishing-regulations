/**
 * THE MAP'S STATUS AND ITS PAIRING (P11), as hooks — the release's status half comes from paint, and nothing is
 * coloured until the data and the tiles are a PROVEN pair.
 *
 *   status    `StatusSource` is where the day's status index comes from: the release's paint (`releaseStatus`), or on
 *             the dev bundle path `status_index.bin` beside the tiles (`devStatus`, until P20). A failure is never
 *             kept: the next screen to ask tries again.
 *   pairing   `usePairing` (P10's `pair`): ok true = colour; false = a mixed pair, refused and said; null = not
 *             established (no tile version known yet), which colours nothing and raises no alarm.
 */
import { pair, type Held, type Manifest, type RegsSource } from "@app/data";
import type { StatusIndex } from "@app/core";
import { useAsync } from "./async";
import { loadStatusIndex, statusIndexUrl } from "./hooks";

/** Where the day's status index comes from. `key` names it (one load per key). */
export interface StatusSource {
  readonly key: string;
  load(): Promise<StatusIndex>;
}

const LOADED = new Map<string, Promise<StatusIndex>>();

/** The status index of `src`, loaded once per key — and a FAILURE IS NOT KEPT: the next ask loads again. */
export function statusOf(src: StatusSource): Promise<StatusIndex> {
  let got = LOADED.get(src.key);
  if (!got) {
    got = src.load();
    got.catch(() => { if (LOADED.get(src.key) === got) LOADED.delete(src.key); });
    LOADED.set(src.key, got);
  }
  return got;
}

/** The release's status: paint's status half (decoded once by the loader, checked against the release's handles). */
export function releaseStatus(loader: { release: string; paint(): Promise<{ status: StatusIndex }> }): StatusSource {
  return { key: `paint:${loader.release}`, load: async () => (await loader.paint()).status };
}

/** THE DEV PATH (the bundle, until P20): `status_index.bin` beside the tiles, held to the bundle's digests. */
export function devStatus(source: RegsSource, atlasUrl: string): StatusSource {
  return {
    key: `dev:${atlasUrl}`,
    async load() {
      const info = await source.info();
      const ix = await loadStatusIndex(statusIndexUrl(atlasUrl), info.sectionHandles, fetch, info.reachDigest);
      if (!ix) throw new Error("status index: missing, or not this bundle's");
      return ix;
    },
  };
}

/**
 * THE DAY'S STATUS OF EVERY SECTION AND WATER — the index, once it has loaded. Null until then, and while it cannot be
 * had: every surface then draws no status at all, which is what `statusOn` / `waterStatusOn` in @app/core return for it.
 * A failure is not kept (`statusOf`), so a later mount asks again.
 */
export function useStatusIndex(status: StatusSource | null): StatusIndex | null {
  const got = useAsync(async () => (status ? statusOf(status) : null), `status-index:${status?.key ?? ""}`, !!status);
  return got.state === "ready" ? got.value : null;
}

/** What the pairing is checked from. */
export type PairingInput =
  /** `atlasUrl`: the atlas the map DRAWS when the manifest names no tile set (`releaseTiles` → the dev endpoint). */
  | { kind: "release"; manifest: Manifest; held: Held; atlasUrl?: string }
  /** The dev bundle path (until P20): the bundle's section_handles against the tiles' `atlas.meta.json`. */
  | { kind: "dev"; source: RegsSource; atlasUrl: string | undefined };

/** ok: true = a proven pair · false = a mixed pair · null = not established. `data`/`tiles` name the two sides. */
export interface PairingView {
  ok: boolean | null;
  data: string | null;
  tiles: string | null;
}

const UNKNOWN: PairingView = { ok: null, data: null, tiles: null };

/**
 * The release's pairing as the Shell shows it. A section is an integer handle that means a different river in another
 * atlas, so data and tiles from two atlases do not miss each other — they agree on the wrong water. P10's `pair` says
 * whether the manifest's data pairs with the tiles DRAWN: the manifest's own when it names them, else `drawn`, the
 * version of the tiles the map draws instead (`tileVersionAt` their sidecar). When no tile version is known at all
 * (tiles the rebuild has not stamped yet) nothing has been proven either way: `ok` is null, which colours nothing and
 * shows no "out of step" banner.
 */
export function pairingView(input: { manifest: Manifest; held: Held }, drawn: string | null = null): PairingView {
  const { manifest: m, held } = input;
  const tiles = m.tiles.current?.id ?? drawn;
  if (tiles === null) return { ok: null, data: m.data_release, tiles: null };
  const p = pair(m, { ...held, tiles });
  return { ok: p.ok, data: m.data_release, tiles };
}

type Sidecar = { section_handles?: string; tile_version?: string };

/** The tile sidecar beside an atlas (`pipeline/deliver/tiles/__main__.py` `_write_sidecar`), or null when it has none. */
async function sidecarOf(atlasUrl: string, fetcher: typeof fetch): Promise<Sidecar | null> {
  const res = await fetcher(atlasUrl.replace(/[^/]*$/, "atlas.meta.json"));
  return res.ok ? ((await res.json()) as Sidecar) : null;
}

/**
 * The version of the tiles at `atlasUrl` ("<handles16>.<tilebuild12>", stamped by `pipeline/deliver/tiles/version.py`
 * and written to the sidecar as `tile_version`), or null: no sidecar, or tiles built before versions were stamped.
 */
export async function tileVersionAt(atlasUrl: string, fetcher: typeof fetch = fetch): Promise<string | null> {
  const v = (await sidecarOf(atlasUrl, fetcher).catch(() => null))?.tile_version;
  return typeof v === "string" && v ? v : null;
}

/** Whether the map may be coloured, and the two sides' names for the banner when it may not (`usePairing`'s answer). */
export async function pairingOf(input: PairingInput | null, fetcher: typeof fetch = fetch): Promise<PairingView> {
  if (!input) return UNKNOWN;
  if (input.kind === "release") {
    // the tiles drawn are the manifest's own when it names them; else the ones at `atlasUrl`, proven by their stamp
    const drawn = !input.manifest.tiles.current && input.atlasUrl ? await tileVersionAt(input.atlasUrl, fetcher) : null;
    return pairingView(input, drawn);
  }
  // THE DEV PATH: the tile sidecar, a ~100-byte JSON both map renderers' hosts can read. A sidecar that will not load
  // leaves this null rather than false — an old deployment has none, and that is not evidence of a mismatch.
  const info = await input.source.info();
  const data = info.sectionHandles;
  if (!input.atlasUrl) return { ok: null, data, tiles: null };
  const meta = await sidecarOf(input.atlasUrl, fetcher);
  if (!meta) return { ok: null, data, tiles: null };
  const tiles = meta.section_handles ?? null;
  if (!data || !tiles) return { ok: null, data, tiles };
  return { ok: data === tiles, data, tiles };
}

/** Whether the map may be coloured, and the two sides' names for the banner when it may not. */
export function usePairing(input: PairingInput | null): PairingView {
  const key = !input ? "none" : input.kind === "release"
    ? `release:${input.manifest.data_release}:${input.manifest.tiles.current?.id ?? ""}:${input.atlasUrl ?? ""}`
    : `dev:${input.atlasUrl ?? ""}`;
  const got = useAsync(() => pairingOf(input), `pairing:${key}`);
  return got.state === "ready" ? got.value : UNKNOWN;
}

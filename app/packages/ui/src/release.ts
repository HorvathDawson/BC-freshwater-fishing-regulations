/**
 * THE MAP'S STATUS AND ITS PAIRING (P11), as hooks — the release's status half comes from paint, and nothing is
 * coloured until the data and the tiles are a PROVEN pair.
 *
 *   status    `StatusSource` is where the day's status index comes from: the release's paint (`releaseStatus`). A
 *             failure is never kept: the next screen to ask tries again.
 *   pairing   `usePairing` (P10's `pair`): ok true = colour; false = a mixed pair, refused and said; null = not
 *             established, which colours nothing and raises no alarm — and, once checked with no tile version found
 *             (`unproven`), says why in one quiet line (core `unprovenLine`).
 */
import { pair, type Held, type Manifest } from "@app/data";
import { releaseTiles, type Booted } from "@app/data/release";
import type { StatusIndex } from "@app/core";
import { useAsync } from "./async";

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
export interface PairingInput {
  kind: "release";
  manifest: Manifest;
  held: Held;
  /** The atlas the map DRAWS when the manifest names no tile set (`releaseTiles` → the dev endpoint). */
  atlasUrl?: string;
}

/**
 * ok: true = a proven pair · false = a mixed pair · null = not established. `data`/`tiles` name the two sides.
 * `unproven`: checked, and no tile version was found to prove the pair with (unstamped tiles, no sidecar) — said; a
 * pair not yet checked (loading, no input) is not unproven and says nothing.
 */
export interface PairingView {
  ok: boolean | null;
  data: string | null;
  tiles: string | null;
  unproven: boolean;
}

const UNKNOWN: PairingView = { ok: null, data: null, tiles: null, unproven: false };

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
  if (tiles === null) return { ok: null, data: m.data_release, tiles: null, unproven: true };
  const p = pair(m, { ...held, tiles });
  return { ok: p.ok, data: m.data_release, tiles, unproven: false };
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
  // the tiles drawn are the manifest's own when it names them; else the ones at `atlasUrl`, proven by their stamp
  const drawn = !input.manifest.tiles.current && input.atlasUrl ? await tileVersionAt(input.atlasUrl, fetcher) : null;
  return pairingView(input, drawn);
}

/** The map's endpoints (`releaseTiles`: dev ones where the release names none). */
export type MapTiles = ReturnType<typeof releaseTiles>;

/**
 * THE RELEASE PATH'S MAP INPUT (what App.tsx hands the Shell): the tiles it draws, and the pairing proven against THOSE
 * tiles — `atlasUrl` is the atlas drawn, so when the manifest names no tile set the pair is proven by the dev atlas's
 * stamped `tile_version` (`pairingOf`), never assumed.
 */
export function releaseMapInput(b: Pick<Booted, "manifest" | "held" | "locate">,
                                 dev: { atlas: string; basemap: string; outside: string }):
    { tiles: MapTiles; pairing: PairingInput } {
  const tiles = releaseTiles(b, dev);
  return { tiles, pairing: { kind: "release", manifest: b.manifest, held: b.held, atlasUrl: tiles.atlas } };
}

/** Whether the map may be coloured, and the two sides' names for the banner when it may not. */
export function usePairing(input: PairingInput | null): PairingView {
  const key = !input ? "none"
    : `release:${input.manifest.data_release}:${input.manifest.tiles.current?.id ?? ""}:${input.atlasUrl ?? ""}`;
  const got = useAsync(() => pairingOf(input), `pairing:${key}`);
  return got.state === "ready" ? got.value : UNKNOWN;
}

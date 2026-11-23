/**
 * (T722 F9) WILL THE DOWNLOAD FIT: before a release download the bytes still to fetch (the manifest's sizes, less what of
 * the big files is on disk or in the system's download) are held against the phone's free bytes; too few is a
 * `SpaceError` whose message is the reader's whole sentence (AGENTS 33), never "Check the connection".
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { setFailureSink } from "@app/core";
import type { Manifest } from "../manifest";
import { checkRoom, SpaceError, type DeviceSpace } from "./boot";
import type { BgJob } from "./bgDownload";

const MB = 1_000_000;
/** A manifest with two data files (2 MB) and two big files: tiles 300 MB, basemap 180 MB (482 MB in all). */
const M = {
  data_release: "r-1",
  files: { core: { url: "r-1/core.sqlite", bytes: 1.5 * MB, sha256: "a" }, paint: { url: "r-1/paint.bin", bytes: 0.5 * MB, sha256: "b" } },
  tiles: { compatible: [], current: { id: "v1", url: "tiles/v1/atlas.pmtiles", bytes: 300 * MB, sha256: "t" } },
  basemap: { id: "b1", url: "tiles/basemap-b1.pmtiles", bytes: 180 * MB, sha256: "m" },
} as unknown as Manifest;

const space = (free: number, sizes: Record<string, number> = {}): DeviceSpace =>
  ({ free: async () => free, size: async (p) => sizes[p] ?? 0 });

describe("checkRoom (T722 F9)", () => {
  let reported: unknown[] = [];
  let unsink = () => {};
  beforeEach(() => { reported = []; unsink = setFailureSink((e) => reported.push(e)); vi.spyOn(console, "info").mockImplementation(() => {}); });
  afterEach(() => { unsink(); vi.restoreAllMocks(); });

  it("a first install that will not fit: the whole sentence, with what it needs and what is free", async () => {
    const e = await checkRoom(M, null, space(120 * MB)).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(SpaceError);
    expect((e as Error).message).toBe("Not enough free space: the download needs about 482.0 MB and this phone has "
      + "120.0 MB free. Free up some space and try again.");
  });

  it("fits: nothing thrown, nothing reported", async () => {
    await expect(checkRoom(M, null, space(482 * MB))).resolves.toBeUndefined();
    expect(reported).toEqual([]);
  });

  it("what is on disk already (a .part, a finished file) and what the system download holds are not asked for again", async () => {
    // 200 MB of the tiles in their .part, the basemap 100 MB into its system download: 2 + 100 + 80 = 182 MB still to come
    const jobs = [{ id: 1, url: "https://r2.test/release/tiles/basemap-b1.pmtiles", status: "running", reason: 0,
                    bytes: 100 * MB, total: 180 * MB, metered: true }] satisfies BgJob[];
    const sizes = { "release/tiles/v1/atlas.pmtiles.part": 200 * MB };
    await expect(checkRoom(M, null, space(182 * MB, sizes), jobs)).resolves.toBeUndefined();
    const e = await checkRoom(M, null, space(181 * MB, sizes), jobs).catch((x: unknown) => x);
    expect((e as Error).message).toBe("Not enough free space: the download needs about 182.0 MB and this phone has "
      + "181.0 MB free. Free up some space and try again.");
    // the finished tiles at their path count whole, never more than the file
    await expect(checkRoom(M, null, space(182 * MB, { "release/tiles/v1/atlas.pmtiles": 999 * MB }))).resolves.toBeUndefined();
  });

  it("an update asks only for what it changes", async () => {
    const held = { release: "r-0", tiles: "v1", basemap: "b1", assets: null, files: { core: "old", paint: "b" } };
    const e = await checkRoom(M, held, space(1 * MB)).catch((x: unknown) => x);
    expect((e as Error).message).toBe("Not enough free space: the download needs about 1.5 MB and this phone has "
      + "1.0 MB free. Free up some space and try again.");
  });

  it("(T919 c) a first install that takes the assets PACK needs room for the pack AND the folder it unpacks into", async () => {
    // the live sizes: a 6.1 MB pack that unpacks to 13.8 MB, both on disk until the swap: 482 + 6.1 + 13.8 = 501.9 MB
    const P = { ...M, tiles: { ...M.tiles, assets: { id: "map-assets-1", url: "tiles/v1/map-assets-1/", files_sha256: "f",
      pack: { url: "tiles/v1/map-assets-1.tar.zlib", bytes: 6.1 * MB, sha256: "p", unpacked: 13.8 * MB } } } } as Manifest;
    await expect(checkRoom(P, null, space(501.9 * MB))).resolves.toBeUndefined();
    const e = await checkRoom(P, null, space(495 * MB)).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(SpaceError);
    expect((e as Error).message).toBe("Not enough free space: the download needs about 501.9 MB and this phone has "
      + "495.0 MB free. Free up some space and try again.");
    // the pack already down: only the folder it unpacks into is still asked for beside the rest
    await expect(checkRoom(P, null, space(495.8 * MB, { "release/tiles/v1/map-assets-1.tar.zlib": 6.1 * MB })))
      .resolves.toBeUndefined();
    // an update whose assets id does not move takes no pack and asks no room for one
    const held = { release: "r-1", tiles: "v1", basemap: "b1", assets: "map-assets-1", files: { core: "a", paint: "b" } };
    await expect(checkRoom(P, held, space(0))).resolves.toBeUndefined();
    // a pack that names no unpacked size is reported (the check goes on with what it knows)
    const noSize = { ...P, tiles: { ...P.tiles, assets: { ...P.tiles.assets!, pack: { ...P.tiles.assets!.pack!, unpacked: undefined } } } } as Manifest;
    await expect(checkRoom(noSize, null, space(488.1 * MB))).resolves.toBeUndefined();
    expect(reported.map(String)).toEqual(["Error: [space] the assets pack tiles/v1/map-assets-1.tar.zlib names no unpacked size (undefined)"]);
  });

  it("a phone that will not say its free space is reported, and the download goes on", async () => {
    await expect(checkRoom(M, null, { free: async () => null as unknown as number, size: async () => 0 })).resolves.toBeUndefined();
    expect(reported.map(String)).toEqual(["Error: [space] the phone did not say its free space (null); 482000000 bytes to download"]);
  });

  it("no space reader (the web, a test device): no check", async () => {
    await expect(checkRoom(M, null, undefined)).resolves.toBeUndefined();
  });
});

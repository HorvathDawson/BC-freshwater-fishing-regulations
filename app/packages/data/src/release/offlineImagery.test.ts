/**
 * (T1154) THE SATELLITE IMAGERY KEPT OFFLINE, by offline method: Esri's ("none") lists nothing and refuses the rest by
 * name; the file method lists nothing (no corridor product exists) and refuses a download by name. Nothing is fetched.
 */
import { describe, expect, it } from "vitest";
import { IMAGERY, type ImageryProvider } from "@app/core";
import { offlineImagery } from "./offlineImagery";

describe("offline imagery", () => {
  it("none (the provider in use, Esri): lists nothing; a size, download or removal is refused, named", async () => {
    const o = offlineImagery(IMAGERY);
    expect(await o.list()).toEqual([]);
    await expect(o.size("c1")).rejects.toThrow('offline imagery (none): cannot size "c1": this provider keeps nothing offline');
    await expect(o.download("c1")).rejects.toThrow('offline imagery (none): cannot download "c1": this provider keeps nothing offline');
    await expect(o.remove("c1")).rejects.toThrow('offline imagery (none): cannot remove "c1": this provider keeps nothing offline');
  });

  it("file: lists nothing until a corridor product exists; a download is refused, named", async () => {
    const o = offlineImagery({ ...IMAGERY, offline: "file" } satisfies ImageryProvider);
    expect(await o.list()).toEqual([]);
    await expect(o.download("c1")).rejects.toThrow(
      'offline imagery (file): cannot download "c1": no imagery region is offered: no corridor product exists yet');
    await expect(o.size("c1")).rejects.toThrow(
      'offline imagery (file): cannot size "c1": no imagery region is offered: no corridor product exists yet');
    await expect(o.remove("c1")).rejects.toThrow('offline imagery (file): cannot remove "c1": no imagery region is kept on this device');
  });
});

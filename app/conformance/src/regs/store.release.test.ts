/**
 * T03 on the LIVE release (data/release/r-*, `liveRelease()`; skipped when this machine has none): `openRegsStore` over
 * the loader the app uses accepts it, paired with the tiles its manifest names, and every region file and regs-sid are
 * one build with water. Expected values come from the golden's own store (taps.json `meta.store`, the build the port's
 * oracles were cut from), never typed here — so this also proves the release is the build the goldens describe.
 */
import { describe, expect, it } from "vitest";
import { KNOWN_VERSIONS, SECTION_NAMES } from "@app/core";
import { openRegsStore, RegsStoreError } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";
import { taps } from "./oracles";

const LIVE = liveRelease();
const T = taps();
const golden = T.meta.store;

describe("the port's versions are the golden store's", () => {
  it("KNOWN_VERSIONS = taps.json meta.store.layout.versions (+ glossary 5)", () => {
    expect({ ...KNOWN_VERSIONS }).toEqual({ ...JSON.parse(golden.layout).versions, glossary: 5 });
  });
});

describe.skipIf(!LIVE)(`T03 on the live release (${LIVE ?? "absent"})`, () => {
  it("opens over water alone, paired with the manifest's tiles, and says what it is", async () => {
    const loader = openReleaseDir(LIVE!);
    const st = await openRegsStore(loader);
    expect(loader.opened()).toEqual(["water"]);
    expect(loader.manifest.tiles.current!.id!.split(".")[0]).toBe(golden.section_handles);
    expect({ sectionHandles: st.meta.sectionHandles, reachDigest: st.meta.reachDigest, ruleIdsSha256: st.meta.ruleIdsSha256 })
      .toEqual({ sectionHandles: golden.section_handles, reachDigest: golden.reach_digest, ruleIdsSha256: golden.rule_ids_sha256 });
    expect(st.meta.versions).toEqual(Object.fromEntries(SECTION_NAMES.map((n) => [n, KNOWN_VERSIONS[n]])));
    expect(st.meta.glossaryVersion).toBe(5);
    expect(st.meta.blobCodec).toBe("zdict");
    expect(st.meta.layout).toEqual({ top: JSON.parse(golden.layout).top, sections: JSON.parse(golden.layout).sections });
    expect(st.dict("display_frame")!.length).toBe(31462);
    expect(st.dict("ladder_verdict")).not.toBeNull();             // the region tables' dictionaries are water's
  });

  it("every region file and regs-sid are water's build (opened one by one, each checked)", async () => {
    const loader = openReleaseDir(LIVE!);
    const st = await openRegsStore(loader);
    const water = await st.water();
    const one = await water.all(
      "SELECT region, min(akey) AS akey FROM akey_region WHERE akey IN " +
      "(SELECT akey FROM akey_region GROUP BY akey HAVING count(*) = 1) GROUP BY region ORDER BY region");
    expect(one.map((r) => r.region)).toEqual(["1", "2", "3", "4", "5", "6", "7a", "7b", "8"]);
    for (const r of one) {
      const d = await st.region(Number(r.akey));
      expect((await d.get("SELECT v FROM meta WHERE k = 'region'"))!.v).toBe(r.region);
    }
    await st.sid();
    expect(loader.opened()).toEqual(["water", ...one.map((r) => `regs-${r.region}`), "regs-sid"]);
    expect(st.refused()).toBeNull();
  });

  it("the Vedder (akey 886, Region 2) opens regs-2 and nothing else", async () => {
    const loader = openReleaseDir(LIVE!);
    const st = await openRegsStore(loader);
    await st.region(886);
    expect(loader.opened()).toEqual(["water", "regs-2"]);
  });

  it("refuses the live release against another atlas's tiles", async () => {
    const e = await openRegsStore(openReleaseDir(LIVE!), { tiles: "0000000000000000.000000000000" }).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(RegsStoreError);
    expect((e as RegsStoreError).message)
      .toBe(`regs store: built for atlas ${golden.section_handles}, the map is 0000000000000000.000000000000 — refused`);
  });
});

/**
 * T03 on the LIVE release (data/release/r-*, `liveRelease()`; skipped when this machine has none): `openRegsStore` over
 * the loader the app uses accepts it, paired with the tiles its manifest names, and every region file and regs-sid are
 * one build with water. Expected values come from the golden's own store (taps.json `meta.store`, the build the port's
 * oracles were cut from), never typed here — so this also proves the release is the build the goldens describe.
 */
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { KNOWN_VERSIONS, SECTION_NAMES } from "@app/core";
import { openRegsStore, RegsStoreError } from "@app/data/regs";
import { liveRegsStore, liveRelease, openReleaseDir } from "../fixtureRelease";
import { taps } from "./oracles";

const LIVE = liveRelease();
const T = taps();
const golden = T.meta.store;
/** The port tooling's province store (regs.sqlite), when it is the build taps.json was cut from (its store_digest). */
const PROVINCE = ((): DatabaseSync | null => {
  const p = liveRegsStore();
  if (!p) return null;
  const db = new DatabaseSync(p, { readOnly: true });
  const d = (db.prepare("SELECT v FROM meta WHERE k = 'store_digest'").get() as { v: string } | undefined)?.v;
  if (d === golden.store_digest) return db;
  db.close();
  return null;
})();
const REQUIRED = !!process.env.REGS_STORE_REQUIRED && process.env.REGS_STORE_REQUIRED !== "0";

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
    // No length pin: a dictionary's bytes move with every training change (best-of-8 since e0b09590) and every frame it
    // samples — three rebuilds in a row broke a literal here. Its bytes are the province store's (the next test).
    const d = st.dict("display_frame")!;
    expect(d.length > 0 && d.length <= 32_768, `display_frame dictionary is ${d.length} bytes`).toBe(true);   // zlib's window
    expect(st.dict("ladder_verdict")).not.toBeNull();             // the region tables' dictionaries are water's
  });

  it.skipIf(!PROVINCE && !REQUIRED)("every dictionary is the province store's, byte for byte (regs.sqlite, taps.json's store_digest)", async () => {
    expect(PROVINCE, `no province store with store_digest ${golden.store_digest} (REGS_STORE_REQUIRED is set)`).not.toBeNull();
    const st = await openRegsStore(openReleaseDir(LIVE!));
    const want = PROVINCE!.prepare("SELECT tbl, d FROM zdict ORDER BY tbl").all() as { tbl: string; d: Uint8Array }[];
    expect(want.map((r) => r.tbl)).toEqual(expect.arrayContaining(["display_frame", "ladder_verdict", "rows_frame", "rule"]));
    for (const r of want) expect(Buffer.from(st.dict(r.tbl) ?? []).equals(Buffer.from(r.d)), `zdict ${r.tbl}`).toBe(true);
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

  it("the Vedder (its akey from taps.json vedder_515; Region 2) opens regs-2 and nothing else", async () => {
    const loader = openReleaseDir(LIVE!);
    const st = await openRegsStore(loader);
    await st.region(T.states.vedder_515!.akey);
    expect(loader.opened()).toEqual(["water", "regs-2"]);
  });

  it("refuses the live release against another atlas's tiles", async () => {
    const e = await openRegsStore(openReleaseDir(LIVE!), { tiles: "0000000000000000.000000000000" }).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(RegsStoreError);
    expect((e as RegsStoreError).message)
      .toBe(`regs store: built for atlas ${golden.section_handles}, the map is 0000000000000000.000000000000 — refused`);
  });
});

/**
 * T10 runner: the regs conformance suite (regsConformance.ts) on THE COMMITTED REGS FIXTURE RELEASE
 * (packages/data/dev/regs-release, `pnpm regs-fixture`) — always, CI included. Then what is the fixture's own: it is
 * held to no atlas unless asked (its manifest names no tiles), and — when the release it was cut from is on this
 * machine — every cell and frame it holds is that release's, decoded the same.
 */
import { describe, expect, it } from "vitest";
import { openRegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { liveRelease, openReleaseDir } from "../fixtureRelease";
import { casesFromTaps, refusalOf, runRegsConformance } from "./regsConformance";

/** A water of the live release (the Harrison River, ord 1938) that no golden state names: the cut dropped it. */
const DROPPED = "gnis:15333";
runRegsConformance("regs conformance: the committed fixture release", async () => openRegsStore(openRegsFixture()),
  casesFromTaps(DROPPED));

describe("the fixture release itself", () => {
  it("opens over water alone, held to no atlas (its manifest names no tiles); asked for one, it refuses another", async () => {
    const loader = openRegsFixture();
    const st = await openRegsStore(loader);
    expect([loader.manifest.tiles.current, loader.opened()]).toEqual([null, ["water"]]);
    expect(await refusalOf(openRegsStore(openRegsFixture(), { tiles: "0000000000000000.000000000000" }))).toEqual({
      why: "digest",
      message: `regs store: built for atlas ${st.meta.sectionHandles}, the map is 0000000000000000.000000000000 — refused`,
    });
    expect(await refusalOf(openRegsStore(openRegsFixture(), { tiles: `${st.meta.sectionHandles}.000000000000` }))).toBeNull();
  });

  it("the Vedder (akey 886, Region 2) opens regs-2 and nothing else", async () => {
    const loader = openRegsFixture();
    const st = await openRegsStore(loader);
    await st.frames(886, await st.cell(886, 1));
    expect(loader.opened()).toEqual(["water", "regs-2"]);
  });

  const LIVE = liveRelease();
  it.skipIf(!LIVE)(`answers as its source release (${LIVE ?? "no live release here"}): every kept cell and frame, every rule`, async () => {
    const a = await openRegsStore(openRegsFixture()), water = await a.water();
    const of = String((await water.get("SELECT v FROM meta WHERE k = 'fixture_of'"))!.v);
    if (!LIVE!.endsWith(`/${of}`)) {
      console.warn(`regs fixture: cut from ${of}, the live release is ${LIVE} — compare skipped; run pnpm regs-fixture`);
      return;
    }
    const b = await openRegsStore(openReleaseDir(LIVE!));
    expect([await b.itemOrd(DROPPED), await a.itemOrd(DROPPED)]).toEqual([1938, null]);
    const akeys = (await water.all("SELECT akey FROM akey ORDER BY akey")).map((r) => Number(r.akey));
    expect(akeys.length).toBeGreaterThan(100);
    let frames = 0;
    for (const k of akeys) {
      expect(await a.akey(k), `akey ${k}`).toEqual(await b.akey(k));
      const cells = await a.cells(k);
      expect(cells, `cells ${k}`).toEqual(await b.cells(k));
      for (const c of cells) { expect(await a.frames(k, c), `frames ${k} ${JSON.stringify(c)}`).toEqual(await b.frames(k, c)); frames++; }
    }
    expect(await a.rules()).toEqual(await b.rules());
    expect(await a.statics()).toEqual(await b.statics());
    expect(frames).toBeGreaterThan(akeys.length);
  }, 120_000);
});

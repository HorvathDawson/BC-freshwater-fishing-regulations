/**
 * T670 (the T462 audit's Keep and Gear findings) on the LIVE release: a regulation the release holds and the book prints
 * is ON THE SCREEN — the page a tap builds (`loadRegsInputs` → `buildRegsPage`), read as the tab draws it. One water per
 * finding class, each named by what it is (its water, its part by its ends), each line pinned as the whole string; the
 * printed page is the book's own. Skipped, by name, when this machine has no release.
 *
 *   K1  a water's own possession limit            Charlie Lake, p.65
 *   K2  a row's fish that must go back            Peace River, Site C dam up to the Halfway closure, p.66
 *   K3  a child row's fish nobody may keep        Shuswap Lake's lake trout, p.28
 *   K4  a pooled row with no card                 Bruer Creek's brook trout, p.68
 *   K5  the same, a water's one-kind quota        Mat Creek (Duncan River's tributaries), p.36
 *   G1  a rule held for an angler in a boat       Kispiox River, p.51
 *   G3  gear and boat rules of an undrawn part    Mahood Lake, p.32
 *   G4  a hook-size limit                         Qualicum River, p.18
 *   G7  the classified-water licence terms        Wigwam River (Kootenay booking), Dean River (the draw), p.7 (Li13)
 */
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, plain, type RegsInputs, type RegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { sectionNames, type SectionNames } from "@app/data/release/node";
import { liveReleaseAt, openReleaseDir } from "../fixtureRelease";

const at = liveReleaseAt();

it(`T670 runs against the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T670 skipped: ${at.reason}`);
});

describe.skipIf(!at.dir)(`T670: what the release holds is on the screen (${at.dir ?? at.reason})`, () => {
  let st: RegsStore, names: SectionNames;
  beforeAll(async () => { const loader = openReleaseDir(at.dir!); st = await openRegsStore(loader); names = sectionNames(loader); });
  const page = async (item: string, partIx: number, month: number, day: number, weekday: RegsInputs["weekday"]): Promise<RegsPage> => {
    const inp = await loadRegsInputs(st, { kind: "part", item, partIx }, { month, day }, weekday, 2026);
    return buildRegsPage(inp, { today: { month, day }, who: { age: "adult", residency: "resident" } });
  };

  it("K1 Charlie Lake: 'Walleye daily and possession quotas = 3; northern pike …' (p.65) is under the Possession row", async () => {
    const pos = (await page("wbk:329264876", 0, 7, 15, "Wednesday")).keep.possession!;
    expect([pos.value, pos.also]).toEqual(["2 days’ quota", [
      "Possession: the daily quota for Arctic grayling. Fish at home don’t count.",
      "Possession: the daily quota for Dolly Varden or bull trout. Fish at home don’t count.",
      "Possession: the daily quota for lake trout. Fish at home don’t count.",
      "Have no more than 3 walleye in possession. Fish at home don’t count.",
      "Have no more than 3 northern pike in possession. Fish at home don’t count."]]);
    expect(pos.says.endsWith(pos.also.join(" "))).toBe(true);   // the sheet's sentence holds them too
  });

  it("K2 Peace River above the Site C dam: 'Mountain whitefish catch and release' (p.66) is on the Whitefish row", async () => {
    const part = await names.part("gnis:14619", "peace_river__site_c_dam", "peace_river__halfway_river_into_peace_river_d5000m");
    const keep = (await page("gnis:14619", part, 7, 15, "Wednesday")).keep;
    expect(keep.otherFish!.filter((o) => o.back.length).map((o) => [o.name, o.value, o.tone, o.back.map((b) => [b.status, b.says])])).toEqual([
      ["Whitefish", "15 a day", "keep", [["release", "Release every mountain whitefish."]]]]);
    expect(keep.otherFishKey.map((k) => k.status)).toContain("release");
  });

  it("K3 Shuswap Lake, Nov 11: Region 3's 'Lake trout from Oct 15-Jan 31' release (p.28) is a line of the trout card; none in July", async () => {
    const lines = async (m: number, d: number) => (await page("wbk:329518145", 0, m, d, "Wednesday")).keep.groups
      .flatMap((g) => g.notKept.map((n) => [n.status, n.says, plain(n.whose), n.pictures.map((p) => p.code).join()]));
    expect(await lines(11, 11)).toEqual([["release", "Release every lake trout, Oct 15–Jan 31.", "Region 3 rule · Rule book p.28 ↗", "LT"]]);
    expect(await lines(7, 15)).toEqual([]);
  });

  it("K4 Bruer Creek: '20 brook trout from streams' (p.68) leads the species list and opens its full row", async () => {
    const p = await page("gnis:10061", await names.part("gnis:10061", "mouth", "source"), 7, 15, "Wednesday");
    const o = p.keep.otherFish![0]!;
    expect([o.name, o.value, o.tone, o.open]).toEqual(["Brook trout", "20 a day", "keep", "other:o0"]);
    const sheet = p.sheet(o.open)!;
    expect(sheet.title).toBe("Brook trout");
    expect(JSON.stringify(sheet.blocks)).toContain("In streams, keep up to 20 brook trout a day.");
  });

  it("K5 Mat Creek, Jan 15: the Duncan River's 'bull trout daily quota = 2' (p.36) leads the species list", async () => {
    const p = await page("gnis:14254", await names.part("gnis:14254", "mouth", "source"), 1, 15, "Thursday");
    const o = p.keep.otherFish![0]!;
    expect([o.name, o.value, o.tone]).toEqual(["Dolly Varden/bull trout", "2 a day", "keep"]);
    expect(p.sheet(o.open)!.title).toBe("Dolly Varden/bull trout");
  });

  it("G1 Kispiox River: 'No angling from boats, no powered boats' (p.51) — both on the boats line and the Methods list", async () => {
    const p = await page("gnis:2308", 0, 8, 15, "Saturday");
    expect(p.boats!.map((b) => [b.kind, b.text])).toEqual([["in_boat", "No angling (from a boat)"], ["unpowered", "No powered boats"]]);
    expect(p.methods!.boats!.items).toEqual(["No angling (from a boat)", "No powered boats"]);
  });

  it("G3 Mahood Lake, Jul 15: the bait ban and single barbless hook inside the signs (p.32) are in the in-part block", async () => {
    const c = (await page("wbk:329354708", 0, 7, 15, "Wednesday")).keep.caveats.find((x) => x.kind === "inpart")!;
    expect([c.title, c.items.map((i) => `${plain(i.what)} | ${i.where}`)]).toEqual(["Fishing is different in some spots today", [
      "Trout and char — release all · Jul 1–Dec 31 | Where: Within fishing boundary signs at the western tip of the lake",
      "Trout and char — release all · Jul 1–Dec 31 | Where: Within 200 m of the mouth of the Mahood River outlet",
      "Bait ban · Jul 1–Dec 31 | Where: Within fishing boundary signs at the western tip of the lake",
      "Single barbless hook · Jul 1–Dec 31 | Where: Within fishing boundary signs at the western tip of the lake",
      "Bait ban · Jul 1–Dec 31 | Where: Within 200 m of the mouth of the Mahood River outlet",
      "Single barbless hook · Jul 1–Dec 31 | Where: Within 200 m of the mouth of the Mahood River outlet"]]);
  });

  it("G4 Qualicum River above the hatchery weir: 'no hooks greater than 15 mm from point to shank' (p.18) is on the Hooks line", async () => {
    const part = await names.part("gnis:27804", "qualicum_river__upper_hatchery_weir_125_m_downstream", "source");
    const hook = (await page("gnis:27804", part, 7, 15, "Wednesday")).gear!.lines.find((l) => l.k === "hook")!;
    expect(hook.values).toEqual(["One hook, lure or fly", "Single barbless hook, no trebles",
      "Single barbless hook; no hook more than 15 mm from point to shank"]);
    expect(hook.src!.pages.map((x) => x.printed)).toContain(18);
  });
  it("G7 (RULINGS Li13) the Licence tab lists the p.7 classified-water terms that apply, in the book's words, per angler", async () => {
    const BOOKING = "A first-come-first-serve booking system is in place for these Kootenay Class II waters: Michel Creek, Skookumchuck Creek, and the Wigwam River.";
    const bookOf = async (item: string, partIx: number, who: Parameters<typeof buildRegsPage>[1]["who"]) => {
      const inp = await loadRegsInputs(st, { kind: "part", item, partIx }, { month: 7, day: 15 }, "Wednesday", 2026);
      return (buildRegsPage(inp, { today: { month: 7, day: 15 }, who }).licence?.blocks ?? [])
        .flatMap((b) => b.cls === "needs" ? [...b.items, ...(b.onlyIf?.items ?? [])] : []).flatMap((i) => i.book);
    };
    const wigwam = await names.part("gnis:27703", "mouth", "wigwam_river__wigwam_km42_ram_fsr");
    const visitor = await bookOf("gnis:27703", wigwam, { age: "adult", residency: "non_resident" });
    expect(visitor).toContain(BOOKING);
    expect(visitor.some((t) => t.startsWith("If you are a NON-GUIDED “Non-Resident”"))).toBe(true);
    const resident = await bookOf("gnis:27703", wigwam, { age: "adult", residency: "resident" });
    expect(resident.some((t) => t.startsWith("If you are a B.C. resident: Your Classified Waters Licence is valid"))).toBe(true);
    expect(resident).not.toContain(BOOKING);
    // the Elk is Class II too, but not one of the three booked waters
    const elk = await bookOf("gnis:16880", 0, { age: "adult", residency: "non_resident" });
    expect([elk.some((t) => t.startsWith("If you are a NON-GUIDED “Non-Resident”")), elk.includes(BOOKING)]).toEqual([true, false]);
  });
  it("G8 (owner 2026-11-12 'Show the fee only') the basic licence carries the disabled-resident fee line for a B.C. resident adult, not the one-licence line", async () => {
    const FEE = "If you are a resident of B.C. and disabled: You may qualify for a basic freshwater angling licence at the reduced fee of $1.14.";
    const basic = async (who: Parameters<typeof buildRegsPage>[1]["who"]) => {
      const inp = await loadRegsInputs(st, { kind: "part", item: "gnis:16880", partIx: 0 }, { month: 7, day: 15 }, "Wednesday", 2026);
      return (buildRegsPage(inp, { today: { month: 7, day: 15 }, who }).licence?.blocks ?? [])
        .flatMap((b) => b.cls === "needs" ? [...b.items, ...(b.onlyIf?.items ?? [])] : []).find((i) => /basic/i.test(i.name))!.book;
    };
    expect(await basic({ age: "adult", residency: "resident" })).toEqual([FEE]);
    expect(await basic({ age: "adult", residency: "non_resident" })).toEqual([]);
  });
});

/**
 * T670 (the T462 audit's Keep and Gear findings) on the LIVE release: a regulation the release holds and the book prints
 * is ON THE SCREEN — the page a tap builds (`loadRegsInputs` → `buildRegsPage`), read as the tab draws it. One water per
 * finding class, each named by what it is (its water, its part by its ends), each line pinned as the whole string; the
 * printed page is the book's own. Skipped, by name, when this machine has no release.
 *
 *   K1  a water's own possession limit            Charlie Lake, p.65
 *   K2  a row's fish that must go back            Peace River, Site C dam up to the Halfway closure, p.66
 *   K3  a child row's fish nobody may keep        Shuswap Lake's lake trout, p.28        (T810 S: the trout table's)
 *   K4  a pooled row with no card                 Bruer Creek's brook trout, p.68        (T810 S: the trout table's)
 *   K5  the same, a water's one-kind quota        Mat Creek (Duncan River's tributaries), p.36   (T810 S: the table's)
 *   G1  a rule held for an angler in a boat       Kispiox River, p.51
 *   G3  gear and boat rules of an undrawn part    Mahood Lake, p.32
 *   G4  a hook-size limit                         Qualicum River, p.18
 *   G7  the classified-water licence terms        Wigwam River (Kootenay booking), Dean River (the draw), p.7 (Li13)
 */
import { beforeAll, describe, expect, it } from "vitest";
import { BOAT_WHILE, boatsLine, buildRegsPage, gearAnswer, OTHER_WHILE, plain, type RegsInputs, type RegsPage, type TroutVM } from "@app/core";
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
    // (T810 S, F0) the trout- and char-only lines (Dolly Varden or bull trout, lake trout) are the table's rows now
    expect([pos.value, pos.also]).toEqual(["2 days’ quota", [
      "Possession: the daily quota for Arctic grayling. Fish at home don’t count.",
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

  /** (T810 S) the trout table's boxes as [kind, the fish of its rows, every word of the box] (the payload's, unchanged) */
  const boxes = (p: RegsPage) => {
    const t = p.keep.trout;
    expect(t?.kind, "a trout table").toBe("table");
    return (t as TroutVM).boxes.map((b) => [b.kind, b.rows.map((r) => r.fish), JSON.stringify(b)] as const);
  };

  it("K3 Shuswap Lake, Nov 11: Region 3's 'Lake trout from Oct 15-Jan 31' release (p.28) puts lake trout in the table's release box; not in July", async () => {
    const lt = async (m: number, d: number) => boxes(await page("wbk:329518145", 0, m, d, "Wednesday"))
      .filter(([, fish]) => fish.includes("LT")).map(([kind]) => kind);
    expect(await lt(11, 11)).toEqual(["release"]);
    // exact (main chat, live bundle with L): the lake trout ALONE in the Release box
    expect(boxes(await page("wbk:329518145", 0, 11, 11, "Wednesday")).filter(([kind]) => kind === "release")
      .map(([, fish, j]) => [fish, JSON.parse(j).head])).toEqual([[["LT"], "Release"]]);
    expect(await lt(7, 15)).not.toContain("release");
  });

  it("K4 Bruer Creek: '20 brook trout from streams' (p.68) is the table's: a keep box holds brook trout and says 20", async () => {
    const p = await page("gnis:10061", await names.part("gnis:10061", "mouth", "source"), 7, 15, "Wednesday");
    const eb = boxes(p).filter(([kind, fish]) => kind === "keep" && fish.includes("EB"));
    expect(eb.length).toBe(1);
    expect(eb[0]![2]).toMatch(/\b20\b/);
    expect([eb[0]![1], JSON.parse(eb[0]![2]).head]).toEqual([["EB"], "Keep 20 in all"]);   // brook trout alone in its box
    expect((p.keep.otherFish ?? []).map((o) => o.name)).not.toContain("Brook trout");   // never a second time
  });

  it("K5 Mat Creek, Jan 15: the Duncan River's 'bull trout daily quota = 2' (p.36) is the table's: a keep box holds Dolly Varden and says 2", async () => {
    const p = await page("gnis:14254", await names.part("gnis:14254", "mouth", "source"), 1, 15, "Thursday");
    const dv = boxes(p).filter(([kind, fish]) => kind === "keep" && fish.includes("DV"));
    expect(dv.length).toBe(1);
    expect(dv[0]![2]).toMatch(/\b2\b/);
    expect([dv[0]![1], JSON.parse(dv[0]![2]).head]).toEqual([["DV"], "Keep 2 in all"]);   // (the band's words are G2's)
    expect((p.keep.otherFish ?? []).map((o) => o.name)).not.toContain("Dolly Varden/bull trout");
  });

  it("G1 Kispiox River: 'No angling from boats, no powered boats' (p.51) — both on the boats line and the Methods list", async () => {
    const p = await page("gnis:2308", 0, 8, 15, "Saturday");
    expect(p.boats!.map((b) => [b.kind, b.text])).toEqual([["in_boat", "No angling (from a boat)"], ["unpowered", "No powered boats"]]);
    expect(p.methods!.boats!.items).toEqual(["No angling (from a boat)", "No powered boats"]);
  });

  it("(T797) 'No angling from powered boats' is on the boats line: the Horsefly below Woodjam Bridge (p.45), not above it; the Chilko above Henry's Crossing on its dates only", async () => {
    const boats = async (item: string, lo: string, hi: string, month: number, day: number) =>
      (await page(item, await names.part(item, lo, hi), month, day, "Wednesday")).boats!.map((b) => [b.kind, b.text]);
    expect(await boats("gnis:24531", "mouth", "horsefly_river__woodjam_bridge", 7, 15))
      .toEqual([["in_boat", "No angling (from a powered boat) — downstream of Woodjam Bridge"]]);
    expect(await boats("gnis:24531", "horsefly_river__woodjam_bridge", "horsefly_river__horsefly_river_falls", 7, 15)).toEqual([]);
    const chilko = ["gnis:13741", "chilko_river__henry_crossing_bridge", "lake_outlet:wbk:329079684"] as const;
    expect((await boats(...chilko, 9, 20)).filter(([k]) => k === "in_boat"))
      .toEqual([["in_boat", "No angling (from a powered boat), Sep 11-Sep 30 — upstream of Henry Crossing bridge"]]);
    expect((await boats(...chilko, 7, 15)).filter(([k]) => k === "in_boat")).toEqual([]);
  });

  // The boats line reads the gear frame's circumstances by token; a token it does not list is dropped without a word
  // (T797: `in_powered_boat`, 47 parts of 35 waters, for as long as the line existed). Province-wide, five days of the year.
  it("(T797) province-wide: every rule held for an angler in a boat, powered or not, is on its page's boats line; no circumstance is one the app does not know", async () => {
    const ords = (await (await st.water()).all("SELECT item_ord FROM item WHERE shown = 1")).map((r) => Number(r.item_ord));
    const days = [[1, 15], [4, 15], [7, 15], [9, 20], [11, 15]] as const;
    const unknown = new Set<string>(), missing: string[] = [], held = { in_boat: 0, in_powered_boat: 0 } as Record<string, number>;
    for (const ord of ords) {
      const [it2, parts] = await Promise.all([st.item(ord), st.parts(ord)]);
      for (let partIx = 0; partIx < parts.length; partIx += 1) {
        if (!parts[partIx]!.facts || parts[partIx]!.tidal) continue;
        for (const [month, day] of days) {
          const inp = await loadRegsInputs(st, { kind: "part", item: it2.id, partIx }, { month, day }, "Wednesday", 2026);
          const ga = gearAnswer(inp);
          if (!ga) continue;
          const shown = new Set(boatsLine(inp).filter((c) => c.kind === "in_boat").map((c) => c.rule));
          for (const c of ga.circumstantial) for (const w of c.while ?? []) {
            if (!BOAT_WHILE.includes(w)) { if (!OTHER_WHILE.includes(w)) unknown.add(w); continue; }
            held[w] = (held[w] ?? 0) + 1;
            const key = inp.rules.get(c.clause[0]).key;
            if (!shown.has(key)) missing.push(`${it2.name} part ${partIx} ${month}-${day}: ${key}`);
          }
        }
      }
    }
    expect([...unknown]).toEqual([]);
    expect(missing).toEqual([]);
    // both kinds are really in the release (a walk that met none would prove nothing)
    expect([held.in_boat! > 0, held.in_powered_boat! > 0]).toEqual([true, true]);
  }, 300_000);

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

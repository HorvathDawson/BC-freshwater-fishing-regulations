/**
 * T43 — the regs page foot's "Fish pictures" credits: the pictures THIS page can show (its Keep pictures, its Other fish
 * thumbs, and the pictures inside the Other fish rows' sheets), every picture of each such fish (its sp: sheet shows them
 * all), as the manifest's credit lines — sorted, unique. Over hand-built pages; the vedder_1010 store page is
 * conformance/src/regs/credits.parity.test.ts (retired, T439).
 */
import { describe, expect, it } from "vitest";
import { creditsFor } from "./credits";
import { FISH, FISH_CREDIT_OF } from "./fish.generated";
import type { FishPicVM, RegsPage, SheetBlock, SheetVM } from "./model";

const pic = (code: string): FishPicVM => ({ code, name: code, caption: `${code} ›`, open: `sp:${code}`, sil: false });
const sil = (code: string): FishPicVM => ({ code, name: code, sil: true });
/** a page with just the parts creditsFor reads; the rest of KeepVM is empty */
function page(o: { groups?: FishPicVM[][]; thumbs?: (string | null)[]; sheets?: Record<string, SheetBlock[]> }): RegsPage {
  const keep = {
    order: [], strip: null, inherited: null, banner: null, plainRows: [], otherFishKey: [], caveats: [], possession: null,
    whereNot: null, outside: null,
    groups: (o.groups ?? []).map((pictures) => ({ tiers: [{ kinds: [{ pictures }] }] })),
    otherFish: o.thumbs ? o.thumbs.map((thumb, i) => ({ thumb, name: "x", value: "", tone: "keep", week: null, sizeHint: null, open: `other:o${i}`, stamps: [] })) : null,
  };
  const sheet = (k: string): SheetVM | null => (o.sheets?.[k] ? { key: k as SheetVM["key"], title: "", sub: null, blocks: o.sheets[k]! } : null);
  return { keep, sheet } as unknown as RegsPage;
}

describe("creditsFor", () => {
  it("a Keep picture credits every picture of that fish (Steelhead: both phases), with the manifest's lines", () => {
    expect(creditsFor(page({ groups: [[pic("RB"), pic("ST")]] }))).toEqual([
      "Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)",
      "Steelhead · ocean — Freshwater Fisheries Society of BC (GoFishBC)",
      "Steelhead · spawning — WDFW (eRegulations WA)",
    ]);
  });
  it("a silhouette (no picture) credits nothing; a page with no pictures credits nothing", () => {
    expect(creditsFor(page({ groups: [[sil("RB")]] }))).toEqual([]);
    expect(creditsFor(page({}))).toEqual([]);
  });
  it("an Other fish thumb, and the pictures its row's sheet opens, are credited; a null thumb is not", () => {
    const p = page({ thumbs: ["KO", null], sheets: { "other:o1": [{ k: "pictures", pics: [pic("GE")] }] } });
    expect(creditsFor(p)).toEqual([
      "Goldeye — Sam Stukel / USFWS (Commons)",
      "Kokanee · spawning — WDFW (sockeye male) (eRegulations WA)",
      "Kokanee — Freshwater Fisheries Society of BC (GoFishBC)",
    ]);
  });
  it("sorted and unique: a fish pictured twice is credited once", () => {
    const got = creditsFor(page({ groups: [[pic("DV")], [pic("DV"), pic("EB")]], thumbs: ["EB"] }));
    expect(got).toEqual(["Brook trout — Freshwater Fisheries Society of BC (GoFishBC)", "Bull trout — Joel Sartore / National Geographic (Commons)",
      "Dolly Varden — WDFW (eRegulations WA)"]);
  });
  it("reads the lines it is handed (mutation: a changed credit line is what the foot shows)", () => {
    const lines = { ...FISH_CREDIT_OF, "rainbow-trout.webp": "RB credit" };
    expect(creditsFor(page({ groups: [[pic("RB")]] }), FISH, lines)).toEqual(["RB credit"]);
    expect(creditsFor(page({ groups: [[pic("RB")]] }), { ...FISH, RB: [] }, lines)).toEqual([]);
  });
  it("(T810 cutover, T1031) the trout table: each row's fish, and the pictures its fish sheet's Identification tab (`tf:<row key>|id`) opens", () => {
    const base = page({ sheets: { "tf:ST/hatchery|id": [{ k: "image", code: "DV" } as unknown as SheetBlock],
      // mutation: the Sources tab's pictures are not the Identification tab's (it draws none; were it read, LT would be credited)
      "tf:ST/hatchery": [{ k: "image", code: "LT" } as unknown as SheetBlock] } });
    const p = { ...base, keep: { ...base.keep, trout: { kind: "table", boxes: [{ rows: [{ key: "RB", fish: "RB" }, { key: "ST/hatchery", fish: "ST" }] }] } } } as unknown as RegsPage;
    expect(creditsFor(p)).toEqual([
      "Bull trout — Joel Sartore / National Geographic (Commons)", "Dolly Varden — WDFW (eRegulations WA)",
      "Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)",
      "Steelhead · ocean — Freshwater Fisheries Society of BC (GoFishBC)", "Steelhead · spawning — WDFW (eRegulations WA)",
    ]);
    // mutation: a table not drawn (null) credits none of its fish
    expect(creditsFor({ ...p, keep: { ...p.keep, trout: null } } as unknown as RegsPage)).toEqual([]);
  });
  it("(T1031) an Other fish row's Identification tab (`other:oN|id`): its pictures are credited too", () => {
    const base = page({ thumbs: [null], sheets: { "other:o0|id": [{ k: "image", code: "GE" } as unknown as SheetBlock] } });
    const p = { ...base, keep: { ...base.keep, otherFish: base.keep.otherFish!.map((r) => ({ ...r, id: "other:o0|id" })) } } as unknown as RegsPage;
    expect(creditsFor(p)).toEqual(["Goldeye — Sam Stukel / USFWS (Commons)"]);
    expect(creditsFor(base)).toEqual([]);   // mutation: a row with no Identification key reads no such sheet
  });
  it("a frozen page with no sheet function (a render fixture) reads its Keep alone", () => {
    const p = { ...page({ groups: [[pic("RB")]] }), sheet: undefined } as unknown as RegsPage;
    expect(creditsFor(p)).toEqual(["Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)"]);
  });
});

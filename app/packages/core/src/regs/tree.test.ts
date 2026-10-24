/**
 * tree.ts, store-free, over the hand-built Vedder-like group (testing/keepFixtures.ts; sentences copied from golden
 * vedder_1010). Golden parity on all 130 tiers / 273 kinds is app/conformance/src/regs/tree.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { FishManifest } from "./fish.generated";
import { headCtx } from "./grouphead";
import { plain, type Line } from "./line";
import type { KindVM, TierVM } from "./model";
import { dutyWords, kindRow, pictures, presOf, sizeOf, stampSTOf, tierNode, tierWords, treeEnv, treeOf } from "./tree";
import { cardOf, GROUP, INP, kind, OWN, ROOT, rowOf, SHARE, tier, TROUT_ROW } from "./testing/keepFixtures";

const FISH: FishManifest = {
  EB: [{ file: "b.webp", caption: "Brook trout", credit: "", type: "", source: "", phase: null, standInFor: null, width: 2, height: 1 }],
  ST: [{ file: "s.webp", caption: "Steelhead", credit: "", type: "", source: "", phase: null, standInFor: null, width: 2, height: 1 }],
  RB: [{ file: "r.webp", caption: "Rainbow trout", credit: "", type: "", source: "", phase: null, standInFor: null, width: 2, height: 1 }],
};
const LIC_ST = { holds: {}, profiles: [{ documents: [{ doc: "steelhead_stamp" }] }] };
const envOf = (o: { tot?: "a" | "b"; licence?: unknown; group?: ReturnType<typeof GROUP>; rows?: ReturnType<typeof rowOf>[] } = {}) => {
  const inp = INP({ licence: o.licence });
  const card = cardOf([{ ...TROUT_ROW, group: o.group ?? GROUP() }, ...(o.rows ?? [])]);
  return treeEnv(headCtx(0, card, inp)!, FISH, new Set([0]), o.tot ?? "a");
};
const nbspFree = (l: Line | null | undefined) => plain(l).replace(/ /g, " ");

describe("sizeOf (prototype, verbatim)", () => {
  it("splits the detail into its size words and its other words; a real size beats 'any size'", () => {
    expect(sizeOf("rainbow trout, over 50 cm")).toEqual({ size: "over 50 cm", q: ["rainbow trout"] });
    expect(sizeOf("hatchery 30–50 cm, any size")).toEqual({ size: "30–50 cm", q: ["hatchery"] });
    expect(sizeOf("any size")).toEqual({ size: "any size", q: [] });
    expect(sizeOf("catch and release")).toEqual({ size: "", q: ["catch and release"] });
    expect(sizeOf(null)).toEqual({ size: "", q: [] });
  });
});

describe("tierWords", () => {
  it("the tier's own number big; a size box's extra number after ' — ' a big2 (golden vedder_1010 t4)", () => {
    const w = tierWords(SHARE.children[0] as never);
    expect([nbspFree(w), w.filter((r) => r.t === "num")]).toEqual(["Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead",
      [{ t: "num", s: "1", size: "big" }, { t: "num", s: "2", size: "big2" }]]);
    expect(tierWords(tier({ tier: "size", n: 1, says: "Only 1 over 50 cm — 2 if one is a hatchery steelhead" })).filter((r) => r.t === "num"))
      .toEqual([{ t: "num", s: "1", size: "big" }]);   // no extra: one number only
  });
});

describe("dutyWords", () => {
  it("cap, and 'this river' becomes the water's own name — once", () => {
    expect(dutyWords("stop fishing this river after 2 a day", "Vedder River")).toBe("Stop fishing the Vedder River after 2 a day");
    // the prototype capitalises BEFORE the swap, so a duty that opens on "this creek" reads "the …" (no store duty does)
    expect(dutyWords("this Creek, then this stream", "Wilkes Creek")).toBe("the Wilkes Creek, then this stream");
  });
});

describe("pictures", () => {
  it("the first manifest picture captioned with a non-breaking ›; no picture: a silhouette that opens nothing", () => {
    expect(pictures(["EB", "LT"], FISH, (c) => (c === "EB" ? "Brook trout" : "Lake trout"))).toEqual([
      { code: "EB", name: "Brook trout", caption: "Brook trout ›", open: "sp:EB", sil: false }, { code: "LT", name: "Lake trout", sil: true }]);
  });
});

describe("kindRow", () => {
  const env = envOf({ licence: LIC_ST });
  it("the steelhead (golden vedder_1010): How to tell, the chip, its qualifier, duty with the water's name and the stamp link, presence", () => {
    const k = kindRow((SHARE.children[0] as typeof SHARE).children[1] as never, env);
    expect([nbspFree(k.says), k.size, k.qualifiers, plain(k.duty), plain(k.presence), k.pictures.map((p) => p.code)]).toEqual([
      "Up to 2 may be hatchery steelhead only — wild steelhead go back How to tell ›", { text: "over 50 cm", rule: false }, "rainbow trout",
      "10 a year · Stop fishing the Vedder River after 2 a day · Record each on your licence right away Steelhead stamp ›",
      "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ", ["ST"]]);
    expect([k.says!.at(-1), k.duty!.at(-1)]).toEqual([{ t: "link", s: "How to tell ›", open: "hatch" }, { t: "link", s: "Steelhead stamp ›", open: { tab: "licence" } }]);
  });
  it("no stamp in the licence answer: no stamp link; a size_rule chip is the bold one", () => {
    const k = kindRow((SHARE.children[0] as typeof SHARE).children[0] as never, envOf());
    expect([nbspFree(k.says), k.size, k.duty]).toEqual(["Up to 2 may be hatchery brook, brown or cutthroat trout",
      { text: "keep 30 cm (12 in) or longer; release the rest", rule: true }, null]);
    const st = kindRow((SHARE.children[0] as typeof SHARE).children[1] as never, envOf());
    expect(plain(st.duty)).toBe("10 a year · Stop fishing the Vedder River after 2 a day · Record each on your licence right away");
  });
  it("T128: a line naming its total bolds the kind's own number (after 'up to' / 'only'), never the total's", () => {
    const k = kindRow(kind({ species: ["EB", "GB", "CT"], name: "Brook trout, brown trout and cutthroat trout", n: 2,
      says: "Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown or cutthroat trout" }), env);
    expect(k.says).toEqual([{ t: "text", s: "Of your 2 trout and char a day in streams, up to " }, { t: "num", s: "2", size: "inline" },
      { t: "text", s: " may be hatchery brook, brown or cutthroat trout" }]);
    const only = kindRow(kind({ species: ["LT"], name: "Lake trout", n: 1, says: "Of your 1 trout and char a day, only 1 may be lake trout" }), env);
    expect(only.says!.filter((r) => r.t === "num").length).toBe(1);
    expect(only.says![0]).toEqual({ t: "text", s: "Of your 1 trout and char a day, only " });
    // T128 review: an own rule of only some of the group's fish is named by its number ("Of the 3 a day under …")
    const own = kindRow(kind({ species: ["DV"], name: "Dolly Varden/bull trout", n: 1,
      says: "Of the 3 a day under this lake’s rule, only 1 may be Dolly Varden/bull trout" }), env);
    expect(own.says).toEqual([{ t: "text", s: "Of the 3 a day under this lake’s rule, only " }, { t: "num", s: "1", size: "inline" },
      { t: "text", s: " may be Dolly Varden/bull trout" }]);
    // a hatchery-only steelhead held to fewer than its total ("only 1 … — wild steelhead go back") still links How to tell
    const st = kindRow(kind({ species: ["ST"], name: "Steelhead", n: 1,
      says: "Of your 5 trout and char a day, only 1 may be hatchery steelhead — wild steelhead go back" }), env);
    expect(st.says!.at(-1)).toEqual({ t: "link", s: "How to tell ›", open: "hatch" });
    // the old words (no total named) bold as before: the first number
    const old = kindRow(kind({ species: ["RB"], name: "Rainbow trout", n: 2, says: "Up to 2 hatchery rainbow trout (part of the 2 from streams)" }), env);
    expect(old.says![0]).toEqual({ t: "text", s: "Up to " });
  });
  it("stamps (rows 31): each stored stamp is a line — 'You need' + the document linked to the Licence tab + its when, lower-cased first", () => {
    const rb = { doc: "shuswap_rainbow_stamp", act: "retaining" as const, name: "Conservation Surcharge Stamp for Shuswap rainbow trout",
      when: "Only if you keep rainbow trout over 50 cm" };
    const k = kindRow(kind({ species: ["RB"], name: "Rainbow trout", n: 1, says: "Up to 1 may be rainbow trout", stamps: [rb] }), env);
    expect(k.stamps.map(plain)).toEqual(["You need Conservation Surcharge Stamp for Shuswap rainbow trout › only if you keep rainbow trout over 50 cm"]);
    expect(k.stamps[0]).toEqual([{ t: "text", s: "You need " },
      { t: "link", s: "Conservation Surcharge Stamp for Shuswap rainbow trout ›", open: { tab: "licence" } },
      { t: "text", s: " only if you keep rainbow trout over 50 cm" }]);
    expect(kindRow(kind({ species: ["RB"], name: "Rainbow trout", n: 1, says: "x" }), env).stamps).toEqual([]);   // none stored: none drawn
  });
  it("stamps: the steelhead stamp the kind's duty line already links is not drawn twice (the prototype's skip); other stamps stay", () => {
    const st = { doc: "steelhead_stamp", act: "targeting" as const, name: "Steelhead Conservation Surcharge Stamp",
      when: "Only if you fish for steelhead, even to release them" };
    const base = (SHARE.children[0] as typeof SHARE).children[1] as never as ReturnType<typeof kind>;
    expect(kindRow({ ...base, stamps: [st] }, env).stamps).toEqual([]);                              // duty + stamp link: skipped
    expect(kindRow({ ...base, stamps: [st] }, envOf()).stamps.map(plain)).toEqual([                  // no stamp link drawn: the line is
      "You need Steelhead Conservation Surcharge Stamp › only if you fish for steelhead, even to release them"]);
    expect(kindRow({ ...base, duties: [], stamps: [st] }, env).stamps.map(plain)).toEqual([         // no duty line: the line is
      "You need Steelhead Conservation Surcharge Stamp › only if you fish for steelhead, even to release them"]);
  });
  it("a zero kind: no number, no chip; a folded kind: no sentence (its tier names it)", () => {
    const z = kindRow(kind({ species: ["CT"], name: "Cutthroat trout", n: 0, says: "No cutthroat trout may be kept", detail: "catch and release, 30 cm" }), env);
    expect([plain(z.says), z.says!.some((r) => r.t === "num"), z.size, z.qualifiers, z.zero]).toEqual(["No cutthroat trout may be kept", false, null, "catch and release", true]);
    const f = kindRow(OWN.children[0] as never, env);
    expect([f.folded, f.says, f.size]).toEqual([true, null, { text: "50 cm or less", rule: false }]);
  });
});

describe("presOf", () => {
  it("the kind's own presence; else the presence of an UNDRAWN row with the kind's fish (Kitimat's steelhead)", () => {
    const pres = { line: "possible_with_rules" as const, says: "Steelhead rules apply here; steelhead may not be present in this water." };
    const st = rowOf({ pool: 6, members: ["ST"], all_members: ["ST"], presence: pres });
    const card = cardOf([TROUT_ROW, st]);
    const k = kind({ species: ["ST"] });
    expect(presOf(k, card, new Set([0]))).toEqual(pres);
    expect(presOf(k, card, new Set([0, 1]))).toBeNull();             // drawn: it says it on its own row
    expect(presOf(kind({ species: ["ST", "RB"] }), card, new Set([0]))).toBeNull();
  });
});

describe("tierNode / treeOf", () => {
  it("form a: the share that only repeats the lead folds — no kicker, no words; its origin keeps How to tell and the root's page", () => {
    const t = treeOf(envOf())![0] as TierVM;
    expect([t.tid, t.folded, t.kicker, t.line, plain(t.origin!.says), t.origin!.howtell, plain(t.origin!.book)]).toEqual(
      ["t3", true, null, [], "From streams, keep hatchery fish only — wild trout and char go back.", true, "Rule book p.21 ↗"]);
  });
  it("form b: the share draws its kicker (the moved tag) and its words; its page is the root's, so no link", () => {
    const t = treeOf(envOf({ tot: "b" }))![0] as TierVM;
    expect([t.folded, t.kicker, plain(t.line), t.line.filter((r) => r.t === "num")]).toEqual(
      [false, "All Region 2 streams", "Only 2 of the 4 may be from streams", [{ t: "num", s: "2", size: "big" }]]);
  });
  it("a folded share with no origin: its fresh page alone, on its line", () => {
    const g = GROUP({ tree: [{ ...ROOT, children: [{ ...SHARE, origin: null, by: 3 }, OWN] }] });
    const t = treeOf(envOf({ group: g }))![0] as TierVM;
    expect([t.folded, t.origin, plain(t.line)]).toEqual([true, null, "Rule book p.22 ↗"]);
  });
  it("the own tier: its fresh page on its line, its relation under it; a page shown above is not repeated below", () => {
    const own = treeOf(envOf())![1] as TierVM;
    expect([plain(own.line), own.relation, own.sub, own.short]).toEqual(["This river’s rule: up to 4 hatchery rainbow trout a day Rule book p.22 ↗",
      "Not part of the 2 from streams; part of the 4 a day across Region 2", null,
      "4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams"]);
    const env = envOf();
    const nested = tierNode(tier({ id: "x", tier: "own", n: 1, by: 5, says: "up to 1", children: [tier({ id: "y", tier: "count", n: 1, by: 3, says: "1 in all" })] }),
      env, false, new Set([21]));
    expect([plain(nested.line), plain((nested.children[0] as TierVM).line)]).toEqual(["up to 1 Rule book p.22 ↗ Rule book p.23 ↗", "1 in all"]);
    const why = tierNode(tier({ tier: "own", n: 2, by: 3, says: "This river’s rule: up to 2 a day", why_here: "its row covers its tributaries, so it applies here" }), env, false, new Set([22]));
    expect([plain(why.line), why.sub, why.band]).toEqual(["This river’s rule: up to 2 a day", "Its row covers its tributaries, so it applies here", false]);
  });
  it("no tree: null (the legacy buckets); the size tier is a band", () => {
    expect(treeOf(envOf({ group: GROUP({ tree: [] }) }))).toBeNull();
    const size = (treeOf(envOf())![0] as TierVM).children[0] as TierVM;
    expect([size.band, (size.children as KindVM[]).map((k) => k.kind)]).toEqual([true,
      ["Brook trout, brown trout and cutthroat trout", "Steelhead"]]);
  });
  it("stampST reads the licence answer's documents only", () => {
    expect([stampSTOf(INP({ licence: LIC_ST })), stampSTOf(INP()),
      stampSTOf(INP({ licence: { holds: {}, profiles: [{ documents: [], exempt: { by: [97], from: ["steelhead_stamp"] } }] } }))]).toEqual([true, false, false]);
  });
});

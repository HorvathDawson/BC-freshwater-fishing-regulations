/**
 * grouphead.ts, store-free, over the hand-built Vedder-like group (testing/keepFixtures.ts). Golden parity on all 97
 * groups is app/conformance/src/regs/grouphead.parity.test.ts. Form b and the origin tag are drawn by no golden state
 * (golden meta.misses `[data-share-of]`, `.gtag`) — they are pinned here.
 */
import { describe, expect, it } from "vitest";
import { groupHeadOf, headCtx, originOf, rowTitle, scopeBadge, shortOf } from "./grouphead";
import { plain } from "./line";
import { cardOf, GROUP, hyd, INP, OWN, res, ROOT, rowOf, SHARE, tier, TROUT_ROW } from "./testing/keepFixtures";

const inp = INP();

describe("rowTitle (page v36)", () => {
  it("a pool written as one book group is the group's name; trout-and-char except char is 'Trout'", () => {
    expect(rowTitle(hyd(TROUT_ROW), inp)).toBe("Trout and char");
    const r = hyd(rowOf({ pool: 1, members: ["RB"] }));
    const trout = { ...r, pool: { ...r.pool!, f: { species: ["TROUT_CHAR"], species_except: ["CHAR"] } } };
    expect(rowTitle(trout, inp)).toBe("Trout");
  });
  it("else the members' names; protected species; a member-less row by its winner's first species", () => {
    expect(rowTitle(hyd(rowOf({ pool: 3, members: ["LMB", "SMB"] })), inp)).toBe("Bass");
    expect(rowTitle(hyd(rowOf({ members: ["KO", "RB"] })), inp)).toBe("Kokanee and rainbow trout");
    expect(rowTitle(hyd(rowOf({ prot: ["GREEN_STURGEON"] })), inp)).toBe("Protected species");
    expect(rowTitle(hyd(rowOf({ win: 6 })), inp)).toBe("Steelhead");
  });
});

describe("scopeBadge (page v36 scopeBadge + rowHtml's rewrite)", () => {
  it("a region share of streams whose narrow binds every fish: 'All Region 2 streams'; one that doesn't: 'Region 2-wide'", () => {
    const card = cardOf([TROUT_ROW]);
    expect(scopeBadge(card.rows[0]!, card, inp)).toBe("All Region 2 streams");
    const part = cardOf([{ ...TROUT_ROW, members: ["RB", "KO"] }]);
    expect(scopeBadge(part.rows[0]!, part, inp)).toBe("Region 2-wide");
  });
  it("a lake share keeps page v36's own words; no share: '<region>-wide'; a water's own pool: 'This water'; no badge", () => {
    const lake = cardOf([{ ...TROUT_ROW, narrow: 5 }]);
    const lakeRow = { ...lake.rows[0]!, narrow: { ...lake.rows[0]!.narrow!, f: { water: "lake" } } };
    expect(scopeBadge(lakeRow, lake, inp)).toBe("Region 2 · lakes");
    const wide = cardOf([{ ...TROUT_ROW, scope: { of: "region", entry: "r2", share: false, apart: false } }]);
    expect(scopeBadge(wide.rows[0]!, wide, inp)).toBe("Region 2-wide");
    const here = cardOf([{ ...TROUT_ROW, scope: { of: "water", entry: null, share: false, apart: false } }]);
    expect(scopeBadge(here.rows[0]!, here, inp)).toBe("This water");
    const bc = cardOf([{ ...TROUT_ROW, scope: { of: "bc", entry: null, share: false, apart: false } }]);
    expect(scopeBadge(bc.rows[0]!, bc, inp)).toBe("B.C.-wide");
    const rel = cardOf([{ ...TROUT_ROW, kind: "release" }]);
    expect(scopeBadge(rel.rows[0]!, rel, inp)).toBeNull();
  });
  it("a row whose every fish's stored answer names another pool `lifted`: 'Separate from your 4 trout and char' (x25p_1010)", () => {
    const st = rowOf({ pool: 6, daily: 6, members: ["ST"], all_members: ["ST"], scope: { of: "water", entry: null, share: false, apart: false } });
    const card = cardOf([TROUT_ROW, st], { R: { ST: { H: res([[1, "lifted"]]), W: null } } });
    expect(scopeBadge(card.rows[1]!, card, inp)).toBe("Separate from your 4 trout and char");
    const notLifted = cardOf([TROUT_ROW, st], { R: { ST: { H: res([[1, "agrees"]]), W: null } } });
    expect(scopeBadge(notLifted.rows[1]!, notLifted, inp)).toBe("This water");
  });
});

describe("headCtx + groupHeadOf + originOf", () => {
  const card = cardOf([TROUT_ROW]);
  const ctx = headCtx(0, card, inp)!;

  it("the head is the root tier; the share carrying the headline's 2 is relTier; the stream tag moves onto that share", () => {
    expect([ctx.hdrRoot?.id, ctx.relTier?.id, ctx.moveTag, ctx.tag, ctx.scope, ctx.tagShare?.id, ctx.tierOrg, ctx.howtell, [...ctx.rootPages]])
      .toEqual(["t1", "t3", true, "All Region 2 streams", null, "t3", true, true, [21]]);
    expect(headCtx(0, cardOf([{ ...TROUT_ROW, group: { ...GROUP(), headline: null as never } }]), inp)).toBeNull();
  });

  it("form a (the default, r20h): the stream's 2 leads, then 'of the' the region's 4 — whole strings (golden vedder_1010 head)", () => {
    const h = groupHeadOf(ctx, { tot: "a" });
    expect([h.form, h.rootTier, plain(h.lead), h.leadN, plain(h.words), h.ofTotal, h.whyHere, h.shareOf, h.nolimit]).toEqual(
      ["a", "t1", "2 a day per angler from streams", 2, "4 a day per angler across Region 2, lakes included", true, null, null, false]);
    expect(h.lead!.filter((r) => r.t === "num")).toEqual([{ t: "num", s: "2", size: "big" }]);
  });

  it("form b: the total leads, '↳ 2 of them from streams' under it, its 2 a big2 number", () => {
    const h = groupHeadOf(ctx, { tot: "b" });
    expect([h.form, plain(h.words), h.ofTotal, plain(h.shareOf), h.lead]).toEqual(
      ["b", "4 a day per angler across Region 2, lakes included", false, "↳ 2 of them from streams", null]);
    expect(h.shareOf!.find((r) => r.t === "num")).toEqual({ t: "num", s: "2", size: "big2" });
  });

  it("no relTier (the head's number is the headline's): the root's words, why it applies here, no form", () => {
    const g = GROUP({ headline: { n: 4, unit: "a day", says: "4 a day" },
      tree: [{ ...ROOT, why_here: "its row covers its tributaries, so it applies here" }] });
    const h = groupHeadOf(headCtx(0, cardOf([{ ...TROUT_ROW, group: g }]), inp)!, { tot: "a" });
    expect([h.form, plain(h.words), h.whyHere, h.nolimit]).toEqual(
      [null, "4 a day per angler across Region 2, lakes included", "Its row covers its tributaries, so it applies here", false]);
    const nl = groupHeadOf(headCtx(0, cardOf([{ ...TROUT_ROW, group: GROUP({ headline: { n: null, unit: "", says: "No limit" },
      tree: [{ ...ROOT, n: null, says: "No limit on trout and char" }] }) }]), inp)!, { tot: "a" });
    expect([plain(nl.words), nl.nolimit, nl.words.some((r) => r.t === "num")]).toEqual(["No limit on trout and char", true, false]);
  });

  it("origin: none when the origin sits on a tier (vedder_1010); the root's page alone when no origin anywhere (klwa_1010)", () => {
    expect(originOf(ctx)).toBeNull();
    const bare = headCtx(0, cardOf([{ ...TROUT_ROW, group: GROUP({ tree: [{ ...ROOT, children: [{ ...SHARE, origin: null }, OWN] }] }) }]), inp)!;
    expect(originOf(bare)).toEqual({ says: [], tag: null, howtell: false, book: [{ t: "book", printed: 21, pdf: 23 }] });
  });

  it("origin: the group's words whole with How to tell and the root's page; the tag before ' — ' when the header names one", () => {
    const o = "Keep hatchery fish only — wild trout go back.";
    const whole = headCtx(0, cardOf([{ ...TROUT_ROW, group: GROUP({ origin: o }) }]), inp)!;
    expect(originOf(whole)).toEqual({ says: [{ t: "text", s: o }], tag: null, howtell: true, book: [{ t: "book", printed: 21, pdf: 23 }] });
    const tagged = headCtx(0, cardOf([{ ...TROUT_ROW, group: GROUP({ origin: o, header: { says: "", tier: "t1", tag: "Hatchery only" } }) }]), inp)!;
    expect(originOf(tagged)).toEqual({ says: [{ t: "text", s: "wild trout go back." }], tag: "Hatchery only", howtell: true,
      book: [{ t: "book", printed: 21, pdf: 23 }] });
  });
});

describe("shortOf (prototype; T28's pin lines, form b)", () => {
  it("each tier as one short line, whole strings", () => {
    expect([shortOf(ROOT), shortOf(SHARE), shortOf(SHARE.children[0] as never), shortOf(OWN)]).toEqual([
      "4 a day across Region 2", "2 of them from streams", "of those, 1 over 50 cm (2 if one is a hatchery steelhead)",
      "4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams"]);
    expect([shortOf(tier({ tier: "count", says: "1 cutthroat or rainbow trout in all (part of the 2)" })),
      shortOf(tier({ tier: "own", n: 2, whose: "This lake’s rule", replaces: "x" })), shortOf(tier({ tier: "total", n: 5, whose: "The lake" })),
      shortOf(tier({ tier: "share", n: 2, says: "2 a day from this lake" }))]).toEqual([
      "of those, 1 cutthroat or rainbow trout in all", "plus up to 2 under this lake’s rule", "The lake: 5 a day", "2 a day from this lake"]);
  });
});

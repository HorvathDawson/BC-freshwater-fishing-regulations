/**
 * T21 parity: the whole Keep tab — core `buildRegsPage(…).keep` over each golden state's inputs (T12 `cases("keep")`, the
 * live release paired with the oracles) — against golden.json (regs v38 @ 7fb6809f) on all 134 states:
 * - `order`: every block in draw order, mapped to its golden selector, equals golden `keep.order`;
 * - each block equals what its presenter's own parity proved (the composition hands every presenter the same hydrated
 *   place / settled day / card model, and draws its output whole);
 * - `keep.lines`: the tab's every drawn line, block by block, equals golden `keep.lines` (catches a missing, extra or
 *   misplaced block and any line no field parity reads).
 * The keyed exclusions (exclusions.ts) are the only things not compared, and the summary names them: the D19 pooled row
 * (klmb_1010), the pending stamp lines, and the Kitimat-ruled groups (compared to the golden's own group lines
 * there, since tree/tail parity holds those to the store).
 */
import { afterAll, describe, expect, it } from "vitest";
import {
  bannerOf, buildModel, caveatsOf, headerOf, inheritedOf, makePlace, otherFishOf, FISH, plainRowOf, plainRowSeams, possessionOf,
  settle, settleAt, stripOf, whereNotOf, type KeepVM, type RegsPage,
} from "@app/core";
import { eqPlain, norm } from "./eq";
import { cases, requestOf, summary } from "./golden";
import { D19_GROUPS, droppedLines, excluded, keepLines as dropLines } from "./exclusions";
import { groupLinesProj, keepOf as groupsOf } from "./keepGroups";
import { keepLines, plainRowLines, selectorOf } from "./keepPage";

interface GKeep { order: string[]; lines: string[]; plainRows: string[][]; groups: { lines: string[] }[] }

/** D19: the golden's pooled no-headline article (its plainRows entry and its place in order), or null. */
function d19(id: string, g: GKeep): { article: number; lines: string[] } | null {
  const k = g.plainRows.findIndex((_, i) => excluded("keep", id, `plainRows.${i}`) !== null);
  return k < 0 ? null : { article: k, lines: g.plainRows[k]! };
}
/** golden order with the D19 article removed */
function goldenOrder(id: string, g: GKeep): string[] {
  const x = d19(id, g);
  if (!x) return g.order;
  let seen = -1;
  return g.order.filter((s) => s !== "article.row.ledger" || ++seen !== x.article);
}
/** golden keep.lines with the D19 row's lines and the keyed pending lines removed */
function goldenLines(id: string, g: GKeep): string[] {
  let lines = g.lines.map(norm);
  const x = d19(id, g);
  if (x) {
    const want = x.lines.map(norm), at = lines.findIndex((_, i) => want.every((l, j) => lines[i + j] === l));
    if (at >= 0) lines = [...lines.slice(0, at), ...lines.slice(at + want.length)];
  }
  return dropLines(lines, droppedLines("keep", id, "lines"));
}
/** the app's Keep VM with its D19 (a) group(s) set aside (exclusions.ts D19_GROUPS: a group the golden draws as the legacy
 *  article `d19` drops) — each asserted present once — so order and lines compare what both sides draw */
function comparableKeep(id: string, k: KeepVM): KeepVM {
  const off = new Set(k.groups.flatMap((x, i) => (D19_GROUPS.some((d) => d.id === id && d.row === x.row) ? [i] : [])));
  for (const d of D19_GROUPS.filter((x) => x.id === id))
    if (k.groups.filter((x) => x.row === d.row).length !== 1) throw new Error(`D19_GROUPS: ${id} has no single group on row r${d.row}`);
  if (!off.size) return k;
  const renum = (i: number) => i - [...off].filter((j) => j < i).length;
  return { ...k, groups: k.groups.filter((_, i) => !off.has(i)),
           order: k.order.flatMap((b): KeepVM["order"][number][] =>
             (typeof b !== "string" && "group" in b ? (off.has(b.group) ? [] : [{ group: renum(b.group) }]) : [b])) };
}
/** the golden's own group lines where a keyed exclusion holds the group to the store (Kitimat) */
const groupOverride = (id: string, g: GKeep) => (i: number): string[] | null =>
  excluded("keep", id, `groups.${i}.lines`) ? dropLines(g.groups[i]!.lines.map(norm), droppedLines("keep", id, "lines")) : null;

let passed = 0, total = 0;
afterAll(() => { if (total) console.info(summary("keep", passed, total).replace(/^keep/, "keep (T21 whole tab)")); });

describe("T21 Keep tab vs golden keep (every state)", () => {
  // (T115) the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`) are not cases: their 2 groups and 4 plain rows go with them;
  // (T183) and the West Road's x27p1 (its Region 6 stretch shares part 0's rules now, taps.json `gone`): its group goes with it
  it("127/127 states: order, every block as its presenter draws it, and every drawn line", async () => {
    const bad: string[] = [];
    let groups = 0, plains = 0, caveats = 0;
    for await (const c of cases("keep")) {
      total++;
      const g = c.g.keep as GKeep, inp = await c.inputs(), page = await c.page(), k = page.keep, kc = comparableKeep(c.id, k);
      const errs: string[] = [];
      // order
      const order = eqPlain(kc.order.map((b) => selectorOf(b, kc)), goldenOrder(c.id, g));
      if (!order.ok) errs.push(`order\n${order.diff}`);
      // each block = its presenter over the same hydrated day
      const place = makePlace(inp), s = settle(inp, place), card = buildModel(inp, place, s), today = requestOf(c.id).today;
      const open = !!s.status && s.status.status !== "closed" && s.status.status !== "tidal";
      const strip = inp.key?.tidal ? null : stripOf(inp, s, card, today);
      const want: Partial<KeepVM> = {
        strip, banner: bannerOf(inp, s, strip), inherited: inp.key?.tidal ? null : inheritedOf(inp),
        caveats: open ? caveatsOf(inp, s, (j) => settleAt(inp, place, j, s.day)) : [],
        otherFish: open ? otherFishOf(card, inp, FISH) : null, possession: open ? possessionOf(card, inp, s) : null,
        whereNot: inp.key?.tidal ? null : whereNotOf(s),
      };
      for (const [f, v] of Object.entries(want)) if (JSON.stringify(k[f as keyof KeepVM]) !== JSON.stringify(v)) errs.push(`${f} ≠ its presenter`);
      const gl = groupsOf(inp).groups;
      if (k.groups.length !== gl.length) errs.push(`groups: ${k.groups.length} ≠ ${gl.length}`);
      k.groups.forEach((x, i) => {
        if (gl[i] && JSON.stringify(groupLinesProj(gl[i]!)) !== JSON.stringify(keepLines({ ...k, order: [{ group: i }] }, inp.day)))
          errs.push(`group ${i} ≠ T17–T19 groupLinesProj`);
      });
      const seams = plainRowSeams(inp, FISH);
      k.plainRows.forEach((r) => {
        if (JSON.stringify(r) !== JSON.stringify(plainRowOf(r.row, { inp, card, settled: s, seams }))) errs.push(`plain row r${r.row} ≠ plainRowOf`);
      });
      // every drawn line
      const lines = eqPlain(keepLines(kc, inp.day, groupOverride(c.id, g)), goldenLines(c.id, g));
      if (!lines.ok) errs.push(`lines\n${lines.diff}`);
      groups += kc.groups.length; plains += k.plainRows.length; caveats += k.caveats.length;
      if (errs.length) bad.push(`${c.id}: ${errs.join("\n")}`); else passed++;
    }
    expect(bad).toEqual([]);
    expect([total, groups, plains, caveats]).toEqual([127, 94, 27, 9]);
  });

  it("mutation pins: a swapped, dropped or reworded block fails the order or the lines comparison", async () => {
    let c0: { id: string; g: GKeep; page: RegsPage } | null = null;
    for await (const c of cases("keep")) if (c.id === "glenn_501") c0 = { id: c.id, g: c.g.keep as GKeep, page: await c.page() };
    if (!c0) return;
    const k = c0.page.keep, day = c0.page.day;
    const orderOk = (vm: KeepVM) => eqPlain(vm.order.map((b) => selectorOf(b, vm)), goldenOrder(c0!.id, c0!.g)).ok;
    const linesOk = (vm: KeepVM) => eqPlain(keepLines(vm, day, groupOverride(c0!.id, c0!.g)), goldenLines(c0!.id, c0!.g)).ok;
    expect([orderOk(k), linesOk(k)]).toEqual([true, true]);
    const swap = [...k.order]; [swap[3], swap[4]] = [swap[4]!, swap[3]!];                         // Other fish ↔ possession
    const mut: Record<string, KeepVM> = {
      swapped: { ...k, order: swap },
      inheritedLast: { ...k, order: [...k.order.filter((b) => b !== "inherited"), "inherited"] },
      noWhereNot: { ...k, order: k.order.filter((b) => b !== "whereNot") },
      legend: { ...k, strip: { ...k.strip!, legend: k.strip!.legend.slice(1) } },
      noCaution: { ...k, inherited: { ...k.inherited!, lines: k.inherited!.lines.map((l) => ({ ...l, caution: null })) } },
      noWorked: { ...k, groups: k.groups.map((x) => ({ ...x, worked: null })) },
      noKey: { ...k, otherFishKey: [] },
      possession: { ...k, possession: { ...k.possession!, value: "1 day’s quota" } },
    };
    expect(Object.fromEntries(Object.entries(mut).map(([n, vm]) => [n, orderOk(vm) && linesOk(vm)]))).toEqual(
      Object.fromEntries(Object.keys(mut).map((n) => [n, false])));
  });

  const at = async (id: string): Promise<RegsPage | null> => { for await (const c of cases("keep")) if (c.id === id) return c.page(); return null; };

  it("spot checks: the ticket's literal orders", async () => {
    const order = async (id: string) => (await at(id))?.keep.order ?? null;
    const o = await order("vedder_1010");
    if (!o) return;
    expect(o).toEqual(["strip", { group: 0 }, "otherFish", "possession", "whereNot"]);
    expect(await order("vedder_615")).toEqual(["strip", "banner", "whereNot"]);
    expect(await order("glenn_501")).toEqual(["strip", "inherited", { group: 0 }, "otherFish", "possession", "whereNot"]);
    expect(await order("x31p_1010")).toEqual(["banner"]);
    const x25 = (await at("x25p_1010"))!.keep.groups.map((x) => [`r${x.row}`, x.title]);
    expect(x25).toEqual([["r0", "Trout and char"], ["r2", "Steelhead"]]);
  });

  it("vedder_1010: the page's status and header; the group head (form a), the Other fish key, possession, where-not", async () => {
    const p = await at("vedder_1010");
    if (!p) return;
    expect([p.version, p.status, p.today, p.header.dayWord, p.header.date, p.header.dot]).toEqual(["regs-v38", "open", true, "Today", "Oct 10", "open"]);
    const g = p.keep.groups[0]!;
    expect([g.title, g.scope, g.head.form, g.head.leadN, g.more]).toEqual(["Trout and char", null, "a", 2, { text: "See every regulation line ›", open: "src:r0" }]);
    expect(p.keep.otherFishKey.map((x) => `${x.status} ${x.says}`)).toEqual([
      "release You may fish for it, but every one you catch must go back into the water, quickly and carefully. You can't keep any.",
      "closed You may not fish for it at all, not even to release it. If you catch one by accident, release it at once."]);
    expect([p.keep.possession!.label, p.keep.possession!.value, p.keep.whereNot!.heading]).toEqual(["Possession", "2 days’ quota", "Where you can’t fish"]);
    expect([p.gear?.day, p.methods?.ways?.label]).toEqual(["Today, Oct 10", "Ways to fish"]);   // (T30–T32) Gear and Methods are built
    expect(p.licence?.label).toBe("B.C. resident, 16 or older");   // (T33) licence.parity compares the whole tab
    expect(p.sheet(p.keep.possession!.open)?.title).toBe("Possession");   // (T36) the possession row opens its sheet
  });

  it("vedder_615 (closed) and chw0_1010 (closed all year): strip, banner, where-not — no rows, Other fish or possession", async () => {
    for (const id of ["vedder_615", "chw0_1010"]) {
      const p = await at(id);
      if (!p) return;
      expect([p.status, p.header.dot, p.keep.order, p.keep.groups.length, p.keep.plainRows.length, p.keep.otherFish, p.keep.possession], id)
        .toEqual(["closed", "closed", ["strip", "banner", "whereNot"], 0, 0, null, null]);
    }
    expect((await at("chw0_1010"))!.keep.banner!.big).toEqual(["Closed all year"]);
  });

  it("x31p_1010 (tidal): the tidal banner alone; status tidal, the header dot faint", async () => {
    const p = await at("x31p_1010");
    if (!p) return;
    expect([p.status, p.header.dot, p.keep.strip, p.keep.whereNot, p.keep.banner!.kind, p.keep.banner!.big]).toEqual(["tidal", "faint", null, null, "tidal", ["Tidal water"]]);
  });

  it("klmb_1010 (D19 (a), T80 as built): the rainbow row is a group — its words, in place of the golden's legacy article", async () => {
    const p = await at("klmb_1010");
    if (!p) return;
    expect(p.keep.order.filter((b) => typeof b !== "string" && "plain" in b).length).toBe(p.keep.plainRows.length);
    expect(p.keep.plainRows.map((r) => plainRowLines(r)[0]!).some((l) => /^Rainbow trout/.test(l))).toBe(false);
    expect(excluded("keep", "klmb_1010", "plainRows.0")?.reason).toMatch(/^decisions D19/);
    // the group the golden cannot hold (exclusions.ts D19_GROUPS), drawn right after the Region 4 trout and char group
    const d = D19_GROUPS.find((x) => x.id === "klmb_1010")!;
    const i = p.keep.groups.findIndex((x) => x.row === d.row);
    expect([p.keep.groups.map((x) => [x.row, x.title]), p.keep.order.slice(1, 3)])
      .toEqual([[[0, "Trout and char"], [d.row, d.title]], [{ group: 0 }, { group: i }]]);
    expect(keepLines({ ...p.keep, order: [{ group: i }] }, p.day)).toEqual([
      "Rainbow troutSeparate from your 5 trout and char",
      "This lake’s rule: up to 10 a day per angler",
      "Rule book p.37 ↗",
      "Up to 10 may be rainbow trout",
      "any size Rainbow trout ›",
      "20 a year · Record each on your licence right away",
      "You need Conservation Surcharge Stamp for Kootenay Lake rainbow trout › only if you keep rainbow trout over 50 cm",
      "How it adds up",
      "This lake’s rule: 10 a day here. Rainbow trout you kept anywhere else today count toward those 10. These fish are counted apart from the region’s total: they don’t use it up. See every regulation line ›",
    ]);
  });

  it("the page's header is headerOf's; no action (owner call 3: T71 Map, T150 Parts)", async () => {
    for await (const c of cases("keep")) {
      if (c.id !== "vedder_1010") continue;
      const inp = await c.inputs(), p = await c.page();
      expect([p.header, "action" in p.header]).toEqual([headerOf(inp, { today: requestOf(c.id).today, status: p.status }), false]);
    }
  });
});

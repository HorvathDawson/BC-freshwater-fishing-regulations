/**
 * T42 (B): the edge states on the live release paired with the oracles (golden.ts `parityGate`; skipped with the reason
 * otherwise, a failure under REGS_STORE_REQUIRED=1), against golden.json (regs v38 @ 7fb6809f):
 * - tidal water (x31p_1010): the Keep tab is the "Tidal water" banner alone, the dot faint, Gear and Methods say the
 *   banner's words (the v38 golden says them too: decisions D20 (b) is now the prototype's own answer, so no exclusion);
 * - the hours window (x15p2_1010): the card at inside=false and the "⚠ At certain times" caveat right after the strip;
 * - the weekday row (klwa_1010 / klwa_1012): the Kokanee row's stored week status, and the segment each day reads;
 * - every golden state whose tap has moments: the whole Keep tab equals the golden;
 * - an unnamed section (taps.json `unnamed`, golden.ts `baseUnnamedStream`: the first sampled unnamed stream the store
 *   answers by its region's base): the store's place name, no part label, no action, the page otherwise built as for a
 *   named water.
 * The store-free cases (outside B.C., the edge selector, the timed words) are core/src/regs/edge.test.ts.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, edgeKind, segmentIndex, tapDay, type RegsPage } from "@app/core";
import { loadRegsInputs } from "@app/data/regs";
import { ADULT_RESIDENT, baseUnnamedStream, golden, inputsOf, parityGate, parityStore, requestOf, taps } from "./golden";
import { gearLines, methodsLines } from "./gearPage";
import { keepLines, selectorOf } from "./keepPage";
import { norm } from "./eq";
import { T203_SIZE_BOX_IDS } from "./exclusions";

const G = golden().states, T = taps();
const gate = await parityGate();
const TIDAL_NOTE = "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp. See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required.";
/** The golden states whose tap has moments (taps.json): the ticket's twelve. */
const MOMENT_IDS = ["klwa_1010", "klwa_1012", "x15p2_1010", "x15p7_1010", "x17p3_1010", "x17p4_1010", "x17p5_1010", "x17p7_1010",
  "x19p4_1010", "x19p6_1010", "x19p7_1010", "x32p0_1010"];

interface GKeep { order: string[]; lines: string[] }
const pageOf = async (id: string): Promise<RegsPage> =>
  buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
const keepText = (p: RegsPage) => keepLines(p.keep, p.day);

it(`T42 edge parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
  if ("reason" in gate) console.warn(`T42 edge parity skipped: ${gate.reason}`);
});

describe.skipIf(gate.skip)("T42 edge states on the live release", () => {
  it("5. tidal x31p_1010: the banner alone, a faint dot, Gear and Methods in the banner's words", async () => {
    const inp = (await inputsOf("x31p_1010"))!, p = await pageOf("x31p_1010");
    expect([edgeKind(inp), p.status, p.header.dot]).toEqual(["tidal", "tidal", "faint"]);
    expect(p.keep.order).toEqual(["banner"]);
    expect(keepText(p)).toEqual(["Tidal water", TIDAL_NOTE]);
    expect(keepText(p)).toEqual((G.x31p_1010!.keep as GKeep).lines.map(norm));
    expect(gearLines(p.gear!)).toEqual(["Tidal water", TIDAL_NOTE]);
    expect(gearLines(p.gear!)).toEqual((G.x31p_1010!.gear as { lines_: string[] }).lines_.map(norm));
    expect(methodsLines(p.methods!)).toEqual(["Tidal water", TIDAL_NOTE]);
    expect(methodsLines(p.methods!)).toEqual((G.x31p_1010!.methods as { lines: string[] }).lines.map(norm));
  });

  it("5b. mutation pin: the tidal Keep tab with a strip or a where-not block is not the golden", async () => {
    const p = await pageOf("x31p_1010"), want = (G.x31p_1010!.keep as GKeep).order;
    expect(p.keep.order.map((b) => selectorOf(b, p.keep))).toEqual(want);
    const extra = { ...p.keep, order: [...p.keep.order, "outside" as const], outside: "x" };
    expect(extra.order.map((b) => selectorOf(b, extra))).not.toEqual(want);
    expect(keepLines({ ...p.keep, banner: { ...p.keep.banner!, paras: [] } }, p.day)).not.toEqual((G.x31p_1010!.keep as GKeep).lines.map(norm));
  });

  it("6. hours x15p2_1010: the timed caveat right after the strip, its words and book page; no header action (T150)", async () => {
    const p = await pageOf("x15p2_1010"), k = p.keep;
    expect(k.order[0]).toBe("strip");
    expect(k.order[1]).toEqual({ caveat: 0 });
    expect(k.caveats[0]!.kind).toBe("timed");
    const lines = keepText(p), g = (G.x15p2_1010!.keep as GKeep).lines.map(norm);
    expect(lines.slice(4, 7)).toEqual(["⚠ At certain times", "No fishing for any game fish 60 min after sunset–60 min before sunrise Rule book p.23 ↗",
      "Trout and char"]);
    expect(lines).toEqual(g);
    // (T150) the golden offers Parts here; the app's parts are its Map view's list — the header keeps the part's name
    expect([(G.x15p2_1010!.header as { action: { kind: string } | null }).action?.kind, "action" in p.header]).toEqual(["parts", false]);
  });

  it("4. weekday row: the Kokanee row's stored week status; Saturday and Monday read their own moment", async () => {
    for (const id of ["klwa_1010", "klwa_1012"]) {
      const p = await pageOf(id), ko = p.keep.otherFish!.find((r) => r.name === "Kokanee")!;
      expect([id, ko.week]).toEqual([id, "Weekends: keep 5 · Weekdays: release"]);
    }
    const seg = (id: string) => {
      const t = T.states[id]!, r = requestOf(id), d = tapDay(r.day, 2026);
      return segmentIndex(t.starts, d.day, t.moments, d.weekday);
    };
    expect([seg("klwa_1010"), seg("klwa_1012")]).toEqual([T.states.klwa_1010!.s, T.states.klwa_1012!.s]);
    expect([seg("klwa_1010"), seg("klwa_1012")]).toEqual([1, 0]);
    expect([(await inputsOf("klwa_1010"))!.s, (await inputsOf("klwa_1012"))!.s]).toEqual([1, 0]);
  });

  it("7. every moment state: the whole Keep tab (order and every drawn line) equals the golden", async () => {
    expect(Object.keys(T.states).filter((id) => T.states[id]!.moments).sort()).toEqual([...MOMENT_IDS].sort());
    const bad: string[] = [];
    for (const id of MOMENT_IDS) {
      const p = await pageOf(id), g = G[id]!.keep as GKeep;
      const order = p.keep.order.map((b) => selectorOf(b, p.keep));
      if (JSON.stringify(order) !== JSON.stringify(g.order)) bad.push(`${id} order: ${order.join(" ")} ≠ ${g.order.join(" ")}`);
      // (T203) Region 6's shared 1 over 50 cm is one size tier: the drawn lines are not page v36's (exclusions T203_SIZE_BOX_IDS)
      if (!T203_SIZE_BOX_IDS.includes(id) && JSON.stringify(keepText(p)) !== JSON.stringify(g.lines.map(norm))) bad.push(`${id} lines`);
    }
    expect(bad).toEqual([]);
  });

  it("an unnamed stream on its region's base: taps.json's name, no part label, no action, four tabs; the page built as a named water's", async () => {
    const st = (await parityStore())!;
    const u = await baseUnnamedStream(st);
    expect(u.name).toMatch(/^Unnamed stream · Regions? \d/);
    const inp = await loadRegsInputs(st, { kind: "section", handle: u.handle }, { month: 10, day: 10 }, "Saturday", 2026);
    const p = buildRegsPage(inp, { today: { month: 10, day: 10 }, who: ADULT_RESIDENT });
    expect([edgeKind(inp), inp.key!.akey]).toEqual(["unnamed", u.akey]);
    // the whole header name is the Python reference decoder's (taps.json), never composed here
    expect([p.header.name, p.header.place, "action" in p.header, p.header.tabs.length]).toEqual([u.name, null, false, 4]);
    expect(p.keep.order[0]).toBe("strip");
    expect([p.gear !== null, p.methods !== null, p.licence !== null]).toEqual([true, true, true]);
  });

  it("outside B.C. on the release: a handle with no key draws the foot line alone and no other tab", async () => {
    const st = (await parityStore())!;
    const inp = await loadRegsInputs(st, { kind: "section", handle: 2_000_000_000 }, { month: 10, day: 10 }, "Saturday", 2026);
    const p = buildRegsPage(inp, { today: { month: 10, day: 10 }, who: ADULT_RESIDENT });
    expect([edgeKind(inp), p.status, p.keep.order, p.keep.outside, p.gear, p.methods, p.licence]).toEqual(
      ["outside", "outside", ["outside"], "This part of the water is outside B.C.: no B.C. rules apply.", null, null, null]);
    expect([p.header, "action" in p.header]).toMatchObject([{ name: "", place: null, dot: "faint" }, false]);
  });
});

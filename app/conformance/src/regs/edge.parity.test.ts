/**
 * T42 (B): the edge states on the live release paired with the oracles (golden.ts `parityGate`; skipped with the reason
 * otherwise, a failure under REGS_STORE_REQUIRED=1):
 * - tidal water (x31p_1010): the Keep tab is the "Tidal water" banner alone, the dot faint, Gear and Methods say the
 *   banner's words;
 * - the hours window (x15p2_1010): the card at inside=false and the "⚠ At certain times" caveat right after the strip;
 * - the weekday row (klwa_1010 / klwa_1012): the Kokanee row's stored week status, and the segment each day reads;
 * - the states whose tap has moments are exactly taps.json's twelve (T439: their whole Keep tab was held to golden.json; gone);
 * - an unnamed section (taps.json `unnamed`, golden.ts `baseUnnamedStream`: the first sampled unnamed stream the store
 *   answers by its region's base): the store's place name, no part label, no action, the page otherwise built as for a
 *   named water.
 * The store-free cases (outside B.C., the edge selector, the timed words) are core/src/regs/edge.test.ts.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, edgeKind, plain, segmentIndex, tapDay, type CaveatVM, type RegsPage } from "@app/core";
import { loadRegsInputs } from "@app/data/regs";
import { absentHandle, ADULT_RESIDENT, baseUnnamedStream, inputsOf, parityGate, parityStore, requestOf, taps } from "./golden";

const T = taps();
const gate = await parityGate();
const TIDAL_NOTE = "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp. See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required.";
/** The states whose tap has moments (taps.json): the ticket's twelve. */
const MOMENT_IDS = ["klwa_1010", "klwa_1012", "x15p2_1010", "x15p7_1010", "x17p3_1010", "x17p4_1010", "x17p5_1010", "x17p7_1010",
  "x19p4_1010", "x19p6_1010", "x19p7_1010", "x32p0_1010"];

const pageOf = async (id: string): Promise<RegsPage> =>
  buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
/** one caveat item as it reads on the page: what, where, then its Rule book link */
const itemLine = (i: CaveatVM["items"][number]) => [plain(i.what), i.where, plain(i.book)].filter(Boolean).join(" ");

it(`T42 edge parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
  if ("reason" in gate) console.warn(`T42 edge parity skipped: ${gate.reason}`);
});

describe.skipIf(gate.skip)("T42 edge states on the live release", () => {
  it("5. tidal x31p_1010: the banner alone, a faint dot, Gear and Methods in the banner's words", async () => {
    const inp = (await inputsOf("x31p_1010"))!, p = await pageOf("x31p_1010");
    expect([edgeKind(inp), p.status, p.header.dot]).toEqual(["tidal", "tidal", "faint"]);
    expect(p.keep.order).toEqual(["banner"]);
    const b = p.keep.banner!;
    expect([b.big, b.paras, b.chain, b.merged, b.reopen]).toEqual([["Tidal water"], [TIDAL_NOTE], [], null, null]);
    expect([p.gear!.closed, p.gear!.closedMore]).toEqual(["Tidal water", [TIDAL_NOTE]]);
    expect([p.methods!.closed, p.methods!.closedMore]).toEqual(["Tidal water", [TIDAL_NOTE]]);
  });

  it("6. hours x15p2_1010: the timed caveat right after the strip, its words and book page; no header action (T150)", async () => {
    const p = await pageOf("x15p2_1010"), k = p.keep;
    expect(k.order[0]).toBe("strip");
    expect(k.order[1]).toEqual({ caveat: 0 });
    expect(k.caveats[0]!.kind).toBe("timed");
    const c = k.caveats[0]!;
    expect([c.title, itemLine(c.items[0]!)]).toEqual(["⚠ At certain times", "No fishing for any game fish 60 min after sunset–60 min before sunrise Rule book p.23 ↗"]);
    // (T150) the app's parts are its Map view's list — the header keeps the part's name and draws no action
    expect("action" in p.header).toBe(false);
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

  it("7. the states whose tap has moments are taps.json's twelve", () => {
    expect(Object.keys(T.states).filter((id) => T.states[id]!.moments).sort()).toEqual([...MOMENT_IDS].sort());
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
    const inp = await loadRegsInputs(st, { kind: "section", handle: await absentHandle(st) }, { month: 10, day: 10 }, "Saturday", 2026);
    const p = buildRegsPage(inp, { today: { month: 10, day: 10 }, who: ADULT_RESIDENT });
    expect([edgeKind(inp), p.status, p.keep.order, p.keep.outside, p.gear, p.methods, p.licence]).toEqual(
      ["outside", "outside", ["outside"], "This part of the water is outside B.C.: no B.C. rules apply.", null, null, null]);
    expect([p.header, "action" in p.header]).toMatchObject([{ name: "", place: null, dot: "faint" }, false]);
  });
});

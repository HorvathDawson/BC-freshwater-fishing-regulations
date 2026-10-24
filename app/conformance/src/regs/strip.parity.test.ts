/**
 * T13 parity: the Keep tab's year strip — `stripOf(inp, settle(…), buildModel(…), today)` — against the golden
 * (app/fixtures/golden/golden.json, page v38 @ 7fb6809f) `keep.strip` on every golden state, inputs from T09
 * `inputsOf` over the harness's paired store (REGS_STORE_REQUIRED=1 turns a skip into a failure).
 *
 * Every expected value is the golden's, except the ticket's literals (page v38's output, re-read from the golden at v38).
 * The bar's per-day bits and open/closed are the store's display frames; the "Trout …" line words the STORED per-row
 * year strip (rows 30 `rows[].year`) — nothing is derived here.
 */
import { afterAll, describe, expect, it } from "vitest";
import { buildModel, fmtDay, makePlace, settle, stripOf, type StripVM } from "@app/core";
import { eqPlain } from "./eq";
import { caseIds, golden, inputsOf, parityGate, requestOf, summary } from "./golden";
import { excluded } from "./exclusions";

interface GStrip {
  line: { cls: string; text: string }[]; status: string; statusClass: string;
  bar: { cls: string; flex: number }[]; today: string; months: string[]; legend: { swatch: string; text: string }[]; lines: string[];
}
const G = golden();
const gstrip = (id: string) => (G.states[id]!.keep as { strip: GStrip | null }).strip;
const gate = await parityGate();
const ids = caseIds("keep");

const CLS = { status: "wsstat", when: "muted small", soon: "wsoon", soonOpen: "wsoon open" } as const;
/** prototype flagBar style(f): a null frame reads as 0 (`null & 1` is 0). */
const barCls = (bits: number | null) => {
  const f = bits ?? 0;
  return f & 1 ? "f-closed" : [f & 2 ? "f-rel" : "f-open", f & 4 ? "f-fly" : "", f & 8 ? "f-flies" : "", f & 16 ? "f-bait" : ""].join(" ");
};
const sq = (s: string) => s.replace(/\s+/g, " ").trim();

/** The VM in the golden's own shape (the fields T24 draws). */
const project = (v: StripVM, dayLabel: string) => ({
  line: v.line.map((l) => ({ cls: l.kind === "status" ? `${CLS.status} ${v.status}` : CLS[l.kind], text: l.text })),
  status: v.line[0]!.text, statusClass: v.status,
  bar: v.runs.map((r) => ({ cls: sq(barCls(r.bits)), flex: r.days })),
  today: `left:calc(${(v.todayAt * 100).toFixed(2)}% - 1px)`, months: [...v.months],
  legend: v.legend.map((l) => (l.mark ? `${l.mark} ` : "") + l.label),
  lines: [v.line.map((l) => l.text).join(" "), dayLabel, v.months.join(" "), v.legend.map((l) => (l.mark ? `${l.mark} ` : "") + l.label).join(" ")],
});
const golden_ = (g: GStrip) => ({
  line: g.line, status: g.status, statusClass: g.statusClass, bar: g.bar.map((b) => ({ cls: sq(b.cls), flex: b.flex })),
  today: g.today, months: g.months, legend: g.legend.map((l) => l.text), lines: g.lines,
});

async function stripFor(id: string): Promise<StripVM | null> {
  const inp = (await inputsOf(id))!, place = makePlace(inp), s = settle(inp, place);
  return stripOf(inp, s, buildModel(inp, place, s), requestOf(id).today);
}

it(`T13 strip parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
  if ("reason" in gate) console.warn(`T13 strip parity skipped: ${gate.reason}`);
});

describe.skipIf(gate.skip)("T13 year strip vs golden keep.strip", () => {
  let passed = 0;
  afterAll(() => console.info(summary("keep", passed, ids.length).replace(/^keep/, "keep.strip")));

  it.each(ids)("1. %s: stripOf equals golden keep.strip (line, bar, today, months, legend, lines)", async (id) => {
    const v = await stripFor(id), g = gstrip(id);
    if (g === null) { expect(v).toBeNull(); passed++; return; }
    expect(v).not.toBeNull();
    const got = project(v!, fmtDay(requestOf(id).day)), want = golden_(g);
    for (const k of ["line", "lines"] as const) if (excluded("keep", id, `strip.${k}`)) { delete (got as Partial<typeof got>)[k]; delete (want as Partial<typeof want>)[k]; }
    expect(got).toEqual(want);
    const lines = eqPlain(got.lines ?? [], want.lines ?? []);
    expect(lines.ok, lines.ok ? "" : lines.diff).toBe(true);
    expect(v!.runs.reduce((a, r) => a + r.days, 0)).toBe(365);
    passed++;
  });

  const texts = async (id: string) => (await stripFor(id))!.line.map((l) => l.text);
  const flex = async (id: string) => (await stripFor(id))!.runs.map((r) => r.days);

  it("2. vedder_1010: open today; the bar, the marker and the legend", async () => {
    const v = (await stripFor("vedder_1010"))!;
    expect(v.line).toEqual([{ kind: "status", text: "Open" }, { kind: "when", text: "today" }]);
    expect([v.status, v.runs.map((r) => r.days), v.runs.map((r) => sq(barCls(r.bits))), (v.todayAt * 100).toFixed(2)])
      .toEqual(["open", [120, 31, 30, 184], ["f-open", "f-open f-fly f-bait", "f-closed", "f-open"], "77.26"]);
    expect(v.legend.map((l) => (l.mark ? `${l.mark} ` : "") + l.label)).toEqual(["✓ open", "✕ closed", "fly fishing only", "bait ban"]);
    expect(v.months.join("")).toBe("JFMAMJJASOND");
  });

  it("3. vedder_615: closed on Jun 15, opens Jul 1", async () => {
    const v = (await stripFor("vedder_615"))!;
    expect([v.status, v.line, (v.todayAt * 100).toFixed(2)]).toEqual(["closed",
      [{ kind: "status", text: "Closed" }, { kind: "when", text: "on Jun 15" }, { kind: "soonOpen", text: "Opens Jul 1" }], "45.21"]);
  });

  it("4. vedder_515: 'Closed from Jun 1' is rewritten to the whole closed run (round 20i)", async () => {
    expect(await texts("vedder_515")).toEqual(["Open", "on May 15", "Closed Jun 1–30"]);
  });

  it("5. klwa_1012: a day of two weekday moments says its weekday", async () => {
    expect([await texts("klwa_1012"), await flex("klwa_1012")]).toEqual([["Open", "on Oct 12 (Mon)"], [365]]);
    expect((await stripFor("klwa_1012"))!.legend).toEqual([{ bit: 0, mark: "✓", label: "open" }]);
  });

  it("6. x15p7_1010: closed today (Sat), opens Nov 1", async () => {
    expect([await texts("x15p7_1010"), await flex("x15p7_1010")]).toEqual([["Closed", "today (Sat)", "Opens Nov 1"], [120, 184, 61]]);
  });

  it("7. x4p_1010: open, except some parts (a retention rule of an undrawn part in force today)", async () => {
    expect(await texts("x4p_1010")).toEqual(["Open, except some parts", "today"]);
  });

  it("8. the trout line reads the STORED year strip: x5p1_1010 'Trout release only until Mar 31'; none beside a trout keep row", async () => {
    expect(await texts("x5p1_1010")).toEqual(["Open", "today", "Trout release only until Mar 31"]);
    expect([await texts("x16p1_1010"), await texts("iron_720")]).toEqual([["Open", "today"], ["Open", "on Jul 20"]]);
  });

  it("9. cow0_1010: closed today, opens Nov 16", async () => {
    expect([await texts("cow0_1010"), await flex("cow0_1010")]).toEqual([["Closed", "today", "Opens Nov 16"], [184, 135, 46]]);
  });

  it("tidal x31p_1010: no strip (T14's tidal banner)", async () => {
    expect(await stripFor("x31p_1010")).toBeNull();
  });
});

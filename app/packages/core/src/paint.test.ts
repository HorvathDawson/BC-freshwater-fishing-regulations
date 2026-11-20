/** core paint decoder = the pipeline's (pipeline/deliver/release/paint.py), on bytes the pipeline wrote. */
import { describe, expect, it } from "vitest";
import { decodeAccessFile, decodeFlowArea, decodeFlowOwn, decodeFlowSmall, decodePaint, PaintError } from "./paint";
import { noPublicAccess } from "./access";
import { PAINT_FIXTURE as F } from "./paint.fixture";

const b64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

describe("decodePaint", () => {
  const p = decodePaint(b64(F.paint), F.handles, F.reach);
  it("decodes the flow half exactly as the pipeline does", () => {
    expect(p.stations).toEqual(F.stations);
    expect(p.members.map((m) => [m.panel, m.ord, m.station, m.role, m.areaKm2, m.years.discharge, m.years.level,
                                 m.regulated ? 1 : 0, m.sameRiver ? 1 : 0, m.lead, m.lake ? 1 : 0]))
      .toEqual(F.panel_member);
    expect(p.basins.map((b) => [b.basin, b.station, b.areaKm2, b.years.discharge, b.years.level])).toEqual(F.basin_member);
    expect(F.section_panel.map(([sid]) => [sid, p.panelOf(sid!)])).toEqual(F.section_panel);
    expect(p.panelOf(0)).toBeNull();
    expect(p.panelOf(6)).toBeNull();                 // between runs
    expect(p.sectionCount).toBe(F.section_panel.length);
    expect(p.lakes).toBe(true);                       // (T296) paint/5 carries each member's lake crossing
  });
  it("carries the status index whole, held to its handles", () => {
    expect(p.status.handles).toBe(F.handles);
    expect(() => decodePaint(b64(F.paint), "0000000000000000")).toThrow();
  });
  it("reads flow-area in the paint's section order, and refuses another paint's", () => {
    expect(Array.from(decodeFlowArea(b64(F.area), p.sectionCount))).toEqual(F.areas);
    expect(() => decodeFlowArea(b64(F.area), p.sectionCount - 1)).toThrow(PaintError);
  });
  it("(T823) reads flow-small as the pipeline wrote it: one mask per paint section, in flow-area's order", () => {
    expect(Array.from(decodeFlowSmall(b64(F.small), p.sectionCount))).toEqual(F.smalls);
    expect(F.smalls.some((m) => m > 0) && F.smalls.some((m) => m === 0)).toBe(true);     // the fixture flags some, not all
    // another paint's (one section short, one over), another format, a truncated file: refused, never read as no bits
    expect(() => decodeFlowSmall(b64(F.small), p.sectionCount - 1))
      .toThrow(new PaintError("flow-small: its sections are not the paint's"));
    expect(() => decodeFlowSmall(b64(F.small), p.sectionCount + 1)).toThrow(PaintError);
    expect(() => decodeFlowSmall(b64(F.area), p.sectionCount)).toThrow(new PaintError("flow-small: not a flow-small/1 file"));
    expect(() => decodeFlowSmall(b64(F.small).subarray(0, 5), p.sectionCount)).toThrow(PaintError);
    // a mask wider than a panel's 31 members (bit 31): corrupt
    const wide = Uint8Array.from([0x42, 0x43, 0x46, 0x53, 1, 0x80, 0x80, 0x80, 0x80, 0x08]);
    expect(() => decodeFlowSmall(wide, 1)).toThrow(PaintError);
    expect(Array.from(decodeFlowSmall(Uint8Array.from([0x42, 0x43, 0x46, 0x53, 1, 0xff, 0xff, 0xff, 0xff, 0x07]), 1)))
      .toEqual([2 ** 31 - 1]);
  });
  it("refuses a paint/3 file, which carries one record length for both quantities", () => {
    const old = b64(F.paint).slice();
    old[4] = 3;
    expect(() => decodePaint(old, F.handles)).toThrow(new PaintError("paint: version 3 is not 4, 5 or 6"));
  });
  it("(T54) decodes the no-access half exactly as the pipeline does, and hands it to the status it carries", () => {
    const rows = (a: ReturnType<typeof decodeAccessFile>, sids: number[], areas: string[][]) =>
      sids.flatMap((s) => a.of(s).map((r) => [s, areas.findIndex(([t, n]) => t === r.area.tier && n === r.area.name),
                                              Math.round(r.share * 1000)]));
    const sids = [...new Set(F.access_rows.map(([s]) => s as number))];
    expect(rows(p.access, sids, F.access_areas as string[][])).toEqual(F.access_rows);
    expect(p.status.access).toBe(p.access);
    // the status keeps answering as the index: the access never moves a regulation answer
    expect(p.status.handles).toBe(F.handles);
  });
  it("(T54) decodes the access file — the gentler tiers — held to the release's handles", () => {
    const a = decodeAccessFile(b64(F.access_file), F.handles);
    const sids = [...new Set(F.file_rows.map(([s]) => s as number))];
    expect(sids.flatMap((s) => a.of(s).map((r) => [s, (F.file_areas as string[][]).findIndex(([t, n]) => t === r.area.tier && n === r.area.name),
                                                     Math.round(r.share * 1000)]))).toEqual(F.file_rows);
    expect(() => decodeAccessFile(b64(F.access_file), "0000000000000000"))
      .toThrow(new PaintError(`access: cut for section handles ${F.handles}, not 0000000000000000`));
    expect(() => decodeAccessFile(b64(F.paint), F.handles)).toThrow(new PaintError("access: not an access/1 file"));
  });
  it("refuses a truncated file", () => {
    expect(() => decodePaint(b64(F.paint).subarray(0, 40), F.handles)).toThrow();
  });
});

describe("(T679 #16) paint/6: the one access flag, Python's union share", () => {
  const p5 = decodePaint(b64(F.paint), F.handles, F.reach);
  const p6 = decodePaint(b64(F.paint6), F.handles, F.reach);
  it("is paint/5 plus the sections mostly inside no-access land, read as Python wrote them", () => {
    expect(p6.members).toEqual(p5.members);
    expect(p6.basins).toEqual(p5.basins);
    expect([3, 4, 300].map((s) => p6.access.of(s))).toEqual([3, 4, 300].map((s) => p5.access.of(s)));
    expect([0, 3, 4, 5, 6, 300, 301].filter((s) => noPublicAccess(p6.access, s))).toEqual(F.mostly);
  });
  it("the flag decides, not the summed rows (paint/5 sums them)", () => {
    expect(p5.access.mostly).toBeUndefined();
    expect(p6.access.mostly!(4)).toBe(true);
    // mutation: a section the rows put mostly inside but the flag does not is NOT drawn as no access
    expect(noPublicAccess({ of: p6.access.of, mostly: () => false }, 3)).toBe(false);
    expect(noPublicAccess({ of: p6.access.of }, 3)).toBe(true);
  });
  it("(T679 #8) reads flow-own as the pipeline wrote it, and refuses another paint's", () => {
    const p = decodePaint(b64(F.paint), F.handles, F.reach);
    expect(Array.from(decodeFlowOwn(b64(F.own), p.sectionCount))).toEqual(F.owns);
    expect(() => decodeFlowOwn(b64(F.own), p.sectionCount - 1)).toThrow(PaintError);
    expect(() => decodeFlowOwn(b64(F.small), p.sectionCount)).toThrow(/not a flow-own/);
  });
  it("refuses a paint/6 whose flag column is cut", () => {
    const b = b64(F.paint6);
    expect(() => decodePaint(b.subarray(0, b.length - 1), F.handles, F.reach)).toThrow();
  });
});

/** core paint decoder = the pipeline's (pipeline/deliver/release/paint.py), on bytes the pipeline wrote. */
import { describe, expect, it } from "vitest";
import { decodeAccessFile, decodeFlowArea, decodePaint, PaintError } from "./paint";
import { PAINT_FIXTURE as F } from "./paint.fixture";

const b64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

describe("decodePaint", () => {
  const p = decodePaint(b64(F.paint), F.handles, F.reach);
  it("decodes the flow half exactly as the pipeline does", () => {
    expect(p.stations).toEqual(F.stations);
    expect(p.members.map((m) => [m.panel, m.ord, m.station, m.role, m.areaKm2, m.years, m.regulated ? 1 : 0, m.sameRiver ? 1 : 0, m.lead]))
      .toEqual(F.panel_member);
    expect(p.basins.map((b) => [b.basin, b.station, b.areaKm2, b.years])).toEqual(F.basin_member);
    expect(F.section_panel.map(([sid]) => [sid, p.panelOf(sid!)])).toEqual(F.section_panel);
    expect(p.panelOf(0)).toBeNull();
    expect(p.panelOf(6)).toBeNull();                 // between runs
    expect(p.sectionCount).toBe(F.section_panel.length);
  });
  it("carries the status index whole, held to its handles", () => {
    expect(p.status.handles).toBe(F.handles);
    expect(() => decodePaint(b64(F.paint), "0000000000000000")).toThrow();
  });
  it("reads flow-area in the paint's section order, and refuses another paint's", () => {
    expect(Array.from(decodeFlowArea(b64(F.area), p.sectionCount))).toEqual(F.areas);
    expect(() => decodeFlowArea(b64(F.area), p.sectionCount - 1)).toThrow(PaintError);
  });
  it("refuses a paint/2 file, which has no access half", () => {
    const old = b64(F.paint).slice();
    old[4] = 2;
    expect(() => decodePaint(old, F.handles)).toThrow(new PaintError("paint: version 2 is not 3"));
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

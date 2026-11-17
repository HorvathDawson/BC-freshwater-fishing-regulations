/**
 * (T823) THE SMALL-WATER FLAGS, READ: `flow-small` beside flow-area (the loader), on the panel the sheet and the map's
 * classes both get (`panelsFor`, `standingClasses`), with the release's own sentence (the conditions meta's
 * `SMALL_WATER_TEXT_KEY`). On the mini release (the real pipeline's file, its one panel section unflagged), re-flagged or
 * broken through `ReleaseTamper.bytes` after the file was held to its manifest.
 *
 * Mutations each test kills:
 *   - `flowSmall` reads the file without asking the manifest           -> "absent: no bits, nothing fetched"
 *   - `panelsFor` ignores the mask (no `small`)                        -> "a flagged member: the panel carries it"
 *   - `smallFor` takes bit k for the wrong member (k + 1)              -> "a flagged member" + "a bit past the last member"
 *   - the sentence typed in TS, or read from another key               -> "with the release's own sentence"
 *   - `standingClasses` builds its panel without `small`               -> "the map's class speaks with the same warning"
 *   - a missing sentence read as "" (shown bare / hidden)              -> "flags with no sentence: refused"
 */
import { describe, expect, it } from "vitest";
import { SMALL_WATER_TEXT_KEY } from "@app/core";
import { openMiniRelease } from "../sql/drivers/releaseDir";
import type { Db } from "../sql/db";
import { smallNextToGauge, smallWaterText, speakerFor } from "../standing";
import type { SectionId, StationId } from "../types";
import { makeReleaseSource } from "./source";

/** flow-small/1 with these masks */
const smallFile = (...masks: number[]) => {
  const out = [0x42, 0x43, 0x46, 0x53, 1];
  for (let m of masks) { do { let b = m & 0x7f; m = Math.floor(m / 128); if (m) b |= 0x80; out.push(b); } while (m); }
  return Uint8Array.from(out);
};
const flagged = (masks: number[]) => openMiniRelease({ bytes: (n, b) => (n === "flow-small" ? smallFile(...masks) : b) });
/** The mini release's first paint section (its only one), by paint's own runs: no typed id. */
const firstSection = async (loader: ReturnType<typeof openMiniRelease>) =>
  (await loader.paint()).runs.starts[0]! as SectionId;
/** The sentence as the release's conditions file holds it, read here directly (the reader under test reads it too). */
const sentence = async (loader: ReturnType<typeof openMiniRelease>) =>
  String((await (await loader.db("conditions")).get("SELECT v FROM meta WHERE k = ?", SMALL_WATER_TEXT_KEY))?.v);

describe("flow-small, read beside flow-area (T823)", () => {
  it("present: one mask per paint section, as the pipeline wrote it (the mini release flags nothing)", async () => {
    const loader = openMiniRelease(), paint = await loader.paint();
    const small = await loader.flowSmall();
    expect([small?.length, Array.from(small ?? []), loader.opened().includes("flow-small")])
      .toEqual([paint.sectionCount, [0], true]);
    const s = makeReleaseSource(loader), sid = await firstSection(loader);
    expect((await s.panelsFor([sid])).get(sid)?.small).toBeUndefined();
    // the sentence is there all the same: it rides with the file, Python's words
    expect(await sentence(loader)).toMatch(/^\S.{20,}$/);
  });

  it("absent: no bits, nothing fetched — said by the manifest, never by a failed read", async () => {
    const asked: string[] = [];
    const loader = openMiniRelease({
      manifest: (m) => ({ ...m, files: Object.fromEntries(Object.entries(m.files).filter(([k]) => k !== "flow-small")) }),
      bytes: (n, b) => { asked.push(n); return b; },
    });
    expect(await loader.flowSmall()).toBeNull();
    const s = makeReleaseSource(loader), sid = await firstSection(loader);
    const panel = (await s.panelsFor([sid])).get(sid);
    expect([panel !== undefined, panel?.small, asked.includes("flow-small"), loader.opened().includes("flow-small")])
      .toEqual([true, undefined, false, false]);
  });

  it("a flagged member: the panel carries its station with the release's own sentence; the map's class speaks the same", async () => {
    const loader = flagged([1]), s = makeReleaseSource(loader), sid = await firstSection(loader);
    const panel = (await s.panelsFor([sid])).get(sid)!;
    const text = await sentence(loader);
    expect(panel.small).toEqual({ stations: [panel.members[0]!.station], text });
    const speaker = speakerFor(panel, undefined), lead = panel.members[0]!.station;
    expect([smallNextToGauge(speaker, lead), smallWaterText(speaker, lead)]).toEqual([true, text]);
    // the Conditions map's class for that section: the same speaker, warning and all (one speaker, T105)
    const c = await s.standingClasses(), k = c.classes[c.classOf(sid)!]!;
    expect(k.kind === "panel" ? [k.panel.areaKm2, k.panel.members, k.panel.small, smallWaterText(k, lead)] : k)
      .toEqual([panel.areaKm2, panel.members, panel.small, text]);
  });

  it("broken files are refused, never read as no bits: another paint's length, another format, a bit past the last member", async () => {
    const two = flagged([0, 0]);
    await expect(two.flowSmall()).rejects.toThrow("flow-small: its sections are not the paint's");
    const other = openMiniRelease({ bytes: (n, b) => (n === "flow-small" ? Uint8Array.from([0x42, 0x43, 0x46, 0x41, 1, 0]) : b) });
    await expect(other.flowSmall()).rejects.toThrow("flow-small: not a flow-small/1 file");
    // the mini panel has one member: bit 1 names a member it does not have
    const past = flagged([2]), sid = await firstSection(past);
    await expect(makeReleaseSource(past).panelsFor([sid])).rejects.toThrow(/flow-small flags member 1 of panel \d+, which has 1/);
  });

  it("flags with no sentence: refused (a release with flow-small always carries its words)", async () => {
    const loader = openMiniRelease({
      bytes: (n, b) => (n === "flow-small" ? smallFile(1) : b),
      db: (name, d): Db => (name !== "conditions" ? d : {
        ...d, get: (sql, ...p) => (p[0] === SMALL_WATER_TEXT_KEY ? Promise.resolve(undefined) : d.get(sql, ...p)),
      }),
    });
    const sid = await firstSection(loader);
    await expect(makeReleaseSource(loader).panelsFor([sid])).rejects.toThrow(`conditions meta has no ${SMALL_WATER_TEXT_KEY}`);
  });
});

describe("smallNextToGauge / smallWaterText: the shown station's flag, nothing else (T823)", () => {
  const st = (s: string) => s as StationId;
  const member = (station: string) => ({ station: st(station), role: "up" as const, areaKm2: 900, years: { discharge: 30, level: 30 },
                                         regulated: false, sameRiver: true });
  const panel = { areaKm2: 1.4, members: [member("BIG"), member("NEAR")], small: { stations: [st("BIG")], text: "WORDS" } };
  it("the flagged member warns, the unflagged one does not; a lake, nothing, or no lead never does", () => {
    const sp = speakerFor(panel, undefined);
    expect([smallWaterText(sp, st("BIG")), smallWaterText(sp, st("NEAR")), smallWaterText(sp, null),
            smallWaterText(speakerFor(undefined, st("BIG")), st("BIG")), smallWaterText(null, st("BIG")),
            smallWaterText(speakerFor({ areaKm2: 1.4, members: panel.members }, undefined), st("BIG"))])
      .toEqual(["WORDS", null, null, null, null, null]);
    expect([smallNextToGauge(sp, st("BIG")), smallNextToGauge(sp, st("NEAR"))]).toEqual([true, false]);
  });
});

/**
 * T797 item 2 on the LIVE release (owner 2026-11-16 "Show it", reversing decisions D18 a): dead fin fish may be used as
 * bait when fishing for white sturgeon on the lower Fraser and Harrison (p.8, p.23). The stored bait row carries the
 * allowance (`also_allowed`); the Gear tab's "Fish or fish parts" ✕ item says it under the item, in its rule's words.
 * A water with no such allowance (the Vedder) says nothing of it.
 * Skipped, by name, when this machine has no release.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { liveReleaseAt, openReleaseDir } from "../fixtureRelease";

const at = liveReleaseAt();

it(`T797 runs against the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T797 skipped: ${at.reason}`);
});

describe.skipIf(!at.dir)(`T797 item 2: the sturgeon bait allowance is said on the Bait item (${at.dir ?? at.reason})`, () => {
  let st: RegsStore;
  beforeAll(async () => { st = await openRegsStore(openReleaseDir(at.dir!)); });
  const STURGEON = "Dead fin fish may be used, when fishing for white sturgeon";
  /** Every part of the water on Aug 12, 2026: the "Fish or fish parts" item's note ("": none; null: no such item or no Bait item). */
  const finFishNotes = async (item: string): Promise<(string | null)[]> => {
    const ord = (await st.itemOrd(item))!;
    const parts = await st.parts(ord), out: (string | null)[] = [];
    for (let partIx = 0; partIx < parts.length; partIx += 1) {
      if (!parts[partIx]!.facts || parts[partIx]!.tidal) continue;
      const inp = await loadRegsInputs(st, { kind: "part", item, partIx }, { month: 8, day: 12 }, "Wednesday", 2026);
      const bait = buildRegsPage(inp, { today: { month: 8, day: 12 }, who: { age: "adult", residency: "resident" } }).gear?.bait;
      const fin = (bait?.list ?? []).flatMap((c) => c.items).find((x) => x.name === "Fish or fish parts");
      out.push(fin ? fin.notes.join(" ") : null);   // (T922) one sentence per condition
    }
    return out;
  };

  it("the Fraser (Region 2) and the lower Harrison: the ✕ fin-fish item says the allowance, as its rule words it", async () => {
    for (const item of ["gnis:39325", "gnis:15333"]) {
      const said = (await finFishNotes(item)).filter((n) => n?.includes(STURGEON));
      expect(said.length, item).toBeGreaterThan(0);
      for (const n of said) expect(n, item).toMatch(new RegExp(`(^| · )${STURGEON} — `));
    }
  });

  it("the Vedder (no sturgeon allowance): no fin-fish item says it", async () => {
    const notes = await finFishNotes("gnis:3062");
    expect(notes.some((n) => n !== null)).toBe(true);           // the walk met the item
    expect(notes.filter((n) => n?.includes("Dead fin fish"))).toEqual([]);
  });
});

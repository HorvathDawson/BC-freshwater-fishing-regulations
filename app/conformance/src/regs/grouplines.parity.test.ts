/**
 * T17–T19 parity (the whole group): every golden g28 group's `lines` — the group's drawn text, line by line, in draw order
 * (head, origin, tree, not-kept lines, How it adds up, quiet line, "See every regulation line ›") — composed from the same
 * core VMs the field parities project (keepGroups.ts groupLinesProj), against app/design/regs-v11/port/golden/golden.json.
 * The field parities prove each part's words; this proves their order and which parts share a line.
 *
 * Dropped from the golden (exclusions.ts, reported in the summary): the `.gyear` year-window line (YEAR_WINDOW_IDS, T19b).
 * The stamp lines are compared (rows 31: the store's `stamps`, drawn by tree.ts / tail.ts `stampLine`). The 3 Kitimat-ruled states are not compared (their tree and worked
 * lines are held to the store by tree.parity / tail.parity).
 */
import { afterAll, describe, expect, it } from "vitest";
import { caseIds, golden, inputsOf, parityGate, summary } from "./golden";
import { droppedLines, excluded, keepLines, STAMP_LINE, YEAR_WINDOW_LINE } from "./exclusions";
import { groupLinesProj, keepOf } from "./keepGroups";

const gate = await parityGate();
const ids = caseIds("keep");
const goldenGroups = (id: string) => (golden().states[id]!.keep as { groups: { lines: string[] }[] }).groups;

it(`T17–T19 group lines parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T17–T19 group lines", () => {
  let equal = 0, groups = 0, dropped = 0;
  afterAll(() => console.info(`${summary("keep", equal, ids.length)} · ${groups} groups (group lines, ${dropped} golden lines dropped)`));

  it.each(ids)("%s: every group's drawn lines equal the golden, in order", async (id) => {
    const k = keepOf((await inputsOf(id))!), gold = goldenGroups(id);
    expect(k.groups.length).toBe(gold.length);
    k.groups.forEach((g, i) => {
      if (excluded("keep", id, `groups.${i}.lines`)) return;
      const want = keepLines(gold[i]!.lines, droppedLines("keep", id, `groups.${i}.lines`));
      dropped += gold[i]!.lines.length - want.length;
      expect(groupLinesProj(g), `${id} group ${i}`).toEqual(want);
      groups++;
    });
    equal++;
  });

  it("1. anderson_1010: the released steelhead's stamp line (the store's, rows 31) is drawn under its not-kept line, in order", async () => {
    const gold = goldenGroups("anderson_1010")[0]!.lines, got = groupLinesProj(keepOf((await inputsOf("anderson_1010"))!).groups[0]!);
    expect(got).toEqual(gold);
    expect(got.slice(10, 14)).toEqual(["Steelhead and Dolly Varden/bull trout: release every one", "Region 3’s rule · Rule book p.28 ↗",
      "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ Steelhead › Bull trout ›",
      "You need Steelhead Conservation Surcharge Stamp › only if you fish for steelhead, even to release them"]);
    expect(got.filter((l) => STAMP_LINE.test(l))).toHaveLength(1);
  });

  it("2. glenn_501: the .gyear line is dropped; the moved scope tag, the own tier's relation then its tributary sub, in order", async () => {
    const got = groupLinesProj(keepOf((await inputsOf("glenn_501"))!).groups[0]!);
    expect(got.some((l) => YEAR_WINDOW_LINE.test(l))).toBe(false);
    expect(got.slice(3, 10)).toEqual(["All Region 4 streams", "Only 2 of the 5 may be brook, brown, cutthroat or lake trout from streams",
      "any size Brook trout › Brown trout › Coastal cutthroat trout › Lake trout ›", "Only 1 cutthroat trout over 50 cm (20 in)",
      "The Duncan River’s rule: up to 5 rainbow trout a day Rule book p.36 ↗", "Not part of the 2 from streams; part of the 5 a day across Region 4",
      "Its row covers its tributaries, so it applies here"]);
  });

  it("3. bc1_501: the closure's pictures and ✕ share its presence line; 'See every regulation line ›' ends the last line", async () => {
    const got = groupLinesProj(keepOf((await inputsOf("bc1_501"))!).groups[0]!);
    expect(got).toContain("A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ Steelhead › ✕");
    expect(got.at(-1)!.endsWith(". See every regulation line ›")).toBe(true);
  });
});

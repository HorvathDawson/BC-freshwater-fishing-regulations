/**
 * T19 parity (Other fish + the split): which rows the Keep tab draws as groups (page v36 rowsHtml's TROUTISH split, none on
 * a closed day) and the compact Other fish list, built by core splitRows / groupRows / otherFishOf over the state's inputs,
 * against app/design/regs-v11/port/golden/golden.json (regs v38 @ 7fb6809f) on all 134 states.
 */
import { afterAll, describe, expect, it } from "vitest";
import { otherFishOf, FISH, splitRows } from "@app/core";
import { caseIds, golden, inputsOf, parityGate, summary } from "./golden";
import { comparableGroups } from "./exclusions";
import { appOpen, keepOf, othersProj } from "./keepGroups";

const gate = await parityGate();
const ids = caseIds("keep");
type GOther = { thumb: string | null; name: string; week: string | null; open: string | null; lines: string[] };
const gk = (id: string) => golden().states[id]!.keep as { groups: { row: string }[]; otherFish: GOther[] | null };

it(`T19 other-fish parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T19 Other fish", () => {
  let equal = 0, lists = 0;
  afterAll(() => console.info(`${summary("keep", equal, ids.length)} · ${lists} other-fish lists`));

  it.each(ids)("%s: the groups drawn and the Other fish list equal the golden", async (id) => {
    const inp = (await inputsOf(id))!, g = gk(id);
    expect(comparableGroups(id, keepOf(inp).groups).map((x) => `r${x.ctx.rowIx}`)).toEqual(g.groups.map((x) => x.row));   // D19 (a): exclusions.ts
    expect(othersProj(inp)).toEqual(g.otherFish?.map((o) => ({ ...o, open: appOpen(o.open) })) ?? null);
    if (g.otherFish) lists++;
    equal++;
  });

  const others = async (id: string) => otherFishOf(keepOf((await inputsOf(id))!).card, (await inputsOf(id))!, FISH);

  it("6. vedder_1010: kokanee to protected species, each with its one-word answer", async () => {
    expect((await others("vedder_1010"))!.map((o) => [o.thumb, o.name, o.value])).toEqual([["KO", "Kokanee", "release"], ["LW", "Whitefish", "15 a day"],
      ["LMB", "Bass", "20 a day"], ["BCB", "Black crappie", "20 a day"], ["WSG", "White sturgeon", "release"], [null, "Crayfish", "25 a day"],
      [null, "Protected species", "closed"]]);
  });

  it("7. klwa_1010: kokanee's weekday split under its name; bass no limit; pike and walleye closed", async () => {
    const o = (await others("klwa_1010"))!;
    expect(o[0]).toMatchObject({ thumb: "KO", name: "Kokanee", week: "Weekends: keep 5 · Weekdays: release", value: "5 a day", tone: "keep", open: "other:o0" });
    expect(o.filter((x) => /^(Bass|Northern pike and walleye)$/.test(x.name)).map((x) => [x.name, x.value, x.tone])).toEqual([
      ["Bass", "no limit", "nolimit"], ["Northern pike and walleye", "closed", "closed"]]);
  });

  it("8. x12p_1010: Arctic grayling '4 a day · size limit'; 4 states carry a hint", async () => {
    expect((await others("x12p_1010"))!.find((x) => x.name === "Arctic grayling")).toMatchObject({ value: "4 a day", sizeHint: "· size limit" });
    let hinted = 0;
    for (const id of ids) if ((await others(id))?.some((x) => x.sizeHint)) hinted++;
    expect(hinted).toBe(4);
  });

  it("9. vedder_615 (closed): no rows drawn — no groups, no list; x15p8_1010 (open, no g28 group): the list per the split", async () => {
    const shut = keepOf((await inputsOf("vedder_615"))!);
    expect([splitRows(shut.card), shut.groups.length, await others("vedder_615")]).toEqual([{ main: [], rest: null }, 0, null]);
    const x = keepOf((await inputsOf("x15p8_1010"))!);
    expect([x.groups.length, (await others("x15p8_1010"))?.length ?? null]).toEqual([0, gk("x15p8_1010").otherFish?.length ?? null]);
  });
});

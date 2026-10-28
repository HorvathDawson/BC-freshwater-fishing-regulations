import { describe, expect, it } from "vitest";
import { groupGlance, salmonGlance } from "./glance";
import type { GroupVM } from "./regs/model";
import { SALMON_WORDS, type SalmonCardVM } from "./salmon";

const group = (over: Partial<GroupVM["head"]>, tree: GroupVM["tree"] = []): GroupVM => ({
  row: 0, title: "Trout and char", scope: null, pinLine: null, origin: null, year: null, tree, notKept: [], worked: null,
  quiet: null, more: { text: "", open: "src:r0" as never },
  head: { form: null, rootTier: null, lead: null, leadN: null, ofTotal: false, whyHere: null, shareOf: null, terms: [],
          nolimit: false, words: [{ t: "num", s: "5", size: "big" }, { t: "text", s: " a day per angler across Region 6" }], ...over },
});
const pic = (code: string) => ({ code, name: code, caption: code, open: `sp:${code}` as never, sil: false as const });

describe("(T221) the at-a-glance rows say no count", () => {
  it("a group: its title and a rainbow's picture where it has one — never a number", () => {
    const kinds = [{ t: "kind", pictures: [pic("EB")] }, { t: "kind", pictures: [pic("RB")] }] as unknown as GroupVM["tree"];
    expect(groupGlance(group({}, kinds))).toEqual({ thumb: "RB", name: "Trout and char", value: null, tone: "keep" });
  });
  it("salmon: a status word only when every row (undrawn places too) has it", () => {
    const row = (status: string) => ({ status }) as never;
    const card = (st: string[], un: string[] = []) => ({ title: SALMON_WORDS.title, rows: st.map(row),
      unmapped: un.length ? [{ block: { rows: un.map(row) } }] : [] }) as unknown as SalmonCardVM;
    expect([salmonGlance(card(["check", "check"])).value, salmonGlance(card(["closed"])).tone,
            salmonGlance(card(["check"], ["closed"])).value, salmonGlance(card(["open", "closed"])).value])
      .toEqual(["check DFO", "closed", null, null]);
  });
});

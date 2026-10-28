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

describe("(T221) the at-a-glance rows", () => {
  it("a group: the head's number as 'N a day' (form a: the water's share), a rainbow's picture where it has one", () => {
    const kinds = [{ t: "kind", pictures: [pic("EB")] }, { t: "kind", pictures: [pic("RB")] }] as unknown as GroupVM["tree"];
    expect(groupGlance(group({}, kinds))).toEqual({ thumb: "RB", name: "Trout and char", value: "5 a day", tone: "keep" });
    expect(groupGlance(group({ form: "a", lead: [{ t: "num", s: "2", size: "big" }] })).value).toBe("2 a day");
    expect(groupGlance(group({ nolimit: true })).value).toBe("no limit");
  });
  it("salmon: the card's status word, toned as the Other fish rows", () => {
    const card = (s: SalmonCardVM["headlineStatus"]) => ({ title: SALMON_WORDS.title, headlineStatus: s }) as SalmonCardVM;
    expect([salmonGlance(card("check")), salmonGlance(card("closed")).tone, salmonGlance(card("release")).value]).toEqual([
      { thumb: "CO", name: SALMON_WORDS.title, value: "check DFO", tone: "keep" }, "closed", "release only"]);
  });
});

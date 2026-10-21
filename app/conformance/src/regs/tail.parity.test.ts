/**
 * T19 parity (group tail): every golden g28 group's not-kept lines, "How it adds up", steelhead quiet line and "See every
 * regulation line ›", built by core notKeptOf / workedOf / quietOf / moreOf over the state's inputs and projected to the
 * golden's shape (keepGroups.ts), against app/design/regs-v11/port/golden/golden.json (regs v38 @ 7fb6809f).
 */
import { afterAll, describe, expect, it } from "vitest";
import { caseIds, golden, inputsOf, parityGate, summary } from "./golden";
import { excluded } from "./exclusions";
import { keepOf, tailProj } from "./keepGroups";

const gate = await parityGate();
const ids = caseIds("keep");
type GGroup = { notKept: unknown[]; worked: unknown; quiet: string | null; more: string | null };
const goldenGroups = (id: string) => (golden().states[id]!.keep as { groups: GGroup[] }).groups;

it(`T19 tail parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T19 group tail", () => {
  let equal = 0, groups = 0;
  afterAll(() => console.info(`${summary("keep", equal, ids.length)} · ${groups} groups (tail)`));

  it.each(ids)("%s: every group's not-kept lines, How it adds up, quiet line and more equal the golden", async (id) => {
    const k = keepOf((await inputsOf(id))!), gold = goldenGroups(id);
    expect(k.groups.length).toBe(gold.length);
    k.groups.forEach((g, i) => {
      const got = tailProj(g), { notKept, worked, quiet, more } = gold[i]!;
      if (excluded("keep", id, `groups.${i}.worked`)) {
        // the Kitimat ruling: How it adds up held to the STORE's words (rows_row.elsewhere, group.worked), the rest to the golden
        expect({ ...got, worked: null }).toEqual({ notKept, worked: null, quiet, more });
        expect(got.worked).toEqual({ says: g.ctx.row.raw.elsewhere?.says ?? null,
          lines: g.ctx.g.worked.map((w) => ({ at: w.at === "here" ? "here" : "elsewhere", q: w.at === "here" ? `${w.q}:` : w.q, a: w.a })) });
        return;
      }
      expect(got).toEqual({ notKept, worked, quiet, more });
    });
    groups += k.groups.length; equal++;
  });

  const tail = async (id: string, i = 0) => tailProj(keepOf((await inputsOf(id))!).groups[i]!);

  it("1. vedder_515: the rainbow and cutthroat release line, whose rule and its page, the two pictures", async () => {
    expect((await tail("vedder_515")).notKept).toEqual([{ status: "release", says: "Rainbow trout and cutthroat trout: release every one",
      whose: "This river’s rule · Rule book p.22 ↗", mark: null, presence: null,
      pics: [{ code: "RB", caption: "Rainbow trout ›" }, { code: "CT", caption: "Coastal cutthroat trout ›" }], pages: [{ text: "Rule book p.22 ↗", pdf: 24 }] }]);
  });

  it("2. ape_501: the steelhead closure (✕, its presence line) and the char release line", async () => {
    const nk = (await tail("ape_501")).notKept;
    expect(nk.map((x) => [x.status, x.says, x.whose, x.mark, x.presence])).toEqual([
      ["closed", "Steelhead: no fishing for them, not even catch and release — release any you hook by accident", "The Bella Coola River’s rule · Rule book p.43 ↗", "✕",
        "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ"],
      ["release", "Char (Dolly Varden/bull trout, lake trout and brook trout): release every one", "The Bella Coola River’s rule for its tributaries · Rule book p.43 ↗", null, null]]);
  });

  it("3. vedder_1010: How it adds up — the elsewhere sentence, the keep-here questions with their colon, the kept-elsewhere questions", async () => {
    const w = (await tail("vedder_1010")).worked!;
    expect([w.says, w.lines.map((l) => [l.at, l.q])]).toEqual(["Your own limit, counted across every Region 2 stream you fish today.", [
      ["here", "If you keep 4 hatchery rainbow trout here:"], ["here", "If you keep 2 hatchery rainbow trout here:"],
      ["elsewhere", "Kept 1 at a lake?"], ["elsewhere", "Kept trout or char over 50 cm (20 in) elsewhere today?"]]]);
  });

  it("4–5. klwa_1010 (the lake's sentence, no lines); x15p0_1010's quiet line", async () => {
    expect((await tail("klwa_1010")).worked).toEqual({ says: "This lake’s rule: 2 a day here. Trout and char other than steelhead you kept anywhere else today count toward those 2.", lines: [] });
    expect((await tail("x15p0_1010")).quiet).toBe("Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout. ⓘ");
  });
});

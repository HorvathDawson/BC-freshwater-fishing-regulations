/**
 * T34 render: the Gear tab drawn from the FROZEN golden pages (fixtures/*.json, written by `UPDATE_REGS_FIXTURES=1` from
 * buildRegsPage over the parity store — never hand-edited) and compared, line for line, to the golden's own drawn lines
 * (fixtures/golden_lines.json, copied from golden.json by the same writer). Components draw the VM: every word is core's.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import type { GearVM, RegsPage, SheetKey } from "@app/core";
import { LIGHT } from "../theme";
import { GearTab } from "./GearTab";
import type { KeepActions } from "./LineText";
import { domLines } from "./testing/domLines";
import vedder1010 from "./fixtures/vedder_1010.json";
import vedder515 from "./fixtures/vedder_515.json";
import vedder615 from "./fixtures/vedder_615.json";
import bc1501 from "./fixtures/bc1_501.json";
import goldenLines from "./fixtures/golden_lines.json";

afterEach(cleanup);

const gearOf = (f: unknown) => (f as RegsPage).gear as GearVM;
const GL = goldenLines as Record<string, { gear: string[]; methods: string[]; licence: Record<string, string[]> }>;

function mount(vm: GearVM) {
  const sheets: SheetKey[] = [], books: number[] = [];
  const actions: KeepActions = { onSheet: (k) => sheets.push(k), onDay() {}, onTab() {}, onBook: (p) => books.push(p) };
  const r = render(<GearTab vm={vm} palette={LIGHT} actions={actions} />);
  return { r, sheets, books, lines: () => domLines(screen.getByTestId("gear-tab")) };
}

describe("<GearTab> vedder_1010 (open, Oct 10)", () => {
  const vm = gearOf(vedder1010);

  it("the heading, the day, the three rows: label left, each value", () => {
    mount(vm);
    expect(screen.getByRole("heading", { name: "Gear" }).textContent).toBe("Gear");
    expect(screen.getByText("Today, Oct 10")).toBeTruthy();
    expect(["lines", "hook", "weight"].map((k) => domLines(screen.getByTestId(`gear-line-${k}`))[0])).toEqual([
      "Lines 1 line", "Hooks One hook, lure or fly", "Weight on the line Up to 1 kg (2.2 lb) ⓘ"]);
    expect(domLines(screen.getByTestId("gear-line-hook"))).toEqual(["Hooks One hook, lure or fly", "Single barbless hook, no trebles ⓘ",
      "Next May 1–31: fly fishing only", "Province · Region 2 · This water’s › Rule book p.8 ↗ Rule book p.21 ↗ Rule book p.22 ↗"]);
  });

  it("the Hooks change is quiet: chip + dates + the data's words, no link inside; the chip is the tinted caps label", () => {
    mount(vm);
    const change = within(screen.getByTestId("gear-line-hook")).getByTestId("regs-change");
    expect(change.textContent).toBe("NextMay 1–31: fly fishing only");
    expect(within(change).queryByRole("link")).toBeNull();
    expect(within(change).queryByRole("button")).toBeNull();
    const chip = change.firstElementChild as HTMLElement;
    expect([chip.textContent, chip.style.textTransform, chip.style.backgroundColor !== ""]).toEqual(["Next", "uppercase", true]);
  });

  it("the Hooks source line opens srcs:gl|hook; a Rule book link says its PDF page", () => {
    const { sheets, books } = mount(vm);
    const hook = within(screen.getByTestId("gear-line-hook"));
    const btn = hook.getByRole("button", { name: vm.lines[1]!.src!.aria });
    expect([btn.textContent, vm.lines[1]!.src!.aria]).toEqual(["Province · Region 2 · This water’s ›", "Sources: Province, Region 2, This water’s"]);
    expect(hook.getAllByRole("link").map((a) => a.textContent)).toEqual(["Rule book p.8 ↗", "Rule book p.21 ↗", "Rule book p.22 ↗"]);
    fireEvent.click(btn);
    fireEvent.click(hook.getByText("Rule book p.21 ↗"));
    fireEvent.click(hook.getByText("ⓘ"));
    expect([sheets, books]).toEqual([["srcs:gl|hook", "gl:single_barbless_hook"], [23]]);
  });

  it("Bait: ✓ rows with their notes, ✕ Fish or fish parts, the dated ban, Province ›", () => {
    const { sheets } = mount(vm);
    expect(domLines(screen.getByTestId("gear-bait"))).toEqual(["Bait", "Next May 1–31: bait ban", "Allowed", "✓ Worms",
      "✓ Roe (fish eggs) the fish or fish parts ban excepts it", "✓ Water insects and crayfish on streams", "Not allowed",
      "✕ Fish or fish parts", "Province › Rule book p.8 ↗"]);
    expect(screen.getByLabelText("Allowed: Roe (fish eggs), the fish or fish parts ban excepts it")).toBeTruthy();
    fireEvent.click(within(screen.getByTestId("gear-bait")).getByRole("button", { name: "Sources: Province" }));
    expect(sheets).toEqual(["srcs:bait"]);
  });

  it("golden mount: every drawn line equals golden.states.vedder_1010.gear.lines_", () => {
    const { lines } = mount(vm);
    expect(lines()).toEqual(GL.vedder_1010!.gear);
  });
});

describe("<GearTab> other states", () => {
  it("vedder_515: the fly-only mode line with its one ⓘ (fly_fishing); bait ban Now, in force today; Weight has no source", () => {
    const { sheets } = mount(gearOf(vedder515));
    const mode = screen.getByTestId("gear-mode");
    expect(domLines(mode)).toEqual(["Fly fishing only ⓘ", "Next Jul 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)",
      "This water’s · Province › Rule book p.8 ↗ Rule book p.22 ↗"]);
    fireEvent.click(within(mode).getByText("ⓘ"));
    expect(sheets).toEqual(["gl:fly_fishing"]);
    expect(domLines(screen.getByTestId("gear-bait"))).toContain("Now May 1–31: bait ban · in force today");
    expect(within(screen.getByTestId("gear-line-weight")).queryByTestId("regs-src")).toBeNull();
  });

  it("vedder_515 golden mount: every drawn line equals the golden's", () => {
    const { lines } = mount(gearOf(vedder515));
    expect(lines()).toEqual(GL.vedder_515!.gear);
  });

  it("bc1_501 golden mount: equal but for the keyed bait source (exclusions.ts SNAPSHOT_SUB_IDS: a prototype snapshot artifact)", () => {
    const { lines } = mount(gearOf(bc1501));
    const want = GL.bc1_501!.gear.map((l) => (l === "Bella Coola River › Rule book p.43 ↗" ? "This water’s › Rule book p.43 ↗" : l));
    expect(want).not.toEqual(GL.bc1_501!.gear);
    expect(lines()).toEqual(want);
  });

  it("vedder_615 (closed): only the sentence — no heading, no day line, no rows, no bait", () => {
    const { lines } = mount(gearOf(vedder615));
    expect(lines()).toEqual(["No fishing here until Jul 1."]);
    expect(lines()).toEqual(GL.vedder_615!.gear);
    expect([screen.queryByRole("heading"), screen.queryByTestId("gear-bait"), screen.queryByTestId("gear-line-lines")]).toEqual([null, null, null]);
  });

  it("mutation pins: a lost value ⓘ, a dropped change or a reordered row fails the golden mount", () => {
    const vm = gearOf(vedder1010), golden = GL.vedder_1010!.gear;
    const muts: Record<string, GearVM> = {
      term: { ...vm, lines: vm.lines.map((l) => ({ ...l, valueTerms: l.valueTerms.map(() => []) })) },
      change: { ...vm, lines: vm.lines.map((l) => ({ ...l, changes: [] })) },
      order: { ...vm, lines: [...vm.lines].reverse() },
      inForce: { ...vm, bait: { ...vm.bait!, dated: vm.bait!.dated.map((c) => ({ ...c, inForce: true })) } },
    };
    const same = Object.entries(muts).filter(([, m]) => { const { lines } = mount(m), eq = JSON.stringify(lines()) === JSON.stringify(golden); cleanup(); return eq; });
    expect(same.map(([k]) => k)).toEqual([]);
  });
});

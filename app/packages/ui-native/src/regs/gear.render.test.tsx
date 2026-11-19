/**
 * T34 render: the Gear tab drawn from the FROZEN pages (fixtures/*.json, written by `UPDATE_REGS_FIXTURES=1` from
 * buildRegsPage over the parity store — never hand-edited). Components draw the VM: every word is core's. (T439: the
 * line-for-line "golden mounts" against golden.json's drawn lines are gone.)
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

afterEach(cleanup);

const gearOf = (f: unknown) => (f as RegsPage).gear as GearVM;

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
      "Next May 1–31: fly fishing only"]);   // (T900) no source line per row: the section draws one (gear-src)
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

  it("(T900) ONE source line under the section opens srcs:gear; a Rule book link says its PDF page; no row draws its own", () => {
    const { sheets, books } = mount(vm);
    const src = within(screen.getByTestId("gear-src")), v = vm.src!;
    expect(screen.getAllByTestId("regs-src")).toHaveLength(1);
    const btn = src.getByRole("button", { name: v.aria });
    expect([btn.textContent, v.open]).toEqual([v.who, "srcs:gear"]);
    expect(src.getAllByRole("link").map((a) => a.textContent)).toEqual(v.pages.map((p) => `Rule book p.${p.printed} ↗`));
    // every row's pages are in it (nothing a row said is lost)
    for (const l of vm.lines) for (const p of l.src?.pages ?? []) expect(v.pages.map((x) => x.printed)).toContain(p.printed);
    fireEvent.click(btn);
    fireEvent.click(within(screen.getByTestId("gear-line-hook")).getByText("ⓘ"));
    expect([sheets, books]).toEqual([["srcs:gear", "gl:single_barbless_hook"], []]);
  });

  it("Bait: ✓ rows with their notes, ✕ Fish or fish parts, the dated ban, Province ›", () => {
    const { sheets } = mount(vm);
    expect(domLines(screen.getByTestId("gear-bait"))).toEqual(["Bait", "Bait ban May 1–31 · not today ⓘ", "Allowed", "✓ Worms",
      "✓ Roe (fish eggs) the fish or fish parts ban excepts it", "✓ Water insects and crayfish on streams", "Not allowed",
      "✕ Fish or fish parts"]);
    // (T40) no row label (it would hide the row's words and its ⓘ from a screen reader): the column head is read as text,
    // then each row's own words; the ✓ / ✕ that repeats the head is hidden
    expect(screen.queryByLabelText("Allowed: Roe (fish eggs), the fish or fish parts ban excepts it")).toBeNull();
    expect([...screen.getByTestId("gear-bait").querySelectorAll('[aria-hidden="true"]')].map((e) => e.textContent)).toEqual(["✓", "✓", "✓", "✕"]);
    expect(sheets).toEqual([]);   // (T900) its source is the section's one
  });

});

describe("<GearTab> other states", () => {
  it("vedder_515: the fly-only mode line with its one ⓘ (fly_fishing); bait ban Now, in force today; Weight has no source", () => {
    const { sheets } = mount(gearOf(vedder515));
    const mode = screen.getByTestId("gear-mode");
    expect(domLines(mode)).toEqual(["Fly fishing only ⓘ", "Next Jul 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)"]);
    fireEvent.click(within(mode).getByText("ⓘ"));
    expect(sheets).toEqual(["gl:fly_fishing"]);
    expect(domLines(screen.getByTestId("gear-bait"))).toContain("No natural bait May 1–31 · in force today ⓘ");
    expect(within(screen.getByTestId("gear-line-weight")).queryByTestId("regs-src")).toBeNull();
  });

  it("vedder_615 (closed): only the sentence — no heading, no day line, no rows, no bait", () => {
    const { lines } = mount(gearOf(vedder615));
    expect(lines()).toEqual(["No fishing here until Jul 1."]);
    expect([screen.queryByRole("heading"), screen.queryByTestId("gear-bait"), screen.queryByTestId("gear-line-lines")]).toEqual([null, null, null]);
  });

  it("(T227) salmon gear: DFO's conditions under their head, above the day's gear; no closure sentence", () => {
    render(<GearTab vm={gearOf(vedder1010)} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onTab() {}, onBook() {} }}
                    salmonGear={{ lead: "For salmon (DFO)", lines: ["No natural bait allowed (Jan 1–Dec 31)"] }} />);
    expect(screen.getByTestId("gear-salmon").textContent).toBe("For salmon (DFO)No natural bait allowed (Jan 1–Dec 31)");
    expect(screen.getByTestId("gear-tab").textContent).not.toMatch(/No fishing/);
  });

});

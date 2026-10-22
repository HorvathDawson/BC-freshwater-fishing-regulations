/**
 * T41 polish (Claude 2026-10-22, owner delegated: "the prototype is a reference") — THE VERTICAL RHYTHM of Gear / Methods
 * / Licence and the sheets' typography, pinned where a render test can see it (react-native-web writes the style inline).
 *
 * R1: the measured walk (app/tools/regs-heights.mjs) put the app's sections 10–21 % taller than the prototype's. Most of it
 * was one mechanism: a note or a price drawn as a NESTED run inside its item's name Text inherited the name's line box
 * (20.25 / 21.3 px) — every note line drew ~4 px taller than the prototype's `--t4/1.3` (16.22). The fix keeps the one
 * Text per item (the golden lines read name + note as one line; a screen reader reads the item whole) and gives the
 * item's Text the SMALL line box as its strut, the name a run of its own at the name's height. These tests hold:
 *   - a ✓/✕ row: the strut 16.22, the name run 19.5 (the prototype's `.ync li` --t3/1.3), the note run 16.22;
 *   - a licence document ("You need" item): the strut 16.22, the name run 21.3, the when run 17.72, the price run 16.22;
 *   - the sheet's close word is sentence case "Done" (Q4), and a sheet's small heading is sentence case (O4) — caps only
 *     on kickers (the prototype's own round-20 rule #7, `.sbody .lbl, .done { text-transform: none }`).
 * Whole strings are unchanged (gear / methods-licence render tests hold every golden line).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import type { GearVM, LicenceVM, MethodsVM, RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { Sheet } from "../Sheet";
import { GearTab } from "./GearTab";
import { LicenceTab } from "./LicenceTab";
import type { KeepActions } from "./LineText";
import { MethodsTab } from "./MethodsTab";
import { Head } from "./sheet/Para";
import vedder1010 from "./fixtures/vedder_1010.json";
import licences from "./fixtures/licence_vedder_1010.json";

afterEach(cleanup);

const NO: KeepActions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };
const lh = (el: Element) => (el as HTMLElement).style.lineHeight;

describe("R1 — a ✓ / ✕ row keeps the prototype's line boxes (Gear's bait list, vedder_1010)", () => {
  it("strut 16.22 px, name run 19.5 px, note run 16.22 px — the note no longer inherits the name's line", () => {
    render(<GearTab vm={(vedder1010 as unknown as RegsPage).gear as GearVM} palette={LIGHT} actions={NO} />);
    const rows = screen.getAllByTestId("yesno-item");
    const roe = rows.find((r) => r.textContent!.startsWith("Roe (fish eggs)"))!;
    expect([roe.textContent, lh(roe)]).toEqual(["Roe (fish eggs)\nthe fish or fish parts ban excepts it", "16.22px"]);
    expect([within(roe).getByTestId("yesno-name").textContent, lh(within(roe).getByTestId("yesno-name"))])
      .toEqual(["Roe (fish eggs)", "19.5px"]);
    expect([within(roe).getByTestId("yesno-note").textContent, lh(within(roe).getByTestId("yesno-note"))])
      .toEqual(["\nthe fish or fish parts ban excepts it", "16.22px"]);
    // every row, not only the one read: the same three line boxes
    expect([...new Set(rows.map((r) => lh(r)))]).toEqual(["16.22px"]);
    expect([...new Set(rows.map((r) => lh(within(r).getByTestId("yesno-name"))))]).toEqual(["19.5px"]);
  });
});

describe("R1 — Gear and Methods close 16 px under their last line (the section's 8 + the tab's 8), not 32", () => {
  it("the tab's own foot is 8 px (was 24): measured, the Gear tail then equals the prototype's (vedder 652 px)", () => {
    const page = vedder1010 as unknown as RegsPage;
    render(<><GearTab vm={page.gear as GearVM} palette={LIGHT} actions={NO} />
             <MethodsTab vm={page.methods as MethodsVM} palette={LIGHT} actions={NO} /></>);
    expect(["gear-tab", "methods-tab"].map((id) => screen.getByTestId(id).style.paddingBottom)).toEqual(["8px", "8px"]);
  });
});

describe("R1 — a licence document keeps the prototype's line boxes (vedder_1010, adult resident)", () => {
  it("strut 16.22 px; name 21.3, when 17.72, price 16.22", () => {
    const vm = (licences as unknown as Record<string, LicenceVM>)["adult|resident"]!;
    render(<LicenceTab vm={vm} who={{ age: "adult", residency: "resident" }} palette={LIGHT} actions={NO} onWho={() => {}} />);
    const basic = screen.getAllByTestId("need-item").find((e) => e.textContent!.startsWith("Basic angling licence"))!;
    expect(lh(basic)).toBe("16.22px");
    expect([within(basic).getByTestId("need-name").textContent, lh(within(basic).getByTestId("need-name"))])
      .toEqual(["Basic angling licence", "21.3px"]);
    expect([within(basic).getByTestId("need-when").textContent, lh(within(basic).getByTestId("need-when"))])
      .toEqual(["\nNeeded to fish at all", "17.72px"]);
    expect(lh(within(basic).getByTestId("need-price"))).toBe("16.22px");
    expect(within(basic).getByTestId("need-price").textContent!.startsWith("\n$41.15 a year")).toBe(true);
  });
});

describe("Q4 / O4 — sentence case in sheets", () => {
  it("the shared Sheet's close word reads \"Done\", not upper-cased (one style for every sheet in the app)", () => {
    render(<Sheet open title="Where each one comes from" palette={LIGHT} onClose={() => {}}>{null}</Sheet>);
    const done = screen.getByLabelText("Done");
    const word = done.querySelector("[dir=auto]") as HTMLElement;
    expect([word.textContent, word.style.textTransform, word.style.letterSpacing]).toEqual(["Done", "", ""]);
  });

  it("a sheet's small heading (\"To fish here\", \"These decide it (12)\", \"What to look for\") is sentence case", () => {
    render(<Head text="To fish here" palette={LIGHT} />);
    const h = screen.getByRole("heading", { name: "To fish here" });
    expect([h.textContent, h.style.textTransform, h.style.letterSpacing]).toEqual(["To fish here", "", ""]);
  });
});

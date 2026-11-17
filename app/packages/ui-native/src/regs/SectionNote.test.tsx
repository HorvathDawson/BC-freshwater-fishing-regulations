/**
 * (T796) A SECTION'S NOTE ON ITS PAGE: the Keep tab of a page core built from the committed regs fixture's own inputs, with
 * the release's note for the tapped section (core `section_note`, Python's sentence) — drawn first, above the year strip,
 * word for word, as a quiet caveat with nothing to open; the same page with no note draws none. The fixture's core was
 * cut before the table, so the note rides on the inputs the store would read for a release that has one.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { buildRegsPage, type RegsInputs } from "@app/core";
import { loadRegsInputs, targetFor } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { loadRegsStore } from "@app/ui";
import { KeepTab } from "./KeepTab";
import { LIGHT } from "../theme";

afterEach(cleanup);

const NOTE = "Mostly outside Gladys Lake Ecological Reserve: its closure (rule book p.9) does not apply here.";
const DAY = { month: 10, day: 10 };

/** The fixture's first water (by ord) the store answers, on Oct 10 2026. */
async function inputs(): Promise<RegsInputs> {
  const st = await loadRegsStore(openRegsFixture());
  const first = await (await st.core()).get("SELECT item_id FROM item ORDER BY ord LIMIT 1");
  const target = await targetFor(st, String(first!.item_id), null);
  return loadRegsInputs(st, target, DAY, "Saturday", 2026);
}

function drawn(inp: RegsInputs) {
  const page = buildRegsPage(inp, { today: DAY, who: { age: "adult", residency: "resident" } });
  const r = render(<KeepTab keep={page.keep} dateLabel="Oct 10" palette={LIGHT}
                            actions={{ onSheet() {}, onDay() {}, onTab() {}, onBook() {} }} />);
  return [...r.container.querySelectorAll("[data-testid^='keep-']")].map((e) => e.getAttribute("data-testid"))
    .filter((t) => t !== "keep-tab");
}

describe("<KeepTab>: a section's note (core section_note)", () => {
  it("leads the tab, above the strip, in Python's words; it opens nothing", async () => {
    const inp = await inputs();
    expect(inp.notes).toEqual([]);                                        // the fixture's core has no section_note
    const blocks = drawn({ ...inp, notes: [NOTE] });
    expect(blocks[0]).toBe("keep-caveat");
    expect(blocks.indexOf("keep-strip")).toBeGreaterThan(0);
    const note = screen.getByTestId("kicker:note");
    expect(note.textContent).toBe(NOTE);
    expect(note.closest("[data-testid='keep-caveat']")!.querySelector("[role='button']")).toBeNull();
  });

  it("a page with no note draws none", async () => {
    drawn(await inputs());
    expect(screen.queryByTestId("kicker:note")).toBeNull();
  });
});

/**
 * T475 (owner 2026-11-07) — the group card draws the quota block's new VM fields: form a's lead ends in an ⓘ (a link named
 * for a screen reader) opening the row's `share:r<row>` sheet; a closed not-kept line ("Steelhead: closed, no catch and
 * release") carries its glossary ⓘ (`no_fishing_for`, what an accidental hook means); a share tier whose origin the lead
 * says draws only How to tell + its B.C. Synopsis page. Mounted with those fields set as core's presenters make them (core
 * t475.test.ts proves the presenters) over (T810 cutover) the group card a real page still draws: the Vedder's
 * Whitefish, its Other fish sheet (testing/otherGroup.ts). Retired: "the share's origin sentence is not drawn under the
 * headline; How to tell and the B.C. Synopsis stay" — a trout share tier with hatchery words exists in no group card since
 * the cutover; the table says it (TroutCard.test "the hatchery link only where hatch_link; …"; the row's B.C. Synopsis pages
 * are the fish sheet's Sources, conformance trout.release.test.ts "T810 P: the trout fish sheet over every vector"). react-native-web renders
 * the web app from the same component, so this is the web counterpart too (AGENTS 26).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import type { GroupVM, SheetKey, TierVM } from "@app/core";
import { GroupCard } from "./GroupCard";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";
import { otherGroup } from "./testing/otherGroup";

afterEach(cleanup);

const BASE = otherGroup(vedder1010, "other:o1");
const G: GroupVM = {
  ...BASE,
  head: { ...BASE.head, lead: [{ t: "num", s: "2", size: "big" }, { t: "text", s: " " },
    { t: "text", s: "hatchery trout/char a day from streams" }, { t: "text", s: " " },
    { t: "link", s: "ⓘ", open: "share:r0", label: "How this limit is shared" }] },
  tree: BASE.tree!.map((n) => (n.t === "tier" && n.tier === "share" && n.origin ? { ...n, origin: { ...n.origin, says: [] } } as TierVM : n)),
  notKept: [{ status: "closed", says: "Steelhead: closed, no catch and release", term: "no_fishing_for",
    whose: [{ t: "text", s: "This river’s rule" }], mark: "✕", presence: null, pictures: [], stamps: [] }],
};

function mount(g: GroupVM = G) {
  const opened: SheetKey[] = [];
  const actions = { onSheet: (k: SheetKey) => opened.push(k), onDay() {}, onTab() {}, onBook() {} };
  const { container } = render(<GroupCard g={g} palette={LIGHT} actions={actions} />);
  return { opened, container };
}

describe("T475 the quota block", () => {
  it("ask 1: the headline in one line, its ⓘ a named link to the share sheet", () => {
    const { opened, container } = mount();
    expect(container.textContent).toContain("2 hatchery trout/char a day from streams ⓘ");
    const info = screen.getByRole("link", { name: "How this limit is shared" });
    expect(info.textContent).toBe("ⓘ");
    fireEvent.click(info);
    expect(opened).toEqual(["share:r0"]);
  });
  it("ask 2: the steelhead line is short, its ⓘ opens what an accidental hook means", () => {
    const { opened, container } = mount();
    const line = container.querySelector("[data-testid='st:closed']")!;
    expect(line.textContent).toBe("Steelhead: closed, no catch and release ⓘ");
    fireEvent.click(within(line as HTMLElement).getByRole("button", { name: /What (does|this word)/ }));
    expect(opened).toEqual(["gl:no_fishing_for"]);
  });
  it("no term, no ⓘ (a release line; a store before T475)", () => {
    const { container } = mount({ ...G, notKept: [{ ...G.notKept[0]!, status: "release", says: "Dolly Varden: release every one", term: null, mark: null }] });
    expect(container.querySelector("[data-testid='st:release']")!.textContent).toBe("Dolly Varden: release every one");
  });
});

/**
 * T479 (owner 2026-11-07) — the group card draws: "Exception to regional quota" over a tier (or the head) whose VM carries
 * it; the worked list as a fold titled "Examples of how to interpret regulations", CLOSED by default, its lines drawn only
 * once opened. (T810 cutover) Mounted over the group cards a real page still draws (an Other fish row's sheet,
 * testing/otherGroup.ts): x35p's Arctic grayling, whose head is flagged as cut, and the Vedder's Whitefish, with its
 * examples. Retired: "How to tell hatchery › opens the hatch sheet" — no group card holds a hatchery line since the
 * cutover; the table's own link is TroutCard.test "the hatchery link only where hatch_link; …" (it opens `hatch`), and
 * HowToTell.test 6 on the Vedder's Keep tab. react-native-web renders the web app from the same component (AGENTS 26).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { GroupVM, SheetKey } from "@app/core";
import { GroupCard } from "./GroupCard";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";
import x35p1010 from "./fixtures/x35p_1010.json";
import { otherGroup } from "./testing/otherGroup";

afterEach(cleanup);

const GRAYLING = otherGroup(x35p1010, "other:o2");
const WHITEFISH = otherGroup(vedder1010, "other:o1");
const EXC = "Exception to regional quota";

function mount(g: GroupVM) {
  const opened: SheetKey[] = [];
  const actions = { onSheet: (k: SheetKey) => opened.push(k), onDay() {}, onTab() {}, onBook() {} };
  const { container } = render(<GroupCard g={g} palette={LIGHT} actions={actions} />);
  return { opened, container };
}

describe("T479 the group card", () => {
  it("2: a flagged head says Exception to regional quota; cleared, nothing; the fixture as cut flags the grayling's head", () => {
    // as cut (r-d274bf0a1e26): x35p's Arctic grayling quota is the water's own, over the region's
    expect(GRAYLING.head.exception).toBe(EXC);
    expect(screen.queryAllByTestId("kicker:exception")).toEqual([]);
    mount(GRAYLING);
    expect(screen.getAllByTestId("kicker:exception").map((e) => e.textContent)).toEqual([EXC]);
    cleanup();
    expect(mount({ ...GRAYLING, head: { ...GRAYLING.head, exception: null } }).container.querySelector("[data-testid='kicker:exception']")).toBeNull();
  });
  it("3: the examples fold starts closed and opens on a press", () => {
    const G = WHITEFISH;
    expect(G.worked?.lines.length).toBeGreaterThan(0);
    const { container } = mount(G);
    const summary = screen.getByTestId("worked-summary");
    expect(screen.getByTestId("kicker:worked").textContent).toBe("Examples of how to interpret regulations");
    expect(summary.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByTestId("worked-body")).toBeNull();
    expect(container.textContent).not.toContain(G.worked!.lines[0]!.a);
    fireEvent.click(summary);
    expect(summary.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByTestId("worked-body").textContent).toContain(G.worked!.lines[0]!.a);
  });
});

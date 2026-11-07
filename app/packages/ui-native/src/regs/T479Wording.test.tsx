/**
 * T479 (owner 2026-11-07) — the group card draws: "How to tell hatchery ›" beside the hatchery-only wording (opens the
 * hatch sheet); "Exception to regional quota" over a tier (or the head) whose VM carries it; the worked list as a fold
 * titled "Examples of how to interpret regulations", CLOSED by default, its lines drawn only once opened. Mounted over the
 * frozen vedder_1010 Keep group; react-native-web renders the web app from the same component (AGENTS 26).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { GroupVM, RegsPage, SheetKey, TierVM } from "@app/core";
import { GroupCard } from "./GroupCard";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const G = (vedder1010 as unknown as RegsPage).keep.groups[0]!;
const EXC = "Exception to regional quota";

function mount(g: GroupVM = G) {
  const opened: SheetKey[] = [];
  const actions = { onSheet: (k: SheetKey) => opened.push(k), onDay() {}, onTab() {}, onBook() {} };
  const { container } = render(<GroupCard g={g} palette={LIGHT} actions={actions} />);
  return { opened, container };
}

describe("T479 the group card", () => {
  it("1: How to tell hatchery › opens the hatch sheet", () => {
    const { opened } = mount();
    const links = screen.getAllByText("How to tell hatchery ›");
    expect(links.length).toBeGreaterThan(0);
    expect(screen.queryByText("How to tell ›")).toBeNull();
    fireEvent.click(links[0]!);
    expect(opened).toEqual(["hatch"]);
  });
  it("2: a flagged own tier, and a flagged head, say Exception to regional quota; the fixture as cut says none", () => {
    expect(mount().container.querySelector("[data-testid='kicker:exception']")).toBeNull();
    cleanup();
    const tree = G.tree!.map((n) => (n.t === "tier" && n.tier === "own" ? { ...n, exception: EXC } as TierVM : n));
    expect(tree.some((n) => n.t === "tier" && n.exception)).toBe(true);
    mount({ ...G, tree, head: { ...G.head, exception: EXC } });
    expect(screen.getAllByTestId("kicker:exception").map((e) => e.textContent)).toEqual([EXC, EXC]);
  });
  it("3: the examples fold starts closed and opens on a press", () => {
    expect(G.worked?.lines.length).toBeGreaterThan(0);
    const { container } = mount();
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

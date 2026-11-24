/**
 * (T1016) THE NOTICE ON SCREEN — since the owner's "Always auto refresh" (2026-11-24) only the after-loop fallback: shown
 * when a verdict's automatic refresh could not be made (its failure shown; its action the refresh by hand). Nothing until then; the data notice for the release in use only (one raised for the
 * release an apply just replaced is not shown); one line and one action, pressed → the kind's action; the code notice
 * outranks the data one (a reload loads both).
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { REPLACED_WORDS, type ReplacedKind } from "@app/core";
import { createReplacedState, useReplacedNotice, type ReplacedState } from "@app/ui";
import { ReplacedBanner } from "./ReplacedBanner";
import { LIGHT } from "./theme";

afterEach(cleanup);

function Host({ state, release, acts }: { state: ReplacedState; release: string; acts: Record<ReplacedKind, () => void> }) {
  return <ReplacedBanner palette={LIGHT} notice={useReplacedNotice(state, release, acts)} />;
}

describe("<ReplacedBanner>", () => {
  it("raised for the release in use: its line and its one action; pressed: the action; the code notice outranks it", async () => {
    // every automatic refresh answers that it could not be made (as a reload that would loop does): the notice shows
    let release = "r-a";
    const state = createReplacedState({ refresh: async () => "it would loop", sent: async () => {}, inUse: () => release });
    const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 0)); });
    const pressed: string[] = [];
    const acts = { data: () => pressed.push("data"), code: () => pressed.push("code") };
    const { rerender } = render(<Host state={state} release="r-a" acts={acts} />);
    expect(screen.queryByTestId("replaced-data")).toBeNull();
    state.raise({ kind: "data", release: "r-a" });
    await settle();
    const bar = screen.getByTestId("replaced-data");
    expect(bar.textContent).toBe(`${REPLACED_WORDS.data.line}${REPLACED_WORDS.data.action}`);
    expect(bar.getAttribute("aria-label")).toBe(REPLACED_WORDS.data.line);
    fireEvent.click(bar);
    expect(pressed).toEqual(["data"]);
    // the apply put r-c in use: the notice raised for r-a is not shown
    release = "r-c";
    rerender(<Host state={state} release="r-c" acts={acts} />);
    expect(screen.queryByTestId("replaced-data")).toBeNull();
    state.raise({ kind: "code", release: null });
    await settle();
    state.raise({ kind: "data", release: "r-c" });
    await settle();
    const code = screen.getByTestId("replaced-code");
    expect(code.textContent).toBe(`${REPLACED_WORDS.code.line}${REPLACED_WORDS.code.action}`);
    fireEvent.click(code);
    expect(pressed).toEqual(["data", "code"]);
  });
});

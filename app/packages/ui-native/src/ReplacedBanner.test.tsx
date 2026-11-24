/**
 * (T1016) THE NOTICE ON SCREEN: nothing until raised; the data notice for the release in use only (one raised for the
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
  it("raised for the release in use: its line and its one action; pressed: the action; the code notice outranks it", () => {
    const state = createReplacedState();
    const pressed: string[] = [];
    const acts = { data: () => pressed.push("data"), code: () => pressed.push("code") };
    const { rerender } = render(<Host state={state} release="r-a" acts={acts} />);
    expect(screen.queryByTestId("replaced-data")).toBeNull();
    act(() => state.raise({ kind: "data", release: "r-a" }));
    const bar = screen.getByTestId("replaced-data");
    expect(bar.textContent).toBe(`${REPLACED_WORDS.data.line}${REPLACED_WORDS.data.action}`);
    expect(bar.getAttribute("aria-label")).toBe(REPLACED_WORDS.data.line);
    fireEvent.click(bar);
    expect(pressed).toEqual(["data"]);
    // the apply put r-c in use: the notice raised for r-a is not shown
    rerender(<Host state={state} release="r-c" acts={acts} />);
    expect(screen.queryByTestId("replaced-data")).toBeNull();
    act(() => state.raise({ kind: "code", release: null }));
    act(() => state.raise({ kind: "data", release: "r-c" }));
    const code = screen.getByTestId("replaced-code");
    expect(code.textContent).toBe(`${REPLACED_WORDS.code.line}${REPLACED_WORDS.code.action}`);
    fireEvent.click(code);
    expect(pressed).toEqual(["data", "code"]);
  });
});

/**
 * OWNER DECISION 2026-10-20: search's substring scan (~5 ms p95 on the laptop, A5 in tools/release-perf.test.ts) is
 * accepted PROVIDED the box is debounced so it does not query on every keystroke. This pins that: a burst of typing
 * issues ONE search, after the pause.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { RegsSource } from "@app/data";
import { SEARCH_DEBOUNCE_MS, useSearch } from "./hooks";

const stub = () => {
  const searchNames = vi.fn(async () => []);
  const searchPlaces = vi.fn(async () => []);
  return { source: { searchNames, searchPlaces } as unknown as RegsSource, searchNames, searchPlaces };
};

function Probe({ source, q }: { source: RegsSource; q: string }) {
  const r = useSearch(source, q);
  return <p>{r.state}</p>;
}

/** Type `word` a letter at a time, `gapMs` apart — every gap shorter than the debounce. */
async function type(rerender: (ui: React.ReactElement) => void, source: RegsSource, word: string, gapMs: number) {
  for (let i = 1; i <= word.length; i++) {
    await act(async () => { rerender(<Probe source={source} q={word.slice(0, i)} />); });
    await act(async () => { vi.advanceTimersByTime(gapMs); });
  }
}
const pause = (ms: number) => act(async () => { vi.advanceTimersByTime(ms); await Promise.resolve(); });

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("useSearch is debounced (owner decision 2026-10-20)", () => {
  it("the delay is 200 ms: past a fast typist's gap between letters, under the point a reader waits for results", () => {
    expect(SEARCH_DEBOUNCE_MS).toBe(200);
  });

  it("eight rapid keystrokes issue exactly one search, for the whole word, after the pause", async () => {
    const { source, searchNames, searchPlaces } = stub();
    const { rerender } = render(<Probe source={source} q="" />);
    await type(rerender, source, "chilliwa", 120);
    expect(searchNames).toHaveBeenCalledTimes(0);
    await pause(SEARCH_DEBOUNCE_MS);
    expect(searchNames.mock.calls).toEqual([["chilliwa", 40]]);
    expect(searchPlaces.mock.calls).toEqual([["chilliwa", 6]]);
  });

  it("a pause just short of the delay sends nothing; the full delay sends one; a second burst sends one more", async () => {
    const { source, searchNames } = stub();
    const { rerender } = render(<Probe source={source} q="" />);
    await type(rerender, source, "fra", 0);
    await pause(SEARCH_DEBOUNCE_MS - 1);
    expect(searchNames).toHaveBeenCalledTimes(0);
    await pause(1);
    expect(searchNames.mock.calls).toEqual([["fra", 40]]);
    await type(rerender, source, "fraser", 50);
    await pause(SEARCH_DEBOUNCE_MS);
    expect(searchNames.mock.calls).toEqual([["fra", 40], ["fraser", 40]]);
  });
});

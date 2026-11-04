/**
 * (T247, T368: these stubs answer an empty search index, and each search then asks the source for its pointers — which is
 * what is counted. There is no fallback search any more: an index that cannot be read fails the search.)
 * OWNER DECISION 2026-10-20: search's substring scan (~5 ms p95 on the laptop, A5 in tools/release-perf.test.ts) is
 * accepted PROVIDED the box is debounced so it does not query on every keystroke. This pins that: a burst of typing
 * issues ONE search, after the pause.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { WaterSource } from "@app/data";
import { SEARCH_DEBOUNCE_MS, useSearch } from "./hooks";

const stub = () => {
  const searchNames = vi.fn(async (_q: string, _limit: number) => []);
  const searchIndex = vi.fn(async () => ({ waters: [], aliases: [], places: [] }));
  return { source: { searchIndex, searchPointers: searchNames } as unknown as WaterSource, searchNames, searchIndex };
};

function Probe({ source, q }: { source: WaterSource; q: string }) {
  const r = useSearch(source, q);
  return <p>{r.state}</p>;
}

/** Type `word` a letter at a time, `gapMs` apart — every gap shorter than the debounce. */
async function type(rerender: (ui: React.ReactElement) => void, source: WaterSource, word: string, gapMs: number) {
  for (let i = 1; i <= word.length; i++) {
    await act(async () => { rerender(<Probe source={source} q={word.slice(0, i)} />); });
    await act(async () => { vi.advanceTimersByTime(gapMs); });
  }
}
const pause = (ms: number) => act(async () => { vi.advanceTimersByTime(ms); for (let i = 0; i < 8; i++) await Promise.resolve(); });

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("useSearch is debounced (owner decision 2026-10-20)", () => {
  it("the delay is 200 ms: past a fast typist's gap between letters, under the point a reader waits for results", () => {
    expect(SEARCH_DEBOUNCE_MS).toBe(200);
  });

  it("eight rapid keystrokes issue exactly one search, for the whole word, after the pause", async () => {
    const { source, searchNames, searchIndex } = stub();
    const { rerender } = render(<Probe source={source} q="" />);
    await type(rerender, source, "chilliwa", 120);
    expect(searchNames).toHaveBeenCalledTimes(0);
    await pause(SEARCH_DEBOUNCE_MS);
    expect(searchNames.mock.calls).toEqual([["chilliwa", 3]]);    // the pointers, POINTER_LIMIT
    expect(searchIndex).toHaveBeenCalledTimes(1);
  });

  it("a pause just short of the delay sends nothing; the full delay sends one; a second burst sends one more", async () => {
    const { source, searchNames } = stub();
    const { rerender } = render(<Probe source={source} q="" />);
    await type(rerender, source, "fra", 0);
    await pause(SEARCH_DEBOUNCE_MS - 1);
    expect(searchNames).toHaveBeenCalledTimes(0);
    await pause(1);
    expect(searchNames.mock.calls).toEqual([["fra", 3]]);
    await type(rerender, source, "fraser", 50);
    await pause(SEARCH_DEBOUNCE_MS);
    expect(searchNames.mock.calls).toEqual([["fra", 3], ["fraser", 3]]);
  });
});

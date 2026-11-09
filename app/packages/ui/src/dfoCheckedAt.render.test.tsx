/** (T526) The DFO feed's checked time for the Data line: the feed's `at`; a missing or unreadable one is REPORTED, not guessed. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { setFailureSink } from "@app/core";
import type { SalmonFeedSource } from "@app/data";
import { useDfoCheckedAt } from "./dfoNotice";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });
const src = (f: unknown): SalmonFeedSource => ({ latest: async () => f as never });
function Probe({ source }: { source: SalmonFeedSource | null }) {
  return <p>{String(useDfoCheckedAt(source))}</p>;
}

describe("useDfoCheckedAt", () => {
  it("is the feed's checked time, not reported", async () => {
    const seen: unknown[] = []; const off = setFailureSink((e) => seen.push(e));
    const { container } = render(<Probe source={src({ at: "2026-11-09T12:00:00Z" })} />);
    await settle(); off();
    expect(container.textContent).toBe("2026-11-09T12:00:00Z");
    expect(seen).toEqual([]);
  });
  it("is null and REPORTED when no feed was read, or its stamp is not a time", async () => {
    for (const f of [null, { at: "soon" }]) {
      const seen: unknown[] = []; const off = setFailureSink((e) => seen.push(e));
      const { container } = render(<Probe source={src(f)} />);
      await settle(); off();
      expect(container.textContent).toBe("null");
      cleanup();
      expect(seen.length).toBe(1);
    }
  });
});

/** (T526) The DFO feed's checked time for the Data line: the feed's `at`; a missing or unreadable one is REPORTED, not guessed. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { setFailureSink } from "@app/core";
import type { SalmonFeedSource } from "@app/data";
import { useDfoCheckedAt } from "./dfoNotice";

// the pick itself (`pickSalmon`) is core's and tested there; here only that More follows it
vi.mock("@app/core", async (orig) => ({ ...(await orig<typeof import("@app/core")>()),
  salmonFeedOf: (f: { fed?: boolean } | null) => (f?.fed ? { at: "x", stale: [] } : null) }));

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

describe("useDfoCheckedAt with the card's pick (T620 G14)", () => {
  function Probe2({ fed }: { fed: boolean }) {
    return <p>{String(useDfoCheckedAt(src({ at: "2026-11-09T12:00:00Z" }), { salmon: async () => ({ fed }) as never }))}</p>;
  }
  it("More says the feed's time when the card shows the feed; nothing when a newer release's own file is what the card shows", async () => {
    const fresh = render(<Probe2 fed />);
    await settle();
    expect(fresh.container.textContent).toBe("2026-11-09T12:00:00Z");
    cleanup();
    const old = render(<Probe2 fed={false} />);
    await settle();
    expect(old.container.textContent).toBe("null");
  });
});

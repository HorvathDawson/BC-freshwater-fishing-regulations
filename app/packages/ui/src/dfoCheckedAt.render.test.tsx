/** (T526) The DFO feed's checked time for the Data line: the feed's `at`; a missing or unreadable one is REPORTED, not guessed. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { parseSalmonFeed, pickSalmon, setFailureSink } from "@app/core";
import { SALMON_FEED_GOLDEN } from "../../core/src/salmonFeed.golden";
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

describe("useDfoCheckedAt with the card's pick (T620 G14)", () => {
  const G = SALMON_FEED_GOLDEN as never as { release: never; sections: [number, number][]; feed: { at: string } };
  const by = new Map<number, number>(G.sections);
  const release = { handles: "147b20dce7d8576c", chains: 1, payload: G.release, chainOf: (s: number) => by.get(s) ?? 0, loaded: [] } as never;
  function Probe2({ feedAt }: { feedAt: string }) {
    const feed = { ...G.feed, at: feedAt };
    return <p>{String(useDfoCheckedAt(src(feed), { salmon: async () => pickSalmon(release, parseSalmonFeed(feed)) }))}</p>;
  }
  it("More says the feed's time when the card shows the feed; nothing when a newer release's own file is what the card shows", async () => {
    const fresh = render(<Probe2 feedAt="2026-11-09T12:00:00Z" />);
    await settle();
    expect(fresh.container.textContent).toBe("2026-11-09T12:00:00Z");
    cleanup();
    const old = render(<Probe2 feedAt="2026-08-01T00:00:00Z" />);   // the release's newest DFO page is dated after this capture
    await settle();
    expect(old.container.textContent).toBe("null");
  });
});

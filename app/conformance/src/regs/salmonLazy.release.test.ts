/**
 * (T716, owner 2026-11-13: "The salmon build seems way to long? Maybe a card builds its item when it needs it?") THE DFO
 * FEED OVER THE RELEASE'S SALMON, BUILT PER CHAIN WHEN READ, IS THE WHOLE BUILD. On the live release's salmon head and a
 * frozen DFO feed (`fixtures/dfo-feed-2026-11-13.json`, the feed as published that day), every chain of the province read
 * from the lazy payload — in reverse order, so merged verdicts are interned in another order than a whole build's — says
 * the same per fish, day and gear, by content, as the composition asked on EVERY day of the year, chain by chain
 * (`composeChainEveryDay`, the pass as it was before T716). Skipped without a live release.
 *
 * And nothing is built for nobody: reading one chain builds that one only.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { composeChainEveryDay, feedTables, parseSalmonFeed, withSalmonFeed, type SalmonFacts } from "@app/core";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
const FEED = parseSalmonFeed(JSON.parse(readFileSync(new URL("../../../fixtures/dfo-feed-2026-11-13.json", import.meta.url), "utf8")))!;

describe.skipIf(!LIVE)(`(T716) the salmon feed built per chain on the live release (${LIVE ?? "absent"})`, () => {
  it("every chain, read lazily in reverse, equals the every-day whole build by content", async () => {
    const facts = (await openReleaseDir(LIVE!).salmon()) as SalmonFacts;
    expect(FEED).not.toBeNull();
    const P = facts.payload;
    const live = withSalmonFeed(facts, FEED).payload;
    const ref = feedTables(FEED);
    const want = P.chains.map(({ c: _c, n, a, ...c }) =>
      ({ ...c, ...composeChainEveryDay(c.l, FEED, c.u ?? [], n !== undefined, c.s, ref, a !== undefined) }));
    /** a chain with its verdict and gear indexes read out: the content, whatever order they were interned in */
    const said = (c: (typeof want)[number], V: readonly unknown[], G: readonly unknown[]) => ({
      ...c,
      sp: c.sp.map((runs) => runs.map((v, i) => (i % 2 ? V[v] : v))),
      g: c.g.map((v, i) => (i % 2 ? (v >= 0 ? G[v] : null) : v)),
    });
    const n = P.chains.length;
    const got: unknown[] = new Array(n);
    for (let i = n - 1; i >= 0; i--) got[i] = said(live.chains[i]!, live.verdicts, live.gears);
    expect(got).toEqual(want.map((c) => said(c, ref.verdicts, ref.gears)));
    expect(n).toBeGreaterThan(100);
  });

  it("reading one chain builds that chain only: the rest are still unbuilt", async () => {
    const facts = (await openReleaseDir(LIVE!).salmon()) as SalmonFacts;
    const chains = withSalmonFeed(facts, { ...FEED }).payload.chains;
    // counted by the slots' descriptors, never by reading them (a read builds)
    const built = () => { let n = 0; for (let i = 0; i < chains.length; i++) if (!Object.getOwnPropertyDescriptor(chains, i)!.get) n++; return n; };
    const before = built();
    void chains[7]!.sp;
    expect([before, built(), !Object.getOwnPropertyDescriptor(chains, 7)!.get]).toEqual([0, 1, true]);
  });

});

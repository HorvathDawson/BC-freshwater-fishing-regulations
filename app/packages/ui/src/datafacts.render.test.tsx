/**
 * P11: the data's counts are read when a screen that shows them is up (search, the layers sheet), not at mount — on the
 * release `counts()` opens core, and a cold start is meant to fetch the manifest and paint only before the first map.
 * Once read they are kept.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { BundleCounts, WaterSource } from "@app/data";
import { useDataFacts } from "./hooks";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });

function Probe({ source, wanted }: { source: WaterSource; wanted: boolean }) {
  const f = useDataFacts(source, undefined, wanted);
  return <p>{f.state}:{f.state === "ready" ? String(f.value.counts?.waters) : ""}</p>;
}

const counts: BundleCounts = { waters: 19699, reaches: 6967, surveyed: null, stations: 2324 };

describe("useDataFacts: read on demand, then kept", () => {
  it("asks nothing until wanted, once when wanted, and keeps the answer when no longer wanted", async () => {
    let asked = 0;
    const source = { counts: async () => { asked++; return counts; } } as unknown as WaterSource;
    const { container, rerender } = render(<Probe source={source} wanted={false} />);
    await settle();
    expect(asked).toBe(0);
    rerender(<Probe source={source} wanted />);
    await settle();
    expect(asked).toBe(1);
    expect(container.textContent).toBe("ready:19699");
    rerender(<Probe source={source} wanted={false} />);
    await settle();
    rerender(<Probe source={source} wanted />);
    await settle();
    expect(asked).toBe(1);
    expect(container.textContent).toBe("ready:19699");
  });
});

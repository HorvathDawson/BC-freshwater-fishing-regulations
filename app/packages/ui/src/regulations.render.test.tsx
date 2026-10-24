/** P11: opening a water asks the source for its regulations (the per-region file on the release); no water, no ask. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { ItemId, WaterSource } from "@app/data";
import { useRegulationKeys } from "./hooks";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });

function Probe({ source, item }: { source: WaterSource; item: ItemId | null }) {
  const got = useRegulationKeys(source, item);
  return <p>{got.state}:{got.state === "ready" ? got.value.join(",") : ""}</p>;
}

const source = (asked: string[]) =>
  ({ regulationKeys: async (id: ItemId) => { asked.push(id); return [7]; } }) as unknown as WaterSource;

describe("useRegulationKeys", () => {
  it("asks once for the water that is open, and holds its keys", async () => {
    const asked: string[] = [];
    const { container } = render(<Probe source={source(asked)} item={"gnis:3062" as ItemId} />);
    await settle();
    expect(asked).toEqual(["gnis:3062"]);
    expect(container.textContent).toBe("ready:7");
  });
  it("asks nothing while no water is open", async () => {
    const asked: string[] = [];
    render(<Probe source={source(asked)} item={null} />);
    await settle();
    expect(asked).toEqual([]);
  });
});

/**
 * T01: the water screen takes what the Regulations page will read — the release loader, the one day and its setter, the
 * section tapped and the part — and, until T11/T23 draw from them, draws exactly what it drew without them and opens
 * NOTHING through the loader (a loader whose every method throws proves it).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);

/** Every call is recorded and refused: the screen may hold the loader, never use it yet. */
function untouchable(asked: string[]): ReleaseLoader {
  return new Proxy({} as ReleaseLoader, {
    get: (_t, k) => {
      if (k === "then") return undefined;                 // not a thenable
      asked.push(String(k));
      return () => { throw new Error(`the water screen used loader.${String(k)} before T11`); };
    },
  });
}

const text = (c: HTMLElement) => c.textContent;

describe("<WaterScreen> with the T01 props", () => {
  it("draws the same sheet with and without them, and asks the loader nothing", async () => {
    const plain = render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} />);
    await waitFor(() => expect(screen.getByText("Chilliwack River")).toBeTruthy());
    const without = text(plain.container);
    cleanup();

    const asked: string[] = [];
    const days: (Date | null)[] = [];
    const withProps = render(
      <WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                   loader={untouchable(asked)} day={new Date(2026, 9, 10)} onDay={(d) => days.push(d)}
                   section={7 as SectionId} part={0} />);
    await waitFor(() => expect(screen.getByText("Chilliwack River")).toBeTruthy());
    expect(screen.getByRole("heading", { name: "Regulations are coming" })).toBeTruthy();
    expect(text(withProps.container)).toBe(without);
    expect(asked).toEqual([]);
    expect(days).toEqual([]);
  });
});

/**
 * (T420, T476) The map lab's rows in a development or staging build: a switch flips one thing and the foot names it; in
 * production the switches read today's map whatever was set.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { mapChrome } from "./chrome";

afterEach(() => { cleanup(); vi.unstubAllEnvs(); vi.resetModules(); });

async function labBuild(env: Record<string, string>) {
  for (const [k, v] of Object.entries(env)) vi.stubEnv(k, v);
  vi.resetModules();
  return { lab: await import("./lab"), card: await import("./MapLab") };
}

/** react-native-web draws a Switch as a checkbox inside the testID'd view */
const flip = (key: string) => fireEvent.click(screen.getByTestId(`map-lab-${key}`).querySelector("input")!);

describe("the map lab card (T420)", () => {
  it("flips one switch and says the map differs from today's until it is flipped back", async () => {
    const { lab, card: { MapLab } } = await labBuild({ EXPO_PUBLIC_APP_ENV: "staging" });
    expect(lab.LAB_ON).toBe(true);
    render(<MapLab chrome={mapChrome("light")} />);
    expect(screen.getByTestId("map-lab-state").textContent).toBe("Today's map.");
    act(() => { flip("labels"); });
    expect(screen.getByTestId("map-lab-state").textContent)
      .toBe("Differs from today: Labels. Resets when the app restarts.");
    act(() => { flip("labels"); });
    expect(screen.getByTestId("map-lab-state").textContent).toBe("Today's map.");
  });

  it("is on in a development or staging build with no flag, and off in production whatever was set", async () => {
    expect((await labBuild({ EXPO_PUBLIC_APP_ENV: "staging" })).lab.LAB_ON).toBe(true);
    vi.unstubAllEnvs();
    expect((await labBuild({})).lab.LAB_ON).toBe(true);
    vi.unstubAllEnvs();
    const { lab } = await labBuild({ EXPO_PUBLIC_MAP_LAB: "1", EXPO_PUBLIC_APP_ENV: "production" });
    expect(lab.LAB_ON).toBe(false);
    lab.setLab({ labels: false });
    const { useLab, LAB_DEFAULT } = lab;
    let seen: unknown;
    function Probe() { seen = useLab(); return null; }
    render(<Probe />);
    expect(seen).toBe(LAB_DEFAULT);
  });
});

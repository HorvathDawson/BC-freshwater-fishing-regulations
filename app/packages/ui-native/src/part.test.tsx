/**
 * (T282) A part whose code cannot load is SHOWN and REPORTED, never left blank (owner: no silent fallbacks). The web's
 * lazy chunk failed as "Requiring unknown module" (Metro's importAll hands back nothing) and the part drew nothing.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text } from "react-native";

const reported: unknown[] = [];
vi.mock("@app/ui", () => ({ reportFailure: (e: unknown) => { reported.push(e); } }));

import { PART_FAILED } from "./part";

/** each test its own module: `preloadParts` walks every part made in the module */
async function fresh() { vi.resetModules(); return import("./part"); }

afterEach(() => { cleanup(); reported.length = 0; });

const Hello = ({ who }: { who: string }) => <Text>hello {who}</Text>;
const flush = () => act(async () => { await new Promise((r) => setTimeout(r, 0)); });

describe("part()", () => {
  it("a chunk that fails: the error line with Try again in its place, and reported; Try again loads it", async () => {
    const { part } = await fresh();
    let fail = true;
    const P = part(() => (fail ? Promise.reject(new Error("Requiring unknown module \"1057\"")) : Promise.resolve(Hello)));
    render(<P who="you" />);
    await flush();
    expect(screen.getByTestId("part-failed").textContent).toContain(PART_FAILED);
    expect(reported.map((e) => (e as Error).message)).toEqual(["Requiring unknown module \"1057\""]);
    fail = false;
    fireEvent.click(screen.getByTestId("part-retry"));
    await flush();
    expect(screen.queryByTestId("part-failed")).toBeNull();
    expect(screen.getByText("hello you")).toBeTruthy();
  });

  it("a chunk that hands back nothing (Metro's importAll after an unknown module) is a failure, not a blank part", async () => {
    const { part } = await fresh();
    const P = part(() => Promise.resolve(undefined as unknown as typeof Hello));
    render(<P who="you" />);
    await flush();
    expect(screen.getByTestId("part-failed")).toBeTruthy();
    expect(reported).toHaveLength(1);
  });

  it("an idle preload that fails is reported once, and the walk stops (every part is in the one chunk)", async () => {
    const { part, preloadParts } = await fresh();
    const second = vi.fn(() => Promise.reject(new Error("chunk 404")));
    part(() => Promise.reject(new Error("chunk 404")));
    part(second);
    await preloadParts();
    expect(reported.map((e) => (e as Error).message)).toEqual(["chunk 404"]);
    expect(second).not.toHaveBeenCalled();
  });

  it("a closed sheet that failed draws nothing until it is opened", async () => {
    const { part } = await fresh();
    const P = part<{ open: boolean }>(() => Promise.reject(new Error("x")), (p) => p.open);
    const { rerender } = render(<P open={false} />);
    await flush();
    expect(screen.queryByTestId("part-failed")).toBeNull();
    rerender(<P open />);
    await flush();
    expect(screen.getByTestId("part-failed")).toBeTruthy();
  });
});

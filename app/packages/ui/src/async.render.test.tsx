/**
 * Mounted, because the bug this file exists to prevent is invisible to a unit test.
 *
 * `useAsync` had `run` in its dependency array. Every caller passes an inline arrow, so
 * the effect re-ran every render and set state, forever. 122 tests passed over it; the
 * first browser to load the app printed "Maximum update depth exceeded" and spun.
 *
 * `.render.test.tsx` rather than `.test.ts` so vitest puts it in the jsdom project — the
 * rest of @app/ui is arithmetic and belongs in node.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { useState } from "react";
import { useAsync } from "./async";

afterEach(cleanup);

/**
 * Counts renders and lets a test force one without changing the query.
 *
 * The arrow handed to `useAsync` is built INSIDE the component, which is what every real
 * caller does — `useWater` and friends close over their arguments on every render.
 * An earlier version of this probe took the arrow as a prop, so its identity only changed
 * when the parent re-rendered, and the test passed against the very bug it was written
 * for. A probe that does not reproduce the caller's shape proves nothing.
 */
function Probe({ impl, queryKey, enabled = true, onRender }: {
  impl: () => Promise<string>; queryKey: string; enabled?: boolean; onRender: () => void;
}) {
  const out = useAsync(() => impl(), queryKey, enabled);
  const [, bump] = useState(0);
  onRender();
  return (
    <button onClick={() => bump((n) => n + 1)}>
      {out.state}:{out.value ?? out.error?.message ?? ""}
    </button>
  );
}

const settle = () => act(async () => { await Promise.resolve(); await Promise.resolve(); });

describe("useAsync", () => {
  it("runs the query once, not once per render", async () => {
    const run = vi.fn(async () => "one");
    let renders = 0;
    const { getByRole } = render(
      <Probe impl={run} queryKey="a" onRender={() => { renders++; }} />,
    );
    await settle();
    expect(run).toHaveBeenCalledTimes(1);
    expect(getByRole("button").textContent).toBe("ready:one");

    // force re-renders that change nothing about the query
    const before = renders;
    await act(async () => { getByRole("button").click(); });
    await act(async () => { getByRole("button").click(); });
    expect(run).toHaveBeenCalledTimes(1);
    // and each click settles, rather than triggering a render storm
    expect(renders - before).toBeLessThan(6);
  });

  it("re-runs when the key changes, because that is what the key is for", async () => {
    const run = vi.fn(async (k: string) => k);
    const { rerender, getByRole } = render(
      <Probe impl={() => run("a")} queryKey="a" onRender={() => {}} />,
    );
    await settle();
    rerender(<Probe impl={() => run("b")} queryKey="b" onRender={() => {}} />);
    await settle();
    expect(run).toHaveBeenCalledTimes(2);
    expect(getByRole("button").textContent).toBe("ready:b");
  });

  it("calls the newest closure, not the one captured when the key changed", async () => {
    // The ref exists so that dropping `run` from the deps does not staple the hook to a
    // stale closure — which would be a quieter bug than the loop it replaced.
    const seen: string[] = [];
    const { rerender } = render(
      <Probe impl={async () => { seen.push("first"); return "x"; }} queryKey="a"
             onRender={() => {}} />,
    );
    rerender(<Probe impl={async () => { seen.push("second"); return "x"; }} queryKey="b"
                    onRender={() => {}} />);
    await settle();
    expect(seen.at(-1)).toBe("second");
  });

  it("does not run at all while disabled", async () => {
    const run = vi.fn(async () => "no");
    const { getByRole } = render(
      <Probe impl={run} queryKey="a" enabled={false} onRender={() => {}} />,
    );
    await settle();
    expect(run).not.toHaveBeenCalled();
    expect(getByRole("button").textContent).toBe("loading:");
  });

  it("reports a failure as a state, not as an empty result", async () => {
    const { getByRole } = render(
      <Probe impl={async () => { throw new Error("feed down"); }} queryKey="a"
             onRender={() => {}} />,
    );
    await settle();
    expect(getByRole("button").textContent).toBe("failed:feed down");
  });
});

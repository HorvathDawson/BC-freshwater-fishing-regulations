/**
 * T45 — `useLayoutClass()` mounted: it reads the window width the app shell provides (`LayoutWidthContext`, from
 * `useWindowDimensions` in apps/mobile), never the window itself, and follows a resize.
 */
import { createElement as h, useState } from "react";
import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { LayoutWidthContext, useLayoutClass } from "./index";

afterEach(cleanup);

function Probe() { return h("span", { "data-testid": "probe" }, useLayoutClass()); }

describe("useLayoutClass (T45)", () => {
  it("no provider: phone (every test that mounts a screen alone draws the phone design)", () => {
    render(h(Probe));
    expect(screen.getByTestId("probe").textContent).toBe("phone");
  });

  it("follows the width the shell provides, through a resize", () => {
    let set: (w: number) => void = () => {};
    function Shell() {
      const [w, setW] = useState(390);
      set = setW;
      return h(LayoutWidthContext.Provider, { value: w }, h(Probe));
    }
    render(h(Shell));
    const seen = [screen.getByTestId("probe").textContent];
    for (const w of [1024, 1440, 899]) {
      act(() => set(w));
      seen.push(screen.getByTestId("probe").textContent);
    }
    expect(seen).toEqual(["phone", "tablet", "desktop", "phone"]);
  });
});

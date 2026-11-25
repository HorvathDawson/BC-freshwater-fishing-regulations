/**
 * (T1083 review, W46) A START LINK THE WEB REFUSES IS REPORTED, not only written to the console: production 0.9.0 refused
 * every `?regs=wsc:…` link with a console line nobody saw. The map opens (null), and the refusal goes to the crash
 * reporter once, after the module has run (the reporter is installed after the start link is read).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { setFailureSink } from "@app/core";

vi.mock("@app/ui-native", () => ({ preloadParts: () => Promise.resolve() }));
const { startLink } = await import("./startLink.web");

const at = (q: string) => window.history.replaceState(null, "", `/${q}`);
afterEach(() => { at(""); vi.restoreAllMocks(); });

describe("(T1083) the web start link's refusal is reported", () => {
  it("a refused link opens the map, says it on the console and reports it once", async () => {
    const reported: unknown[] = [];
    const off = setFailureSink((e) => reported.push(e));
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      at("?regs=gnis:3062abc");
      expect(startLink()).toBeNull();
      at("?regs=Vedder");
      expect(startLink()).toBeNull();
      await Promise.resolve();
      expect(err.mock.calls.map((c) => String(c[0]))).toEqual([
        "start link: regs=gnis:3062abc is not an item id", "start link: regs=Vedder is not an item id"]);
      expect(reported.map(String)).toEqual(["Error: start link: regs=gnis:3062abc is not an item id"]);
    } finally { off(); }
  });

  it("a `wsc:` water's link opens that water, nothing reported", async () => {
    const reported: unknown[] = [];
    const off = setFailureSink((e) => reported.push(e));
    try {
      at("?regs=wsc:100-019698-831000");
      expect(startLink()?.arrive).toBe("wsc:100-019698-831000");
      await Promise.resolve();
      expect(reported).toEqual([]);
    } finally { off(); }
  });
});

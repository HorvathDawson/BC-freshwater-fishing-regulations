/**
 * T01, reconciled with README §0 / D22 / IMPLEMENT-HANDOFF §8 C2: the Regulations page reads the RELEASE's files through
 * the loader, so there is NO `/regs.sqlite` route and no regs URL in the app — the dev server serves the release
 * (`/release/…`) and nothing else carries regulations. Read as text: serve-tiles.mjs starts a server on import.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const read = (p: string) => readFileSync(new URL(p, import.meta.url), "utf8");

describe("no regs store of its own (T01)", () => {
  it("the dev server has no /regs.sqlite route and no REGS_STORE override; it serves the release", () => {
    const s = read("./serve-tiles.mjs");
    expect(s.includes("/regs.sqlite")).toBe(false);
    expect(s.includes("REGS_STORE")).toBe(false);
    expect(s.includes("/release/")).toBe(true);
  });

  it("the app names no regs.sqlite and hands the water screen the release loader", () => {
    const app = read("../apps/mobile/src/App.tsx"), cfg = read("../apps/mobile/src/config.ts");
    expect(/regs\.sqlite/.test(app) || /regs\.sqlite/.test(cfg)).toBe(false);
    expect(app.includes("loader: b.loader")).toBe(true);
    expect(app.includes("loader={data.loader}")).toBe(true);
  });
});

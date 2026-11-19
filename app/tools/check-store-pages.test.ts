/** T901: the store-page check's verdicts (the curl itself is run by hand: `pnpm run store-pages`). */
import { describe, expect, it } from "vitest";
// @ts-expect-error plain .mjs tool
import { judge } from "./check-store-pages.mjs";

const row = (flag: boolean, status: number) => ({ name: "Google Play", flag, status, url: "u" });
describe("check-store-pages judge", () => {
  it("a true flag over a 404 fails", () => expect(judge([row(true, 404)]).failures).toHaveLength(1));
  it("a false flag over a live page is reported, not failed", () => {
    const j = judge([row(false, 200)]); expect(j.failures).toEqual([]); expect(j.flips).toHaveLength(1);
  });
  it("matching states are quiet", () => {
    for (const r of [row(true, 200), row(false, 404)]) expect(judge([r])).toEqual({ failures: [], flips: [] });
  });
});

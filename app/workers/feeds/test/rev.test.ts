// (owner 2026-11-05) A DEPLOY RECOMPUTES THE FEEDS: the deployed version is part of every job's reuse key, so new output
// never waits on a hand-bumped GAUGE_REV / DFO_REV.
import { afterEach, describe, expect, it } from "vitest";
import { rev, setDeployId } from "../src/io";

afterEach(() => setDeployId(undefined));

describe("rev", () => {
  it("is the base alone with no deployed version (tests, wrangler dev)", () => {
    expect(rev("2026-10-28.1")).toBe("2026-10-28.1");
  });
  it("changes with the deployed version, so the last run's key no longer matches", () => {
    setDeployId("v1");
    const before = rev("2026-10-28.1");
    setDeployId("v2");
    expect(rev("2026-10-28.1")).not.toBe(before);
    expect(rev("2026-10-28.1")).toBe("2026-10-28.1@v2");
  });
});

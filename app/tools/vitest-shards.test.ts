/**
 * (T866 S1) tools/vitest-shards.mjs, the check that CI's vitest shards run every app test file exactly once. The live
 * check (vitest's own sharding over the real file list) runs in each app-ci leg; here, each fault it exists for is
 * planted and must be named.
 */
import { describe, expect, it } from "vitest";
import { partitionProblems, ranProblems } from "./vitest-shards.mjs";

const ALL = ["logic:a.test.ts", "logic:b.test.ts", "render:c.test.tsx", "render:d.test.tsx"];

describe("vitest-shards: the slices are a partition of the unsharded list", () => {
  it("each file in exactly one shard passes", () => {
    expect(partitionProblems(ALL, [["logic:a.test.ts", "render:d.test.tsx"], ["logic:b.test.ts", "render:c.test.tsx"]])).toEqual([]);
  });

  it("MUTATION: a file no shard holds, a file two hold, a file not in the run, an empty shard: each is named", () => {
    expect(partitionProblems(ALL, [["logic:a.test.ts", "logic:b.test.ts"], ["render:c.test.tsx"]]))
      .toEqual(["runs in no shard: render:d.test.tsx"]);
    expect(partitionProblems(ALL, [["logic:a.test.ts", "logic:b.test.ts", "render:c.test.tsx"], ["render:c.test.tsx", "render:d.test.tsx"]]))
      .toEqual(["runs in shards 1, 2: render:c.test.tsx"]);
    expect(partitionProblems(ALL, [[...ALL], ["logic:z.test.ts"]]))
      .toEqual(["in a shard but not in the unsharded run: logic:z.test.ts"]);
    expect(partitionProblems(ALL, [[...ALL], []])).toEqual(["shard 2/2 is empty"]);
    // the same file in another project is another key (a file in both projects runs twice, once in each)
    expect(partitionProblems(ALL, [["logic:a.test.ts", "logic:b.test.ts", "logic:c.test.tsx"], ["render:d.test.tsx"]]))
      .toEqual(["runs in no shard: render:c.test.tsx", "in a shard but not in the unsharded run: logic:c.test.tsx"]);
  });
});

describe("vitest-shards: a leg ran its slice", () => {
  it("the same files pass, in any order", () => {
    expect(ranProblems(["a.test.ts", "c.test.tsx"], ["c.test.tsx", "a.test.ts"])).toEqual([]);
  });

  it("MUTATION: a file the leg holds but did not report, and one it ran but does not hold, are named", () => {
    expect(ranProblems(["a.test.ts", "c.test.tsx"], ["a.test.ts", "b.test.ts"])).toEqual([
      "the shard holds it but the run did not report it: c.test.tsx",
      "the run reported it but the shard does not hold it: b.test.ts"]);
  });
});

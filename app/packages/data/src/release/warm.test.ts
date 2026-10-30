/** (T287) An applied update is decoded like the set on screen before it is committed. */
import { describe, expect, it } from "vitest";
import { warmLike, type ReleaseLoader } from "./loader";

const fake = (opened: string[], asked: string[] = []) => ({
  opened: () => opened,
  paint: async () => { asked.push("paint"); },
  method: async () => { asked.push("method"); },
  salmon: async (r?: string[]) => { asked.push(`salmon:${(r ?? ["*"]).join(",")}`); },
  lakes: async () => { asked.push("lakes"); },
  access: async () => { throw new Error("gone"); },
  flowArea: async () => { asked.push("flow-area"); },
}) as unknown as ReleaseLoader;

describe("warmLike (T287)", () => {
  it("decodes on the new set what the old one had: paint, method, the salmon slices it read; a failure is left", async () => {
    const asked: string[] = [];
    await warmLike(fake([], asked), fake(["paint", "core", "method", "salmon", "salmon-6", "access"]));
    expect(asked).toEqual(["paint", "method", "salmon:6"]);
  });
  it("nothing more than paint for a set that read nothing else", async () => {
    const asked: string[] = [];
    await warmLike(fake([], asked), fake(["paint"]));
    expect(asked).toEqual(["paint"]);
  });
});

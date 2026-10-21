/** (T34, D6 a) where a "Rule book p.N ↗" link goes: web honours `#page=<pdf>`, native opens the synopsis PDF itself. */
import { describe, expect, it } from "vitest";
import { bookUrl, SYNOPSIS_PDF } from "./book";

describe("bookUrl", () => {
  it("web: the synopsis at its PDF page (the default)", () => {
    expect([bookUrl(23), bookUrl(23, "web")]).toEqual([`${SYNOPSIS_PDF}#page=23`, `${SYNOPSIS_PDF}#page=23`]);
  });
  it("native (D6 a): the bare PDF URL — the link's label already names the printed page", () => {
    expect([bookUrl(23, "ios"), bookUrl(23, "android")]).toEqual([SYNOPSIS_PDF, SYNOPSIS_PDF]);
  });
});

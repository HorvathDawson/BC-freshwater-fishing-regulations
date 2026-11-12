/** (T670 G5, owner 2026-11-12) the Licence tab is hidden only on a page fully closed: game fish AND salmon. */
import { describe, expect, it } from "vitest";
import { licenceTabFor } from "./page";
import type { LicenceVM } from "./model";

const own = { tag: "own" } as unknown as LicenceVM, asOpen = { tag: "as-open" } as unknown as LicenceVM;
const strip = (status: string) => ({ status }) as never;

describe("licenceTabFor", () => {
  it("an open day keeps the page's own licence", () => expect(licenceTabFor({ licence: own }, asOpen, strip("closed"))).toBe(own));
  it("game fish closed, salmon open / release only / unknown: the as-open licence", () => {
    for (const s of ["open", "release", "unknown"]) expect(licenceTabFor({ licence: null }, asOpen, strip(s))).toBe(asOpen);
  });
  it("fully closed (salmon closed, or no salmon strip): no tab", () => {
    expect(licenceTabFor({ licence: null }, asOpen, strip("closed"))).toBeNull();
    expect(licenceTabFor({ licence: null }, asOpen, null)).toBeNull();
  });
  it("no key (outside B.C.): nothing to draw", () => expect(licenceTabFor({ licence: null }, null, strip("open"))).toBeNull());
});

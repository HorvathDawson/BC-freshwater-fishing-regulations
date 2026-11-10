/**
 * (T582, T580 F3) WEB AND PHONE RESTORE THE SAME CONDITIONS DAY. The phone has no address, so a cold start there opens
 * Conditions on Now. The web writes an address and restores from it: if the address named the horizon, a reload would
 * lead with another gauge than the phone for the same water. The rule is one: the address keeps the regulations day, not
 * the horizon; a link that carries a day opens Conditions on today with that day linked.
 *
 * MUTATION PIN: writing the horizon back into the address (the old `routeAddress`) fails the second test.
 */
import { describe, expect, it } from "vitest";
import { addressKept, parseStartLink, routeSearch, type Route } from "./index";

const NOW = new Date(2026, 10, 10, 9);
const ROUTE = { mode: "browse", tab: "map", spot: null, stack: [] } as unknown as Route;

describe("(T582) the kept address", () => {
  it("keeps the regulations day", () => {
    const q = routeSearch(ROUTE, "", null, addressKept({ day: new Date(2026, 10, 14, 12), horizon: 3 }), NOW);
    expect(q).toContain("date=2026-11-14");
  });
  it("does not keep the horizon, so a reload restores what a phone cold start does: Now", () => {
    const q = routeSearch(ROUTE, "", null, addressKept({ day: new Date(2026, 10, 14, 12), horizon: 3 }), NOW);
    expect(q).not.toContain("horizon");
    const reloaded = parseStartLink(q, NOW);
    expect(reloaded?.horizon ?? null).toBeNull();   // the phone's cold start: no start link at all, Now
    expect(reloaded?.day?.getDate()).toBe(14);
  });
});

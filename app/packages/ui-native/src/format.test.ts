/**
 * How the app writes a quantity.
 *
 * These are three lines each and they still earn a test, because the failure they exist to
 * prevent is silent: the Layers sheet wrote "60,648 reaches" and the search header wrote
 * "19699 named waters" on the same build, from two helpers nobody had noticed were two.
 */
import { describe, expect, it } from "vitest";
import { ago, count, plural } from "./format";

describe("count", () => {
  it("groups digits, the way every other figure in the app does", () => {
    expect(count(19699, "named waters")).toBe("19,699 named waters");
    expect(count(60648, "reaches")).toBe("60,648 reaches");
  });

  it("returns undefined for a count we do not have, never a zero", () => {
    // The whole point. `surveyed` is null until the bathymetry matcher lands, and the sheet
    // has to OMIT that row rather than claim there are no surveyed lakes.
    expect(count(undefined, "reaches")).toBeUndefined();
    expect(count(null, "reaches")).toBeUndefined();
  });

  it("passes a real zero through, because that IS an answer", () => {
    expect(count(0, "written rules")).toBe("0 written rules");
  });
});

describe("plural", () => {
  it("picks the form by the number", () => {
    expect(plural(1, "stretch", "stretches")).toBe("1 stretch");
    expect(plural(6, "stretch", "stretches")).toBe("6 stretches");
    expect(plural(0, "stretch", "stretches")).toBe("0 stretches");
  });

  it("groups digits too — the Fraser has 201 of them", () => {
    expect(plural(1201, "stretch", "stretches")).toBe("1,201 stretches");
  });
});

describe("ago", () => {
  const NOW = Date.parse("2026-09-03T18:00:00Z");

  it.each([
    ["2026-09-03T17:40:00Z", "just now"],
    ["2026-09-03T11:00:00Z", "7h ago"],
    ["2026-09-02T17:33:00Z", "24h ago"],
    // Past two days it switches to days, because "72h ago" is arithmetic the reader
    // should not have to do.
    ["2026-08-30T07:20:00Z", "4d ago"],   // 106.7h — the hardcoded stamp App.tsx used to ship
  ])("%s -> %s", (iso, want) => {
    expect(ago(iso, NOW)).toBe(want);
  });

  it("says it does not know rather than inventing an age", () => {
    expect(ago("not a date", NOW)).toBe("unknown age");
  });
});

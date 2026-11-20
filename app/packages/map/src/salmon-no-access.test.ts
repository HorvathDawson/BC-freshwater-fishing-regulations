/**
 * (T937, owner 2026-11-20: "salmon should have the no access land set to grey like trout") NO PUBLIC ACCESS LOOKS THE SAME
 * ON THE SALMON MAP: the same dotted grey layer (`stream-noaccess`, `color.status.no_access`), shown where the STATUS
 * channel labels a section `no_access` — no second test of access — and the keep casing never rings it.
 */
import { describe, expect, it } from "vitest";
import { closedShown, issueShown, markShownIn, noAccessShown, noaccessPaint, paintFor, salmonCasingPaint } from "./paint";
import { NO_VALUES, resolveTheme, type ChannelValues } from "./style";

const t = resolveTheme("light");
const status = { closed: [], own: ["p1"], base: [], no_access: ["p7", "p8"], none: [] } as unknown as ChannelValues["status"];
const salmon = { open: ["s1"], release: [], closed: [], check: [], none: [] } as unknown as ChannelValues["salmon"];
const values: ChannelValues = { ...NO_VALUES, status, salmon };

describe("no access on the Salmon map", () => {
  it("is drawn by the same layer, with the same paint, as Standard's", () => {
    expect(markShownIn("noaccess", "salmon")).toBe(true);
    const a = noaccessPaint("salmon", t, values), b = noaccessPaint("profile", t, values);
    expect(a["line-color"]).toBe(b["line-color"]);
    expect(a["line-dasharray"]).toEqual(b["line-dasharray"]);
    expect(a["line-width"]).toEqual(b["line-width"]);
    expect(a["line-opacity"]).toEqual(b["line-opacity"]);
  });
  it("reads the status labels, and the salmon stream line gives way to the dots there", () => {
    expect(JSON.stringify(noAccessShown("stream", "salmon", values))).toContain("p7");
    expect(JSON.stringify(paintFor("stream", "salmon", t, values)["line-opacity"])).toContain("p7");
  });
  it("no status day: no dots (not asked is not an answer); other modes unchanged", () => {
    expect(noAccessShown("stream", "salmon", NO_VALUES)).toBeNull();
    expect(markShownIn("noaccess", "runs")).toBe(false);
  });
  it("the keep casing is held off the no-access sections", () => {
    const c = salmonCasingPaint("salmon", { ...t, "width.salmon.casing": 1 }, values);
    expect(JSON.stringify(c["line-opacity"])).toContain("p7");
  });
  it("(round 2) lakes take Standard's no-access colour, and no closed or issue stripe sits on a no-access stream", () => {
    for (const l of ["lake", "lake_flowing", "wetland_flowing"]) {
      const p = paintFor(l, "salmon", t, values);
      const fill = JSON.stringify(p["fill-color"] ?? p["line-color"]);
      expect(fill, l).toContain(JSON.stringify(t["color.status.no_access"]));
      expect(fill, l).toContain("p7");
    }
    const sv = { ...values, salmon: { open: [], release: [], closed: ["s2"], check: [], none: [] } } as ChannelValues;
    expect(JSON.stringify(closedShown("stream", "salmon", sv))).toContain("p7");
    expect(JSON.stringify(issueShown("salmon", sv))).toContain("p7");
    expect(JSON.stringify(paintFor("lake", "salmon", t, NO_VALUES))).not.toContain("p7");
  });
});

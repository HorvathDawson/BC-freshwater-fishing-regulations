/**
 * T567 (d) — THE WEB AND THE PHONE PICK THE SAME LEAD GAUGE AND QUANTITY FROM ONE FEED. The owner saw Fraser at Mission
 * lead on the web and Hansard on the phone; the cause was the DAY, not the platform: ahead, Mission has a flow forecast
 * and no ranked level forecast, so the lead flips with the quantity on a forecast day (the web was on Now).
 *
 * Both platforms draw the one `ConditionsPanel` over `useConditionsView` (rule 23), whose lead is `sheetLead` over
 * `answerFrom`'s rows: so one table of (quantity, day) -> lead (owner 2026-11-10: flow picks it, level never does), over one hand-made feed in the owner's shape, is what
 * either would pick, and no platform-specific file may exist to fork it.
 */
import { existsSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { answerFrom, leadOver, speakerFor, type Panel } from "@app/data";

const member = (station: string, role: "up" | "down") =>
  ({ station: station as never, role, areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true });
const PANEL: Panel = { areaKm2: 50, members: [member("08MH024", "down"), member("08KA004", "up")] };
// Mission (08MH024) reports LEVEL only; Hansard (08KA004) both, ranked in flow. Ahead: Mission's CLEVER flow forecast,
// Hansard's level.
const INDEX = {
  listed: ["08MH024", "08KA004"],
  stations: {
    "08MH024": { percentile: 0.66, parameter: "level" as const, level: 1.548, observedAt: "2026-11-09T20:00:00-08:00",
                 ahead: { "3": { percentile: 0.7, discharge: 0.7 } } },
    "08KA004": { percentile: 0.27, parameter: "discharge" as const, discharge: 197, level: 2.18,
                 observedAt: "2026-11-09T20:00:00-08:00", ahead: { "3": { percentile: 0.4, level: 0.4 } } },
  },
};

function leadOf(q: "discharge" | "level", h: 0 | 3): string | null {
  // the quantity asked never picks the lead: the hook reads the flow and level answers, `leadOver` chooses once
  void q;
  const rows = { discharge: answerFrom(PANEL, INDEX as never, "discharge", h).rows,
                 level: answerFrom(PANEL, INDEX as never, "level", h).rows };
  return leadOver(speakerFor(PANEL, undefined), rows, null).lead;
}

describe("the lead gauge of Fraser at Mission, from one feed", () => {
  it("is the same function of (quantity, day) wherever it is asked", () => {
    expect({ flowNow: leadOf("discharge", 0), levelNow: leadOf("level", 0),
             flowDay3: leadOf("discharge", 3), levelDay3: leadOf("level", 3) })
      .toEqual({ flowNow: "08KA004", levelNow: "08KA004", flowDay3: "08MH024", levelDay3: "08MH024" });
  });

  it("falls to the level ranking only where no gauge answers in flow", () => {
    const levelOnly = { listed: ["08MH024"], stations: { "08MH024": INDEX.stations["08MH024"] } };
    const p: Panel = { areaKm2: 50, members: [member("08MH024", "down")] };
    const rows = { discharge: answerFrom(p, levelOnly as never, "discharge", 0).rows,
                   level: answerFrom(p, levelOnly as never, "level", 0).rows };
    expect(leadOver(speakerFor(p, undefined), rows, null)).toEqual({ lead: "08MH024", levelOnly: true });
    const both = { discharge: answerFrom(PANEL, INDEX as never, "discharge", 0).rows,
                   level: answerFrom(PANEL, INDEX as never, "level", 0).rows };
    expect(leadOver(speakerFor(PANEL, undefined), both, null).levelOnly).toBe(false);
  });

  it("has one code path: no platform file forks the panel or its view", () => {
    const pk = join(__dirname, "../../packages");
    for (const f of ["ui-native/src/ConditionsPanel.web.tsx", "ui-native/src/ConditionsPanel.native.tsx",
                     "ui/src/panel/conditions.web.ts", "ui/src/panel/conditions.native.ts"])
      expect(existsSync(join(pk, f)), f).toBe(false);
  });
});

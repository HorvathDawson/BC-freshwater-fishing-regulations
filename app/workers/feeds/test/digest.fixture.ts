// T627: the days the digest's layout is pinned on and its samples are drawn from. Every event is made by the builder
// the worker itself uses (reportEvent, crashEvent, healthEvents, gaugeEvents + hydatUnfinished, dfoEvents + goneNotice),
// so a fixture cannot say something a real run could not.
import type { FeedEvent } from "../src/events";
import { crashEvent, type CrashKind } from "../src/crashes";
import { reportEvent, type StoredReport } from "../src/reports";
import { dfoEvents, gaugeEvents, healthEvents, hydatUnfinished } from "../src/index";
import { goneNotice } from "../src/dfo/live";

export const AT = "2026-11-10T15:05:00.000Z";

const report = (over: Partial<StoredReport> & { context?: Record<string, unknown> }): StoredReport => ({
  format: "report/2", id: "0123456789abcdef", created: "2026-11-10T02:41:10.000Z", received: "2026-11-10T02:41:12.000Z",
  reason: "rule", text: "The closure ends at the bridge.", email: null, contact: false,
  ...over,
  context: { app: "0.5.0 (27)", env: "staging", platform: "android 35", release: "r-0fa7927b16e5", ...(over.context ?? {}) },
} as StoredReport);

const kind = (hash: string, sample: Record<string, unknown>, over: Partial<CrashKind> = {}): CrashKind => ({
  format: "crash-kind/1", hash, first: "2026-11-09T22:14:03.000Z", last: "2026-11-10T03:20:00.000Z", count: 1,
  versions: { "0.5.0+27": 1 },
  ...over,
  sample: { created: "2026-11-09T22:14:00.000Z", kind: "js", fatal: false, route: "water", app: "0.5.0+27", env: "staging",
            platform: "android 35", release: "r-0fa7927b16e5", name: "TypeError", message: "x is undefined",
            stack: "TypeError: x is undefined\n    at render (address at index.android.bundle:1:123456)", ...sample },
} as CrashKind);

export const reports = {
  short: reportEvent(report({
    id: "5d1c0a9e77b3f210", reason: "rule", text: "The spring closure shows as open on Nov 9. The book says closed Nov 1 to Apr 30.",
    email: "angler@example.org", contact: true, context: { place: "Vedder River", water: "gnis:3062", day: "Mon, Nov 9", mode: "regs" } }), AT),
  long: reportEvent(report({
    id: "a41f6c2d9e0b7735", received: "2026-11-10T05:07:44.000Z", reason: "bug",
    text: "I opened Conditions for the Cowichan and the flow chart was blank for about a minute, then it showed last week's line.\n\n" +
      "After I pulled down to refresh it came back, but the level under it still said Nov 3. It happened twice tonight, both times after " +
      "switching from the map with the forecast layer on. My phone was on wifi the whole time and other apps were loading fine. " +
      "The gauge is the one at Duncan.",
    context: { place: "Cowichan River", water: "gnis:1392", day: "Today, Nov 9", mode: "flow", station: "08HA011", quantity: "flow", span: "30 d",
               feed: "gauges/3", layer: "flow", zoom: 11, lat: 48.78, lon: -123.71, install: "9f3b2a6c1d0e4f57" } }), AT),
  nowhere: reportEvent(report({ id: "c0ffee1234567890", received: "2026-11-10T13:55:02.000Z", reason: "feature",
                                text: "Could saved spots be sorted by distance?", context: { platform: "ios 18.1" } }), AT),
};

export const errors = {
  sid: crashEvent(kind("3f9a1c7b2d5e8a40", { name: "TypeError", message: "Cannot read properties of undefined (reading 'sid')", route: "water" },
                       { count: 14, versions: { "0.5.0+27": 12, "0.5.0+26": 2 }, installs: ["a".repeat(16), "b".repeat(16), "c".repeat(16)] }), AT),
  range: crashEvent(kind("77b0e2d4c6a81f39", { name: "RangeError", message: "Invalid time value", route: "conditions" }, { count: 2 }), AT),
  native: crashEvent(kind("0c4d8e1f2a3b5c6d", { kind: "native", fatal: true, name: "ApplicationExitInfo", route: "map",
                                                message: "REASON_CRASH_NATIVE: signal 11 (SIGSEGV), code 1 (SEGV_MAPERR) in libmaplibre.so while the map was drawing the stream layer at zoom 12 with the forecast layer on" },
                          { count: 1, first: "2026-11-10T04:02:51.000Z" }), AT),
  render: crashEvent(kind("9e8d7c6b5a4f3e2d", { name: "Error", message: "Rendered fewer hooks than expected", route: "regs" }, { count: 5 }), AT),
};
/** the two count lines `crashEvents` makes: kinds that are not defects, and (T627) known kinds seen again */
export const notDefects: FeedEvent = {
  key: "crash-other:2026-11-10", kind: "crash", at: AT, title: "6 new error kinds that are not defects (2 expected, 4 network)",
  text: "6 error kinds first seen today or yesterday are not defects: 2 expected, 4 network. List them: GET /crashes?class=network,expected on the feeds worker (Bearer RUN_TOKEN).",
};
export const repeats: FeedEvent = {
  key: "crash-repeats:2026-11-09", kind: "crash", fine: true, at: AT, title: "3 known error kinds were seen again on 2026-11-09",
  text: "3 error kinds already told were reported again on 2026-11-09 (UTC). List them, newest first: GET /crashes on the feeds worker (Bearer RUN_TOKEN).",
  data: { repeats: 3, day: "2026-11-09" },
};

export const gaugesFailing = healthEvents([{ job: "gauges", from: "ok", to: "failing", text: "Gauge feed FAILED 2 run(s) in a row: Error: no station answered (asked 438)" }], AT);
export const dfoFailing = healthEvents([{ job: "dfo", from: "ok", to: "failing", text: "DFO salmon feed FAILED 3 run(s) in a row: Error: HTTP 503 from https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region1-eng.html" }], AT);
export const inseasonRecovered = healthEvents([{ job: "inseason", from: "failing", to: "ok", text: "In-season check OK again after 2 failed run(s)" }], AT);

/** a HYDAT notice told again: the workflow the worker started twice did not put the new envelope on R2 */
export const hydatRetold = (() => {
  const [e] = gaugeEvents([{ kind: "hydat", since: "2026-11-08T09:00:00Z", current: "20250715", latest: "20261015", action: "scripts/update-hydat.sh" }], AT);
  const dis = { state: "started" as const, at: "2026-11-09T15:10:00.000Z", first: "2026-11-08T09:05:00.000Z", workflow: "feeds-hydat.yml",
                ref: "redesign/stream-sections", starts: 2, overdue: true };
  e!.data = { ...e!.data, dispatch: dis };
  hydatUnfinished(e!, dis, AT);
  return e!;
})();

/** a curated DFO place that left DFO's page (urgent by itself; here it waited for the digest after its own email failed) */
export const dfoGone = dfoEvents([goneNotice("1", { location_id: "1:nanaimo-river:cedar-bridge-down", water: "Nanaimo River",
  place: "in Subarea 17-14 (tidal portions of the Nanaimo River) from Cedar Bridge downstream", page: "b06fab380224" })], AT);

/** T627 item 5: two reports, three error kinds, one failed run */
export const fixtureDay: FeedEvent[] = [reports.short, reports.long, errors.sid, errors.range, errors.native, ...gaugesFailing];
/** a quiet day: one report, a feed that came back */
export const quietDay: FeedEvent[] = [reports.short, ...inseasonRecovered];
/** a bad day: a failed job, a DFO place gone, HYDAT told again, several error kinds, repeats, three reports */
export const badDay: FeedEvent[] = [
  ...dfoFailing, ...gaugesFailing, ...inseasonRecovered, ...dfoGone, hydatRetold,
  reports.short, reports.long, reports.nowhere, errors.sid, errors.range, errors.native, errors.render, notDefects, repeats,
];

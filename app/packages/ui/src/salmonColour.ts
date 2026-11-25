/**
 * (T211) THE STANDARD / SALMON TOGGLE AND THE SALMON COLOURING (owner 2026-10-27: in the main map's standard regulation
 * mode, a toggle between Standard and Salmon; Salmon paints the map from DFO's salmon rules, styled differently).
 *
 * FAST BY CONSTRUCTION (owner 2026-10-27: "toggling Standard/Salmon should be instant — no re-fetch, no re-tile"). The
 * release's `salmon` file is read ONCE per source, on the Map tab on a proven pair, after the day's status is in (so it
 * never competes with first paint) and WHATEVER THE TOGGLE SAYS; the feed handed to the map does not depend on the toggle,
 * so the engine labels every tile with its chain as it arrives. A toggle then moves only `mapModes`' modes — one paint
 * change per layer, no fetch, no relabel (`salmonColour.render.test.tsx` and `mapModes.test.ts` pin both).
 *
 * The answer per chain and day was decided at deliver time (`pipeline/deliver/salmon.py`); nothing here reads a rule.
 */
import { useMemo, useState } from "react";
import { bookHalves, feedFinfishOn, salmonIssueItems, type MethodFacts, type SalmonFacts } from "@app/core";
import type { SalmonFeedSource, WaterSource } from "@app/data";
import { useAsync } from "./async";
import { useFeedVersion } from "./feedVersion";
import { sameFacts, sourceKey } from "./conditionsColour";
import { wallIso } from "@app/core";
import { useBcToday } from "./today";

/** Which regulations the Map tab paints: the provincial day's status (bait ban / fly only marked on it, T213), or DFO's
 *  salmon. (T261) Run timing left this switch for its own tab, Runs. */
export type RegsLayer = "standard" | "salmon";

/** What the map is handed (`MapProps.salmon`): the release's facts and the day. */
export interface SalmonFeedVM {
  readonly facts: SalmonFacts;
  readonly day: Date;
}

/**
 * The release's DFO salmon on the Map tab, on a proven pair, once the day's status is in. `day` the map's day (null
 * today). The SAME object while the facts and the day are — the toggle is not an input. (T182) `settled` false holds the
 * read back: the web passes it until its idle prefetch has started (`useReleasePrefetch`'s `onIdle`), so the file is
 * fetched after first paint, never with it; a phone (its release installed) leaves it out.
 *
 * (T182, owner 2026-10-28) `onDemand` (the web): the file is read only once the reader first chooses Salmon (`wanted`) —
 * not on every visit; `loading` is true while that first read is on its way, and `offered` (the toggle is drawn) holds
 * from the start, until a read says the release ships no salmon. Without it (a phone) the file is read up front and
 * `offered` waits for it. Once read it is kept either way, so later toggles are instant.
 */
export function useSalmonColour(i: { source: WaterSource; paired: boolean; statusOk: boolean; mapTab: boolean;
                                     day: Date | null; settled?: boolean; onDemand?: boolean; wanted?: boolean }):
    { feed: SalmonFeedVM | null; facts: SalmonFacts | null; loading: boolean; offered: boolean } {
  const on = i.paired && i.statusOk && i.mapTab && i.settled !== false && (!i.onDemand || i.wanted === true);
  // asked once per source and kept: leaving the Map tab does not forget it (a return is instant too)
  const [asked, setAsked] = useState<string | null>(null);
  const key = `salmonfacts:${sourceKey(i.source)}`;
  if (on && asked !== key) setAsked(key);
  // (owner 2026-10-29) a DFO feed the update service took recolours the map in place (the loader's salmon is keyed on
  // the feed's version, `withLiveSalmon`): a refresh, the last colours stay meanwhile
  const dfo = useFeedVersion("dfo");
  // (T287) and an applied update in the same section numbering is a refresh too (`sameFacts`)
  const facts = useAsync(() => i.source.salmonFacts(), `${key}#${dfo}`, asked === key, sameFacts("salmonfacts", i.source) ?? key);
  const f = facts.state === "ready" ? facts.value : null;
  // (T551) no day picked = B.C.'s today, which rolls over at midnight (`useBcToday`), so the key carries it
  const today = useBcToday();
  const dayKey = wallIso(i.day ?? today);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- the day's identity is its date, not the Date object
  const day = useMemo(() => i.day ?? today, [dayKey]);
  const feed = useMemo(() => (f && i.paired ? { facts: f, day } : null), [f, day, i.paired]);
  const loading = asked === key && facts.state === "loading";
  const offered = i.onDemand ? i.paired && facts.state !== "failed" && !(facts.state === "ready" && f === null)
    : feed !== null;
  return { feed, facts: f, loading, offered };
}

/**
 * (T796 A6, owner 2026-11-16: "Closes the whole page") THE WEB'S CUE TO READ THE SALMON FILES FOR THE STANDARD MAP. A DFO
 * "Finfish closure" closes all fishing, so the Standard map paints its stretch closed (map `statusInput`) — which needs
 * the salmon chains on the tiles, and the web reads the salmon files only on the first press of Salmon (`onDemand`). They
 * are read for Standard too (`useSalmonColour`'s `wanted`) only when this is true: the live DFO feed the app already
 * holds (`SalmonFeedSource.latest()`, the same capture the bell and the More tab's Data line read — no request of its
 * own) closes some place to all fishing on the map's day (@app/core `feedFinfishOn`). A day with none reads nothing
 * more. `latest()` never rejects (a failed fetch is reported there and the last capture answers); no feed ever read is
 * no cue — the release's own build day is the status index's to answer.
 */
export function useFinfishCue(i: { feed: SalmonFeedSource | null | undefined; day: Date | null; enabled: boolean }): boolean {
  const dfo = useFeedVersion("dfo");   // a feed the update service took is asked again
  const on = i.enabled && !!i.feed;
  const a = useAsync(() => i.feed!.latest(), `finfish-cue:${dfo}:${i.feed?.version?.() ?? 0}`, on);
  const today = useBcToday();
  return on && a.state === "ready" && a.value !== null && feedFinfishOn(a.value, i.day ?? today);
}

/** (T213) What the map is handed (`MapProps.method`): the release's bait ban / fly only profiles and the day — the
 *  Standard map's gear marks. */
export interface MethodFeedVM {
  readonly facts: MethodFacts;
  readonly day: Date;
}

/**
 * (T213) THE GEAR MARKS (bait ban / fly only on the Standard map) — `useSalmonColour`'s shape exactly: the release's
 * `method` file read ONCE per source on the Map tab, on a proven pair, after the day's status is in. Null where the
 * release ships none (cut before T213): the marks and their legend rows are then not drawn.
 */
export function useMethodColour(i: { source: WaterSource; paired: boolean; statusOk: boolean; mapTab: boolean;
                                     day: Date | null; settled?: boolean }): { feed: MethodFeedVM | null } {
  const on = i.paired && i.statusOk && i.mapTab && i.settled !== false && !!i.source.methodFacts;
  const [asked, setAsked] = useState<string | null>(null);
  const key = `methodfacts:${sourceKey(i.source)}`;
  if (on && asked !== key) setAsked(key);
  const facts = useAsync(() => i.source.methodFacts?.() ?? Promise.resolve(null), key, asked === key,
                         sameFacts("methodfacts", i.source));
  const f = facts.state === "ready" ? facts.value : null;
  // (T551) no day picked = B.C.'s today, which rolls over at midnight (`useBcToday`), so the key carries it
  const today = useBcToday();
  const dayKey = wallIso(i.day ?? today);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- the day's identity is its date, not the Date object
  const day = useMemo(() => i.day ?? today, [dayKey]);
  const feed = useMemo(() => (f && i.paired ? { facts: f, day } : null), [f, day, i.paired]);
  return { feed };
}

/**
 * (T759) THE ISSUE STRIPE's feed: the sections of every water the salmon feed carries a shown issue for
 * (`salmonIssueItems`: the rows behind the river page's banner), from the salmon facts the map already holds, so no
 * file is read for it. Handed to the map whatever the toggle says (the toggle is a mode change and nothing else); the
 * map's style draws the stripe in the Salmon view alone (`paint.ts` `issuePaint`). Null: no issue water, or no facts.
 */
export interface IssueFeedVM { readonly sections: ReadonlySet<number> }
export function useIssueColour(i: { source: WaterSource; facts: SalmonFacts | null; mapTab: boolean }): IssueFeedVM | null {
  const items = useMemo(() => (i.facts ? [...salmonIssueItems(i.facts.payload)] : []), [i.facts]);
  const key = `issuesections:${sourceKey(i.source)}:${items.join(",")}`;
  const read = useAsync(async () => {
    const waters = await Promise.all(items.map((it) => i.source.water(it as never)));
    return new Set(waters.flatMap((w) => (w ? w.sections.map((s) => Number(s)) : [])));
  }, key, i.mapTab && items.length > 0);
  const sections = read.state === "ready" ? read.value : null;
  return useMemo(() => (i.mapTab && sections && sections.size ? { sections } : null), [i.mapTab, sections]);
}

/**
 * (T1029 Q6, owner 2026-11-24: "Yes, stripe it too") THE RULE BOOK'S ONE-HALF CLOSURES for the Standard map: per section,
 * the side of the drawn line its closed half is on (@app/core `bookHalves`, the salmon head's `bs`). Read from the salmon
 * HEAD ALONE (`source.salmonHead`, no `salmon-<r>` slice): the web reads the slices only on the first Salmon press, and
 * these are the Standard map's — a cold Standard map draws them. Asked like the salmon (`useSalmonColour`): on the Map
 * tab, on a proven pair, once the day's status is in and the web has settled; kept per source. Null: none in the release,
 * no salmon, or a source with no head to read.
 */
export interface HalvesFeedVM { readonly halves: ReadonlyMap<number, "L" | "R"> }
export function useBookHalves(i: { source: WaterSource; paired: boolean; statusOk: boolean; mapTab: boolean;
                                   settled?: boolean }): HalvesFeedVM | null {
  const on = i.paired && i.statusOk && i.mapTab && i.settled !== false && !!i.source.salmonHead;
  const [asked, setAsked] = useState<string | null>(null);
  const key = `salmonhead:${sourceKey(i.source)}`;
  if (on && asked !== key) setAsked(key);
  const head = useAsync(() => i.source.salmonHead?.() ?? Promise.resolve(null), key, asked === key);
  const halves = head.state === "ready" && head.value ? bookHalves(head.value) : null;
  return useMemo(() => (i.mapTab && i.paired && halves && halves.size ? { halves } : null), [i.mapTab, i.paired, halves]);
}

/** The toggle's choice, kept for the session (Standard by default). */
export function useRegsLayer(): readonly [RegsLayer, (p: RegsLayer) => void] {
  const [p, setP] = useState<RegsLayer>("standard");
  return [p, setP] as const;
}

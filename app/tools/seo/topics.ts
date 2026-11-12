/**
 * "Specific regulation" search: queries like "vedder river steelhead regulations", "<river> bait ban", "is <river> closed",
 * "<lake> trout limit". A TOPIC is a lens on the sentences the presenter already wrote for a water: nothing here writes a rule.
 * A topic's lines are the water's own presenter items (same words, same dates) that mention it, picked by the word lists below,
 * so the topic hubs can never say more than the app's pane does. (T269: the water page is the pane; no FAQ markup.)
 *
 * (T672, T462 audit W3) THE BAIT BAN AND FLY ONLY HUBS READ THE YEAR, not the build day's sentences: a water is on the
 * hub when its page's year strip holds that state on some day (`PartModel.marks`: the strip's own legend — the stored
 * strip bits the app's year bar and the map's "Bait & fly" colouring draw). The day's sentences listed a fraction and a
 * different one each day: built Nov 11, Region 2's fly-only hub listed 4 of 49 waters (Eaton Creek's "Fly fishing only,
 * Jul 1-Oct 31" was not in force that day, so no sentence said it). Nothing is decided here: the bits are the release's.
 */
import type { WaterModel } from "./model";

export type TopicKey = "steelhead" | "salmon" | "trout" | "bait-ban" | "fly-only" | "closed" | "boat";

export interface TopicDef {
  readonly key: TopicKey;
  /** the hub's name: "Steelhead regulations" */
  readonly hub: string;
  readonly test: RegExp;
}

export const TOPICS: readonly TopicDef[] = [
  { key: "steelhead", hub: "Steelhead regulations", test: /steelhead/i },
  { key: "salmon", hub: "Salmon fishing regulations", test: /\b(salmon|coho|chinook|sockeye|chum)\b/i },
  { key: "trout", hub: "Trout and char limits", test: /\b(trout|char|dolly varden)\b/i },
  { key: "bait-ban", hub: "Bait ban waters", test: /\bno (natural )?bait\b|bait ban|bait is not allowed/i },
  { key: "fly-only", hub: "Fly fishing only waters", test: /fly fishing only|artificial fly only|hooks: one fly\b/i },
  { key: "closed", hub: "Fishing closures", test: /\bclosed\b|no fishing/i },
  { key: "boat", hub: "Boat and motor rules", test: /\b(boat|boats|motor|motors|engine|engines|electric motor)\b/i },
];

/** The strip states a year-read topic stands on: the release's strip bits (the answers' `display.strip` legend:
 *  fly_only 4, artificial_fly_only 8, bait_ban 16; held to the release's own legend by topics.test.ts). */
export const YEAR_TOPICS: Readonly<Partial<Record<TopicKey, readonly number[]>>> = { "fly-only": [4, 8], "bait-ban": [16] };

const MAX_LINES = 4;
/** sections that are about the angler's licence or the bag-limit bookkeeping, not the water's topic rules */
const SKIP_SECTION = /licen[cs]e|possession/i;

/** The water's sentences on each topic it has, in page order (at most four each, no repeats). */
export function topicLines(m: WaterModel): Map<TopicKey, string[]> {
  const out = new Map<TopicKey, string[]>();
  for (const p of m.parts) for (const s of p.sections) {
    if (SKIP_SECTION.test(s.title)) continue;
    for (const it of s.items) {
      if (it.head) continue;
      for (const t of TOPICS) {
        if (YEAR_TOPICS[t.key]) continue;   // (T672) read from the year below, never from the day's sentences
        if (t.key === "closed" && !(s.title === "Why it is closed" || s.title === "Closures" || /where you can.t fish/i.test(s.title))) continue;
        if (!t.test.test(it.text)) continue;
        const l = out.get(t.key) ?? [];
        if (l.length < MAX_LINES && !l.includes(it.text)) { l.push(it.text); out.set(t.key, l); }
      }
    }
  }
  // (T672) the year-read topics: the water's strip holds the state on some day; its lines are the strip's own labels
  for (const t of TOPICS) {
    const bits = YEAR_TOPICS[t.key];
    if (!bits) continue;
    const labels = [...new Set(m.parts.flatMap((p) => p.marks.filter((k) => bits.includes(k.bit)).map((k) => k.label)))];
    if (labels.length) out.set(t.key, labels);
  }
  if (m.status === "closed" && !out.has("closed")) out.set("closed", [m.answer]);
  return out;
}

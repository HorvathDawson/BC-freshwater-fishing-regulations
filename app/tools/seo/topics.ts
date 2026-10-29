/**
 * "Specific regulation" search: queries like "vedder river steelhead regulations", "<river> bait ban", "is <river> closed",
 * "<lake> trout limit". A TOPIC is a lens on the sentences the presenter already wrote for a water: nothing here writes a rule.
 * A topic's lines are the water's own presenter items (same words, same dates) that mention it, picked by the word lists below,
 * so the topic hubs can never say more than the app's pane does. (T269: the water page is the pane; no FAQ markup.)
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
        if (t.key === "closed" && !(s.title === "Why it is closed" || s.title === "Closures" || /where you can.t fish/i.test(s.title))) continue;
        if (!t.test.test(it.text)) continue;
        const l = out.get(t.key) ?? [];
        if (l.length < MAX_LINES && !l.includes(it.text)) { l.push(it.text); out.set(t.key, l); }
      }
    }
  }
  if (m.status === "closed" && !out.has("closed")) out.set("closed", [m.answer]);
  return out;
}

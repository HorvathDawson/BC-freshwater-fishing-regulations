/**
 * "Specific regulation" search: queries like "vedder river steelhead regulations", "<river> bait ban", "is <river> closed",
 * "<lake> trout limit". A TOPIC is a lens on the sentences the presenter already wrote for a water: nothing here writes a rule.
 * A topic's lines are the water page's own items (same words, same dates) that mention it, picked by the word lists below, so the
 * page, its FAQ JSON-LD and the topic hubs can never say more than the app does.
 */
import type { WaterModel } from "./model";

export type TopicKey = "steelhead" | "salmon" | "trout" | "bait-ban" | "fly-only" | "closed" | "boat";

export interface TopicDef {
  readonly key: TopicKey;
  /** the heading on a water page: "Steelhead" */
  readonly heading: string;
  /** the hub's name: "Steelhead regulations" */
  readonly hub: string;
  /** the question a visitor asks, for the FAQ: `${q(name)}` */
  readonly ask: (name: string) => string;
  readonly test: RegExp;
}

export const TOPICS: readonly TopicDef[] = [
  { key: "steelhead", heading: "Steelhead", hub: "Steelhead regulations", ask: (n) => `What are the steelhead rules on ${n}?`, test: /steelhead/i },
  { key: "salmon", heading: "Salmon", hub: "Salmon fishing regulations", ask: (n) => `What are the salmon rules on ${n}?`, test: /\b(salmon|coho|chinook|sockeye|chum)\b/i },
  { key: "trout", heading: "Trout and char", hub: "Trout and char limits", ask: (n) => `What is the trout limit on ${n}?`, test: /\b(trout|char|dolly varden)\b/i },
  { key: "bait-ban", heading: "Bait ban", hub: "Bait ban waters", ask: (n) => `Is bait allowed on ${n}?`, test: /\bno (natural )?bait\b|bait ban|bait is not allowed/i },
  { key: "fly-only", heading: "Fly fishing only", hub: "Fly fishing only waters", ask: (n) => `Is ${n} fly fishing only?`, test: /fly fishing only|artificial fly only|hooks: one fly\b/i },
  { key: "closed", heading: "Closures", hub: "Fishing closures", ask: (n) => `What closures apply on ${n}?`, test: /\bclosed\b|no fishing/i },
  { key: "boat", heading: "Boats and motors", hub: "Boat and motor rules", ask: (n) => `What are the boat and motor rules on ${n}?`, test: /\b(boat|boats|motor|motors|engine|engines|electric motor)\b/i },
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

/** Real questions with the bundle's own answers: one per topic the water has rules on (never an invented answer). */
export function faqOf(m: WaterModel, lines: ReadonlyMap<TopicKey, readonly string[]>): { q: string; a: string }[] {
  const out: { q: string; a: string }[] = [];
  for (const t of TOPICS) {
    const l = lines.get(t.key);
    if (!l?.length) continue;
    out.push({ q: t.ask(m.name), a: l.slice(0, 2).join(" ") });
  }
  return out.slice(0, 5);
}

/** `id` attribute for a heading: "Trout and char" -> "trout-and-char" */
export const anchorOf = (s: string): string => s.normalize("NFKD").replace(/\p{M}+/gu, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "x";

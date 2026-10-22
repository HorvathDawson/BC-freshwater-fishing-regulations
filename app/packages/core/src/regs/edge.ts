/**
 * T42 — the page's edge states: which of the four shapes a tap's page takes, and the Keep tab of the two that draw no card.
 *
 * - "outside": the place has no key (`inp.key == null`: a handle outside B.C., or a part with no rule set) — page v36
 *   renderCard's one foot line, nothing else; Gear / Methods are not drawn and the Licence is null (page.ts).
 * - "tidal": the key is tidal (`akey.tidal`) or the day's stored display frame says "tidal" — page v36 tidalHtml: the
 *   "Tidal water" banner in the stored frame's own note, nothing else (no strip, groups, Other fish, possession, where-not).
 * - "unnamed": a map tap on water with no registry item (`target.kind === "section"`) — built like a named water; only
 *   the header differs (the store's `place.names[kind]`, no part label, no Map / Parts action: header.ts, D11 a).
 * - "normal": everything else.
 *
 * Owner rule 2026-10-21: nothing here decides a regulation. The key, its tidal flag and the display status are the
 * pipeline's stored answers; this file only reads which of them is present (contracts §3 "unnamed", §2.2 "outside").
 *
 * Ports (page_v36.js @ 04765ab5): renderCard 701 (`PLACE.k == null` → the foot; `isTidal(PLACE)` → waterStrip only),
 * tidalHtml 449. The timed caveat is T15b's `timedOf` (caveats.ts), not this file's.
 */
import { bannerOf } from "./banner";
import type { RegsInputs } from "./inputs";
import type { KeepVM } from "./model";
import type { Settled } from "./settle";
import { WORDS } from "./words";

export type Edge = "normal" | "unnamed" | "tidal" | "outside";

/** Which shape the page takes: no key → outside; a tidal key or a tidal display frame → tidal; a section tap → unnamed. */
export function edgeKind(inp: RegsInputs): Edge {
  if (inp.key == null) return "outside";
  if (inp.key.tidal || inp.frames?.display.status === "tidal") return "tidal";
  return inp.target.kind === "section" ? "unnamed" : "normal";
}

/** A Keep tab with no block drawn (each edge fills the one it has). */
const NONE: Omit<KeepVM, "order"> = Object.freeze({
  strip: null, inherited: null, banner: null, groups: [], plainRows: [], otherFish: null, otherFishKey: [], caveats: [], possession: null,
  whereNot: null, outside: null,
});

/** Outside B.C.: page v36 renderCard's foot line alone. */
export function outsideKeep(): KeepVM {
  return { ...NONE, order: ["outside"], outside: WORDS.OUTSIDE_BC };
}

/**
 * Tidal water: page v36 tidalHtml — the "Tidal water" banner in the stored tidal frame's note, alone (golden x31p_1010
 * `["div.banner"]`). `settled` is the day as T07 read it (its display frame is the one the banner words); a key whose
 * frame is missing draws nothing (page v36 `missing(…)`; the store always carries it).
 */
export function tidalKeep(inp: RegsInputs, settled: Settled): KeepVM {
  const banner = bannerOf(inp, settled, null);
  return { ...NONE, order: banner ? ["banner"] : [], banner };
}

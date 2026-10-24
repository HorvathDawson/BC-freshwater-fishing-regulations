/**
 * T21 — `buildRegsPage`: the whole page for one tap on one day. This slice fills the header (T16) and the Keep tab
 * (keep.ts), Gear (T30, T31) and Methods (T32); Licence (T33) and the sheets (T36) are null-stubs behind the same signature until their
 * tickets land.
 *
 * Owner rule 2026-10-21: nothing here decides a regulation. The place, the settled day and the card model are the store's
 * answers hydrated once (T07/T08) and handed to every presenter; `status` is the strip's word for the stored display
 * status (never the status index).
 */
import { buildModel } from "./card";
import { buildBait } from "./bait";
import { buildGearTop, gearCtxOf } from "./gear";
import { buildMethods } from "./methods";
import { edgeKind } from "./edge";
import { FISH, type FishManifest } from "./fish.generated";
import { headerOf } from "./header";
import type { RegsInputs } from "./inputs";
import { keepOf } from "./keep";
import { buildLicence } from "./licence";
import type { Day, RegsPage, Who } from "./model";
import { makePlace } from "./place";
import { settle } from "./settle";
import { sheetOf } from "./sheets";
import type { SheetKey } from "./sheetKey";
import { sameDay } from "../day";

export interface RegsPageOpts {
  readonly today: Day; readonly who: Who;
  /** the group head form (decisions D12): "a" (default) the share leads, "b" the total leads */
  readonly tot?: "a" | "b";
  /** the fish manifest (T20); default the generated one */
  readonly fish?: FishManifest;
  /** (T53) an unnamed section: the named water it flows into, for the header's label slot (`downstreamLabel`) */
  readonly downstream?: string | null;
  /** test-harness page id only — sheet keys never embed it (contracts §6 #1) */
  readonly pid?: string;
}


/** The whole page for one tap (T21: header + Keep). */
export function buildRegsPage(inp: RegsInputs, opts: RegsPageOpts): RegsPage {
  const place = makePlace(inp), settled = settle(inp, place), card = buildModel(inp, place, settled);
  const env = { place, settled, card, fish: opts.fish ?? FISH, today: opts.today, tot: opts.tot ?? "a" } as const;
  const keep = keepOf(inp, env);
  // (T42) the edge decides the status word: outside / tidal from the stored key and frame, else the strip's reading
  const edge = edgeKind(inp);
  const status: RegsPage["status"] = edge === "outside" || edge === "tidal" ? edge : keep.strip ? keep.strip.status : "unknown";
  // (T30–T32) the Gear and Methods tabs: none outside B.C. (no key: the tabs are not drawn)
  const gctx = edge === "outside" ? null : gearCtxOf(inp, { today: opts.today, banner: keep.banner });
  const gear = gctx && { ...buildGearTop(inp, gctx), bait: buildBait(inp, gctx) };
  const methods = gctx && buildMethods(inp, gctx);
  const page: Omit<RegsPage, "sheet"> = {
    version: "regs-v38",
    target: inp.target, day: inp.day, weekday: inp.weekday, today: sameDay(inp.day, opts.today), who: opts.who,
    status,
    header: headerOf(inp, { today: opts.today, status, downstream: opts.downstream ?? null }),
    keep,
    gear, methods,
    licence: buildLicence(inp, opts.who, { closed: settled.status?.status === "closed",   // (T33)
      glossary: new Set(inp.statics.glossary.terms.map((t) => t.id)) }),
    terms: Object.fromEntries(inp.statics.glossary.terms.map((t) => [t.id, t.term])),   // (T40) the ⓘ's spoken title
  };
  // T36: lazy and pure over the same inputs (a sheet is built when it is opened)
  return { ...page, sheet: (key: SheetKey) => sheetOf({ inp, page, today: opts.today, env }, key) };
}

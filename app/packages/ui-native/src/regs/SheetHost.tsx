/**
 * T37 — THE ONE SHEET over the Regulations view: whatever a press opens (an ⓘ, a rule line, "Source ›", "How to tell ›",
 * a fish picture, an Other fish row, the possession row, "See every regulation line ›") is `page.sheet(key)` drawn in the
 * app's one `Sheet` (one scrim, one look — the Layers / Day sheets' component). A press inside opens the next sheet in
 * its place (no back stack, as the prototype); Done, the scrim, a drag down (G1: from the handle, `useSheetDrag`) and the
 * one back (G1 `useBackLayer`: Android's back, the browser's back, Escape) close it. The water's header stays mounted
 * behind the scrim. A key the page cannot open (a stale one) draws nothing.
 *
 * The open key lives with the caller (RegsView; T23's RegsScreen when it lands). No logic: `page.sheet` is core's (T36).
 */
import { useEffect, useMemo, useRef } from "react";
import type { ScrollView } from "react-native";
import type { RegsPage, SheetKey, SourceBox, Tab } from "@app/core";
import { useLayoutClass, type LayoutClass } from "@app/ui";
import { Sheet } from "../Sheet";
import type { Palette } from "../theme";
import { SheetBody } from "./SheetBody";
import type { AdiposeImages, FishImages } from "./types";

export interface SheetHostProps {
  page: RegsPage;                       // page.sheet(key) is lazy + pure (T36)
  open: SheetKey | null;
  onOpen: (k: SheetKey) => void;        // nested open: replaces
  onClose: () => void;
  onPart?: (partIx: number) => void;    // parts sheet rows
  onTab?: (t: Tab) => void;             // "Steelhead stamp ›" when it sits in a sheet
  palette: Palette;
  fish?: FishImages;                    // T20 map code → ImageSourcePropType (registered by apps/mobile)
  adipose?: AdiposeImages;              // T38 the hatch sheet's two drawings (apps/mobile ADIPOSE_IMAGES)
  onBook?: (pdf: number, box?: readonly SourceBox[]) => void;       // a Rule book page (default: the synopsis at that page, D6 a)
  layout?: LayoutClass;                 // (T45/T46) absent → the app's (`useLayoutClass`)
}

export function SheetHost(p: SheetHostProps) {
  // (T45/T46) the layout class: the caller's (RegsScreen hands its own), else the app's
  const shell = useLayoutClass();
  const layout = p.layout ?? shell;
  const vm = useMemo(() => (p.open ? p.page.sheet(p.open) : null), [p.page, p.open]);
  const scroll = useRef<ScrollView>(null);
  // a sheet opened in another's place starts at its top
  useEffect(() => { scroll.current?.scrollTo({ y: 0, animated: false }); }, [p.open]);
  // (refinement pass) the last sheet drawn stays mounted, closed, so the one Sheet can slide it away (its scrim fading in
  // place) instead of it vanishing on the press of Done; a key the page cannot open still draws nothing
  const last = useRef(vm);
  if (vm) last.current = vm;
  const drawn = vm ?? (p.open ? null : last.current);
  if (!drawn) return null;
  return (
    <Sheet open={vm !== null} title={drawn.title} sub={drawn.sub} scroll scrollRef={scroll} palette={p.palette} onClose={p.onClose} layout={layout} dialog>
      <SheetBody vm={drawn} layout={layout} palette={p.palette} fish={p.fish} adipose={p.adipose} onOpen={p.onOpen} onPart={p.onPart} onTab={p.onTab} onBook={p.onBook} />
    </Sheet>
  );
}

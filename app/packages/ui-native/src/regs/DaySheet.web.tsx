/**
 * (T915, owner 2026-11-19: "make calendar popup look better on desktop (good for mobile but desktop doesnt like mobile
 * popup)") THE BROWSER'S "WHICH DAY?". A phone or a narrow window keeps the phone's bottom sheet exactly
 * (the same `<Sheet>` as `DaySheet.native.tsx`). At the desktop layout class (`tabsAsRail`, >= 1280 px, the class T904's legend card and map foot
 * use) the calendar is a POPOVER: a card hung under the button that opened it (the map's day strip, the page's date
 * pill), square, the ink rule and the hard lift every floating card has, as wide as the month grid needs. No scrim: the
 * map it recolours stays in full view. Escape, a click outside it, Done and the browser's back close it (`useBackLayer`,
 * as the sheet), and focus goes back to the button. On open the keyboard is on the chosen day, so the arrow keys move
 * it at once (`DayCalendar`'s grid). See `DaySheet.d.ts`; the phone's is `DaySheet.native.tsx`.
 */
import { useId, useLayoutEffect, useRef, useState } from "react";
import { Modal, View, useWindowDimensions } from "react-native";
import { SHEET_WORDS, WORDS, reportFailure } from "@app/core";
import { tabsAsRail, useBackLayer, useLayoutClass } from "@app/ui";
import { DayCalendar } from "../DayCalendar";
import { Pressable } from "../Pressable";
import { Text } from "../Text";
import { TYPE } from "../type";
import { Sheet } from "../Sheet";
import { GUTTER } from "../theme";
import type { DaySheetProps } from "./DaySheet";
import { hitProps } from "./hit";

/** the popover: its width (the grid's seven 44 px days and the card's padding, with room), its gap under the button,
 *  and the least room it keeps from the window's edges */
const POPOVER = { width: 344, gap: 6, edge: 16, pad: 12 } as const;

type Box = { x: number; y: number; w: number; h: number };

/** the opening button on the page: the one drawn (a page kept mounted off screen has a twin with no box) */
function anchorEl(testID: string): HTMLElement | undefined {
  return [...document.querySelectorAll<HTMLElement>(`[data-testid="${testID}"]`)].find((e) => e.getBoundingClientRect().width > 0);
}

export function DaySheet({ open, onClose, palette, day, onDay, range, now, anchor }: DaySheetProps) {
  const desktop = tabsAsRail(useLayoutClass());
  // the popover's one back (the browser's back and Escape); below desktop the <Sheet> registers its own
  const pop = desktop && open;
  const requestClose = useBackLayer(pop, onClose);
  // a resize re-reads the button and the window (the button moves with the layout); the numbers are the browser's own,
  // read together, so the card and the button are placed in one measure
  const resized = useWindowDimensions();
  const titleId = useId();
  const [view, setView] = useState({ width: 0, height: 0 });
  const [at, setAt] = useState<Box | null>(null);
  const [cardH, setCardH] = useState(0);
  useLayoutEffect(() => {
    if (!pop) return;
    setView({ width: window.innerWidth, height: window.innerHeight });
    const el = anchorEl(anchor);
    if (!el) {
      // a shown failure, reported: the card is still drawn (at the window's top), never a calendar that does not open
      reportFailure(new Error(`day popover: its button "${anchor}" is not on the page`));
      setAt(null);
      return;
    }
    const r = el.getBoundingClientRect();
    setAt({ x: r.left, y: r.top, w: r.width, h: r.height });
  }, [pop, resized.width, resized.height, anchor]);
  const { width, height } = view;
  // focus back to the button once the popover is gone (the Modal's own restore needs the button focused at the open,
  // which a mouse click in Safari does not do)
  const wasOpen = useRef(pop);
  useLayoutEffect(() => {
    if (wasOpen.current && !pop) {
      const el = anchorEl(anchor);
      requestAnimationFrame(() => el?.focus());
    }
    wasOpen.current = pop;
  }, [pop, anchor]);
  // open: the keyboard starts on the chosen day (the grid's one tab stop), so the arrow keys move it at once — a frame
  // after the Modal's focus trap has put the focus on its first stop
  useLayoutEffect(() => {
    if (!pop) return;
    const f = requestAnimationFrame(() =>
      document.querySelector<HTMLElement>('[data-testid="day-popover"] [data-testid="day-grid"] [tabindex="0"]')?.focus());
    return () => cancelAnimationFrame(f);
  }, [pop]);

  const w = Math.min(POPOVER.width, Math.max(0, width - 2 * POPOVER.edge));
  const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(v, hi));
  const left = at ? clamp(at.x, POPOVER.edge, width - w - POPOVER.edge) : Math.max(POPOVER.edge, (width - w) / 2);
  // under the button; above it when the window has no room below and does above; else as low as fits
  const below = at ? at.y + at.h + POPOVER.gap : POPOVER.edge;
  const above = at ? at.y - POPOVER.gap - cardH : -1;
  const top = cardH > 0 && below + cardH > height - POPOVER.edge && above >= POPOVER.edge ? above
    : clamp(below, POPOVER.edge, Math.max(POPOVER.edge, height - POPOVER.edge - cardH));

  // a phone or a narrow window: the phone's bottom sheet, exactly as DaySheet.native.tsx draws it
  if (!desktop) return (
    <Sheet open={open} onClose={onClose} title={WORDS.DAY_SHEET_TITLE} palette={palette}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: 8 }}>
        <DayCalendar palette={palette} day={day} onDay={onDay} range={range} now={now} />
      </View>
    </Sheet>
  );
  return (
    <Modal visible={open} transparent animationType="none" onRequestClose={requestClose} aria-labelledby={titleId}>
      <View style={{ flex: 1 }}>
        {/* a click outside the card closes it; no dim, so the map the day recolours stays as it is */}
        <Pressable testID="day-popover-outside" accessibilityRole="button" accessibilityLabel={SHEET_WORDS.close} onPress={onClose}
                   tabIndex={-1} style={{ position: "absolute", top: 0, bottom: 0, left: 0, right: 0 }} />
        <View testID="day-popover" accessibilityViewIsModal
              onLayout={(e) => setCardH(e.nativeEvent.layout.height)}
              style={{ position: "absolute", top, left, width: w, maxHeight: Math.max(0, height - 2 * POPOVER.edge),
                       backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink, ...palette.lift }}>
          <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between",
                         paddingLeft: POPOVER.pad + 2, paddingRight: POPOVER.pad - 2, minHeight: 44,
                         borderBottomWidth: 1, borderBottomColor: palette.line }}>
            <Text nativeID={titleId} accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{WORDS.DAY_SHEET_TITLE}</Text>
            <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel={SHEET_WORDS.done}
                       {...hitProps({ w: 44, h: 44 })} style={{ minHeight: 44, minWidth: 44, alignItems: "center", justifyContent: "center" }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{SHEET_WORDS.done}</Text>
            </Pressable>
          </View>
          <View style={{ padding: POPOVER.pad, paddingTop: POPOVER.pad - 4 }}>
            <DayCalendar palette={palette} day={day} onDay={onDay} range={range} now={now} />
          </View>
        </View>
      </View>
    </Modal>
  );
}

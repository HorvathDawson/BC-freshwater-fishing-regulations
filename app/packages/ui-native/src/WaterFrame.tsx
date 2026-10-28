/**
 * T44 — THE WATER FRAME: the one parent every view of a water (Regulations · Conditions · Map) draws inside, and the
 * only thing that sets the layout they share (owner 2026-10-21: "in desktop switching between regulation and condition
 * tab makes the margin on the left change and everything move … maybe each is a sub component of the main component
 * setting the spacing").
 *
 *   ┌──────────── the window (the frame's background) ────────────┐
 *   │      ┌──── the column: WATER_FRAME.maxWidth, centred ───┐     │
 *   │      │ ‹ Back                       (the header: FaceBar, │     │
 *   │      │ Regulations  Conditions  Map  drawn HERE only)    │     │
 *   │      │ Vedder River          (the name row, `WaterTitle`) │     │
 *   │      │ the view's content — no gutter, width or header    │     │
 *   │      │ of its own (`tools/water-frame.test.ts`)           │     │
 *   │      └────────────────────────────────────────────────────┘     │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * WHAT IT OWNS: the background, the column (its measure and its centring on a wide screen), the header — the back row and
 * the view strip at the gutter, `headerHeight` tall whether or not a back is drawn — and the header's swipe (the
 * Shell's `WaterViewsContext.swipe`: a sideways swipe on the header moves between the views on every view). The swipe
 * moves nothing the views share: the header listens `still`, and every detector in the frame drives the frame's one
 * offset (`SwipeOffsetContext`), which only the view's body follows (its own detector's, or `SwipeFollow` here when
 * the body has none — the Map's canvas). Each view
 * remounts on a switch; the frame is the same element at the same place, so nothing both views draw moves (measured by
 * `tools/water-frame-layout.test.ts` at 390, 768 and 1440).
 *
 * WHAT A VIEW DRAWS: its content, filling the column. The water's name row is `WaterTitle` — under the header, in the
 * frame's `title` slot (Conditions, Map, and the Regulations view's waiting states), or as the first row of the regs
 * header (`RegsHeader`, whose date pill sits in it; no Parts button since T150). Either way the row is the same box at the same
 * place. An inset block is `WaterInset` (the frame's gutter), never a padding of the view's own.
 *
 * THE MAP MAY GO FULL-BLEED (`bleed`): the canvas takes the window's width under the header; the header, the name row
 * and the bar's words (`WaterInset column`) stay on the column. That is the one difference allowed between the views.
 *
 * Safe areas are the app's (`apps/mobile` App.tsx wraps everything in a SafeAreaView); the frame adds none of its own.
 */
import { useContext, useRef } from "react";
import { Animated, Text, View, type StyleProp, type ViewStyle } from "react-native";
import { FaceBar, type Face } from "./Faces";
import { SwipeFollow, SwipeOffsetContext, ViewSwipe, WaterViewsContext } from "./ViewSwipe";
import { useCappedScale } from "./textScale";
import { CAPPED_SCALE, TYPE } from "./type";
import { WATER_FRAME, type Palette } from "./theme";

/** The column: the frame's measure, centred. */
const COLUMN: ViewStyle = { width: "100%", maxWidth: WATER_FRAME.maxWidth, alignSelf: "center" };

export function WaterFrame({ palette, face, onBack, backLabel, title, bleed = false, children }: {
  palette: Palette;
  /** The view in front, for a frame drawn without the Shell's `WaterViewsContext` (a view mounted alone). */
  face: Face;
  /** Back, for a frame drawn alone; in the app the context's back wins (FaceBar). */
  onBack?: () => void;
  backLabel?: string;
  /** The water's name row (`WaterTitle`) for a view that does not draw it as part of its own header. */
  title?: React.ReactNode;
  /** The content takes the window's width, not the column's (the Map view's canvas). */
  bleed?: boolean;
  children: React.ReactNode;
}) {
  const views = useContext(WaterViewsContext);
  const swipe = views?.swipe ?? null;
  // the frame's one swipe offset: the header's detector and the body's both write it; only the body follows it
  const offset = useRef(new Animated.Value(0)).current;
  const header = (
    <View testID="water-frame-header">
      <View style={{ paddingHorizontal: WATER_FRAME.gutter, paddingTop: WATER_FRAME.headerTop,
                     paddingBottom: WATER_FRAME.headerBottom, minHeight: WATER_FRAME.headerHeight }}>
        <FaceBar palette={palette} face={face} onFace={() => {}} onBack={onBack} backLabel={backLabel} />
      </View>
      {title}
    </View>
  );
  const head = swipe ? <ViewSwipe swipe={swipe} fill={false} still testId="view-swipe-header">{header}</ViewSwipe> : header;
  // a body with no detector of its own (Map) still follows a drag on the header
  const body = swipe && !views?.bodySwipe ? <SwipeFollow>{children}</SwipeFollow> : children;
  return (
    <SwipeOffsetContext.Provider value={offset}>
      <View testID="water-frame" style={{ flex: 1, backgroundColor: palette.card }}>
        {bleed ? (
          <>
            <View style={COLUMN}>{head}</View>
            <View style={{ flex: 1 }}>{body}</View>
          </>
        ) : (
          <View style={[COLUMN, { flex: 1 }]}>
            {head}
            <View style={{ flex: 1 }}>{body}</View>
          </View>
        )}
      </View>
    </SwipeOffsetContext.Provider>
  );
}

/**
 * The water's name row, the same box on every view: `titleHeight` tall, the name at the gutter in the regs header's
 * face (two lines at most — more at the OS's larger text, T40), and beside it whatever the view puts there (the regs header's date pill).
 * `name` null: the row is held empty (the name is still loading) so nothing under it moves when it arrives.
 */
/** (T224) the line under a name that is not the official one: where it came from */
const WATER_NAME_SOURCE = (source: string) => `Name from ${source}`;

export function WaterTitle({ name, palette, source = null, children }: {
  name: string | null; palette: Palette;
  /**
   * (T224) Where the name came from when it is not the official (gazetted) one ("City of Abbotsford stream layer"),
   * said under it. Null: an official name, nothing said.
   */
  source?: string | null;
  /** Beside the name, right-aligned. */
  children?: React.ReactNode;
}) {
  // (T40) at the OS's larger text the name keeps its words: two lines at 1×, as many more as the scale (four at 2×), the
  // row at least its one-scale height and growing with them rather than cutting the name off
  const lines = Math.ceil(2 * useCappedScale());
  return (
    <View testID="water-title"
          style={{ minHeight: WATER_FRAME.titleHeight, paddingLeft: WATER_FRAME.gutter, paddingRight: 8, gap: 8,
                   flexDirection: "row", alignItems: "center" }}>
      {source && name ? (
        <View style={{ flex: 1 }}>
          <Text accessibilityRole="header" numberOfLines={lines} maxFontSizeMultiplier={CAPPED_SCALE}
                style={[TYPE.headerName, { color: palette.ink }]}>{name}</Text>
          <Text testID="water-name-source" maxFontSizeMultiplier={CAPPED_SCALE}
                style={[TYPE.meta, { color: palette.sub }]}>{WATER_NAME_SOURCE(source)}</Text>
        </View>
      ) : (
        <Text accessibilityRole={name ? "header" : undefined} numberOfLines={lines} maxFontSizeMultiplier={CAPPED_SCALE}
              style={[TYPE.headerName, { flex: 1, color: palette.ink }]}>{name ?? ""}</Text>
      )}
      {children}
    </View>
  );
}

/**
 * A block of a view's content set in from the sides by the frame's gutter. `column`: also held to the frame's column
 * (inside full-bleed content: the Map view's bar, whose words line up with the header above the canvas).
 */
export function WaterInset({ column = false, style, testID, children }: {
  column?: boolean; style?: StyleProp<ViewStyle>; testID?: string; children: React.ReactNode;
}) {
  return (
    <View testID={testID} style={[column ? COLUMN : null, { paddingHorizontal: WATER_FRAME.gutter }, style]}>
      {children}
    </View>
  );
}

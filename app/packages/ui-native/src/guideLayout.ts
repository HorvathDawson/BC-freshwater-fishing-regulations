/**
 * (T456, a tester: "the menu stays in the way" — the day chips sat under the card) WHERE THE WALKTHROUGH'S CARD GOES, and
 * how far a page scrolls so the control a step is about can be seen with the card beside it. Pure: the card measures, this
 * decides, and a test sweeps every ring at phone and desktop sizes (`Walkthrough.test.tsx`).
 *
 * THE CARD NEVER MEETS THE RING. Beside the ring on a wide window (right of it, else left of it), else under it or over
 * it, whichever side has the room (under first: the eye reads down). The ring is drawn round the control AND what it
 * opens (the forecast days under Forecast), so what a tap opens stays usable too. When neither side has room the page
 * is scrolled (`revealBy`) before the card is placed again; a ring too tall for any room is reported (`covers`), never
 * hidden.
 */
import { GUTTER, SPACE } from "./theme";
import type { Rect } from "./CoachMarks";

/** ring to card */
export const CARD_GAP = 12;
/** at or above this width the card may sit beside the ring (a desktop's columns) */
export const BESIDE_AT = 900;

export interface CardRoom {
  /** the top the card may not rise above (under the search field on the map; the status bar elsewhere) */
  readonly top: number;
  /** what the foot keeps (the tab bar, when it is shown) */
  readonly bottom: number;
}

export interface CardPlace { readonly left: number; readonly top: number; readonly covers: boolean }

const overlaps = (a: Rect, b: Rect) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;

/**
 * The card's box (left, top) for a ring (null: nothing ringed — docked at the foot, or the top for a `dock: "top"` step).
 * `card` is the card's measured size; `covers` is true only when no place keeps it off the ring.
 */
export function placeGuideCard(ring: Rect | null, win: { w: number; h: number }, card: { w: number; h: number },
                               room: CardRoom, dock?: "top"): CardPlace {
  const floor = win.h - room.bottom;
  const clampTop = (t: number) => Math.max(room.top, Math.min(t, floor - card.h));
  const sideLeft = win.w >= BESIDE_AT ? win.w - card.w - GUTTER : (win.w - card.w) / 2;
  if (!ring) return { left: sideLeft, top: dock === "top" ? room.top : floor - SPACE.s3 - card.h, covers: false };
  const fits = (left: number, top: number) => top >= room.top - 0.5 && top + card.h <= floor + 0.5
    && left >= 0 && left + card.w <= win.w && !overlaps({ x: left, y: top, w: card.w, h: card.h }, ring);
  // centred on the ring, inside the gutters
  const under = Math.max(GUTTER, Math.min(ring.x + ring.w / 2 - card.w / 2, win.w - card.w - GUTTER));
  const tries: [number, number][] = [];
  if (win.w >= BESIDE_AT) {
    tries.push([ring.x + ring.w + CARD_GAP, clampTop(ring.y)], [ring.x - CARD_GAP - card.w, clampTop(ring.y)]);
  }
  const below = ring.y + ring.h + CARD_GAP, above = ring.y - CARD_GAP - card.h;
  // the side with more room first
  const room1 = floor - (ring.y + ring.h), room2 = ring.y - room.top;
  if (room1 >= room2) tries.push([under, below], [under, above]); else tries.push([under, above], [under, below]);
  // docked at the far edge from the ring
  tries.push([sideLeft, room.top], [sideLeft, floor - card.h]);
  for (const [l, t] of tries) if (fits(l, t)) return { left: l, top: t, covers: false };
  return { left: under, top: clampTop(room1 >= room2 ? below : above), covers: true };
}

/**
 * How far to scroll the ring's page (down: positive) so the ring is wholly in the page's view AND the card fits beside
 * it; 0 when it already does. `view` is the page's own box in the window (under a fixed header, a ring may be "in the
 * window" and still hidden).
 */
export function revealBy(ring: Rect, view: Rect, win: { w: number; h: number }, card: { w: number; h: number },
                         room: CardRoom): number {
  const inView = ring.y >= view.y - 0.5 && ring.y + ring.h <= view.y + view.h + 0.5;
  if (inView && !placeGuideCard(ring, win, card, room).covers) return 0;
  const vTop = view.y + SPACE.s2, vBottom = Math.min(view.y + view.h, win.h - room.bottom) - SPACE.s2;
  // the ring at the top of the page's view with the card under it; else at the foot with the card over it
  if (vTop + ring.h + CARD_GAP + card.h <= win.h - room.bottom) return ring.y - vTop;
  return ring.y + ring.h - vBottom;
}

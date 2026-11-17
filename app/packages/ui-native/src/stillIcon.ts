/**
 * (T814) THE STILL LOADING ICON'S SLOT. Under Reduce Motion the loading fish stands still (both `FishSpinner` halves),
 * and a frozen fish frame can read as a hung app (owner, iPhone build 36). Owner 2026-11-16 ~23:50: "loading icons seem
 * like they can exist still maybe instead we make a second loading icon i will make it then send it here".
 *
 * TODO(T814): the owner's still loading icon goes here — a square picture, grayscale+alpha like the fish strip (white
 * pixels, the drawing in the alpha), as a data URI or a self-hosted URL; it is tinted at run time like the fish (the
 * native `tintColor`, the web's CSS mask), so one file serves every palette. Null until it arrives: a still spinner
 * draws the fish's first frame, as before.
 */
export const STILL_ICON: string | null = null;

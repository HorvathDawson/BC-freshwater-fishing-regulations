/**
 * THE BOTTOM BAR'S TABS (T48: Map · Conditions · Spots · More), named ONCE from the words each place already has — the
 * water views' "Map" and "Conditions", the Spots menu's title, the More tab's title — so the bar, a water's ‹ ("back to
 * Map"), a spot's ‹ and the More tab's ‹ all print the same word for the same place. The bar typed its own four names
 * before T60's data-vs-UI audit (AGENTS rule 23: words live in core).
 */
import { SPOTS_MENU } from "./mapRail";
import { MENU_WORDS } from "./menu";
import { WATER_VIEW_LABEL } from "./waterViews";

export const BROWSE_TABS = [
  { k: "map", t: WATER_VIEW_LABEL.map },
  { k: "conditions", t: WATER_VIEW_LABEL.conditions },
  { k: "spots", t: SPOTS_MENU.title },
  { k: "more", t: MENU_WORDS.title },
] as const;

/**
 * THE LOOK, ONE SEQUENCE FOR BOTH MAPS (N2): the view, the Conditions wash, the gauge dots' scale, each layer's own mode,
 * the view's hidden layers and the user's groups, in that order, through the adapter. `Map.web.tsx` runs it against
 * the live map; `nativeStyle` runs it against a copy of the style JSON MapLibre Native is handed. One function, so a
 * step added for one renderer is a step the other takes too.
 *
 * A bad mode or group name is a CALLER bug. It is reported (`report`), never thrown: throwing from a render unmounts
 * the tree, and a map still showing the previous colouring is recoverable where a white screen is not.
 */
import type { MapAdapter, MapHandle } from "./adapters/contract";
import { gaugeDotColour } from "./runtime-style";
import { STYLE_META } from "./style";

export interface Look {
  view: string;
  theme: string;
  modes?: Record<string, string>;
  hide?: readonly string[];
  /** The view's own layers to draw (`shownLayers`); every other view-only layer stays hidden. */
  show?: readonly string[];
  groups?: Record<string, boolean>;
}

/** `has`: is this layer in the style the handle writes to. */
export function applyLook(adapter: MapAdapter, h: MapHandle, has: (layerId: string) => boolean, look: Look,
                          report: (e: Error) => void): void {
  const { view, theme } = look;
  const modes = look.modes ?? {};
  const asError = (e: unknown) => (e instanceof Error ? e : new Error(String(e)));
  try { adapter.applyView(h, view, theme); } catch (e) { report(asError(e)); }
  // THE WASH follows the stream layer's mode: the Conditions ramp needs a quiet ground, every other mode wants the map
  // legible as a map. 0.42: enough that roads and landcover stop competing with a 1.4 px coloured line, little enough
  // that a reader can still find the town they launched from.
  if (has("basemap-wash")) h.setPaint("basemap-wash", "background-opacity", modes.stream === "standing" ? 0.42 : 0);
  // The gauge dots are a runtime layer, so the adapter cannot find them by id (`gaugeDotColour`). Repainted explicitly,
  // or a switch to temperature leaves the stations painted on the flow scale.
  if (has("gauge-dot")) h.setPaint("gauge-dot", "circle-color", gaugeDotColour(theme, modes.gauges));
  for (const [id, mode] of Object.entries(modes)) {
    if (!has(id)) continue;
    try { adapter.setLayerMode(h, id, mode, theme); } catch (e) { report(asError(e)); }
  }
  // Every layer ANY view can hide is set each pass — not just the ones in `hide` — because leaving Conditions has to put
  // them back, and a one-way toggle is how a boundary disappears for the rest of the session.
  for (const v of STYLE_META.views)
    for (const id of v.hide ?? [])
      if (has(id)) h.setVisibility(id, !(look.hide ?? []).includes(id));
  // AND EVERY LAYER ANY VIEW ALONE DRAWS, both ways for the same reason: on only while the look names it.
  for (const v of STYLE_META.views)
    for (const id of v.show ?? [])
      if (has(id)) h.setVisibility(id, (look.show ?? []).includes(id));
  for (const [id, on] of Object.entries(look.groups ?? {})) {
    try { adapter.setGroupVisible(h, id, on); } catch (e) { report(asError(e)); }
  }
}

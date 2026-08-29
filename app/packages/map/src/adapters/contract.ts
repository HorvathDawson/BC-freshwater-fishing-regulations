/**
 * The ONE map surface an app may use. Both adapters implement exactly this — anything a
 * platform could do beyond it is a capability the other silently lacks.
 *
 * Note what is absent: no way to add a layer, and no way to set a colour outright.
 * Layers come from the generated catalog; colours come from tokens; the active
 * colouring comes from a view. That is what keeps the platforms looking the same.
 */
import {
  MAP_STYLE, STYLE_META, colorExpression, defaultView, resolveTheme, type Tokens,
} from "../style.js";

/** The thin platform-specific part each adapter supplies. */
export interface MapHandle {
  setVisibility(layerId: string, visible: boolean): void;
  setPaint(layerId: string, prop: string, value: unknown): void;
  setFeatureState(layerId: string, featureId: string, state: Record<string, unknown>): void;
  clearFeatureStates(layerId: string): void;
}

export interface MapAdapter {
  readonly platform: "native" | "web";
  style(): typeof MAP_STYLE;
  layerIds(): string[];
  /** Toggle a GROUP — apps never address individual layers. */
  setGroupVisible(h: MapHandle, groupId: string, visible: boolean): void;
  /** Switch the whole picture: which colouring each layer uses. */
  applyView(h: MapHandle, viewId: string, themeName: string, overrides?: Tokens): void;
  /** Re-apply colours after a theme or user-palette change, keeping the current view. */
  applyTheme(h: MapHandle, viewId: string, themeName: string, overrides?: Tokens): void;
  /** Highlight by id via feature-state — never by mutating paint. */
  highlight(h: MapHandle, featureIds: string[]): void;
  /** Push the per-feature data a colour mode reads (status, discharge, …). */
  setData(h: MapHandle, layerId: string, values: Record<string, Record<string, unknown>>): void;
}

export function baseAdapter(platform: "native" | "web"): MapAdapter {
  const paintProp = (layerId: string) => {
    const l = MAP_STYLE.layers.find((x) => x.id === layerId) as { type?: string } | undefined;
    return l?.type === "line" ? "line-color" : l?.type === "fill" ? "fill-color" : "circle-color";
  };

  const apply = (h: MapHandle, viewId: string, themeName: string, overrides: Tokens = {}) => {
    const view = STYLE_META.views.find((v) => v.id === viewId);
    if (!view) throw new Error(`unknown view "${viewId}"`);
    const tokens = resolveTheme(themeName, overrides);
    for (const [layerId, mode] of Object.entries(view.modes))
      h.setPaint(layerId, paintProp(layerId), colorExpression(layerId, mode, tokens));
    for (const [layerId, token] of Object.entries(STYLE_META.widths)) {
      const w = tokens[token];
      if (w !== undefined) h.setPaint(layerId, paintProp(layerId).replace("-color", "-width"), w);
    }
  };

  return {
    platform,
    style: () => MAP_STYLE,
    layerIds: () => MAP_STYLE.layers.map((l) => l.id),

    setGroupVisible(h, groupId, visible) {
      const g = STYLE_META.groups.find((x) => x.id === groupId);
      if (!g) throw new Error(`unknown layer group "${groupId}"`);
      if (!g.toggleable) throw new Error(`layer group "${groupId}" is not toggleable`);
      for (const [layerId, gid] of Object.entries(STYLE_META.layerGroup))
        if (gid === groupId) h.setVisibility(layerId, visible);
    },

    applyView: apply,
    applyTheme: apply,

    highlight(h, featureIds) {
      for (const { id } of STYLE_META.highlightable) {
        h.clearFeatureStates(id);
        for (const f of featureIds) h.setFeatureState(id, f, { selected: true });
      }
    },

    setData(h, layerId, values) {
      if (!STYLE_META.colorModes[layerId]) throw new Error(`unknown layer "${layerId}"`);
      for (const [featureId, state] of Object.entries(values))
        h.setFeatureState(layerId, featureId, state);
    },
  };
}

export const DEFAULT_VIEW = defaultView;

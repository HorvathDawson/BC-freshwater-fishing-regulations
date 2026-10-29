/**
 * T269 — THE WATER PAGE IS THE APP: one named water's regs pane, drawn by the app's OWN components (the real `Shell`, in
 * water mode on the Regulations view, exactly as a tap or `?regs=<item>` opens it) through react-native-web's server
 * render, so `/water/<slug>` paints the pane a tap paints before the app's script has run. No second template: every
 * element and word on the page is the Shell's; this file only wires it the way `apps/mobile` App.tsx does (layout width,
 * the safe-area view, the palette, the fish pictures) and answers the hooks' questions before rendering
 * (`setPrerenderAnswers`, @app/ui async.ts). `prerender.ts` makes the renders one document.
 *
 * Per water: a render per layout class (phone, tablet, desktop) and per theme (light, dark); the two themes of a layout
 * merge into one markup over CSS variables (`mergeThemes`).
 *
 * Loaded by tools/build-seo.mjs through vite-node (the TSX and react-native-web need vitest's own module handling).
 */
import "./globals";
import { createElement, type ReactElement } from "react";
import { renderToStaticMarkup, renderToString } from "react-dom/server";
import { AppRegistry, View } from "react-native";
import { FACES_REQUIRED, Shell, StripWidthHint, THEMES, type AdiposeImages, type FishImages } from "@app/ui-native";
// react-native-web's record of pictures already loaded: a server render draws a picture only when it is in there
import { ImageUriCache } from "react-native-web/dist/modules/ImageLoader";
import { HOME, LayoutWidthContext, openWater, reduce, setPrerenderAnswers, type Async, type LayoutClass } from "@app/ui";
import type { ItemId, WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { layoutCss, LayoutStyles, mergeLayouts } from "./layouts";
import { LAYOUTS, linkHeader, mergeThemes, PRERENDER_CSS, ThemeVars } from "./prerender";

/** A width inside each layout class (`LAYOUT_BREAKPOINTS`): the width that class's render is drawn at. */
export const CLASS_WIDTH: Readonly<Record<LayoutClass, number>> = { phone: 390, tablet: 1024, desktop: 1440 };
/**
 * The year bars' width at each class (measured in the app, 2026-10-29): a phone's column less its two 16 px gutters (a
 * phone's scroll bar takes no room); the tablet's and desktop's reading column, 528. Other widths scale the bar to fit.
 */
const STRIP_WIDTH: Readonly<Record<LayoutClass, number>> = { phone: CLASS_WIDTH.phone - 32, tablet: 528, desktop: 528 };

export interface PaneInputs {
  readonly source: WaterSource; readonly loader: ReleaseLoader;
  /** the app's fish pictures (`apps/mobile` fishImages.generated.ts), as the web export serves them */
  readonly fishImages?: FishImages; readonly adiposeImages?: AdiposeImages;
}

/** react-native-web's server render goes through its app registry: one key, drawing whichever tree is being rendered */
const APP_KEY = "cift-pane";
/** react-native-web's registry (react-native's types do not name its server render) */
const registry = AppRegistry as unknown as typeof AppRegistry & {
  getApplication(key: string, params: object): { element: ReactElement; getStyleElement(): ReactElement };
};
let drawn: ReactElement | null = null;
AppRegistry.registerComponent(APP_KEY, () => () => drawn);

const LOADING: Async<unknown> = { state: "loading", value: null, error: null };
const NOOP = () => {};
const NO_TILES = { pmtiles: "", basemap: "", glyphs: "", sprite: "" } as never;

/** The app's tree for one water, as App.tsx draws it once booted: its wrappers, then the Shell on that water. */
function tree(p: PaneInputs, item: ItemId, layout: LayoutClass, theme: "light" | "dark"): ReactElement {
  const palette = THEMES[theme], width = CLASS_WIDTH[layout];
  // App.tsx's SafeAreaView is this box on the web (a browser window has no insets to pad)
  return createElement(StripWidthHint.Provider, { value: STRIP_WIDTH[layout] }, createElement(LayoutWidthContext.Provider, { value: width },
      createElement(View, { style: { flex: 1, backgroundColor: palette.card } },
        createElement(Shell, {
          source: p.source, palette, loader: p.loader, theme, themeName: theme, onTheme: NOOP, tiles: NO_TILES,
          fishImages: p.fishImages, adiposeImages: p.adiposeImages,
          // a tap's route: the water on its Regulations view, opened from the map (the start link's `?regs=<item>`)
          initialRoute: reduce(HOME, openWater({ item, part: null, view: "regs" })), initialDay: null,
        }))));
}

export class PaneRenderer {
  /** every hook's answer, shared across waters (the store and the release's files are asked once for the build) */
  private readonly answers = new Map<string, Async<unknown>>();
  readonly vars = new ThemeVars();
  readonly layoutStyles = new LayoutStyles();
  /** waters whose two themes did not merge (shipped twice instead): reported in budget.json */
  readonly unmerged: string[] = [];
  /** the answers every water shares (the store, the release's files): kept; a water's own are dropped after it (memory) */
  private shared: Set<string> | null = null;
  constructor(private readonly p: PaneInputs, private readonly waitMs = 4000) {
    // the pictures are the web export's files, which the page loads like any other: drawn as loaded, as the app draws them
    const uris = [...Object.values(p.fishImages ?? {}).flat(), ...Object.values(p.adiposeImages ?? {})]
      .map((s) => (s && typeof s === "object" && "uri" in s ? (s as { uri?: string }).uri : undefined));
    for (const u of uris) if (u) ImageUriCache.add(u);
  }

  /**
   * Render until the page asks nothing new: each pass answers what the last asked (the hooks' own `run`s). A question
   * not settled in `waitMs` stays loading — the app's own waiting state for it, as on a slow line.
   */
  private async settle(el: ReactElement): Promise<void> {
    for (let i = 0; i < 12; i++) {
      const asked = new Map<string, () => Promise<unknown>>();
      setPrerenderAnswers({ answers: this.answers, asked });
      try { renderToStaticMarkup(el); } finally { setPrerenderAnswers(null); }
      if (!asked.size) return;
      await Promise.all([...asked].map(async ([k, run]) => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const late = new Promise<typeof LOADING>((r) => { timer = setTimeout(() => r(LOADING), this.waitMs); });
        try {
          const v = await Promise.race([run(), late]);
          this.answers.set(k, v === LOADING ? LOADING : { state: "ready", value: v, error: null });
        } catch (e) {
          this.answers.set(k, { state: "failed", value: null, error: e instanceof Error ? e : new Error(String(e)) });
        } finally { clearTimeout(timer); }
      }));
    }
    throw new Error("seo: the pane kept asking new questions after 12 renders");
  }

  /** One render, as react-native-web's own server render makes it (its app container, its class names). */
  private async render(item: ItemId, layout: LayoutClass, theme: "light" | "dark"): Promise<string> {
    const el = tree(this.p, item, layout, theme);
    await this.settle(el);
    drawn = el;
    const app = registry.getApplication(APP_KEY, {});
    setPrerenderAnswers({ answers: this.answers, asked: new Map() });
    try { return renderToString(app.element); } finally { setPrerenderAnswers(null); }
  }

  /** The water's pane: every layout as one tree (layouts.ts), themes merged, the header's ‹ and × linked to the map at this water. */
  async water(item: ItemId): Promise<string> {
    const byLayout = new Map<LayoutClass, string>();
    for (const layout of LAYOUTS) {
      const [light, dark] = [await this.render(item, layout, "light"), await this.render(item, layout, "dark")];
      let one = mergeThemes(light, dark, this.vars);
      if (one === null) {
        this.unmerged.push(`${item}:${layout}`);
        one = `<div class="th th-light">${light}</div><div class="th th-dark">${dark}</div>`;
      }
      byLayout.set(layout, one);
    }
    if (!this.shared) this.shared = new Set(this.answers.keys());
    else for (const k of [...this.answers.keys()]) if (!this.shared.has(k)) this.answers.delete(k);
    return linkHeader(mergeLayouts(byLayout, this.layoutStyles), item);
  }

  /**
   * The shared stylesheet of every water page: react-native-web's sheet (every class any render used), the theme
   * variables, the app's faces, and the fallback for a water shipped in both themes.
   */
  css(fontFaces: string): string {
    const app = registry.getApplication(APP_KEY, {});
    const rnw = renderToStaticMarkup(app.getStyleElement()).replace(/^<style[^>]*>/, "").replace(/<\/style>$/, "");
    const bg = this.vars.name(THEMES.light.card, THEMES.dark.card);
    return `${fontFaces}${this.vars.css()}:root{--cift-bg:var(${bg})}html,body{background:var(--cift-bg)}` +
      `.th{display:flex;flex:1;min-width:0}.th-dark{display:none}@media (prefers-color-scheme:dark){.th-light{display:none}.th-dark{display:flex}}\n${rnw}\n` +
      `${layoutCss()}${this.layoutStyles.css()}`;
  }

  /** The inlined part: the box the pane stands in. */
  static critical(): string { return PRERENDER_CSS; }
}

/** The @font-face rules for the app's faces, at the web export's own URLs (`font` maps a face to its served path). */
export function fontFaceCss(font: (face: string) => string | null): { css: string; missing: string[] } {
  const missing: string[] = [];
  const css = FACES_REQUIRED.map((f) => {
    const url = font(f);
    if (!url) { missing.push(f); return ""; }
    return `@font-face{font-family:"${f}";src:url(${url}) format("truetype");font-display:swap}`;
  }).join("");
  return { css, missing };
}

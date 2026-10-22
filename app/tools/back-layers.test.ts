/**
 * BACK, EVERYWHERE (owner 2026-10-21: "make sure android back button works for entire app where a user expects it
 * to") — the parts a render test cannot reach: ONE back mechanism, and no surface that back would go under.
 *
 *   one mechanism   Android's `BackHandler` is named only by apps/mobile/src/systemBack.native.ts; the History API
 *                   only by systemBack.web.ts; a Modal's `onRequestClose` is always the one back — the
 *                   `requestClose` its own component got from `useBackLayer(` (Sheet, the menu's drawer and page).
 *                   Nothing else listens for a back of its own — two listeners is a double back.
 *   every surface   each COMPONENT (not file: a registered neighbour clears nothing) that draws something back must
 *                   close first — a Modal, a dialog (`role="dialog"`, `aria-modal`, `accessibilityViewIsModal`), a
 *                   close/back prop (`onClose|onDismiss|onCancel|onBack|onHide|onDiscard|onExit`, as `onX:` or
 *                   `onX(): void`, on itself or on a props interface it names; a bare `close|hide|dismiss|cancel`
 *                   function), a name ending Sheet|Menu|Drawer|Dialog|Popover|Popup|Overlay|Viewer|Lightbox|Gallery|
 *                   Picker|Pop|Confirm|Prompt|Page, or open state of its own (`const [xOpen, setXOpen] = useState`) —
 *                   registers with the one back: it calls `useBackLayer(` or draws a `<Sheet>` (which registers for
 *                   it); open state must be handed to `useBackLayer(` or to a registered surface's element. A
 *                   component whose back another layer takes is listed in COVERED with why. A NEW SURFACE THAT DOES
 *                   NONE OF THESE FAILS HERE; see app/HANDOFF-ui.md "Back, everywhere" for how to register one.
 *
 * The scan is a net, not a proof: what a back DOES on each surface is pinned by the render tests
 * (apps/mobile/src/back.test.tsx, through the app's own BackHandler wiring).
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps/mobile/src", "apps/web/src", "packages/ui/src", "packages/ui-native/src", "packages/map/src"];

function sources(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
/** Comments out (a doc comment may name `BackHandler`); strings kept. */
const code = (src: string) => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
const read = (p: string) => code(readFileSync(join(APP, p), "utf8"));
const where = (re: RegExp) => files().filter((p) => re.test(read(p)));
/** The files that draw: components (.tsx). A hook (`useSheetDrag(onClose)`) draws nothing back could close. */
const drawing = () => files().filter((p) => p.endsWith(".tsx"));

/**
 * The scan works PER COMPONENT, not per file: one `useBackLayer` in a file must not clear another surface beside it.
 * A file is cut at its top-level declarations (column 0); a type or interface belongs to every component in the file
 * that names it (a props interface declared above its component), or stands alone when none does.
 */
export interface Decl { name: string; kind: "component" | "type" | "other"; src: string }
export function declarations(src: string): Decl[] {
  const head = /^(?:export\s+)?(?:default\s+)?(?:declare\s+)?(?:async\s+)?(function|const|let|var|class|interface|type|enum)\s+(\w+)/gm;
  const at: { i: number; kw: string; name: string }[] = [];
  for (const m of src.matchAll(head)) at.push({ i: m.index!, kw: m[1]!, name: m[2]! });
  return at.map((d, k) => ({
    name: d.name,
    kind: d.kw === "interface" || d.kw === "type" ? "type"
      : /^[A-Z]/.test(d.name) && d.kw !== "enum" ? "component" : "other",
    src: src.slice(d.i, k + 1 < at.length ? at[k + 1]!.i : src.length),
  }));
}

/** The names that mean a component can be closed or stepped back out of. */
const CLOSE = "Close|Dismiss|Cancel|Back|Hide|Discard|Exit";
/**
 * Why this declaration draws something back must close first, or null:
 *   a Modal; a dialog (`role="dialog"`, `aria-modal`, `accessibilityViewIsModal`); a prop or method that closes it
 *   (`onClose`/`onDismiss`/`onCancel`/`onBack`/`onHide`/`onDiscard`/`onExit`, as `onX:` or `onX(): void`, or a bare
 *   `close`/`hide`/`dismiss`/`cancel` typed as a function); or a component named like a surface. (Open state kept
 *   in a component and drawn inline is `openStates` below.)
 */
export function surfaceReason(src: string): string | null {
  if (/<Modal\b/.test(src)) return "a Modal";
  if (/role=["'](?:alert)?dialog["']|aria-modal|accessibilityViewIsModal/.test(src)) return "a dialog";
  const prop = new RegExp(`\\b(on(?:${CLOSE}))\\??\\s*(?::|\\(\\s*\\)\\s*:)`).exec(src)
    ?? /\b(close|hide|dismiss|cancel)\??\s*:\s*\(/.exec(src);
  if (prop) return `takes ${prop[1]}`;
  for (const m of src.matchAll(/(?:function|const|class)\s+([A-Z]\w*)/g))
    if (/(?:Sheet|Menu|Drawer|Dialog|Popover|Popup|Overlay|Viewer|Lightbox|Gallery|Picker|Pop|Confirm|Prompt|Page)$/.test(m[1]!))
      return `draws ${m[1]}`;
  return null;
}
/** Open state a component keeps (`const [menuOpen, setMenuOpen] = useState(…)`): something it opens and closes. */
export const openStates = (src: string) =>
  [...src.matchAll(/\[\s*(\w*(?:Open|Shown|Showing|Visible|Expanded))\s*,\s*\w+\s*\]\s*=\s*(?:React\.)?useState\b/g)]
    .map((m) => m[1]!);
/** Does this component register with the one back? */
export const registers = (src: string) => /\buseBackLayer\s*\(/.test(src) || /<Sheet\b/.test(src);
/** Is this open state handed to the one back — `useBackLayer(open…)`, or a registered surface's element? */
export function stateRegistered(src: string, v: string, surfaces: ReadonlySet<string>): boolean {
  if (new RegExp(`\\buseBackLayer\\s*\\([^;]*\\b${v}\\b`).test(src)) return true;
  for (const m of src.matchAll(/<([A-Z]\w*)\b[^>]*>/g))
    if (surfaces.has(m[1]!) && new RegExp(`\\b${v}\\b`).test(m[0])) return true;
  return [...surfaces].some((n) => new RegExp(`\\b${v}\\s*(?:&&|\\?)\\s*\\(?\\s*<${n}\\b`).test(src));
}

/** Every unregistered surface in one file, `path#Name: why`; `surfaces` are the component names that register. */
export function unregistered(path: string, src: string, surfaces: ReadonlySet<string>): string[] {
  const decls = declarations(src);
  const comps = decls.filter((d) => d.kind === "component");
  const out: string[] = [];
  for (const d of decls) {
    if (d.kind === "other") continue;
    const why = surfaceReason(d.src);
    if (d.kind === "type") {
      if (!why) continue;
      const users = comps.filter((c) => new RegExp(`\\b${d.name}\\b`).test(c.src));
      if (users.length === 0) out.push(`${path}#${d.name}: ${why}, used by no component here`);
      for (const c of users) if (!registers(c.src)) out.push(`${path}#${c.name}: ${why} (${d.name}), no useBackLayer`);
      continue;
    }
    if (why && !registers(d.src)) out.push(`${path}#${d.name}: ${why}, no useBackLayer`);
    for (const v of openStates(d.src))
      if (!stateRegistered(d.src, v, surfaces)) out.push(`${path}#${d.name}: opens ${v}, not handed to useBackLayer or a registered surface`);
  }
  return [...new Set(out)];
}
/** The component names, across the app, that register with the one back (Sheet among them). */
const registeredSurfaces = () => new Set(drawing().flatMap((p) =>
  declarations(read(p)).filter((d) => d.kind === "component" && registers(d.src)).map((d) => d.name)).concat("Sheet"));

/**
 * Surfaces whose back another layer already takes, keyed `path#Component`, each with its reason. An entry here is a
 * decision, said in app/HANDOFF-ui.md's table — not a way past the scan.
 */
const COVERED: Record<string, string> = {
  "packages/ui-native/src/Shell.tsx#Shell: takes onBack": "the Shell IS the route layer (useEdgeBack); `onBack: back` hands that one back to the water's ‹",
  "packages/ui-native/src/WaterScreen.tsx#WaterScreen: takes onBack": "the on-screen ‹; system back is the Shell's route layer (water → tab)",
  "packages/ui-native/src/WaterFrame.tsx#WaterFrame: takes onBack": "T44: hands the on-screen ‹ to the FaceBar it draws (a view mounted alone); system back is the Shell's route layer",
  "packages/ui-native/src/MapScreen.tsx#MapScreen: takes onBack": "the on-screen ‹; system back is the Shell's route layer",
  "packages/ui-native/src/Faces.tsx#FaceBar: takes onBack": "the water header's ‹ button; system back is the Shell's route layer",
  "packages/ui-native/src/ViewSwipe.tsx#WaterViewsContext: takes onBack (WaterViews)": "carries the Shell's back to the header's ‹ and the edge swipe; not a surface",
  "packages/ui-native/src/water/WaterMapView.tsx#Head: takes onClose": "a header's × button; MarkPop and Chooser, which draw it, register",
  "packages/ui-native/src/SpotScreen.tsx#Confirm: takes onCancel": "SpotScreen's layer answers back while a confirm is shown (keep)",
  "packages/ui-native/src/SpotCapture.tsx#WhenScreen: takes onBack": "a capture step; SpotCapture's layer steps back from it",
  "packages/ui-native/src/SpotCapture.tsx#Coach: takes onCancel": "the capture's banner; SpotCapture's layer is its back",
  "packages/ui-native/src/menu/InfoPages.tsx#AboutPage: draws AboutPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/InfoPages.tsx#SourcesPage: draws SourcesPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/InfoPages.tsx#ContactPage: draws ContactPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/InfoPages.tsx#DisplayPage: draws DisplayPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/ReportPage.tsx#ReportPage: draws ReportPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/MenuPage.tsx#MenuPageHead: takes onBack": "the page header's ‹ button; its MenuPage registers (useBackLayer)",
};
/** A finding's key: `path#Component: why` (one component can be covered for one reason and not another). */
const keyOf = (m: string) => m.split(", ")[0]!;

describe("back, everywhere: one mechanism", () => {
  it("Android's BackHandler is named only by the app's systemBack.native.ts", () => {
    expect(where(/\bBackHandler\b/)).toEqual(["apps/mobile/src/systemBack.native.ts"]);
  });

  it("the browser's history is touched only by the app's systemBack.web.ts", () => {
    expect(where(/\bpopstate\b|\bpushState\b|\breplaceState\b|history\.(?:go|back|forward)\s*\(/))
      .toEqual(["apps/mobile/src/systemBack.web.ts"]);
  });

  it("a Modal's own back (onRequestClose) is always the one back: the requestClose its component got from useBackLayer", () => {
    const wired = (src: string) => /\bconst requestClose = useBackLayer\(/.test(src)
      && [...src.matchAll(/\bonRequestClose\s*=\s*\{([^}]*)\}/g)].every((m) => m[1]!.trim() === "requestClose");
    const users = where(/\bonRequestClose\b/);
    expect([users, users.filter((p) => !wired(read(p)))]).toEqual([[
      "packages/ui-native/src/Sheet.tsx", "packages/ui-native/src/menu/MenuDrawer.tsx", "packages/ui-native/src/menu/MenuPage.tsx"], []]);
  });
});

describe("back, everywhere: every surface registers a back layer", () => {
  it("every component that draws a sheet, menu, page, dialog or overlay registers it (useBackLayer, or a <Sheet>)", () => {
    const surfaces = registeredSurfaces();
    const missing = drawing().flatMap((p) => unregistered(p, read(p), surfaces))
      .filter((m) => !(keyOf(m) in COVERED));
    expect(missing).toEqual([]);
  });

  it("every COVERED entry still names a surface (a stale one would hide the next)", () => {
    const surfaces = registeredSurfaces();
    const found = new Set(drawing().flatMap((p) => unregistered(p, read(p), surfaces)).map(keyOf));
    expect(Object.keys(COVERED).filter((k) => !found.has(k))).toEqual([]);
  });

  it("the surfaces found today, so the net is seen to catch something", () => {
    const found = drawing().flatMap((p) => declarations(read(p)).flatMap((d) => {
      if (d.kind !== "component") return [];
      const w = surfaceReason(d.src) ?? (openStates(d.src)[0] ? `opens ${openStates(d.src)[0]}` : null);
      return w ? [`${p}#${d.name}: ${w}`] : [];
    }));
    expect(found).toEqual([
      "packages/ui-native/src/Faces.tsx#FaceBar: takes onBack",
      "packages/ui-native/src/LayersSheet.tsx#LayersSheet: takes onClose",
      "packages/ui-native/src/MapScreen.tsx#MapScreen: takes onBack",
      "packages/ui-native/src/Sheet.tsx#Sheet: a Modal",
      "packages/ui-native/src/Shell.tsx#Shell: takes onBack",
      "packages/ui-native/src/SpotCapture.tsx#WhenScreen: takes onBack",
      "packages/ui-native/src/SpotCapture.tsx#Coach: takes onCancel",
      "packages/ui-native/src/SpotDropSheet.tsx#SpotDropSheet: takes onCancel",
      "packages/ui-native/src/SpotScreen.tsx#SpotScreen: takes onBack",
      "packages/ui-native/src/SpotScreen.tsx#Confirm: takes onCancel",
      "packages/ui-native/src/SpotsSheet.tsx#SpotsSheet: takes onClose",
      "packages/ui-native/src/WaterFrame.tsx#WaterFrame: takes onBack",
      "packages/ui-native/src/WaterScreen.tsx#WaterScreen: takes onBack",
      "packages/ui-native/src/menu/GlossaryPage.tsx#GlossaryPage: draws GlossaryPage",
      "packages/ui-native/src/menu/InfoPages.tsx#AboutPage: draws AboutPage",
      "packages/ui-native/src/menu/InfoPages.tsx#SourcesPage: draws SourcesPage",
      "packages/ui-native/src/menu/InfoPages.tsx#ContactPage: draws ContactPage",
      "packages/ui-native/src/menu/InfoPages.tsx#DisplayPage: draws DisplayPage",
      "packages/ui-native/src/menu/MenuDrawer.tsx#MenuDrawer: a Modal",
      "packages/ui-native/src/menu/MenuPage.tsx#MenuPage: a Modal",
      "packages/ui-native/src/menu/MenuPage.tsx#MenuPageHead: takes onBack",
      "packages/ui-native/src/menu/ReportPage.tsx#ReportPage: draws ReportPage",
      "packages/ui-native/src/regs/DaySheet.tsx#DaySheet: takes onClose",
      "packages/ui-native/src/water/WaterMapView.tsx#Head: takes onClose",
      "packages/ui-native/src/water/WaterMapView.tsx#MarkPop: a dialog",
      "packages/ui-native/src/water/WaterMapView.tsx#Chooser: a dialog",
    ]);
  });

  it("MUTATION PIN: the shapes the map-rail and menu lanes will build are caught; registering clears them", () => {
    const S = new Set(["Sheet", "LayersSheet"]);
    const cases: Record<string, string> = {
      menu: `export function SpotsMenu({ open, onClose }: { open: boolean; onClose: () => void }) {
        return open ? <View role="dialog"><Pressable onPress={onClose} /></View> : null;
      }`,
      sheet: "export function GlossarySheet({ term }: { term: string }) { return <View>{term}</View>; }",
      pop: "const LongPressPop = ({ at }: { at: number }) => <View />;",
      card: "export function AddHereCard({ at, onCancel }: { at: number; onCancel: () => void }) { return <View />; }",
      hamburger: "export const AppMenu = ({ open }: { open: boolean }) => (open ? <View /> : null);",
      // reviewer 2026-10-21: the hamburger's pages, other close props, method signatures, a props interface
      aboutPage: "export function AboutPage({ onBack }: { onBack: () => void }) { return <View />; }",
      glossaryPage: "export function GlossaryPage({ onBack }: { onBack: () => void }) { return <View />; }",
      spotsPanel: "export function SpotsPanel({ open, onHide }: { open: boolean; onHide: () => void }) { return <View />; }",
      discardCard: "export function AddHereCard({ onDiscard }: { onDiscard: () => void }) { return <View />; }",
      gallery: "export function PhotoGallery({ uri, close }: { uri: string; close: () => void }) { return <View />; }",
      method: "interface RailProps { onClose(): void }\nexport function Rail(p: RailProps) { return null; }",
      inline: `export function Shell() {
  const [menuOpen, setMenuOpen] = useState(false);
  return <>{menuOpen && <View><Pressable onPress={() => setMenuOpen(false)} /></View>}</>;
}`,
    };
    const caught = Object.fromEntries(Object.entries(cases).map(([k, s]) => [k, unregistered("x.tsx", s, S).length > 0]));
    expect(caught).toEqual(Object.fromEntries(Object.keys(cases).map((k) => [k, true])));

    // per component: a registered neighbour does not clear an unregistered one in the same file
    const pair = `export function MarkPop({ open, onClose }: { open: boolean; onClose: () => void }) {
  useBackLayer(open, onClose);
  return <View role="dialog" />;
}
export function AboutPage({ onBack }: { onBack: () => void }) { return <View />; }`;
    expect(unregistered("x.tsx", pair, S)).toEqual(["x.tsx#AboutPage: takes onBack, no useBackLayer"]);

    // registering clears each: useBackLayer, a <Sheet>, or open state handed to a registered surface
    const fixed = cases.menu!.replace("return open", "useBackLayer(open, onClose);\n        return open");
    expect(unregistered("x.tsx", fixed, S)).toEqual([]);
    expect(unregistered("x.tsx", cases.method!.replace("return null", "useBackLayer(true, p.onClose); return null"), S)).toEqual([]);
    expect(unregistered("x.tsx", cases.inline!.replace("useState(false);", "useState(false);\n  useBackLayer(menuOpen, () => setMenuOpen(false));"), S)).toEqual([]);
    expect(unregistered("x.tsx", `export function Shell() {
  const [layersOpen, setLayersOpen] = useState(false);
  return <LayersSheet open={layersOpen} onClose={() => setLayersOpen(false)} />;
}`, S)).toEqual([]);
    expect(unregistered("x.tsx", 'export function GlossarySheet() { return <Sheet open={open} onClose={onClose} title="Glossary" palette={palette}>x</Sheet>; }', S)).toEqual([]);
  });
});

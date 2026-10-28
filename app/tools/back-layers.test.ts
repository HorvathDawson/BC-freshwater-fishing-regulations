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
import type { ItemId, SectionId } from "@app/data";
import { HOME, crossAction, exitAction, openWater, partAction, reduce as reduceRoute, showRegsActions, viewSwipeAction,
         type Route, type RouteAction } from "@app/ui";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps/mobile/src", "packages/ui/src", "packages/ui-native/src", "packages/map/src"];

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
  "packages/ui-native/src/WaterScreen.tsx#WaterScreen: takes onBack": "the on-screen ‹; system back is the Shell's route layer (water → tab)",
  "packages/ui-native/src/WaterFrame.tsx#WaterFrame: takes onBack": "T44: hands the on-screen ‹ to the FaceBar it draws (a view mounted alone); system back is the Shell's route layer",
  "packages/ui-native/src/TopBar.tsx#TopBar: takes onClose (TopBarProps)": "T48: the field's ‹ and Escape; search open is ROUTE state (route `search`), so system back closes it through the Shell's route layer, first",
  "packages/ui-native/src/Faces.tsx#FaceBar: takes onBack": "the water header's ‹ button; system back is the Shell's route layer",
  "packages/ui-native/src/ViewSwipe.tsx#WaterViewsContext: takes onBack (WaterViews)": "carries the Shell's back to the header's ‹ and the edge swipe; not a surface",
  "packages/ui-native/src/water/WaterMapView.tsx#Head: takes onClose": "a header's × button; MarkPop and Chooser, which draw it, register",
  "packages/ui-native/src/spot/Confirm.tsx#Confirm: takes onCancel": "SpotScreen's layer answers back while a confirm is shown (keep)",
  "packages/ui-native/src/SpotCapture.tsx#WhenScreen: takes onBack": "a capture step; SpotCapture's layer steps back from it",
  "packages/ui-native/src/SpotCapture.tsx#Coach: takes onCancel": "the capture's banner; SpotCapture's layer is its back",
  "packages/ui-native/src/menu/InfoPages.tsx#AboutPage: draws AboutPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/InfoPages.tsx#SourcesPage: draws SourcesPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/ReportPage.tsx#ReportPage: draws ReportPage": "a menu page's CONTENT: MenuHost draws it inside <MenuPage>, which registers (useBackLayer)",
  "packages/ui-native/src/menu/MenuPage.tsx#MenuPageHead: takes onBack": "the page header's ‹ button; its MenuPage registers (useBackLayer)",
  "packages/ui-native/src/menu/MoreScreen.tsx#MoreScreen: takes onBack (MoreScreenProps)": "owner 2026-10-22: the More TAB's ‹ to the tab it came from — a route tab (`prev`), not a layer: the Shell hands it the ONE back (`useBack`), which the system back also takes",
  "packages/ui-native/src/menu/MoreScreen.tsx#Head: takes onBack": "the More tab's ‹ button; MoreScreen hands it the Shell's one back",
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
      "packages/ui-native/src/Sheet.tsx", "packages/ui-native/src/menu/MenuPage.tsx"], []]);
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
      // (T173) a tapped fish obstacle mark's sheet, drawn by the Shell over the map: a <Sheet>, so it registers
      "packages/ui-native/src/ObstacleSheet.tsx#ObstacleSheet: takes onClose",
      "packages/ui-native/src/SearchSheet.tsx#SearchSheet: takes onClose",
      "packages/ui-native/src/Sheet.tsx#Sheet: a Modal",
      // (T62 3.4) the route layer's `onBack: back` moved into shell/useShellNav (a hook); the Shell is now found by the
      // sheet it opens itself (Layers, the map's day), each handed to its registered <Sheet>
      "packages/ui-native/src/Shell.tsx#Shell: opens dayOpen",
      "packages/ui-native/src/SpotCapture.tsx#WhenScreen: takes onBack",
      "packages/ui-native/src/SpotCapture.tsx#Coach: takes onCancel",
      "packages/ui-native/src/SpotDropSheet.tsx#SpotDropSheet: takes onCancel",
      "packages/ui-native/src/SpotPickBanner.tsx#SpotPickBanner: takes onCancel",
      "packages/ui-native/src/TidalSheet.tsx#TidalSheet: takes onClose",
      // (T60) SpotScreen read "takes onBack" only from its bottom row's `? p.onBack : leaveEdit` ternary; that second
      // Back button is gone (one surface per action). It still registers (`useBackLayer`) and back.test drives it.
      "packages/ui-native/src/WaterFrame.tsx#WaterFrame: takes onBack",
      "packages/ui-native/src/WaterScreen.tsx#WaterScreen: takes onBack",
      "packages/ui-native/src/menu/GlossaryPage.tsx#GlossaryPage: draws GlossaryPage",
      "packages/ui-native/src/menu/InfoPages.tsx#AboutPage: draws AboutPage",
      "packages/ui-native/src/menu/InfoPages.tsx#SourcesPage: draws SourcesPage",
      "packages/ui-native/src/menu/MenuPage.tsx#MenuPage: a Modal",
      "packages/ui-native/src/menu/MenuPage.tsx#MenuPageHead: takes onBack",
      "packages/ui-native/src/menu/MoreScreen.tsx#Head: takes onBack",
      "packages/ui-native/src/menu/ReportPage.tsx#ReportPage: draws ReportPage",
      "packages/ui-native/src/regs/DaySheet.tsx#DaySheet: takes onClose",
      "packages/ui-native/src/spot/Confirm.tsx#Confirm: takes onCancel",
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

/**
 * BACK STEPS OUT ONE LAYER (owner 2026-10-22 late: "it gets extremely hard to go from the regulation, condition, map
 * page for a specific body back to the main view"). Back never undoes a move: every move within one water — a part
 * step, a view through the strip or the swipe, "Show regulations for this part", a split lake's other closure area,
 * the same water tapped again, and ▲ / ▼ across a name change into the water this one continues as / from (owner
 * 2026-10-22 late: "one system", the Vedder ↔ Chilliwack) — REPLACES its frame; a DIFFERENT water opened from inside
 * ("Flows into" from a tributary, a tap) is one layer, and a water
 * already under it is gone back to rather than stacked again; close leaves every water at once. The browser's
 * history and Android's back count the same frames (`backDepth` is the reducer's own), so pinning the route pins all
 * three inputs; apps/mobile/src/back.test.tsx drives the same walks through the real Shell on Android and the web.
 *
 * The walks use the app's own action builders (`partAction`, `showRegsActions`, `crossAction`, `exitAction`,
 * `viewSwipeAction`, `openWater`), so a builder that starts pushing fails here as surely as the reducer.
 */
type Reduce = (r: Route, a: RouteAction) => Route;
type Move = RouteAction | readonly RouteAction[] | ((r: Route) => RouteAction | readonly RouteAction[] | null);
const VEDDER = "gnis:3062" as ItemId, CHW = "gnis:8634" as ItemId, TRIB = "gnis:11481" as ItemId;
const LAKE_A = "wbk:-21" as ItemId, LAKE_B = "wbk:-22" as ItemId;
const onScreen = (r: Route) => (r.mode === "water" ? r.stack[r.stack.length - 1]!.item! : null)!;

/** Every walk the rule is about, as one whole value: the stack after each move and where ONE back (or close) lands. */
export function layerWalks(reduce: Reduce) {
  const name = (r: Route) => r.mode === "water"
    ? r.stack.map((f) => `${f.item}@${f.part ?? "-"}/${f.view}`).join(" > ")
    : `main ${r.tab}${r.search ? " +search" : ""}`;
  const step = (r: Route, m: Move): Route => {
    const a = typeof m === "function" ? m(r) : m;
    return a === null ? r : (Array.isArray(a) ? a : [a as RouteAction]).reduce(reduce, r);
  };
  const walk = (start: Route, moves: readonly Move[]) => {
    const seen = [name(start)];
    let r = start;
    for (const m of moves) { r = step(r, m); seen.push(name(r)); }
    return { r, seen };
  };
  const BACK: RouteAction = { type: "back" }, CLOSE: RouteAction = { type: "close" };
  const from: Route = { ...HOME, tab: "conditions", search: true };
  const a = reduce(from, openWater({ item: VEDDER, tapSid: 5 as SectionId, view: "conditions" }));

  const inside = walk(a, [
    (r) => partAction(onScreen(r), VEDDER, 1), { type: "view", view: "regs" },
    (r) => viewSwipeAction(r, "next"), (r) => viewSwipeAction(r, "next"),
    (r) => partAction(onScreen(r), VEDDER, 2), (r) => showRegsActions(onScreen(r), VEDDER, 0),
    openWater({ item: VEDDER, tapSid: 6 as SectionId, view: "map" }),
  ]);
  const lake = walk(reduce(from, openWater({ item: LAKE_A, part: 0, view: "map" })), [
    (r) => partAction(onScreen(r), LAKE_B, 0), (r) => showRegsActions(onScreen(r), LAKE_A, 1),
  ]);
  const cross = walk(a, [crossAction(CHW, 0), crossAction(VEDDER, 0), crossAction(CHW, 0)]);
  // (owner 2026-10-22 late, "one system") a tributary's mouth on the Chilliwack is a layer; the Chilliwack ↔ Vedder
  // crossings above it replace each other, so one back leaves the system for the tributary it was entered from
  const system = walk(reduce(from, openWater({ item: TRIB, view: "regs" })),
                      [exitAction(CHW, 0), crossAction(VEDDER, 0), crossAction(CHW, 0)]);
  const chain = walk(a, [openWater({ item: TRIB, tapSid: 9 as SectionId, view: "regs" }),
                         (r) => partAction(onScreen(r), TRIB, 1), { type: "view", view: "map" }, BACK, BACK]);
  const revisit = walk(a, [exitAction(CHW, 2), openWater({ item: TRIB, view: "regs" }), exitAction(VEDDER, 0)]);
  const close = walk(a, [openWater({ item: TRIB, view: "regs" }), exitAction(CHW, 0), CLOSE]);
  // (T58) a split lake is ONE water: area A (opened from search, its group unknown), a tributary, then "Flows into"
  // lands in area B (the opener knows B's areas) — the lake gone back to, never held twice; one back is main
  const splitLake = walk(reduce(from, openWater({ item: LAKE_A, part: 0, view: "regs" })),
                         [openWater({ item: TRIB, tapSid: 9 as SectionId, view: "regs" }), exitAction(LAKE_B, 0, [LAKE_A, LAKE_B])]);
  return {
    inside: [...inside.seen, `one back: ${name(reduce(inside.r, BACK))}`],
    lake: [...lake.seen, `one back: ${name(reduce(lake.r, BACK))}`],
    cross: [...cross.seen, `one back: ${name(reduce(cross.r, BACK))}`],
    system: [...system.seen, `one back: ${name(reduce(system.r, BACK))}`],
    chain: chain.seen,
    revisit: [...revisit.seen, `one back: ${name(reduce(revisit.r, BACK))}`],
    close: close.seen,
    splitLake: [...splitLake.seen, `one back: ${name(reduce(splitLake.r, BACK))}`],
  };
}

const EXPECTED_WALKS = {
  inside: [
    "gnis:3062@-/conditions", "gnis:3062@1/conditions", "gnis:3062@1/regs",
    "gnis:3062@1/conditions", "gnis:3062@1/map", "gnis:3062@2/map", "gnis:3062@0/regs", "gnis:3062@-/map",
    "one back: main conditions +search"],
  lake: ["wbk:-21@0/map", "wbk:-22@0/map", "wbk:-21@1/regs", "one back: main conditions +search"],
  cross: ["gnis:3062@-/conditions", "gnis:8634@0/map", "gnis:3062@0/map", "gnis:8634@0/map",
          "one back: main conditions +search"],
  system: ["gnis:11481@-/regs", "gnis:11481@-/regs > gnis:8634@0/regs", "gnis:11481@-/regs > gnis:3062@0/map",
           "gnis:11481@-/regs > gnis:8634@0/map", "one back: gnis:11481@-/regs"],
  chain: ["gnis:3062@-/conditions", "gnis:3062@-/conditions > gnis:11481@-/regs", "gnis:3062@-/conditions > gnis:11481@1/regs",
          "gnis:3062@-/conditions > gnis:11481@1/map", "gnis:3062@-/conditions", "main conditions +search"],
  revisit: ["gnis:3062@-/conditions", "gnis:3062@-/conditions > gnis:8634@2/regs",
            "gnis:3062@-/conditions > gnis:8634@2/regs > gnis:11481@-/regs", "gnis:3062@0/regs",
            "one back: main conditions +search"],
  close: ["gnis:3062@-/conditions", "gnis:3062@-/conditions > gnis:11481@-/regs",
          "gnis:3062@-/conditions > gnis:11481@-/regs > gnis:8634@0/regs", "main conditions +search"],
  splitLake: ["wbk:-21@0/regs", "wbk:-21@0/regs > gnis:11481@-/regs", "wbk:-22@0/regs", "one back: main conditions +search"],
};

describe("back steps out one layer (owner 2026-10-22 late)", () => {
  it("N moves inside one water then ONE back is where it was opened from; A -> B, back is A, back is main; close is main", () => {
    expect(layerWalks(reduceRoute)).toEqual(EXPECTED_WALKS);
  });

  it("MUTATION PIN: a section change, a view change, a revisit or a cross into the same system that pushes again — or " +
     "a close that only backs — fails", () => {
    const top = (r: Route & { mode: "water" }) => r.stack[r.stack.length - 1]!;
    const pushing = (when: RouteAction["type"], patch: (a: RouteAction) => object): Reduce => (r, a) =>
      r.mode === "water" && a.type === when ? { ...r, stack: [...r.stack, { ...top(r), ...patch(a) }] } : reduceRoute(r, a);
    const mutants: Record<string, Reduce> = {
      partPushes: pushing("part", (a) => (a.type === "part" ? { part: a.part, ...(a.item ? { item: a.item } : {}) } : {})),
      viewPushes: pushing("view", (a) => (a.type === "view" ? { view: a.view } : {})),
      revisitStacks: (r, a) => (r.mode === "water" && a.type === "openWater"
        ? { ...r, stack: [...r.stack, { item: a.item, part: a.part ?? null, tapSid: a.tapSid ?? null, at: null, view: a.view ?? "regs" }] }
        : reduceRoute(r, a)),
      closeIsBack: (r, a) => reduceRoute(r, a.type === "close" ? { type: "back" } : a),
      // the cross as it was before the owner's "one system" call: the other water opened on top, a layer
      crossPushes: (r, a) => reduceRoute(r, a.type === "cross" ? openWater({ item: a.item, part: a.part, view: "map" }) : a),
      // (T58) the route blind to a split lake's areas: another area of a lake held is a new water, stacked again
      groupBlind: (r, a) => reduceRoute(r, a.type === "openWater" ? { ...a, group: undefined } : a),
    };
    const broken = Object.fromEntries(Object.entries(mutants).map(([k, m]) => {
      const w = layerWalks(m);
      return [k, (Object.keys(EXPECTED_WALKS) as (keyof typeof EXPECTED_WALKS)[])
        .filter((j) => JSON.stringify(w[j]) !== JSON.stringify(EXPECTED_WALKS[j]))];
    }));
    expect(broken).toEqual({
      partPushes: ["inside", "lake", "chain"],
      viewPushes: ["inside", "lake", "chain"],
      revisitStacks: ["inside", "revisit", "splitLake"],
      closeIsBack: ["close"],
      crossPushes: ["cross", "system"],
      groupBlind: ["splitLake"],
    });
  });
});

/**
 * WHAT A LAYER IS PAINTED WITH, for one mode — the paint functions and the runtime layers' ids and constants that the
 * style composer (`runtime-style.ts`, the first frame) and the adapter (`adapters/contract.ts`, every frame after it)
 * share. One module, so the first frame and every frame after it are painted the same way.
 */
import type { GearMark } from "@app/core";
import { MAP_STYLE, NO_VALUES, STYLE_META, colorExpression, finfishOver, profileChannel, profileDay, salmonLabelIn,
         type ChannelValues, type Tokens } from "./style";

/**
 * (T796 A6) `shown` held off the sections DFO's finfish closures paint (`finfishOver`: closed, or isolated away) on a
 * status profile mode — a closed section has no access dots, gear marks or order fade. `shown` itself on every other day.
 */
function offFinfish(m: Parameters<typeof finfishOver>[0], values: ChannelValues | null, shown: unknown): unknown {
  const fin = finfishOver(m, values);
  return fin ? ["all", shown, ["!", salmonLabelIn([...fin.closed, ...fin.none])]] : shown;
}

/**
 * "MEASURED HERE, BUT NOTHING TO COMPARE IT TO" — the dashed line over a stream whose standing is the -1 sentinel
 * (`runtime-style.ts`). A runtime layer outside the catalogue, so the adapter paints it with the stream's own mode:
 * the `standing` mode reads the number the colour reads, every other mode hides it.
 */
export const NOBASELINE_LAYER = "stream-nobaseline";

/**
 * WHAT IS SELECTED IS OUTLINED, NEVER RECOLOURED (owner 2026-10-21, on the water map's parts: "we should have the color
 * there still but add outline for the current selected one"). The selected sections keep the colour their mode gives
 * them — the regulation status on the Map tab, the standing on Conditions — and are ringed in `color.highlight` (the
 * water you tapped or searched for; held apart from every status colour by tools/water-status-cvd.test.ts, and read on
 * the light and dark grounds and on satellite): a CASING under the stream line, a RING on a lake's or a wetland's
 * outline. Runtime layers (`runtime-style.ts`) on the sections' own source-layers, shown by the same feature-state
 * `selected` the one `highlight` call sets — no second mechanism — and painted with the stream (`paint` below), so a
 * theme or mode change repaints them in the same call.
 */
export const SELECTION_CASING = "selection-casing";
/**
 * HOW A HIGHLIGHT MARKS WHAT IT LIGHTS — one decision, this type (T151, owner 2026-10-23: "the bold in the section map
 * view is also weird and makes the reg legend hard to follow"):
 *   "outline"  a tapped or searched water on the browse map: feature-state `selected` — the casing / ring above, and a
 *              width floor (`width.stream.highlight`).
 *   "dim"      the part you are on in a WATER'S MAP VIEW: feature-state `part` — no casing, no ring, no thicker line; the
 *              water's other parts (`water`) are drawn at `opacity.water.others`, and the part and the others share one
 *              width floor (`width.stream.water`), so every part reads against the legend exactly as the legend draws it.
 */
export type SelectionCue = "outline" | "dim";
const PART = ["boolean", ["feature-state", "part"], false];
const WATER = ["boolean", ["feature-state", "water"], false];
export const SELECTION_RINGS = { lake: "selection-ring-lake", wetland: "selection-ring-wetland" } as const;
/** The outline each side of a selected stream line, and a selected polygon's ring, in px. */
export const CASING_PX = 2.5;
export const RING_PX = 3;
const SELECTED = ["boolean", ["feature-state", "selected"], false];
/** (T518) a section an in-season change on the water's map covers (`MapProps.inSeason`) */
const INSEASON = ["boolean", ["feature-state", "inseason"], false];
/**
 * (T203, T214) THE PART'S HALO — the selection's own casing/ring (`OUTLINE_SHOWN` lights both). Dimming the others alone did not read when the water is amber (own rules) or any colour
 * close to its dimmed self, so the part you are on is also ringed in `color.highlight`. T203 drew it at HALF the outline
 * and the owner still could not pick the part out (T214, 2026-10-27: "much more obvious, selected vs others"), so it is
 * now the browse map's FULL outline (`CASING_PX` each side, `RING_PX` on a lake or wetland) and the others sit lower
 * (`opacity.water.others` 0.4 -> 0.25). Still no width floor and no thicker line for the part itself (T151's "no bold"):
 * the water keeps the legend's width; only its outline grows. tools/water-status-cvd.test.ts holds highlight apart from
 * every status, cvd included.
 */
const OUTLINE_SHOWN = ["case", SELECTED, 1, PART, 1, INSEASON, 1, 0];
/** (T518) the outline's colour: the attention ink (the in-season banner's bar) on a section an in-season change covers,
 *  else the highlight's */
const outlineColour = (tokens: Tokens) => ["case", INSEASON, tokens["color.ui.attention"], tokens["color.highlight"]];

/** (T735) the part you are on, or a selected stream */
const ON = ["any", SELECTED, PART];
/** (T735) the casing's px each side (`width.selection.casing`: light 2.5, dark 1.5) */
const casingPx = (tokens: Tokens): number => Number(tokens["width.selection.casing"]);
/** (T735, look C) `flag.selection.edge` 1: the part's casing is the ground's colour, the status carries; 0: the highlight ring */
const edgeLook = (tokens: Tokens): boolean => Number(tokens["flag.selection.edge"]) === 1;
/** (T735) the soft glow layer outside the part's casing — drawn by a theme with `opacity.selection.glow` above 0 only */
export const SELECTION_GLOW = "selection-glow";
export const hasGlow = (tokens: Tokens): boolean => Number(tokens["opacity.selection.glow"]) > 0;

/** The stream casing's paint: the stream's own width (`streamWidth`, with its floors) plus the outline each side. */
export function casingPaint(streamWidth: unknown, tokens: Tokens): Record<string, unknown> {
  const px = casingPx(tokens);
  const paint: Record<string, unknown> = { "line-color": outlineColour(tokens),
           "line-width": withinZoomCurve(streamWidth, (v) => ["+", v, 2 * px]),
           "line-opacity": OUTLINE_SHOWN };
  if (edgeLook(tokens)) {
    // look C: the part is edged in the ground's colour; an in-season casing stays on the OTHER parts, lowered
    paint["line-color"] = ["case", ON, tokens["color.selection.edge"], tokens["color.ui.attention"]];
    paint["line-opacity"] = ["case", ON, 1, ["all", INSEASON, WATER], Number(tokens["opacity.selection.casing.others"]), OUTLINE_SHOWN];
  }
  return paint;
}

/** (T735, look C) the soft glow outside the part's casing; opacity 0 elsewhere */
export function glowPaint(streamWidth: unknown, tokens: Tokens): Record<string, unknown> {
  const px = casingPx(tokens);
  return { "line-color": tokens["color.ui.attention"], "line-blur": 6,
           "line-width": withinZoomCurve(streamWidth, (v) => ["+", v, 2 * px + 12]),
           "line-opacity": ["case", ON, Number(tokens["opacity.selection.glow"]), 0] };
}

/** A selected lake's or wetland's ring. */
export function ringPaint(tokens: Tokens): Record<string, unknown> {
  return { "line-color": outlineColour(tokens), "line-width": RING_PX, "line-opacity": OUTLINE_SHOWN };
}

/**
 * (T175) THE WATER COPY'S LINE (`water-copy.ts`): the stream's own paint for its mode — colour, width, opacity, every
 * feature-state it reads — with the width floored at the highlight's (`width.stream.highlight`): the copy is drawn as a
 * highlight, at zooms where the water's own width is a hairline. Inside the zoom curve, as every floor here is.
 */
export function copyLinePaint(stream: Record<string, unknown>, tokens: Tokens): Record<string, unknown> {
  const floor = Number(tokens["width.stream.highlight"]);
  const w = stream["line-width"];
  return { ...stream, ...(w !== undefined && Number.isFinite(floor)
    ? { "line-width": withinZoomCurve(w, (v) => ["max", v, floor]) } : {}) };
}

/** (T175) A copied lake's fill: the lake's own paint, its fill properties only (a lake's paint is a fill's). */
export function copyFillPaint(lake: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(lake).filter(([k]) => k.startsWith("fill-") && k !== "fill-pattern"));
}

/** The dash's opacity for the stream layer in `mode`: 1 where the stream's standing is the -1 sentinel, else 0. */
export function nobaselineOpacity(mode: string): unknown {
  return mode === "standing" ? ["case", ["==", ["to-number", ["feature-state", "standing"], 0], -1], 1, 0] : 0;
}

/**
 * (T165) NO PUBLIC ACCESS IS A DOTTED GREY LINE (owner 2026-10-23: "maybe the stream color through no access areas should
 * be dotted grey? like under lakes but a little more visible?"). `line-dasharray` cannot vary per feature, so — like
 * `stream-nobaseline` — the dots are their own runtime layer over every stream (`runtime-style.ts`), shown only where
 * the stream's mode paints the section `no_access`, and the `stream` layer itself is hidden there (`paintFor`), so the
 * gaps are gaps. Which sections those are is the colouring's own answer: the profile mode's day (@app/core
 * `dayProfiles`, `no_access` labels) or the categorical mode's `no_access` member — never a second test of access.
 */
export const NOACCESS_LAYER = "stream-noaccess";

/** Where `mode` paints a stream section `no_access`, as a boolean expression; null where the mode never does. */
export function noAccessShown(layerId: string, mode: string, values: ChannelValues | null = NO_VALUES): unknown | null {
  const m = STYLE_META.colorModes[layerId]?.[mode];
  if (!m || !("categories" in m) || !m.categories["no_access"]) return null;
  if (m.scale === "profile") {
    const labels = (values ?? NO_VALUES).status?.no_access ?? [];
    return labels.length ? offFinfish(m, values, ["in", ["coalesce", ["feature-state", m.property], ""], ["literal", [...labels]]]) : null;
  }
  if (m.scale === "categorical") return ["==", ["feature-state", m.property], "no_access"];
  return null;
}

/** The dots' opacity for the stream layer in `mode`: 1 on a no-access section, else 0 (every mode without one). */
export function noaccessOpacity(mode: string, values: ChannelValues | null = NO_VALUES): unknown {
  const shown = noAccessShown("stream", mode, values);
  return shown === null ? 0 : ["case", shown, 1, 0];
}

/**
 * The dots' paint: `color.status.no_access` (the legend's grey), a little stronger than the under-lake route it is
 * modelled on (`under_lake`: `width.underlake` 0.5 → 1.5 px at `opacity.route`, `dash.route`) — a thicker line at full
 * strength (1.2 → 2.8 px against the route's 0.8 → 2.4 at 0.35), with the route's own dash, so it reads as the same idea ("not water for you") without fading into the ground.
 */
export function noaccessPaint(mode: string, tokens: Tokens, values: ChannelValues | null = NO_VALUES): Record<string, unknown> {
  return {
    "line-color": tokens["color.status.no_access"],
    "line-width": ["interpolate", ["linear"], ["zoom"], 6, 1.2, 10, 1.6, 13, 2.2, 16, 2.8],
    "line-dasharray": tokens["dash.route"],
    "line-opacity": noaccessOpacity(mode, values),
  };
}

/**
 * (T213, owner 2026-10-28: "it should just be more styles on the regulations map") BAIT BAN AND FLY FISHING ONLY ARE
 * MARKS ON THE STANDARD MAP, never a colouring of their own. The status colour stays the line's fill — the primary read —
 * and a thin EDGE in `color.gear.edge` (ink: no hue, so it reads the same for every reader) is drawn under it: SOLID for
 * fly fishing only, DASHED (`dash.gear.bait`) for a bait ban. Picked over breaks in the line (read as the no-access
 * dots, and bait bans cover most of some regions) and symbols along it (clutter at z11):
 * handoff/PLAN/app-integration/T213-shots/variants/.
 *
 * Runtime layers for the same reason as the dots (`line-dasharray` cannot vary per feature): one per mark over every
 * stream (under the stream line, over the selection casing, so a selected water keeps both rings) and one per mark round
 * every lake. Shown only where the layer's mode is the Standard status mode (`profile`), the method channel's day
 * places the section's `gm` label in that class, and the status day neither closes the section nor marks it no access
 * (a closed water has no gear to choose; no-access is dotted already). Which is the colouring's own answer — no second
 * test of a rule.
 */
export const GEAR_LAYERS = {
  stream: { fly: "stream-gear-fly", bait: "stream-gear-bait" },
  lake: { fly: "lake-gear-fly", bait: "lake-gear-bait" },
} as const satisfies Record<"stream" | "lake", Record<GearMark, string>>;
// (T436) the marks are core's (`GEAR_LEGEND` / `GearMark`): a new class is a legend row AND a layer, or tsc refuses this
export type { GearMark };
/** The edge each side of a marked stream line, and a marked lake's ring, in px. */
export const GEAR_EDGE_PX = 1.5;
export const GEAR_RING_PX = 1.6;
/**
 * (T226, owner 2026-10-28: "very aggressive when zoomed out") THE MARKS FADE IN WITH ZOOM. Each ramp is [zoom, value]
 * stops: `opacity` (0 below the first stop: the mark is noise there), `edge` (the fraction of `GEAR_EDGE_PX` / `GEAR_RING_PX`
 * drawn). Full strength, the T213 look, from the last stop up. `gentle` (default) ramps over z8-z14; `late` holds the
 * mark back until z9.5 and is full by z12. The dash is lengthened below z11 so a thin line reads as dashes, not a shimmer.
 */
export const GEAR_RAMPS = {
  gentle: { opacity: [[8, 0], [9, 0.3], [11, 0.6], [14, 1]], edge: [[8, 0.35], [11, 0.65], [14, 1]] },
  late: { opacity: [[9.5, 0], [10.5, 0.5], [12, 1]], edge: [[9.5, 0.5], [12, 1]] },
} as const;
export const GEAR_RAMP: keyof typeof GEAR_RAMPS = "gentle";
type Ramp = readonly (readonly [number, number])[];
/** A ramp's value at `zoom`, linear between stops and held outside them. */
export function rampAt(ramp: Ramp, zoom: number): number {
  const first = ramp[0]!;
  if (zoom <= first[0]) return first[1];
  for (let i = 1; i < ramp.length; i++) {
    const [z1, v1] = ramp[i]!, [z0, v0] = ramp[i - 1]!;
    if (zoom <= z1) return v0 + (v1 - v0) * (zoom - z0) / (z1 - z0);
  }
  return ramp[ramp.length - 1]![1];
}
/** The dash: the token from z11 up, a longer one below it (the line is thin; a 1.2 dash is a few pixels). */
function gearDash(tokens: Tokens): unknown {
  const d = tokens["dash.gear.bait"] as number[];
  return ["step", ["zoom"], ["literal", d.map((n) => n * 2)], 11, ["literal", d]];
}
/** The modes the marks ride on: the Standard map's status mode, and (owner 2026-10-28: "bait and fly rules also apply
 *  to salmon") the Salmon map's — the provincial gear rule holds for a salmon angler too. */
const GEAR_MODES: readonly string[] = ["profile", "salmon"];

/**
 * Where `mark` is shown on `layerId` (`stream` / `lake`) in `mode`, as a boolean expression; null: nowhere. Hidden where
 * the mode's own day closes the section: on Standard the status day (closed / no access, feature-state `p`), on Salmon
 * the salmon day (salmon closed, feature-state `sp`) — the line's own read, never a second test of a rule.
 */
export function gearShown(layerId: "stream" | "lake", mode: string, mark: GearMark,
                          values: ChannelValues | null = NO_VALUES): unknown | null {
  const v = values ?? NO_VALUES;
  const m = STYLE_META.colorModes[layerId]?.[mode];
  if (!GEAR_MODES.includes(mode) || !m || m.scale !== "profile" || !v.method) return null;
  const day = mode === "salmon" ? v.salmon : v.status;
  if (!day) return null;
  const labels = v.method[mark];
  if (!labels.length) return null;
  // (T377) and none: a section the day gives no answer — or one a legend tap has greyed (`isolateDay`)
  const off = mode === "salmon" ? [...(v.salmon?.closed ?? []), ...(v.salmon?.none ?? [])]
    : [...(v.status?.closed ?? []), ...(v.status?.no_access ?? []), ...(v.status?.none ?? [])];
  const marked = ["in", ["coalesce", ["feature-state", "gm"], ""], ["literal", [...labels]]];
  // (T796 A6) and a section DFO's finfish closure closes on Standard (`offFinfish`) — the Salmon day closes it already
  return offFinfish(m, v, off.length
    ? ["all", marked, ["!", ["in", ["coalesce", ["feature-state", m.property], ""], ["literal", off]]]]
    : marked);
}

/** One mark's paint on a stream (an edge round the line, `streamWidth` its own) or a lake (a ring). */
export function gearPaint(layerId: "stream" | "lake", mode: string, mark: GearMark, tokens: Tokens,
                          values: ChannelValues | null = NO_VALUES, streamWidth?: unknown): Record<string, unknown> {
  const shown = gearShown(layerId, mode, mark, values);
  const ramp = GEAR_RAMPS[GEAR_RAMP];
  const width = layerId === "stream" && streamWidth !== undefined
    ? withinZoomCurve(streamWidth, (w, z) => ["+", w, 2 * GEAR_EDGE_PX * rampAt(ramp.edge, z)])
    : ["interpolate", ["linear"], ["zoom"],
       ...ramp.edge.flatMap(([z, f]) => [z, GEAR_RING_PX * f])];
  return {
    "line-color": tokens["color.gear.edge"],
    "line-width": width,
    // solid for fly only; dashed for a bait ban (a layer's dash is fixed, so each mark is its own layer)
    ...(mark === "bait" ? { "line-dasharray": gearDash(tokens) } : {}),
    // zoom may only be the input of a top-level interpolate, so the day's test sits inside each stop
    "line-opacity": shown === null ? 0
      : ["interpolate", ["linear"], ["zoom"], ...ramp.opacity.flatMap(([z, o]) => [z, ["case", shown, o, 0]])],
  };
}

/**
 * (T887 option B, owner 2026-11-18: "Should be bright green for keep in light I think too?") THE KEEP LINE'S CASING: a
 * thin ink edge each side of a Salmon-view stream section DFO lets you keep from, drawn UNDER the line, so a bright green
 * (light) still stands off the light basemap — the casing meets the ground at 3:1, the green meets the casing. The theme
 * switches it: `width.salmon.casing` 0 is no casing (option A, dark, colour-blind). Its own runtime layer (a line has no
 * stroke of its own); its width is the stream's base width (no feature-state), shown only on the day's `open` labels.
 */
export const SALMON_CASING_LAYER = "stream-salmon-casing";

export function salmonCasingPaint(mode: string, tokens: Tokens, values: ChannelValues | null = NO_VALUES,
                                  streamWidth?: unknown): Record<string, unknown> {
  const px = Number(tokens["width.salmon.casing"] ?? 0);
  const open = (values ?? NO_VALUES).salmon?.open ?? [];
  const w = streamWidth ?? ["interpolate", ["linear"], ["zoom"], 6, 1.2, 10, 2, 14, 3.2];
  return {
    "line-color": tokens["color.salmon.casing"],
    "line-width": withinZoomCurve(w, (v) => ["+", v, 2 * px]),
    "line-opacity": mode === "salmon" && px > 0 && open.length ? ["case", salmonLabelIn(open), 1, 0] : 0,
  };
}

/**
 * (T199) A PASSING RUN IS A DASH (Q144, owner 2026-10-28: "spawning reaches + passage reaches dashed"). `line-dasharray`
 * cannot vary per feature, so — like the no-access dots — the dash is its own runtime layer over every stream, shown
 * only where the stream's mode is `runs` and its label `rt` is `pass`; the `stream` layer paints that section the quiet
 * `color.run.none` line underneath, so the dash carries the whole distinction. Its width is the stream's own.
 */
export const RUNS_PASS_LAYER = "stream-runs-pass";

export function runsPassPaint(mode: string, tokens: Tokens, streamWidth?: unknown): Record<string, unknown> {
  return {
    "line-color": tokens["color.run.pass"],
    "line-width": streamWidth ?? ["interpolate", ["linear"], ["zoom"], 6, 1.2, 10, 2, 14, 3.2],
    "line-dasharray": tokens["dash.run.pass"],
    "line-opacity": mode === "runs" ? ["case", ["==", ["feature-state", "rt"], "pass"], 1, 0] : 0,
  };
}

/**
 * (T377) CLOSED HAS A SECOND CUE BESIDES ITS COLOUR (map-UI research, a proposal for the owner): a dashed stripe down the
 * middle of a closed stream, in the paper's colour (`color.cue.closed`), so the line reads as broken — whatever
 * the reader can tell of its red. Its own runtime layer over the stream line, for the same reason as the no-access dots
 * (`line-dasharray` cannot vary per feature), shown only where the stream's own mode paints the section closed: the
 * profile day's `closed` labels (Standard: the status day; Salmon: the salmon day, whose closed is "closed to salmon"),
 * or a categorical mode's `closed` member. Never a second test of a rule. Lakes keep colour alone (a fill has no line).
 */
export const CLOSED_CUE_LAYER = "stream-closed";

/** Where `mode` paints a stream section closed, as a boolean expression; null where the mode never does. */
export function closedShown(layerId: string, mode: string, values: ChannelValues | null = NO_VALUES): unknown | null {
  const m = STYLE_META.colorModes[layerId]?.[mode];
  if (!m || !("categories" in m) || !m.categories["closed"]) return null;
  if (m.scale === "profile") {
    const labels = profileDay(m, values)?.["closed"] ?? [];
    const byDay = labels.length ? ["in", ["coalesce", ["feature-state", m.property], ""], ["literal", [...labels]]] : null;
    // (T796 A6) a section DFO's finfish closure paints closed is closed: its stripe, its width
    const fin = finfishOver(m, values)?.closed ?? [];
    if (!fin.length) return byDay;
    return byDay === null ? salmonLabelIn(fin) : ["any", salmonLabelIn(fin), byDay];
  }
  if (m.scale === "categorical") return ["==", ["feature-state", m.property], "closed"];
  return null;
}

/** the stripe's width as a share of the stream line's: the line's colour stays the edges either side */
export const CLOSED_CUE_SHARE = 0.4;

/** The stripe's paint: under half the stream's own width (`streamWidth`), the paper's colour, short dashes. */
export function closedCuePaint(mode: string, tokens: Tokens, values: ChannelValues | null = NO_VALUES,
                               streamWidth?: unknown): Record<string, unknown> {
  const shown = closedShown("stream", mode, values);
  return {
    "line-color": tokens["color.cue.closed"],
    "line-width": streamWidth !== undefined ? withinZoomCurve(streamWidth, (w) => ["*", w, CLOSED_CUE_SHARE])
      : ["interpolate", ["linear"], ["zoom"], 6, 0.5, 10, 0.8, 14, 1.4],
    "line-dasharray": tokens["dash.cue.closed"],
    "line-opacity": shown === null ? 0 : ["case", shown, 1, 0],
  };
}
/**
 * (T759, owner 2026-11-15: "I like the stripe option best for the ones with banners ... the stripes should only be on the
 * salmon view") THE ISSUE STRIPE: dashes down the middle of a stream whose water has a DFO salmon rule the feed could not
 * place (the rows that carry the river page's banner). The line keeps the colour it would have anyway; the stripe is an
 * extra pair of layers over it, ONLY while the stream's mode is `salmon` (the view the banner belongs to) and ONLY on
 * the sections the engine's `issue` channel labelled (feature-state `gi`) — never a second test of a rule.
 *
 * Two layers, one dash: the casing (the paper's colour, `color.issue.casing`) and the ink (`color.issue.stripe`) on it.
 * The closed colour was tried for the dashes and fails (it is the closed line's own colour: ratio 1.00); and any one ink
 * loses on one of the five lines a Salmon stream can wear. Dark ink on a paper dash leaves every line, and every ground
 * under it, a contrast of its own (tools/issue-stripe.test.ts). It fades in with zoom as the gear marks do (`GEAR_RAMPS`),
 * so a whole province is not speckled.
 */
export const ISSUE_LAYERS = { casing: "stream-issue-casing", stripe: "stream-issue-stripe" } as const;
/** the stripe's width, as a share of the stream line's own width: ONE share for both layers. (Owner 2026-11-16, on the dark
 *  blue "Keep salmon" line: "Make it wider") it was 0.4 ink inside a 0.85 casing. MapLibre divides dash distance by
 *  floor(line-width), so layers of different widths have different periods and drift out of phase (the casing then covers
 *  the line). Same width, same period: the pale part lies ALONG the line (`dash.issue.casing` 5 on 3 off, under
 *  `dash.issue.stripe` 3 on 5 off), so the line's own colour shows in the gaps and down both edges. */
export const ISSUE_SHARE = 0.6;
/** the mode the stripe is drawn in: the Salmon view's */
export const ISSUE_MODE = "salmon";

/** Where the stripe is shown on a stream in `mode`, as a boolean expression; null: nowhere (any other view). */
export function issueShown(mode: string): unknown | null {
  return mode === ISSUE_MODE ? ["==", ["coalesce", ["feature-state", "gi"], ""], "1"] : null;
}

export function issuePaint(part: keyof typeof ISSUE_LAYERS, mode: string, tokens: Tokens, streamWidth?: unknown): Record<string, unknown> {
  const w = streamWidth ?? ["interpolate", ["linear"], ["zoom"], 6, 1.2, 10, 2, 14, 3.2];
  const shown = issueShown(mode);
  return {
    "line-color": tokens[part === "casing" ? "color.issue.casing" : "color.issue.stripe"],
    "line-width": withinZoomCurve(w, (v) => ["*", v, ISSUE_SHARE]),
    "line-dasharray": tokens[part === "casing" ? "dash.issue.casing" : "dash.issue.stripe"],
    "line-opacity": shown === null ? 0
      : ["interpolate", ["linear"], ["zoom"], ...GEAR_RAMPS[GEAR_RAMP].opacity.flatMap(([z, o]) => [z, ["case", shown, o, 0]])],
  };
}

type WidthSpec = Exclude<(typeof STYLE_META.widths)[string], string>;
/** One stop of a "linear" width: its zoom, the width there (zoom and the tile's order only), the T470 closed factor. */
interface WidthStop { at: number; w: unknown; f: number }

function linearStops(spec: WidthSpec, scale: number): WidthStop[] {
  const attr = ["coalesce", ["to-number", ["get", spec.by]], 1];
  return spec.ramp.map(([at, base, slope]) => ({
    at, w: ["*", scale, ["+", base, ["*", attr, slope]]], f: spec.closed ? rampAt(spec.closed, at) : 1 }));
}

/** A stop's width with the T470 factor on the sections `closed` holds (the stop itself where there is none). */
function closedPart(s: WidthStop, closed: unknown | null): unknown {
  return s.f === 1 || closed === null ? s.w : ["*", ["case", closed, s.f, 1], s.w];
}

/**
 * THE HIGHLIGHT'S WIDTH FLOOR (px, by feature-state), or null where the theme has none. P15: the water's other parts
 * (`water`) take their own, lower floor — after `selected`, which wins. T151: the part you are on in a water's Map view
 * (`part`) takes the SAME floor as the others — one width for every part.
 */
function highlightFloor(tokens: Tokens): unknown | null {
  if (tokens["width.stream.highlight"] === undefined) return null;
  return tokens["width.stream.water"] !== undefined
    ? ["case", SELECTED, Number(tokens["width.stream.highlight"]), ["any", PART, WATER], Number(tokens["width.stream.water"]), 0]
    : ["case", SELECTED, Number(tokens["width.stream.highlight"]), 0];
}

/**
 * (T848) THE STREAM LAYER DRAWN OVER `stream` WHERE ITS WIDTH READS FEATURE-STATE — a selected or lit section (the
 * highlight's floor) and a closed one (T470's heavier mid-zoom line). Owner 2026-11-18, the web map lagging zooming out
 * over Intata Reach: every labelled section re-evaluated each feature-state paint property of every layer it is in, and
 * the costliest was the stream's own width — a 2.7 KB zoom curve with `max`, `in` and `feature-state`, evaluated twice per
 * feature and shared by nine layers. So the `stream` layer's width reads zoom and order only (`StreamWidths.base`), and
 * this layer — the stream's own colour and opacity — draws the sections whose width differs (`heavy`), at the width the
 * stream had before (`drawn`); `stream` is hidden under it there, so a translucent line is never drawn twice. Each mark
 * drawn with the stream's width reads the drawn width only where it can show and the two differ (`markWidth`).
 */
export const STREAM_HEAVY = "stream-heavy";

/** (T848) The stream's width, split: what reads feature-state, and where. */
export interface StreamWidths {
  /** the width from zoom and stream order alone: no feature-state */
  readonly base: unknown;
  /** the stream's width as drawn (the floors and closed's factor): `paintFor`'s, the one width before T848 */
  readonly drawn: unknown;
  /** where `drawn` may differ from `base` (lit or closed), as a boolean expression; null: nowhere */
  readonly heavy: unknown | null;
  /** where the highlight's floor applies (selected, part, water); null: the theme has no floor */
  readonly lit: unknown | null;
  /** `drawn` on the lit sections, the closed factor (unconditionally) elsewhere: right wherever a section is heavy */
  readonly heavyWidth: unknown;
  /** `drawn` where `guard` holds, `base` elsewhere (null: `base`) */
  within(guard: unknown | null): unknown;
}

/** (T848) The stream's widths in `mode` — `paintFor("stream", …)["line-width"]` is their `drawn`. */
export function streamWidths(mode: string, tokens: Tokens, given: ChannelValues | null = NO_VALUES): StreamWidths {
  const values = given ?? NO_VALUES;
  const m = STYLE_META.colorModes["stream"]?.[mode];
  // a profile mode with no day is the plain map (`paintFor`)
  const eff = profileChannel(m) && profileDay(m, values) === null ? "plain" : mode;
  const spec = STYLE_META.widths["stream"];
  if (spec === undefined || typeof spec === "string" || (spec.mode ?? "linear") !== "linear")
    throw new Error("streamWidths: the stream's width is no longer a linear ramp over order");
  const stops = linearStops(spec, Number(tokens[spec.token] ?? 1));
  const closed = spec.closed ? closedShown("stream", eff, values) : null;
  const floor = highlightFloor(tokens);
  const lit = floor === null ? null : tokens["width.stream.water"] !== undefined ? ["any", SELECTED, PART, WATER] : SELECTED;
  const drawnAt = (s: WidthStop) => (floor === null ? closedPart(s, closed) : ["max", closedPart(s, closed), floor]);
  const factorAt = (s: WidthStop) => (s.f === 1 || closed === null ? s.w : ["*", s.f, s.w]);
  const curve = (f: (s: WidthStop) => unknown) => ["interpolate", ["linear"], ["zoom"], ...stops.flatMap((s) => [s.at, f(s)])];
  const heavy = lit !== null && closed !== null ? ["any", lit, closed] : lit ?? closed;
  return {
    base: curve((s) => s.w),
    drawn: curve(drawnAt),
    heavy, lit,
    heavyWidth: curve((s) => (lit === null ? factorAt(s) : ["case", lit, drawnAt(s), factorAt(s)])),
    within: (guard) => (guard === null ? curve((s) => s.w) : curve((s) => ["case", guard, drawnAt(s), s.w])),
  };
}

/**
 * (T848) `paint` (the stream's, as `paintFor` and the focus make it) split between the two layers that draw it: `stream`
 * at the base width, hidden where `heavy`; `STREAM_HEAVY` at the drawn width, shown only there. The same colour on both.
 */
export function splitStream(paint: Record<string, unknown>, sw: StreamWidths):
    { stream: Record<string, unknown>; heavy: Record<string, unknown> } {
  const op = paint["line-opacity"] ?? 1;
  if (sw.heavy === null)
    return { stream: { ...paint, "line-width": sw.base }, heavy: { ...paint, "line-width": sw.base, "line-opacity": 0 } };
  return {
    stream: { ...paint, "line-width": sw.base, "line-opacity": withinZoomCurve(op, (v) => ["case", sw.heavy, 0, v]) },
    heavy: { ...paint, "line-width": sw.heavyWidth, "line-opacity": withinZoomCurve(op, (v) => ["case", sw.heavy, v, 0]) },
  };
}

/** (T848) The marks drawn with the stream's width. */
export type StreamMark = "casing" | "glow" | "gear" | "closed" | "runs" | "issue";
/** a casing shows on a selected section, the part you are on, or one an in-season change covers (`casingPaint`) */
const CASED = ["any", SELECTED, PART, INSEASON];

/**
 * (T848) The stream width a mark is drawn with in `mode`: the drawn width wherever the mark can show AND it differs
 * from the base, the base (no feature-state) everywhere else — so a mark reads the floor or closed's factor only where
 * it would be seen, and not at all in a mode that never shows it.
 */
export function markWidth(mark: StreamMark, mode: string, sw: StreamWidths): unknown {
  switch (mark) {
    case "casing": return sw.within(CASED);
    case "glow": return sw.within(ON);
    // never on a closed section (`gearShown`): the floor is all that differs
    case "gear": return markShownIn("gear", mode) ? sw.within(sw.lit) : sw.base;
    // only on a closed section: the factor always, the floor where lit
    case "closed": return markShownIn("closed", mode) ? sw.heavyWidth : sw.base;
    case "runs": return markShownIn("runs", mode) ? sw.within(sw.heavy) : sw.base;
    case "issue": return markShownIn("issue", mode) ? sw.within(issueShown(mode)) : sw.base;
  }
}

/**
 * (T848 fix 2) THE MODES A STREAM MARK CAN SHOW IN — by the mode alone, never its day (a day arriving must not flip a
 * layer's visibility: that re-lays out every tile). A mark outside them is `visibility: none`: no bucket, no
 * feature-state work. `nobaseline` / `noaccess` / `runs` / `issue` / `gear` / `closed` as their paint functions read them.
 */
export type ModalMark = "nobaseline" | "noaccess" | "gear" | "closed" | "runs" | "issue" | "salmonCasing";
export function markShownIn(mark: ModalMark, mode: string, layerId: "stream" | "lake" = "stream"): boolean {
  const m = STYLE_META.colorModes[layerId]?.[mode];
  const cats = m && "categories" in m ? m.categories : {};
  switch (mark) {
    case "nobaseline": return mode === "standing";
    case "noaccess": return cats["no_access"] !== undefined && (m?.scale === "profile" || m?.scale === "categorical");
    case "gear": return GEAR_MODES.includes(mode) && m?.scale === "profile";
    case "closed": return cats["closed"] !== undefined && (m?.scale === "profile" || m?.scale === "categorical");
    case "runs": return mode === "runs";
    case "issue": return issueShown(mode) !== null;
    case "salmonCasing": return mode === "salmon";
  }
}

/** The MapLibre paint property that carries colour, per layer type. */
// Module-private: an implementation detail of `paintFor`.
function colourPropFor(type: string | undefined): string {
  return type === "line" ? "line-color" : type === "fill" ? "fill-color"
    : type === "symbol" ? "text-color" : "circle-color";
}

/**
 * Everything painted on one layer for one mode: colour, width, opacity.
 *
 * Shared by the runtime style and the adapter ON PURPOSE. The style used to ship with no
 * paint at all and the adapter applied it on `load`, so every map showed a frame or two of
 * MapLibre's defaults — black lines over the basemap — before snapping to the palette.
 * One function means the first frame and every frame after it are painted the same way.
 */
export function paintFor(layerId: string, mode: string, tokens: Tokens,
                         given: ChannelValues | null = NO_VALUES): Record<string, unknown> {
  // null is "nothing installed", the same as `NO_VALUES` — what a caller with no channel at hand passes
  const values = given ?? NO_VALUES;
  /*
   * A PROFILE MODE WITH NO DAY IS THE PLAIN MAP. The day is installed only on a proven pair whose index passed its digest
   * checks (`setChannel`); without one there is no answer to draw, and the app's rule is that "not asked" stays plain
   * rather than going grey or — worse — keeping another release's colours.
   */
  if (profileChannel(STYLE_META.colorModes[layerId]?.[mode]) && profileDay(STYLE_META.colorModes[layerId]?.[mode], values) === null)
    return paintFor(layerId, "plain", tokens);
  const type = (MAP_STYLE.layers.find((l) => l.id === layerId) as { type?: string } | undefined)
    ?.type;
  /**
   * SELECTION IS A PAINT RULE — BUT NOT A COLOUR (owner 2026-10-21).
   *
   * `highlight()` writes `{selected: true}` into feature-state. It used to swap the colour here for `color.highlight`,
   * so the part you were on lost the very thing the map is for — its regulation status — and read as "magenta". The
   * colour is now the mode's, always; what is selected is OUTLINED by the selection layers (`SELECTION_CASING`,
   * `SELECTION_RINGS`), and a selected stream is drawn at least `width.stream.highlight` wide (below).
   */
  const isHighlightable = STYLE_META.highlightable.some((h) => h.id === layerId);
  let out: Record<string, unknown> = { [colourPropFor(type)]: colorExpression(layerId, mode, tokens, values) };

  const spec = STYLE_META.widths[layerId];
  const widthKey = type === "line" ? "line-width" : type === "circle" ? "circle-radius" : null;
  if (spec !== undefined && widthKey !== null) {
    if (typeof spec === "string") {
      const w = tokens[spec];
      if (w !== undefined) out[widthKey] = w;
    } else {
      // v1's formula: width is LINEAR in the attribute, and both the intercept and the
      // slope move with zoom. Not a ramp over the attribute — that shape differentiates
      // every stream at every zoom, and the low-zoom province becomes noise.
      //
      // `["get", ...]` and not `["feature-state", ...]`: stream order ships in the tile and
      // never changes, so line weight must not wait on the app pushing anything.
      const scale = Number(tokens[spec.token] ?? 1);
      const z: unknown[] = ["interpolate", ["linear"], ["zoom"]];
      if (spec.mode === "zoom") {
        // NO ATTRIBUTE AT ALL — a curve in the camera and nothing else. The route through a
        // lake is a construction line: it says the river continues, not how big it is, and
        // drawing it at the river's own weight makes it read as more river.
        for (const [at, base] of spec.ramp) z.push(at, scale * base);
      } else if (spec.mode === "sqrt") {
        // v1's lake/area outline: base + k * sqrt(area), clamped. Square-rooted because
        // area grows as the square of a shoreline, so a linear ramp makes one big lake
        // enormous and every small one invisible. MapLibre has no clamp, hence max(min()).
        const attr = ["coalesce", ["to-number", ["get", spec.by]], 10000];
        // (T470) `recede`: thinner short of its last zoom, a closed water's outline untouched
        const closed = spec.recede ? closedShown(layerId, mode, values) : null;
        for (const [at, base, k, max] of spec.ramp) {
          const w = ["*", scale, ["max", ["min", ["+", base, ["*", k, ["sqrt", attr]]], max ?? base], base]];
          const f = spec.recede ? rampAt(spec.recede, at) : 1;
          z.push(at, f === 1 ? w : ["*", closed === null ? f : ["case", closed, 1, f], w]);
        }
      } else {
        // (T470) `closed`: a section the day paints closed is drawn this much heavier, short of the factor's last zoom
        const closed = spec.closed ? closedShown(layerId, mode, values) : null;
        for (const s of linearStops(spec, scale)) z.push(s.at, closedPart(s, closed));
      }
      out[widthKey] = z;
    }
    /**
     * A HIGHLIGHTED REACH IS ALSO THICKER — as a FLOOR, applied INSIDE the zoom curve.
     *
     * `max`, not a multiplier: `width.stream.highlight` is a width in pixels (3.2), so
     * multiplying would make the Fraser thirty pixels wide while leaving a creek thinner
     * than the highlight is supposed to guarantee.
     *
     * THE ZOOM CURVE MUST STAY AT THE TOP. MapLibre allows exactly one zoom-based
     * `interpolate` per expression AND requires it to be the outermost one — a
     * "zoom-and-property" function is a zoom curve whose OUTPUTS are data expressions,
     * never the other way round. Both wrong shapes were shipped in turn: first
     * `case(selected, max(<curve>, 3.2), <curve>)` (two curves), then
     * `max(<curve>, case(...))` (one curve, but nested). Each one fails to PARSE, so the
     * layer is dropped and the map comes up unstyled with a single console line.
     * `tools/style-valid.test.ts` now runs the spec's own validator over this.
     */
    const floor = isHighlightable && type === "line" ? highlightFloor(tokens) : null;
    if (floor !== null) out[widthKey] = withinZoomCurve(out[widthKey], (v) => ["max", v, floor]);
  }

  const dash = (STYLE_META.dashes ?? {})[layerId];
  if (dash !== undefined && type === "line") {
    const pattern = tokens[dash];
    if (Array.isArray(pattern)) out["line-dasharray"] = pattern;
  }

  /**
   * THE WATER IS THE ANSWER IN THE REGIONAL VIEW — it used to be faded out.
   *
   * `standing` mode below z7 used to drop the rivers to 12% opacity so they would not
   * compete with a haze of blurred discs drawn over the land. That was the wrong thing
   * made quiet to protect the wrong thing: the discs claimed a condition for country no
   * gauge speaks for, while the water that DOES carry a reading was hidden.
   *
   * The discs are gone (see `stream-glow` in runtime-style) and the rivers are at full
   * weight at every zoom. The atlas has already thinned itself to the mainstems by z5, and
   * a mainstem is exactly the water most likely to be gauged — so what is left is what we
   * can actually answer for.
   */

  const edgeOpacity = (STYLE_META.edgeOpacities ?? {})[layerId];
  if (edgeOpacity !== undefined && type === "line") {
    const o = tokens[edgeOpacity];
    // (T470) a zoom curve where the outline declares one (`outline.opacity.ramp`): the token times each factor
    const ramp = (STYLE_META.edgeOpacityRamps ?? {})[layerId];
    if (o !== undefined)
      out["line-opacity"] = ramp && typeof o === "number"
        ? ["interpolate", ["linear"], ["zoom"], ...ramp.flatMap(([z, k]) => [z, Math.round(o * k * 1000) / 1000])]
        : o;
  }

  const opacity = (STYLE_META.opacities ?? {})[layerId];
  if (opacity !== undefined && type) {
    const o = tokens[opacity];
    // a zoom curve where the layer declares one (`opacity.ramp`): the token at full strength, times each factor
    const ramp = (STYLE_META.opacityRamps ?? {})[layerId];
    if (o !== undefined)
      out[`${type}-opacity`] = ramp && typeof o === "number"
        ? ["interpolate", ["linear"], ["zoom"], ...ramp.flatMap(([z, k]) => [z, Math.round(o * k * 1000) / 1000])]
        : o;
  }

  // (T470) small waters recede at mid zoom: the day's own / base sections faded by stream order (`fadeByOrder`)
  const fade = (STYLE_META.orderFades ?? {})[layerId];
  if (fade !== undefined && type === "line") {
    const m = STYLE_META.colorModes[layerId]?.[mode];
    const day = m?.scale === "profile" ? profileDay(m, values) : null;
    const labels = day && m && "property" in m ? fade.classes.flatMap((c) => day[c] ?? []) : [];
    // (T494) a mode with nothing to fade keeps the same zoom curve, flat: MapLibre builds a tile's opacity for the kind
    // of expression it had (zoom AND data here), and a mode swap to a data-only one threw on every tile already built
    // (`interpolationFactor is not a function`, entering Conditions or Runs from the Map tab)
    out["line-opacity"] = labels.length
      ? fadeByOrder(out["line-opacity"] ?? 1, fade,
          // (T796 A6) closed is never faded: nor is a section DFO's finfish closure paints closed
          offFinfish(m, values, ["in", ["coalesce", ["feature-state", (m as { property: string }).property], ""], ["literal", labels]]))
      : fadeByOrder(out["line-opacity"] ?? 1, { ...fade, stops: fade.stops.map(([z, ks]) => [z, ks.map(() => 1)]) }, false);
  }

  // (T165) a no-access stream section is drawn by its dots (`NOACCESS_LAYER`), never as a solid line under them
  if (layerId === "stream" && type === "line") {
    const shown = noAccessShown(layerId, mode, values);
    if (shown !== null) out["line-opacity"] = withinZoomCurve(out["line-opacity"] ?? 1, (v) => ["case", shown, 0, v]);
  }

  out = dimTheOthers(layerId, type, out, tokens);

  /**
   * A LABEL'S HALO — the paper showing through behind the word.
   *
   * Not part of the colour mode, because it is not an encoding: it is what makes a name
   * legible over landcover, a road and a contour at once. v1 drew every water label with
   * one, and a river name without one is readable on open ground and gone the moment it
   * crosses anything.
   */
  const halo = (STYLE_META.labelHalos ?? {})[layerId];
  if (halo !== undefined) {
    const c = tokens[halo.color];
    if (c !== undefined) {
      out["text-halo-color"] = c;
      out["text-halo-width"] = halo.width;
      out["text-halo-blur"] = 0.5;
    }
  }

  /**
   * A FILL PATTERN, AND ONLY OVER A STATIC COLOURING.
   *
   * MapLibre's `fill-pattern` overrides `fill-color` outright. So hatching the wetland
   * unconditionally would draw "open", "closed" and "we could not parse this" as the same
   * green weave — the answer replaced by the texture. The texture is for the mode that is
   * NOT answering anything, which is exactly `scale: "static"`.
   *
   * The token names a COLOUR, and the image is woven from it at runtime (src/hatch.ts).
   * The adapter registers it under `<layer>-hatch` before the style is applied.
   */
  const pattern = (STYLE_META.patterns ?? {})[layerId];
  if (pattern !== undefined && type === "fill"
      && STYLE_META.colorModes[layerId]?.[mode]?.scale === "static") {
    out["fill-pattern"] = `${layerId}-hatch`;
  }
  return solidField(layerId, type, out, tokens);
}

/**
 * THE FIELD AS ONE SOLID SHEET (`field.solid`, review 2026-10-22). Its basins were simplified one at a time, so they
 * gap and overlap along every shared border; drawn translucent, each overlap was a darker seam and the ground showed
 * through every gap and through the field itself. Opaque, with each colour pre-mixed on the paper at the field's own
 * opacity — the same colour the translucent field had on blank paper — nothing shows through, an overlap is invisible,
 * and the seam drawn under it (its follower, `outline.under`) closes the gaps in the basin's own colour.
 */
function solidField(layerId: string, type: string | undefined, out: Record<string, unknown>,
                    tokens: Tokens): Record<string, unknown> {
  const f = STYLE_META.field;
  if (!f?.solid || (layerId !== f.layer && STYLE_META.follows?.[layerId] !== f.layer)) return out;
  const over = tokens[f.solid.over], k = Number(tokens[f.solid.opacity]);
  if (typeof over !== "string" || !Number.isFinite(k)) return out;
  const prop = type === "line" ? "line-color" : "fill-color";
  return { ...out, [prop]: mixColours(out[prop], over, k), [`${type}-opacity`]: 1 };
}

/**
 * (T151) THE WATER'S OTHER PARTS, DIMMED: on a per-section layer (a highlightable one, or the edge that follows it —
 * feature-state is the source-layer's, so a lake's outline knows its lake is `water`), a line or fill lit `water` is drawn
 * at its own opacity times `opacity.water.others`. Only a water's Map view sets `water` (the "dim" cue), so the browse map
 * is untouched. The colour is never changed: the part you are on is the full-strength one.
 */
export function dimTheOthers(layerId: string, type: string | undefined, out: Record<string, unknown>,
                             tokens: Tokens): Record<string, unknown> {
  const k = Number(tokens["opacity.water.others"]);
  const owner = STYLE_META.follows?.[layerId] ?? layerId;
  if (!Number.isFinite(k) || (type !== "line" && type !== "fill")
      || !STYLE_META.highlightable.some((h) => h.id === owner)) return out;
  const prop = `${type}-opacity`;
  return { ...out, [prop]: withinZoomCurve(out[prop] ?? 1, (v) => ["case", WATER, ["*", v, k], v]) };
}

/*
 * (T459, owner 2026-11-06: "in the section navigation with up/downstream all other rivers should be greyed out I think,
 * except downstream maybe. That way it is less distracting") A WATER'S MAP VIEW, FOCUSED: on a per-section layer (a
 * highlightable one, or the edge that follows it), a feature that is not this water (`part` / `water`) nor the water it
 * flows into (`near`) is drawn in `color.water.context` at its opacity times `opacity.water.context`; `near` keeps its
 * colours at `opacity.water.near`. Only a water's Map view turns it on (`MapProps.focus`, `MapAdapter.setFocus`): the
 * browse map is untouched. The marks a stream layer carries (gear edges, closed's stripe, a run's dash, the no-access
 * dots) are not drawn on the others (`hideTheOthers`): a grey water says nothing.
 */
const NEAR = ["boolean", ["feature-state", "near"], false];
const OWN = ["any", PART, WATER];
const KEPT = ["any", PART, WATER, NEAR];

const typeOf = (layerId: string) =>
  (MAP_STYLE.layers.find((l) => l.id === layerId) as { type?: string } | undefined)?.type;

export function focusTheOthers(layerId: string, out: Record<string, unknown>, tokens: Tokens): Record<string, unknown> {
  const type = typeOf(layerId);
  const owner = STYLE_META.follows?.[layerId] ?? layerId;
  if ((type !== "line" && type !== "fill") || !STYLE_META.highlightable.some((h) => h.id === owner)) return out;
  const grey = tokens["color.water.context"], kc = Number(tokens["opacity.water.context"]),
        kn = Number(tokens["opacity.water.near"]);
  if (typeof grey !== "string" || !Number.isFinite(kc) || !Number.isFinite(kn))
    throw new Error("focus: color.water.context / opacity.water.context / opacity.water.near missing from the theme");
  const cprop = `${type}-color`, oprop = `${type}-opacity`;
  const res: Record<string, unknown> = { ...out };
  if (out[cprop] !== undefined) res[cprop] = withinZoomCurve(out[cprop], (v) => ["case", KEPT, v, grey]);
  res[oprop] = withinZoomCurve(out[oprop] ?? 1, (v) => ["case", OWN, v, NEAR, ["*", v, kn], ["*", v, kc]]);
  return res;
}

/** (T459) A stream mark layer's paint with its opacity 0 on every water but this one and the one it flows into. */
export function hideTheOthers(out: Record<string, unknown>): Record<string, unknown> {
  const res: Record<string, unknown> = { ...out };
  for (const k of Object.keys(out)) if (k.endsWith("-opacity")) res[k] = withinZoomCurve(out[k], (v) => ["case", KEPT, v, 0]);
  return res;
}

/** Every `#rrggbb` in `expr` laid over `ground` at `alpha`, per sRGB channel — what the renderer's blend gives. */
export function mixColours(expr: unknown, ground: string, alpha: number): unknown {
  if (Array.isArray(expr)) return expr[0] === "literal" ? expr : expr.map((x) => mixColours(x, ground, alpha));
  if (typeof expr !== "string" || !HEX6.test(expr) || !HEX6.test(ground)) return expr;
  const rgb = (h: string) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16));
  const [a, b] = [rgb(expr), rgb(ground)];
  return "#" + a.map((v, i) => Math.round(v * alpha + b[i]! * (1 - alpha)).toString(16).padStart(2, "0"))
    .join("").toUpperCase();
}
const HEX6 = /^#[0-9a-f]{6}$/i;

/** (T470) One layer's order fade, as `layers.source.json` `fadeByOrder` declares it (the tuning knob). */
export type OrderFade = { by: string; classes: readonly string[]; orders: readonly number[];
                          stops: readonly (readonly [number, readonly number[]])[] };

/**
 * (T470, owner 2026-11-07) SMALL WATERS RECEDE: `opacity` times a factor by stream order where `faded` holds (the day's
 * own / base labels — closed is never in it), as a zoom curve at the top of the expression (MapLibre's rule: one zoom
 * `interpolate`, outermost; the order `step` sits inside each stop). A stop whose factors are all 1 is `opacity` itself,
 * so from the last stop in (z12) the line is exactly what it was.
 */
export function fadeByOrder(opacity: unknown, fade: OrderFade, faded: unknown): unknown {
  if (Array.isArray(opacity) && JSON.stringify(opacity).includes('["zoom"]'))
    throw new Error("fadeByOrder: the line's opacity is already a zoom curve; merge its stops before fading");
  const ord = ["coalesce", ["to-number", ["get", fade.by]], 1];
  return ["interpolate", ["linear"], ["zoom"], ...fade.stops.flatMap(([z, ks]) => [z, ks.every((k) => k === 1) ? opacity
    : ["*", opacity, ["case", faded, ["step", ord, ks[0], ...fade.orders.flatMap((o, i) => [o, ks[i + 1]])], 1]]])];
}

/**
 * Apply `f` to what an expression EVALUATES TO, leaving any zoom curve on the outside.
 *
 * A zoom-based `interpolate` has the shape `["interpolate", interp, ["zoom"], at, out, …]`,
 * and MapLibre requires it to be outermost. So a data-dependent adjustment cannot wrap the
 * curve; it has to be pushed into each of the curve's outputs, which is the same value
 * everywhere and legal. Anything that is not such a curve is transformed directly.
 */
function withinZoomCurve(expr: unknown, f: (value: unknown, zoom: number) => unknown): unknown {
  const isZoomCurve = Array.isArray(expr)
    && (expr[0] === "interpolate" || expr[0] === "step")
    && JSON.stringify(expr[expr[0] === "step" ? 1 : 2]) === '["zoom"]';
  if (!isZoomCurve) return f(expr, 99);
  const a = expr as unknown[];
  const head = a[0] === "step" ? 2 : 3;           // step: op, input, default; interpolate: op, interp, input
  return [...a.slice(0, head),
          ...a.slice(head).map((v, i, all) => ((i % 2 === 0) ? v : f(v, all[i - 1] as number)))];
}

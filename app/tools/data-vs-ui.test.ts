/**
 * DATA vs UI (T60 check 3; AGENTS rules 23, 25, 32, 33: "words for them live here [core] — no hand-written labels in a
 * component"; "a screen may not assert a fact about the data it is sitting on — it asks"). Every word and number on
 * screen traces to the bundle / release (data the component was handed) or to core's words modules (WORDS, MENU_WORDS,
 * CONDITIONS_WORDS, WATER_MAP_WORDS, …). A component that types a sentence, a label or a figure of its own is a second
 * place that fact lives — the §0.5 drift (nine status surfaces, four vocabularies) one layer down.
 *
 * THE SCAN reads every component (.tsx under packages/ui-native/src and apps/mobile/src; hooks in @app/ui are arithmetic
 * and draw nothing) with comments out, and lists its HAND-WRITTEN COPY:
 *   - a JSX text node with words in it (`<Text>Loading this water</Text>`, `>Done<`);
 *   - a string literal that reads as copy: two or more words, or a capitalised word of two letters or more, or a word
 *     with a typed glyph (▾ ↗ › …) — a style word ("row", "center", "transparent"), a key ("72h", "regs-date") and an
 *     identifier are not copy;
 *   - a string literal carrying a FIGURE (a digit beside a unit, a sign or a word) — a number on screen.
 *
 * WHAT THE SCAN HOLDS: (1) no figure is typed in a component — every one of them is data or core's (an allowance here
 * says why); (2) the hand-written copy is EXACTLY the frozen list below — the words that were in components when the
 * auditor ran (T60), each kept where it is until ticket T60-copy moves it to core. A NEW string fails here: put it in core
 * (`WORDS`, `MENU_WORDS`, … beside its screen's other words) and read it from there. A string that LEAVES a component
 * fails too, so the list only ever shrinks on purpose (delete its line).
 *
 * Mutation-pinned: a planted label, sentence and figure are each found; a style word, a key and an identifier are not.
 */
import { describe, expect, it } from "vitest";
import { code, files, read } from "./consistency";

/** The component roots (what draws). */
const DRAWING = ["apps/mobile/src", "packages/ui-native/src"] as const;
/** Files that are not screens: the icon table (path data has digits), the test-only harness, a measurement fixture. */
const NOT_SCREENS: Record<string, string> = {
  "packages/ui-native/src/icons.tsx": "the icon table: Lucide path data, copied verbatim (one-icon-set.test.ts owns it)",
  "packages/ui-native/src/fish-sprite.generated.ts": "generated sprite frames",
  "packages/ui-native/src/ConditionsLayout.before.ts": "a fixture of the OLD panel's lines, kept for the layout test's before/after",
};
const components = () => files(DRAWING).filter((f) => /\.tsx$/.test(f) && !(f in NOT_SCREENS) && !/\/testing\//.test(f));

/** Style words and keys that are string literals in a component but never copy. */
const NOT_COPY = new Set(["transparent", "currentColor", "none", "row", "column", "center", "flex-start", "flex-end",
  "space-between", "stretch", "wrap", "absolute", "relative", "hidden", "visible", "bold", "normal", "italic", "cover",
  "contain", "web", "ios", "android", "native", "light", "dark", "cvd", "button", "tab", "tablist", "header", "link",
  "radio", "radiogroup", "switch", "dialog", "alert", "toolbar", "image", "text", "search", "regs", "conditions", "map",
  "spots", "more", "slide", "fade", "auto", "box-none", "no-hide-descendants", "yes", "no", "height", "width", "down",
  "up", "left", "right", "nowrap", "clip", "ellipsis", "tail", "head", "middle", "flex", "solid", "ghost", "danger",
  "phone", "tablet", "desktop", "year", "72h", "keep", "gear", "methods", "licence", "edit", "view", "draft", "pick",
  "mine", "camera", "library", "discharge", "level", "temperature", "change", "today", "plain", "satellite", "line",
  "square", "glossary", "sources", "about", "report", "en-CA", "en-US", "numeric", "short", "long", "2-digit", "UTC",
  "polite", "assertive", "off", "handled", "ANDROID", "RGB", "HSL", "regulations", "open", "closed", "unknown", "status",
  "Escape", "Enter", "Tab", "ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown", "Backspace",     // KeyboardEvent.key names
  "standing", "stocked", "survey", "streams", "lakes", "basin", "lake", "stream", "section", "part", "item", "water"]);

/** Style, CSS and path data: a string of code, never copy. */
const CODE_LIKE = /\b(?:px|ms|deg)\b|@keyframes|\b(?:rotate|steps|translate|scale)\(|\binfinite\b|\bsolid\b|^[MmLlHhVvCcSsQqTtAaZz]\s*-?\d|^\d+% \d+%$/;
/** A string literal's text, if it reads as copy (words a reader sees), else null. */
export function copyOf(s: string): string | null {
  const t = s.trim();
  if (/^[▾▸▶▼›‹↗ⓘ]$/.test(t)) return t;                                                             // a typed glyph alone
  if (t.length < 2 || NOT_COPY.has(t) || CODE_LIKE.test(t)) return null;
  if (/^[a-z][\w-]*$/.test(t) || /^[\w.-]+:[\w.$-]*$/.test(t) || /^[A-Z_]+$/.test(t)) return null;   // a key, an id, a CONST
  if (/^[\d.,%+\-–−×\s]+$/.test(t)) return null;                                                      // a bare number
  if (/^[#./@]|^https?:|^\$\{|\.(tsx?|json|png|svg)$/.test(t)) return null;                          // a path, a url
  const words = /[A-Za-z]{2,}\s+[A-Za-z]/.test(t);
  const capital = /^[(“"']?[A-Z][a-z]+/.test(t) && !/^[A-Z][a-z]+[A-Z]/.test(t);                     // not PascalCase
  const glyph = /[▾▸▶▼›‹↗ⓘ…·×—–]/.test(t);
  return words || capital || glyph ? t : null;
}
/** A measure: a number with its unit or sign ("+1 h", "50 km²", "10th–90th", "3rd"), as a reader sees one. */
const MEASURE = /(?:^|[\s(])[+−±-]?\d+(?:[.,]\d+)?\s?(?:h|hr|min|d|cm|mm|m|km|km²|m²|m³\/s|%|°C|°|th|st|nd|rd)(?![\w])|[−+±]\s?\d/;
/** A string literal carrying a FIGURE a reader sees (a number with a unit or a sign), else null. */
export function figureOf(s: string): string | null {
  const t = s.trim();
  if (!/\d/.test(t) || CODE_LIKE.test(t)) return null;
  if (/^[\w.-]+:[\w.$-]*$/.test(t) || /^\w+$/.test(t) || /^\d+(\.\d+)?(px|%)$/.test(t)) return null;   // a key ("72h"), a css length
  return MEASURE.test(t) ? t : null;
}

/** Every hand-written literal in one component's code: `[kind, text]`. */
export function literals(source: string): [kind: "copy" | "figure", text: string][] {
  // a testID or a key is a name for a test or React, never shown; a `case "…":` is a key being matched
  const src = source.replace(/\b(?:testID|key)=(?:\{[^}]*\}|"[^"]*")/g, "").replace(/\bcase\s+"[^"]*"\s*:/g, "case _:");
  const out: [kind: "copy" | "figure", text: string][] = [];
  const seen = new Set<string>();
  const add = (kind: "copy" | "figure", text: string) => { const k = `${kind}:${text}`; if (!seen.has(k)) { seen.add(k); out.push([kind, text]); } };
  // JSX text nodes: between a tag's `>` and the next `<`, with no code in them
  for (const m of src.matchAll(/>([^<>{}]*[A-Za-z0-9][^<>{}]*)</g)) {
    const t = m[1]!.replace(/\s+/g, " ").trim();
    if (/[=;(){}[\]]|=>|^\w+\??:|^(void|Promise|Partial|Pick|Record|Map|Set|Date|Omit|Readonly)\b/.test(t)) continue;
    const c = copyOf(t); if (c) add("copy", c);
    const f = figureOf(t); if (f) add("figure", f);
  }
  // string literals: "…", '…', and one-line template literals with their `${…}` holes marked (a template spanning lines
  // is CSS or a message built from parts, and a stray backtick would swallow code)
  for (const m of src.matchAll(/"((?:[^"\\\n]|\\.)*)"|'((?:[^'\\\n]|\\.)*)'|`((?:[^`\\\n]|\\.)*)`/g)) {
    const raw = m[1] ?? m[2] ?? m[3]!;
    const t = raw.replace(/\$\{[^}]*\}/g, "〈〉").trim();
    if (!/[A-Za-z0-9▾▸▶▼›‹↗ⓘ]/.test(t.replace(/〈〉/g, ""))) continue;
    const c = copyOf(t.replace(/〈〉/g, "").trim()); if (c) add("copy", t);
    const f = figureOf(t.replace(/〈〉/g, "").trim()); if (f) add("figure", t);
  }
  return out;
}

/**
 * THE FROZEN COPY: every hand-written string in a component when T60 ran, by file. Each is a finding of T60 handed to
 * ticket T60-copy (move to core words). Deleting a line is the only intended change; a new string belongs in core.
 */
const KNOWN_COPY: Record<string, readonly string[]> = {
  // the data credits (the map's attribution line) and the dev-server hint — core's `sourcesVM` already names the publishers
  "apps/mobile/src/App.tsx": [
    "(https://www2.gov.bc.ca/gov/content/home/copyright). Users should use", "Basemap © OpenStreetMap contributors · © Protomaps",
    "British Columbia, and used under the Province's copyright terms", "Forecast data provided by the BC River Forecast Centre, Province of",
    "Hydrometric data: Environment and Climate Change Canada", "Run `pnpm tiles` to serve the release.",
    "Stocking: Province of British Columbia, Fisheries Inventory Data Queries", "Weather from Open-Meteo.com (CC BY 4.0)", "pnpm tiles",
    "the information on this website with caution and at their own risk.",
  ],
  // the legend ramp's screen-reader sentence, built from parts
  "packages/ui-native/src/Chrome.tsx": ["near the 〈〉 percentile", "〈〉: colours from 〈〉 to", "〈〉; 〈〉 is the middle half of past years"],
  "packages/ui-native/src/Credits.tsx": ["Sources"],
  "packages/ui-native/src/FishSpinner.native.tsx": ["Loading"],
  "packages/ui-native/src/FishSpinner.web.tsx": ["Loading"],
  "packages/ui-native/src/GaugeTrace.tsx": ["How this spot reaches the gauge", "at the gauge", "not known", "on the reach", "so it shows", "the gauge", "this reach"],
  "packages/ui-native/src/Hydrograph.tsx": [
    "No envelope: this station's record is too short to say what is normal here.", "Past TODAY is a model forecast, not a reading.",
    "River flow against its normal range", "Shaded is the middle half of everything this station has recorded for these", "Values in 〈〉.",
    "days; the dashed line is the median.",
  ],
  "packages/ui-native/src/SearchSheet.tsx": [
    "All results", "Back to all results", "Could not read what is near this town.", "Searching", "Show 〈〉 on the map", "Water near 〈〉",
    "named waters", "〈〉 · every named water around it",
  ],
  // ONE close word for every sheet (T41 Q4): still typed in the one component that draws every sheet
  "packages/ui-native/src/Sheet.tsx": ["Close", "Done"],
  "packages/ui-native/src/SpotCapture.tsx": [
    "Back", "Cancel", "Earlier day", "Earlier hour", "Later day", "Later hour", "Next", "Now the exact spot", "Pick the water",
    "Reading the water", "Tap the point on 〈〉.", "Tap the stream or lake your spot is on.", "When were you there?",
  ],
  "packages/ui-native/src/SpotScreen.tsx": [
    "Add a title", "Back", "Cancel", "Cancel editing", "Cloud", "Confirm delete", "Delete", "Delete spot", "Delete this spot?", "Discard",
    "Edit", "Edit spot", "Humidity", "Keep editing", "Keep it", "Keep them, or throw them away and go back to what was saved.",
    "Name this spot",
    "No gauge is entitled to speak for this water, so no reading was recorded. A station draining a far larger watershed would have given a number, and the number would have been wrong.",
    "No weather was recorded for this spot.", "None.", "Notes", "Pressure", "Refresh", "Refresh this spot", "Save and close", "Save changes",
    "Save spot", "Show on map", "Spots", "Temp", "The gauge reading, the weather and the rules that were in force are recorded",
    "Unsaved changes", "Weather", "What happened, what you used, what the water looked like", "What the water was doing",
    "You have unsaved changes.", "nowhere else. This cannot be undone.",
  ],
  "packages/ui-native/src/SpotsScreen.tsx": ["NO PHOTO"],
  "packages/ui-native/src/WaterScreen.tsx": ["Loading this water", "No such water in this bundle.", "no data release is open"],
  "packages/ui-native/src/WaterViews.tsx": ["Loading this water"],
};

/**
 * FIGURES a component may type, each with why. NONE: a number on screen is data or core's (T60 moved the chart key's
 * "10th–90th" to `CONDITIONS_WORDS.key` and the spot's weather window heads to `SPOT_WINDOW`).
 */
const KNOWN_FIGURES: Record<string, readonly string[]> = {};

const dump = process.env.DUMP_COPY === "1";

describe("data vs UI: every word and number on screen comes from the data or from core (T60)", () => {
  const found = () => {
    const copy: Record<string, string[]> = {}, figures: Record<string, string[]> = {};
    for (const f of components())
      for (const [kind, text] of literals(read(f)))
        ((kind === "copy" ? copy : figures)[f] ??= []).push(text);
    for (const r of [copy, figures]) for (const k of Object.keys(r)) r[k] = [...new Set(r[k])].sort();
    return { copy, figures };
  };

  it("no component types a figure", () => {
    const { figures } = found();
    if (dump) console.log(JSON.stringify(figures, null, 2));
    expect(figures).toEqual(KNOWN_FIGURES);
  });

  it("the hand-written copy is exactly the frozen list (new words go to core; a moved word leaves the list)", () => {
    const { copy } = found();
    if (dump) console.log(JSON.stringify(copy, null, 2));
    const want = Object.fromEntries(Object.entries(KNOWN_COPY).map(([f, v]) => [f, [...v].sort()]));
    expect(copy).toEqual(want);
  });

  it("MUTATION PIN: a planted label, sentence, glyph and figure are found; a style word, a key and an id are not", () => {
    const planted = code(`export const P = () => (<View style={{ flexDirection: "row", alignItems: "center" }} testID="regs-date">
      <Text accessibilityLabel="Open the map">Loading this water</Text>
      <Pressable accessibilityRole="button" accessibilityLabel={on ? "Hide" : "Show"} />
      <Text>{\`Closed on \${day}\`}</Text>
      <Text>{"72h"}</Text><Text>−1 h</Text><Text style={{ width: "100%" }}>3rd–49th percentile · drains 50 km²</Text></View>);
      switch (k) { case "classbox youth": return 1; }`);
    expect(literals(planted)).toEqual([
      ["copy", "Loading this water"], ["figure", "−1 h"], ["copy", "3rd–49th percentile · drains 50 km²"],
      ["figure", "3rd–49th percentile · drains 50 km²"], ["copy", "Open the map"], ["copy", "Hide"], ["copy", "Show"],
      ["copy", "Closed on 〈〉"],
    ]);
    // a typed glyph is copy (the icon gate refuses the triangles besides); a key, a style word or an identifier is not
    expect(copyOf("▼")).toBe("▼");
    expect([copyOf("row"), copyOf("regs-date"), copyOf("72h"), copyOf("100%"), copyOf("SectionId"), copyOf("btn:x"), copyOf("0deg")])
      .toEqual([null, null, null, null, null, null, null]);
    expect([figureOf("72h"), figureOf("100%"), figureOf("btn:1"), figureOf("3px 3px 0"), figureOf("+1 h"), figureOf("50 km²")])
      .toEqual([null, null, null, null, "+1 h", "50 km²"]);
  });
});

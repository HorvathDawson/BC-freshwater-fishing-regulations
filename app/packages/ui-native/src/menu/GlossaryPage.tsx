/**
 * THE GLOSSARY: every term the release carries (`answers.glossary` — the same terms the regs page's `gl:` sheets open,
 * and every definition the Synopsis prints on p.80), A–Z under sticky letter heads with an A–Z rail, and a typo-tolerant
 * search that marks what it matched (T288: `glossarySections`, `glossaryRows`, core menu/glossary.ts). A term opens its
 * definition: `glossBlocks` (core regs/sheets.ts) drawn by the regs sheets' own renderer (`SheetBlockView`, T37) — the
 * very blocks and components a `gl:` sheet shows. Nothing here holds a word of one.
 *
 * THE RAIL: a 44 px strip down the list's edge, one button per letter (a letter with no term dimmed and disabled). A touch
 * or a drag anywhere on the strip jumps to the letter under it — the strip answers the touch itself, so its 44 px width
 * and full height are the target, not each letter's small box; the letters are buttons for the keyboard and a screen
 * reader. While searching the list is ranked, not lettered, so the rail steps aside.
 *
 * A term open is a back layer of its own: back closes it to the list before it closes the page. The page's head (MenuHost)
 * names the term and its ‹ returns to the list.
 */
import { useMemo, useRef, useState } from "react";
import { FlatList, ScrollView, TextInput, View, type GestureResponderEvent, type LayoutChangeEvent } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { glossaryRows, glossarySections, glossBlocks, markedRuns, MENU_WORDS, type GlossaryRowVM,
         type GlossaryMark, type SheetKey } from "@app/core";
import { useBackLayer, type Async, type Glossary } from "@app/ui";
import { LoadingState } from "../LoadingState";
import { SheetBlockView } from "../regs/SheetBody";
import { openBook } from "../regs/openBook";
import { hitProps } from "../regs/hit";
import { Icon } from "../icons";
import { TYPE } from "../type";
import { GUTTER, SPACE, type Palette } from "../theme";

/** the rail's width: the strip a thumb lands on */
const RAIL_W = 44;

export function GlossaryPage({ glossary, term, onTerm, onCloseTerm, palette }: {
  glossary: Async<Glossary | null>; term: string | null; onTerm: (id: string) => void; onCloseTerm: () => void;
  palette: Palette;
}) {
  const [q, setQ] = useState("");
  const g = glossary.value;
  const searching = q.trim().length > 0;
  const rows = useMemo(() => (g && searching ? glossaryRows(g.terms, q) : []), [g, q, searching]);
  const lettered = useMemo(() => (g ? glossarySections(g.terms) : { sections: [], rail: [] }), [g]);
  const open = g && term ? g.terms.find((t) => t.id === term) ?? null : null;
  useBackLayer(open !== null, onCloseTerm);

  if (glossary.state === "failed")
    return <Note palette={palette} text={MENU_WORDS.glossary.failed(glossary.error.message)} tone="closed" />;
  if (glossary.state === "loading")
    return (
      <LoadingState palette={palette} size={64} label={MENU_WORDS.glossary.loading}
                    style={{ flex: 1, alignItems: "center", justifyContent: "center" }} />
    );
  if (!g) return <Note palette={palette} text={MENU_WORDS.glossary.unavailable} />;

  // the term's name and "‹ Glossary" are the page's head (MenuHost): one head, whichever level is open
  if (open)
    return (
      <View testID="glossary-term" style={{ flex: 1 }}>
        <ScrollView contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, paddingBottom: 40, gap: SPACE.s3 }}>
          {glossBlocks({ statics: { pdfPages: g.pdfPages } }, open).map((b, i) => (
            // a term named inside another opens in its place, as a gl: sheet does; nothing else here opens a sheet
            <SheetBlockView key={i} b={b} palette={palette} onBook={(pdf, box) => { void openBook(pdf, box); }}
                            onOpen={(k: SheetKey) => { if (/^gl:[a-z0-9_]+$/.test(k)) onTerm(k.slice(3)); }} />
          ))}
        </ScrollView>
      </View>
    );

  return (
    <View style={{ flex: 1 }}>
      <SearchBox q={q} setQ={setQ} palette={palette} />
      {searching
        ? <FlatList
            testID="glossary-results"
            data={rows}
            keyExtractor={(r) => r.id}
            keyboardShouldPersistTaps="handled"
            contentContainerStyle={{ paddingBottom: 40 }}
            ListEmptyComponent={<Note palette={palette} text={MENU_WORDS.glossary.none(q.trim())} />}
            renderItem={({ item, index }) => <TermRow r={item} first={index === 0} palette={palette} onTerm={onTerm} />} />
        : <Lettered sections={lettered.sections} rail={lettered.rail} palette={palette} onTerm={onTerm} />}
    </View>
  );
}

function SearchBox({ q, setQ, palette }: { q: string; setQ: (s: string) => void; palette: Palette }) {
  // the box shows focus (the search bar's way: its border turns accent), so the field draws no ring of its own inside it
  const [focused, setFocused] = useState(false);
  return (
    <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3, borderBottomWidth: 1,
                   borderBottomColor: palette.line }}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, borderWidth: focused ? 2 : 1.5,
                     borderColor: focused ? palette.accent : palette.ink, borderRadius: palette.r.box,
                     paddingHorizontal: focused ? 11.5 : 12, backgroundColor: palette.card }}>
        <Icon name="search" colour={palette.sub} size={18} />
        <TextInput value={q} onChangeText={setQ} placeholder={MENU_WORDS.glossary.search}
                   accessibilityLabel={MENU_WORDS.glossary.search} placeholderTextColor={palette.faint}
                   autoCorrect={false} autoCapitalize="none" clearButtonMode="while-editing"
                   onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
                   style={{ ...TYPE.body, flex: 1, minWidth: 0, color: palette.ink, paddingVertical: focused ? 10.5 : 11,
                            outlineWidth: 0, outlineStyle: "solid" } as object} />
        {q.length > 0 ? (
          <Pressable {...hitProps({ w: 32, h: 40 })} onPress={() => setQ("")} accessibilityRole="button"
                     accessibilityLabel={MENU_WORDS.glossary.clear}
                     style={{ width: 32, alignSelf: "stretch", alignItems: "center", justifyContent: "center" }}>
            <Icon name="close" colour={palette.sub} size={18} />
          </Pressable>
        ) : null}
      </View>
    </View>
  );
}

/** The A–Z list: a sticky letter head over each letter's terms, the rail beside it. */
function Lettered({ sections, rail, palette, onTerm }: {
  sections: readonly { letter: string; rows: readonly GlossaryRowVM[] }[];
  rail: readonly { letter: string; has: boolean }[]; palette: Palette; onTerm: (id: string) => void;
}) {
  const scroll = useRef<ScrollView>(null);
  const at = useRef<Record<string, number>>({});
  const head = useRef(0);
  const [showing, setShowing] = useState<string | null>(null);
  // the letter to land on: itself, else the next one with terms, else the last that has any
  const landing = (letter: string): string | null => {
    const i = rail.findIndex((r) => r.letter === letter);
    const next = rail.slice(i).find((r) => r.has) ?? [...rail].reverse().find((r) => r.has);
    return next?.letter ?? null;
  };
  const jump = (letter: string) => {
    const l = landing(letter);
    if (l === null) return;
    const y = at.current[l];
    if (y !== undefined) scroll.current?.scrollTo({ y: Math.max(0, y - head.current), animated: false });
    setShowing(letter);
  };
  // children: [head A, rows A, head B, rows B, …] — the heads are the sticky ones
  const children: React.ReactNode[] = [];
  const sticky: number[] = [];
  for (const s of sections) {
    sticky.push(children.length);
    children.push(
      <View key={`h-${s.letter}`} testID={`glossary-letter-${s.letter}`}
            onLayout={(e: LayoutChangeEvent) => { head.current = e.nativeEvent.layout.height; }}
            style={{ backgroundColor: palette.card, paddingLeft: GUTTER, paddingRight: SPACE.s2, paddingTop: SPACE.s4,
                     paddingBottom: SPACE.s1, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
        <Text accessibilityRole="header" style={{ ...TYPE.name, color: palette.accent }}>{s.letter}</Text>
      </View>);
    children.push(
      // where a letter starts: its rows' top less its head's height (a sticky head is wrapped, so its own y is 0)
      <View key={`r-${s.letter}`} onLayout={(e: LayoutChangeEvent) => { at.current[s.letter] = e.nativeEvent.layout.y; }}>
        {s.rows.map((r, i) => <TermRow key={r.id} r={r} first={i === 0} palette={palette} onTerm={onTerm} />)}
      </View>);
  }
  return (
    <View style={{ flex: 1, flexDirection: "row" }}>
      <ScrollView ref={scroll} testID="glossary-list" focusable style={{ flex: 1 }} stickyHeaderIndices={sticky}
                  keyboardShouldPersistTaps="handled" contentContainerStyle={{ paddingBottom: 40 }}
                  onScrollBeginDrag={() => setShowing(null)}>
        {children}
      </ScrollView>
      <Rail rail={rail} palette={palette} onLetter={jump} onEnd={() => setShowing(null)} />
      {showing !== null && (
        <View pointerEvents="none" testID="glossary-letter-shown"
              style={{ position: "absolute", top: "40%", alignSelf: "center", left: "50%", marginLeft: -36, width: 72,
                       height: 72, alignItems: "center", justifyContent: "center", backgroundColor: palette.ink,
                       borderRadius: palette.r.box, ...palette.lift }}>
          <Text style={{ ...TYPE.title, color: palette.card }}>{showing}</Text>
        </View>
      )}
    </View>
  );
}

/** The A–Z strip. A touch or drag on it picks the letter under the finger; each letter is also a button. */
function Rail({ rail, palette, onLetter, onEnd }: {
  rail: readonly { letter: string; has: boolean }[]; palette: Palette; onLetter: (l: string) => void; onEnd: () => void;
}) {
  const box = useRef({ top: 0, height: 1 });
  const strip = useRef<View>(null);
  const last = useRef<string | null>(null);
  const pick = (e: GestureResponderEvent) => {
    const { pageY } = e.nativeEvent;
    const i = Math.max(0, Math.min(rail.length - 1, Math.floor(((pageY - box.current.top) / box.current.height) * rail.length)));
    const l = rail[i]!.letter;
    if (l !== last.current) { last.current = l; onLetter(l); }
  };
  const measure = () => strip.current?.measure?.((_x, _y, _w, h, _px, py) => { box.current = { top: py, height: h || 1 }; });
  const h = `${100 / rail.length}%` as const;
  return (
    <View ref={strip} testID="glossary-rail" accessibilityRole="toolbar" accessibilityLabel={MENU_WORDS.glossary.rail}
          onLayout={measure}
          // the strip answers every touch on it: a tap or a drag picks the letter under the finger
          onStartShouldSetResponderCapture={() => true} onMoveShouldSetResponderCapture={() => true}
          onResponderGrant={(e) => { measure(); last.current = null; pick(e); }} onResponderMove={pick}
          onResponderRelease={() => { last.current = null; onEnd(); }} onResponderTerminate={() => { last.current = null; onEnd(); }}
          style={{ width: RAIL_W, paddingVertical: SPACE.s2, borderLeftWidth: 1, borderLeftColor: palette.line,
                   backgroundColor: palette.card, userSelect: "none" } as object}>
      {rail.map((r) => (
        <Pressable key={r.letter} testID={`glossary-rail-${r.letter}`} {...hitProps({ w: RAIL_W, h: 18 })}
                   accessibilityRole="button" accessibilityLabel={MENU_WORDS.glossary.jump(r.letter)}
                   aria-disabled={!r.has} disabled={!r.has}
                   onPress={() => { onLetter(r.letter); onEnd(); }}
                   style={{ height: h, minHeight: 14, alignItems: "center", justifyContent: "center" }}>
          <Text style={{ ...TYPE.micro, fontSize: 11.5, color: r.has ? palette.accent : palette.faint,
                         opacity: r.has ? 1 : 0.55 }}>{r.letter}</Text>
        </Pressable>
      ))}
    </View>
  );
}

/** One term: its name strong, its page small at the right, its plain words under it; what a search matched marked. */
function TermRow({ r, first, palette, onTerm }: {
  r: GlossaryRowVM; first: boolean; palette: Palette; onTerm: (id: string) => void;
}) {
  return (
    <Pressable testID={`glossary-row-${r.id}`} onPress={() => onTerm(r.id)} accessibilityRole="button"
               accessibilityLabel={r.term} {...hitProps({ w: 280, h: 56 })}
               style={({ pressed, hovered }: { pressed: boolean; hovered?: boolean }) => ({
                 paddingVertical: 14, paddingLeft: GUTTER, paddingRight: SPACE.s3, gap: SPACE.s1, minHeight: 56,
                 borderTopWidth: first ? 0 : 1, borderTopColor: palette.line,
                 backgroundColor: pressed || hovered ? palette.wash : palette.card })}>
      <View style={{ flexDirection: "row", alignItems: "baseline", gap: SPACE.s2 }}>
        <Text style={{ ...TYPE.bodyStrong, fontSize: 16, lineHeight: 22, color: palette.ink, flex: 1 }}>
          <Marked s={r.term} marks={r.termMarks} palette={palette} />
        </Text>
        {r.cite ? <Text accessibilityLabel={MENU_WORDS.glossary.pages(r.cite)}
                        style={{ ...TYPE.caption, color: palette.faint }}>{r.cite}</Text> : null}
      </View>
      <Text selectable numberOfLines={3} style={{ ...TYPE.body, color: palette.sub }}>
        <Marked s={r.says} marks={r.saysMarks} palette={palette} />
      </Text>
    </Pressable>
  );
}

function Marked({ s, marks, palette }: { s: string; marks: readonly GlossaryMark[]; palette: Palette }) {
  if (!marks.length) return <>{s}</>;
  return (
    <>
      {markedRuns(s, marks).map(([t, on], i) => (on
        ? <Text key={i} testID="glossary-mark" style={{ backgroundColor: palette.tint, color: palette.ink,
                                                       textDecorationLine: "underline", textDecorationColor: palette.accent }}>{t}</Text>
        : <Text key={i}>{t}</Text>))}
    </>
  );
}

function Note({ palette, text, tone }: { palette: Palette; text: string; tone?: "closed" }) {
  return (
    <Text style={{ ...TYPE.body, color: tone === "closed" ? palette.closed : palette.sub,
                   paddingHorizontal: GUTTER, paddingVertical: SPACE.s4 }}>{text}</Text>
  );
}

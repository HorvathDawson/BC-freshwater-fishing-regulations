/**
 * THE GLOSSARY: every term the release carries (`answers.glossary` — the same terms the regs page's `gl:` sheets open),
 * searchable; a term opens its definition: `glossBlocks` (core regs/sheets.ts) drawn by the regs sheets' own renderer
 * (`SheetBlockView`, T37) — the very blocks and components a `gl:` sheet shows. Nothing here holds a word of one.
 *
 * A term open is a back layer of its own: back closes it to the list before it closes the page. The page's head (MenuHost)
 * names the term and its ‹ returns to the list.
 */
import { useMemo, useState } from "react";
import { FlatList, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import { glossaryRows, glossBlocks, MENU_WORDS, type SheetKey } from "@app/core";
import { useBackLayer, type Async, type Glossary } from "@app/ui";
import { FishSpinner } from "../FishSpinner";
import { SheetBlockView } from "../regs/SheetBody";
import { openBook } from "../regs/openBook";
import { TYPE } from "../type";
import { GUTTER, SPACE, type Palette } from "../theme";


export function GlossaryPage({ glossary, term, onTerm, onCloseTerm, palette }: {
  glossary: Async<Glossary | null>; term: string | null; onTerm: (id: string) => void; onCloseTerm: () => void;
  palette: Palette;
}) {
  const [q, setQ] = useState("");
  const g = glossary.value;
  const rows = useMemo(() => (g ? glossaryRows(g.terms, q) : []), [g, q]);
  const open = g && term ? g.terms.find((t) => t.id === term) ?? null : null;
  useBackLayer(open !== null, onCloseTerm);

  if (glossary.state === "failed")
    return <Note palette={palette} text={MENU_WORDS.glossary.failed(glossary.error.message)} tone="closed" />;
  if (glossary.state === "loading")
    return (
      <View style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
        <FishSpinner palette={palette} size={64} label={MENU_WORDS.glossary.loading} />
      </View>
    );
  if (!g) return <Note palette={palette} text={MENU_WORDS.glossary.unavailable} />;

  // the term's name and "‹ Glossary" are the page's head (MenuHost): one head, whichever level is open
  if (open)
    return (
      <View testID="glossary-term" style={{ flex: 1 }}>
        <ScrollView contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, paddingBottom: 40, gap: SPACE.s3 }}>
          {glossBlocks({ statics: { pdfPages: g.pdfPages } }, open).map((b, i) => (
            // a term named inside another opens in its place, as a gl: sheet does; nothing else here opens a sheet
            <SheetBlockView key={i} b={b} palette={palette} onBook={(pdf) => { void openBook(pdf); }}
                            onOpen={(k: SheetKey) => { if (/^gl:[a-z0-9_]+$/.test(k)) onTerm(k.slice(3)); }} />
          ))}
        </ScrollView>
      </View>
    );

  return (
    <View style={{ flex: 1 }}>
      <View style={{ paddingHorizontal: GUTTER, paddingVertical: SPACE.s3 }}>
        <TextInput value={q} onChangeText={setQ} placeholder={MENU_WORDS.glossary.search}
                   accessibilityLabel={MENU_WORDS.glossary.search} placeholderTextColor={palette.faint}
                   autoCorrect={false} autoCapitalize="none" clearButtonMode="while-editing"
                   style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5, borderColor: palette.ink,
                            borderRadius: palette.r.box, paddingHorizontal: 12, paddingVertical: 10,
                            backgroundColor: palette.card }} />
      </View>
      <FlatList
        data={rows}
        keyExtractor={(r) => r.id}
        keyboardShouldPersistTaps="handled"
        ListEmptyComponent={q.trim() ? <Note palette={palette} text={MENU_WORDS.glossary.none(q.trim())} /> : null}
        renderItem={({ item }) => (
          <Pressable testID={`glossary-row-${item.id}`} onPress={() => onTerm(item.id)} accessibilityRole="button"
                     accessibilityLabel={item.term}
                     style={({ pressed }) => ({ paddingVertical: 12, paddingHorizontal: GUTTER, gap: 2,
                                                borderTopWidth: 1, borderTopColor: palette.line,
                                                backgroundColor: pressed ? palette.wash : palette.card })}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{item.term}</Text>
            <Text numberOfLines={2} style={{ ...TYPE.small, color: palette.sub }}>{item.says}</Text>
          </Pressable>
        )} />
    </View>
  );
}

function Note({ palette, text, tone }: { palette: Palette; text: string; tone?: "closed" }) {
  return (
    <Text style={{ ...TYPE.body, color: tone === "closed" ? palette.closed : palette.sub,
                   paddingHorizontal: GUTTER, paddingVertical: SPACE.s4 }}>{text}</Text>
  );
}

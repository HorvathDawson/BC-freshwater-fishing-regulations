/**
 * T35 — the licence answer's blocks in draw order (prototype `licence()` / page v36 licParts): a mechanical switch on the
 * block's class — class box (with its "Source" → the licsrc: sheet, D17 a), the youth box, a flag, a scope note, the
 * "You need" list (each item: name + ⓘ, when, price; then "Also, only if…" and its items; the price note), the paper
 * licence line, a not-yet-mapped box, a muted line, the guiding fold (open). No logic: every word is the LicenceVM's.
 */
import { Text, View } from "react-native";
import { LIC, type LicenceBlockVM, type NeedItemVM, type SheetKey } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { TermText, type KeepActions } from "./LineText";

type Acts = Pick<KeepActions, "onSheet">;

function Terms({ ids, palette, actions }: { ids: readonly string[]; palette: Palette; actions: Acts }) {
  return ids.length ? <> <TermText ids={ids} palette={palette} actions={actions} /></> : null;
}

/** A "Source" text button in a box (the class box's, a scope note's): opens its licsrc: sheet. */
function SourceWord({ open, palette, actions }: { open: SheetKey; palette: Palette; actions: Acts }) {
  return <> <Text accessibilityRole="button" onPress={() => actions.onSheet(open)} style={[TYPE.metaBold, { color: palette.accent }]}>{LIC.SOURCE}</Text></>;
}

function NeedItem({ it, palette, actions }: { it: NeedItemVM; palette: Palette; actions: Acts }) {
  return (
    <View style={{ paddingVertical: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
      <Text style={[TYPE.regsBodyBold, { color: palette.ink }]}>
        {it.name}
        <Terms ids={it.terms} palette={palette} actions={actions} />
        {it.when !== "" && <Text style={[TYPE.meta, { color: palette.sub }]}>{"\n"}{it.when}</Text>}
        {it.price !== null && <Text style={[TYPE.metaSemi, { color: palette.ink, fontVariant: ["tabular-nums"] }]}>{"\n"}{it.price}</Text>}
      </Text>
    </View>
  );
}

function Block({ b, palette, actions }: { b: LicenceBlockVM; palette: Palette; actions: Acts }) {
  const small = [TYPE.meta, { color: palette.sub }];
  switch (b.cls) {
    case "classbox":
    case "classbox youth":
      return (
        <View testID="licence-classbox" style={{ marginVertical: SPACE.s1 }}>
          <Text style={[TYPE.regsBodyBold, { color: palette.ink }]}>
            {b.head}
            <Terms ids={b.terms} palette={palette} actions={actions} />
            <Text style={[TYPE.regsBody, { color: palette.ink }]}>{"\n"}{b.body}</Text>
            {b.cls === "classbox" && b.source !== null && <SourceWord open={b.source} palette={palette} actions={actions} />}
          </Text>
        </View>
      );
    case "flag":
      return <Text style={[TYPE.meta, { color: palette.ink, marginVertical: SPACE.s1 }]}><Text style={[TYPE.metaBold, { color: palette.restricted }]}>{LIC.FLAG}</Text> {b.text}</Text>;
    case "scopenote":
      return (
        <Text style={[...small, { marginVertical: SPACE.s1 }]}>
          {b.text}
          {b.source !== null && <SourceWord open={b.source} palette={palette} actions={actions} />}
        </Text>
      );
    case "needs":
      return (
        <View testID="licence-needs" style={{ marginTop: 10 }}>
          <Text style={[TYPE.metaBold, { color: palette.sub }]}>{b.label}</Text>
          <View style={{ marginVertical: SPACE.s1 }}>
            {b.items.map((it, i) => <NeedItem key={i} it={it} palette={palette} actions={actions} />)}
          </View>
          {b.onlyIf && (
            <>
              <Text style={[TYPE.metaBold, { color: palette.sub, marginTop: SPACE.s3 }]}>{b.onlyIf.label}</Text>
              <View>{b.onlyIf.items.map((it, i) => <NeedItem key={i} it={it} palette={palette} actions={actions} />)}</View>
            </>
          )}
          {b.priceNote !== null && <Text style={[...small, { marginVertical: SPACE.s2 }]}>{b.priceNote}</Text>}
        </View>
      );
    case "paper":
      return (
        <Text style={[TYPE.meta, { color: palette.ink, backgroundColor: palette.tint, paddingVertical: 8, paddingHorizontal: 10, marginVertical: 6 }]}>
          {b.lead}<Text style={{ fontFamily: TYPE.metaBold.fontFamily }}>{b.strong}</Text>{b.tail}
        </Text>
      );
    case "nymbox":
      return (
        <View style={{ marginVertical: SPACE.s1 }}>
          <Text style={[TYPE.metaBold, { color: palette.sub }]}>{b.label}</Text>
          <Text style={[TYPE.regsBody, { color: palette.ink }]}>{b.says}</Text>
          <Text style={small}>{b.note}</Text>
        </View>
      );
    case "muted":
      return <Text style={[...small, { marginVertical: SPACE.s1 }]}>{b.text}</Text>;
    case "lostlist lopen":
      return (
        <View testID="licence-lostlist" style={{ marginTop: SPACE.s3, paddingTop: SPACE.s2, borderTopWidth: 1, borderTopColor: palette.line }}>
          <Text style={[TYPE.kicker, { color: palette.sub, marginBottom: 4 }]}>{b.label}</Text>
          {b.cards.map((c, i) => (
            <View key={i} style={{ paddingVertical: 6 }}>
              <Text style={small}>{c.forWho}</Text>
              <Text style={[TYPE.regsBody, { color: palette.ink, paddingVertical: 2 }]}>{c.label}</Text>
              {/* the paths stay in the tree, hidden (prototype `.lopen .lpath, .lopen .lor { display: none }`) */}
              <View style={{ display: "none" }}>
                {c.paths.map((p, j) => (
                  <View key={j}>
                    {j > 0 && <Text style={small}>{LIC.OR}</Text>}
                    <Text style={small}>{p}</Text>
                  </View>
                ))}
              </View>
            </View>
          ))}
        </View>
      );
  }
}

export function LicenceAnswer({ blocks, palette, actions }: { blocks: readonly LicenceBlockVM[]; palette: Palette; actions: Acts }) {
  return <View testID="licence-answer">{blocks.map((b, i) => <Block key={i} b={b} palette={palette} actions={actions} />)}</View>;
}

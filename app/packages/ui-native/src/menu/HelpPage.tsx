/**
 * (T248) THE HELP PAGE: what the map's colours, line styles, badges, the bell and the refresh button mean. It draws
 * `helpVM()` (@app/core) and decides nothing: every swatch is a token resolved through the map's theme (the legend's
 * own resolvers) or the very glyph the app wears (`SyncGlyph`, `BellGlyph`, `FreshDot`), so the page cannot say a colour
 * the map no longer draws. "Show the walkthrough again" re-opens the first-run cards.
 */
import { Text, View } from "react-native";
import { HELP_WORDS, WALKTHROUGH_WORDS, helpRowAloud, helpVM, type HelpRow, type HelpSwatch } from "@app/core";
import { gearEdgeColour, runStateColour, salmonStatusColour, tidalColours } from "@app/map";
import { Button } from "../Button";
import { DottedSwatch } from "../Chrome";
import { BellGlyph, SyncGlyph } from "../TopBar";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { FreshDot } from "./Fresh";

const W = 34;

/** One swatch, in the size of a row's lead cell. */
export function HelpSwatchView({ swatch, palette, theme }: { swatch: HelpSwatch; palette: Palette; theme: string }) {
  const line = (colour: string) => <View style={{ width: W, height: 4, backgroundColor: colour }} />;
  switch (swatch.kind) {
    case "status": {
      const c = palette.waterStatus[swatch.status];
      return swatch.dotted ? <DottedSwatch colour={c} width={W} /> : line(c);
    }
    case "gear":
      return <View testID={`help-edge-${swatch.edge}`}
                   style={{ width: W, height: 10, borderWidth: 1.25, borderColor: gearEdgeColour(theme), borderStyle: swatch.edge }} />;
    case "tidal": {
      const t = tidalColours(theme);
      return <View testID="help-tidal" style={{ width: W, height: 16, backgroundColor: t.fill, borderWidth: 1, borderColor: t.edge }} />;
    }
    case "salmon": return line(salmonStatusColour(theme, swatch.status));
    case "run": {
      const c = runStateColour(theme, swatch.state);
      return swatch.dotted ? <DottedSwatch colour={c} width={W} /> : line(c);
    }
    case "fresh": return <FreshDot palette={palette} kind={swatch.fresh} size={10} />;
    case "sync": return <SyncGlyph palette={palette} sync={swatch.sync} />;
    case "bell": return <BellGlyph palette={palette} count={3} />;
  }
}

function Row({ row, palette, theme, first }: { row: HelpRow; palette: Palette; theme: string; first: boolean }) {
  return (
    <View testID={`help-row-${row.key}`} accessible accessibilityLabel={helpRowAloud(row)}
          style={{ flexDirection: "row", alignItems: "flex-start", gap: SPACE.s3, paddingVertical: 10,
                   borderTopWidth: first ? 0 : 1, borderTopColor: palette.line }}>
      <View style={{ width: 44, minHeight: 22, alignItems: "center", justifyContent: "center", paddingTop: 2 }}>
        <HelpSwatchView swatch={row.swatch} palette={palette} theme={theme} />
      </View>
      <View style={{ flex: 1, gap: 2 }}>
        <Text style={{ ...TYPE.bodyStrong, fontSize: 14.5, color: palette.ink }}>{row.name}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{row.text}</Text>
      </View>
    </View>
  );
}

export function HelpPage({ palette, theme, onWalkthrough }: {
  palette: Palette; theme: string;
  /** re-open the first-run walkthrough; absent: no button */
  onWalkthrough?: () => void;
}) {
  const sections = helpVM();
  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.ink }}>{HELP_WORDS.lead}</Text>
      {onWalkthrough && (
        <View testID="help-walkthrough">
          <Button palette={palette} kind="ghost" label={WALKTHROUGH_WORDS.again} onPress={onWalkthrough} />
        </View>
      )}
      {sections.map((s) => (
        <View key={s.key} testID={`help-section-${s.key}`} style={{ gap: SPACE.s1 }}>
          <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{s.title}</Text>
          {s.intro && <Text style={{ ...TYPE.small, color: palette.sub }}>{s.intro}</Text>}
          <View>
            {s.rows.map((r, i) => <Row key={r.key} row={r} palette={palette} theme={theme} first={i === 0} />)}
          </View>
        </View>
      ))}
    </>
  );
}

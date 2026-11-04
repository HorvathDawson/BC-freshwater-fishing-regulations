/**
 * (T248) THE HELP PAGE: what the map's colours, line styles, badges, the bell and the refresh button mean. It draws
 * `helpVM()` (@app/core) and decides nothing: every swatch is a token resolved through the map's theme (the legend's
 * own resolvers) or the very glyph the app wears (`SyncGlyph`, `BellGlyph`, `FreshDot`), so the page cannot say a colour
 * the map no longer draws. (T293) Above them, short pages: the walkthrough (replay it, and each mode hint), syncing and
 * updates, and offline.
 */
import { Platform, View } from "react-native";
import { Text } from "../Text";
import { HELP_WORDS, TAB_TOUR_WORDS, WALKTHROUGH_WORDS, helpRowAloud, helpVM, type HelpRow, type HelpSwatch } from "@app/core";
import { gearEdgeColour, notFishingColours, runStateColour, salmonStatusColour, tidalColours } from "@app/map/colours";
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
    case "notfishing": {
      const t = notFishingColours(theme);
      return <View testID="help-notfishing" style={{ width: W, height: 16, backgroundColor: t.fill, borderWidth: 1, borderColor: t.edge, borderStyle: "dashed" }} />;
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
    <View testID={`help-row-${row.key}`} accessible role="group" accessibilityLabel={helpRowAloud(row)}
          style={{ flexDirection: "row", alignItems: "flex-start", gap: SPACE.s3, paddingVertical: 10,
                   borderTopWidth: first ? 0 : 1, borderTopColor: palette.line }}>
      <View style={{ width: 44, minHeight: 22, alignItems: "center", justifyContent: "center", paddingTop: 2 }}>
        <HelpSwatchView swatch={row.swatch} palette={palette} theme={theme} />
      </View>
      <View style={{ flex: 1, gap: 2 }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{row.name}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{row.text}</Text>
      </View>
    </View>
  );
}

/** (T293) One of Help's short pages: a heading, a few sentences, and what it opens. */
function Short({ k, title, text, palette, children }: {
  k: string; title: string; text: readonly string[]; palette: Palette; children?: React.ReactNode;
}) {
  return (
    <View testID={`help-page-${k}`} style={{ gap: SPACE.s2 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{title}</Text>
      {text.map((t, i) => <Text key={i} style={{ ...TYPE.body, color: palette.ink }}>{t}</Text>)}
      {children}
    </View>
  );
}

export function HelpPage({ palette, theme, onWalkthrough, onTabTour, onOffline }: {
  palette: Palette; theme: string;
  /** re-open the walkthrough; absent: no button */
  onWalkthrough?: () => void;
  /** (T276, T293) replay a mode's hint; absent: no buttons */
  onTabTour?: (k: "runs" | "conditions" | "salmon") => void;
  /** (T293) open Offline maps (the app's); absent: no button */
  onOffline?: () => void;
}) {
  const sections = helpVM();
  const where = Platform.OS === "web" ? "web" : "app";
  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.ink }}>{HELP_WORDS.lead}</Text>
      {/* (T293) THE SHORT PAGES: the walkthrough, syncing and updates, offline — before what the symbols mean */}
      <Short k="walkthrough" title={HELP_WORDS.guide.title} text={[HELP_WORDS.guide.text]} palette={palette}>
        {onWalkthrough && (
          <View testID="help-walkthrough">
            <Button palette={palette} kind="ghost" label={WALKTHROUGH_WORDS.again} onPress={onWalkthrough} />
          </View>
        )}
        {onTabTour && (["runs", "conditions", "salmon"] as const).map((k) => (
          <View key={k} testID={`help-tour-${k}`}>
            <Button palette={palette} kind="ghost" label={TAB_TOUR_WORDS.again[k]} onPress={() => onTabTour(k)} />
          </View>
        ))}
      </Short>
      <Short k="updates" title={HELP_WORDS.updates.title} text={HELP_WORDS.updates[where]} palette={palette} />
      <Short k="offline" title={HELP_WORDS.offline.title} text={HELP_WORDS.offline[where]} palette={palette}>
        {onOffline && where === "app" && (
          <View testID="help-offline">
            <Button palette={palette} kind="ghost" label={HELP_WORDS.offline.open} onPress={onOffline} />
          </View>
        )}
      </Short>
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

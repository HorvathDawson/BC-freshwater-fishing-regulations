/**
 * (T248) THE HELP PAGE: what the map's colours, line styles, badges, the bell and the refresh button mean. It draws
 * `helpVM()` (@app/core) and decides nothing: every swatch is a token resolved through the map's theme (the legend's
 * own resolvers) or the very glyph the app wears (`SyncGlyph`, `BellGlyph`, `FreshDot`), so the page cannot say a colour
 * the map no longer draws. (T873, owner 2026-11-18: "way way to verbose") Read in ten seconds: the map's key first (each
 * swatch with its name, and a note only where the name needs one), then a few one-line facts on offline and updates with
 * the Offline maps and walkthrough buttons, then the refresh button, the bell and the badges. No intro.
 */
import { Platform, View } from "react-native";
import { phoneScreensOnWeb } from "../storeShots";
import { Text } from "../Text";
import { HELP_MAP_KEY, HELP_WORDS, WALKTHROUGH_WORDS, helpRowAloud, helpVM, type HelpRow, type HelpSection,
         type HelpSwatch } from "@app/core";
import { gearEdgeColour, notFishingColours, runStateColour, salmonCasingColour, salmonStatusColour, tidalColours } from "@app/map/colours";
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
    case "salmon": {
      // (T887) option B's keep: the bright line in its ink casing, as the map draws it
      const cased = swatch.status === "open" ? salmonCasingColour(theme) : null;
      return cased ? <View style={{ width: W, height: 6, backgroundColor: salmonStatusColour(theme, swatch.status), borderWidth: 1, borderColor: cased }} />
        : line(salmonStatusColour(theme, swatch.status));
    }
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
          style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3, paddingVertical: SPACE.s2,
                   borderTopWidth: first ? 0 : 1, borderTopColor: palette.line }}>
      <View style={{ width: 44, minHeight: 22, alignItems: "center", justifyContent: "center" }}>
        <HelpSwatchView swatch={row.swatch} palette={palette} theme={theme} />
      </View>
      <View style={{ flex: 1 }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{row.name}</Text>
        {row.note && <Text style={{ ...TYPE.small, color: palette.sub }}>{row.note}</Text>}
      </View>
    </View>
  );
}

function Section({ s, palette, theme }: { s: HelpSection; palette: Palette; theme: string }) {
  return (
    <View testID={`help-section-${s.key}`} style={{ gap: SPACE.s1 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{s.title}</Text>
      <View>
        {s.rows.map((r, i) => <Row key={r.key} row={r} palette={palette} theme={theme} first={i === 0} />)}
      </View>
    </View>
  );
}

export function HelpPage({ palette, theme, onWalkthrough, onOffline }: {
  palette: Palette; theme: string;
  /** re-open the walkthrough; absent: no button */
  onWalkthrough?: () => void;
  /** (T293) open Offline maps (the app's); absent: no button */
  onOffline?: () => void;
}) {
  // (T319) a store-shots web build shows the app's lines (`storeShots.ts`)
  const where = Platform.OS === "web" && !phoneScreensOnWeb() ? "web" : "app";
  const sections = helpVM(where);
  const key = sections.filter((s) => HELP_MAP_KEY.includes(s.key));
  const rest = sections.filter((s) => !HELP_MAP_KEY.includes(s.key));
  const F = HELP_WORDS.facts;
  return (
    <>
      {key.map((s) => <Section key={s.key} s={s} palette={palette} theme={theme} />)}
      {/* (T873) offline and updates as one-line facts, then what they open */}
      <View testID="help-facts" style={{ gap: SPACE.s2 }}>
        <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{F.title}</Text>
        {F[where].map((t, i) => <Text key={i} style={{ ...TYPE.body, color: palette.ink }}>{t}</Text>)}
        {onOffline && where === "app" && (
          <View testID="help-offline">
            <Button palette={palette} kind="ghost" label={F.offlineMaps} onPress={onOffline} />
          </View>
        )}
        {onWalkthrough && (
          <View testID="help-walkthrough">
            <Button palette={palette} kind="ghost" label={WALKTHROUGH_WORDS.again} onPress={onWalkthrough} />
          </View>
        )}
      </View>
      {rest.map((s) => <Section key={s.key} s={s} palette={palette} theme={theme} />)}
    </>
  );
}

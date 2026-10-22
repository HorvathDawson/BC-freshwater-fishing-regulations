/**
 * THE LAYERS SHEET — direction B (owner 2026-10-21 late): Map · Colour the water by · On the map, then the layers that
 * are always shown. It draws `@app/core` `layersVM` and decides nothing: which tiles exist, which are greyed and why,
 * what is chosen and what the map is handed all come from the layer registry (`core/layers.ts`), so a new layer is one
 * registry entry and no change here.
 *
 *  - ONE water colouring for every feature (no lake-only choice): the legend under it says which waters it does not
 *    cover, drawn neutral.
 *  - Greyed tiles say why in their own tag: NO DATA YET (Stocked, Depth: prepared, not fed), NEEDS A CONNECTION
 *    (Satellite offline, with SHOWING on the drawn map), COMING SOON (declared, not built).
 *  - NOT HERE any more: the data sources, live data and credits (the menu's Data sources & dates page, and the map's own
 *    attribution) and the app's colours (the menu's Display page).
 */
import { Pressable, Text, View } from "react-native";
import { LAYER_WORDS, STOCK_RECENCY, waterLegend, type LayerTileVM, type LayersVM, type LegendSwatch }
  from "@app/core";
import { Sheet } from "./Sheet";
import { OptionRow, type Option } from "./OptionRow";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** A legend swatch's colour, from the theme's map tokens (never restated): what the map draws for it. */
export function legendColour(p: Palette, s: LegendSwatch): string {
  switch (s.kind) {
    case "status": return p.waterStatus[s.status];
    case "stock": return p.stock[STOCK_RECENCY.findIndex((b) => b.band === s.band)]!;
    case "survey": return p.survey[s.state === "digitised" ? 0 : 1];
    case "neutral": return p.water[0];
  }
}

/** Each tile's three colours: what the map will look like before the reader commits to it. */
function swatch(p: Palette, id: string): Option["swatch"] {
  switch (id) {
    case "ground.map": return [p.tint, p.water[0], p.water[1]];
    // the same in every theme, on purpose (`color.ui.imagery.*`): aerial photography is dark water, forest and scrub
    case "ground.satellite": case "ground.corridors": return p.imagery;
    case "water.regulations": return [p.waterStatus.closed, p.waterStatus.own, p.waterStatus.base];
    case "water.stocked": return [p.stock[0], p.stock[2], p.stock[4]];
    case "water.depth": return [p.survey[0], p.survey[1], p.water[0]];
    default: return [p.quiet, p.quiet, p.quiet];
  }
}

const option = (p: Palette, t: LayerTileVM): Option =>
  ({ k: t.id, t: t.label, note: t.note ?? undefined, tag: t.tag ?? undefined, swatch: swatch(p, t.id),
     disabled: !t.enabled });

export function LayersSheet({ open, onClose, palette, vm, onChoose }: {
  open: boolean; onClose: () => void; palette: Palette;
  /** `@app/core` layersVM — the Shell's, for the view the sheet opened on. */
  vm: LayersVM;
  /** A tile or a switch was pressed (`chooseLayer`); greyed ones never call it. */
  onChoose: (id: string) => void;
}) {
  const chosen = (ts: readonly LayerTileVM[]) => ts.find((t) => t.chosen)?.id ?? "";
  return (
    <Sheet open={open} onClose={onClose} title={vm.title} palette={palette} scroll>
      {vm.ground.tiles.length > 0 && (
        <Section palette={palette} title={vm.ground.title}>
          <OptionRow palette={palette} shape="square" label={vm.ground.title} value={chosen(vm.ground.tiles)}
                     options={vm.ground.tiles.map((t) => option(palette, t))} onChange={onChoose} />
          {/* SATELLITE IS ONLINE-ONLY (owner 2026-10-21): the choice is kept, the map draws its own ground, and this
              says why until the imagery is back */}
          {vm.ground.note && <Fine palette={palette} testID="basemap-note">{vm.ground.note}</Fine>}
        </Section>
      )}

      {vm.water.tiles.length > 0 && (
        <Section palette={palette} title={vm.water.title}>
          <OptionRow palette={palette} shape="line" label={vm.water.title} value={chosen(vm.water.tiles)}
                     options={vm.water.tiles.map((t) => option(palette, t))} onChange={onChoose} />
          {/* THE KEY, inside the chosen colouring (it wraps at sheet width): what each colour means, and which waters
              the measure does not reach */}
          <View testID="water-legend" style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 14, rowGap: 6,
                                               paddingTop: 10 }}>
            {waterLegend(vm.water.drawn).map((r) => (
              <View key={r.label} style={{ flexDirection: "row", alignItems: "center", gap: 6,
                                           maxWidth: r.swatch.kind === "neutral" ? "100%" : undefined }}>
                <View style={{ width: 9, height: 9, backgroundColor: legendColour(palette, r.swatch) }} />
                <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub, flexShrink: 1 }}>{r.label}</Text>
              </View>
            ))}
          </View>
          {vm.water.note && <Fine palette={palette} testID="lake-modes-note">{vm.water.note}</Fine>}
        </Section>
      )}

      {vm.overlays.rows.length > 0 && (
        <Section palette={palette} title={vm.overlays.title}>
          {vm.overlays.rows.map((r) => <SwitchRow key={r.id} palette={palette} row={r} onPress={() => onChoose(r.id)} />)}
        </Section>
      )}

      {/* ALWAYS SHOWN: the safety layers and the boundaries, Indigenous land included — listed, never offered */}
      <Text testID="layers-always"
            style={{ ...TYPE.small, fontSize: 12, lineHeight: 17, color: palette.sub, paddingTop: 14, paddingBottom: 6 }}>
        <Text style={{ fontWeight: "700", color: palette.ink }}>{LAYER_WORDS.always.lead}</Text>
        {vm.always.slice(LAYER_WORDS.always.lead.length)}
      </Text>
    </Sheet>
  );
}

function Section({ palette, title, children }: { palette: Palette; title: string; children: React.ReactNode }) {
  return (
    <View style={{ paddingTop: 16, paddingBottom: 6, borderBottomWidth: 1, borderBottomColor: palette.line }}>
      <Text accessibilityRole="header"
            style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6, color: palette.sub, marginBottom: 11 }}>
        {title.toUpperCase()}
      </Text>
      {children}
    </View>
  );
}

/** One "On the map" switch: what it is, a line saying more, and its state — or why it cannot be switched yet. */
function SwitchRow({ palette, row, onPress }: { palette: Palette; row: LayerTileVM; onPress: () => void }) {
  const off = !row.enabled;
  return (
    <Pressable onPress={off ? undefined : onPress} disabled={off} testID={`layer-${row.id}`}
               accessibilityRole="switch" aria-checked={row.chosen} aria-disabled={off} accessibilityLabel={row.label}
               style={{ flexDirection: "row", alignItems: "center", gap: 12, paddingVertical: 10,
                        borderTopWidth: 1, borderTopColor: palette.line, opacity: off ? 0.45 : 1 }}>
      <View style={{ flex: 1 }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{row.label}</Text>
        {row.note && <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>{row.note}</Text>}
      </View>
      {row.tag
        ? <Text style={{ ...TYPE.section, fontSize: 10, letterSpacing: 1.2, color: palette.sub }}>
            {row.tag.toUpperCase()}
          </Text>
        : (
          // a square switch, as everything here is square: filled accent with its knob right when on
          <View style={{ width: 40, height: 22, borderWidth: 1.5, borderColor: row.chosen ? palette.accent : palette.ink,
                         backgroundColor: row.chosen ? palette.accent : "transparent", padding: 2,
                         alignItems: row.chosen ? "flex-end" : "flex-start" }}>
            <View style={{ width: 15, height: 15, backgroundColor: row.chosen ? palette.onAccent : palette.ink }} />
          </View>
        )}
    </Pressable>
  );
}

function Fine({ palette, children, testID }: { palette: Palette; children: React.ReactNode; testID?: string }) {
  return (
    <Text testID={testID} style={{ ...TYPE.small, fontSize: 11.5, lineHeight: 16.5, color: palette.sub, paddingTop: 8 }}>
      {children}
    </Text>
  );
}

/**
 * (T211, restyled owner 2026-10-28) THE SALMON CARD — "Salmon · DFO", drawn like a Keep group (GroupCard): a head under
 * a heavy rule, then one boxed row per answer with a status rail and chip, the species' pictures (ocean AND spawning
 * looks) each opening its fish sheet (sp:CH … — the ID tips), then DFO's source and as-of block. Above the provincial
 * Keep tab. Every species closed: one "Salmon — closed" row. DFO places on this water not drawn yet follow IN THE SAME
 * TABLE (T221), each row tagged with its place and "Location not yet mapped". A water with DFO places not drawn yet leads with the partial-coverage line and DFO's page
 * (owner 2026-10-28). Then (owner 2026-10-28) the provincial bait ban / fly only that day (it applies to salmon too), the
 * header notes naming this water, and the page's DFO regions' header rules in a compact "Region rules (DFO)" block; an
 * open row carries the header's sizes, and (T227) any row that may be fished carries the region notes about its species
 * (DFO's words: what an "adult chinook" is …), which the region block then leaves out. No logic and no words of its own: everything is core `salmonCard`'s.
 */
import { Image, Linking, Text, View } from "react-native";
import { FISH, SALMON_WORDS, WORDS, type SalmonCardVM, type SalmonRowVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { WaterInset } from "../WaterFrame";
import { RegsPress } from "./RegsPress";
import type { KeepActions } from "./LineText";
import type { FishImages } from "./types";

const chipColour = (p: Palette, s: SalmonRowVM["status"]): string =>
  s === "open" ? p.open : s === "release" ? p.restricted : s === "closed" ? p.closed : p.sub;

function Chip({ status, label, palette }: { status: SalmonRowVM["status"]; label: string; palette: Palette }) {
  return (
    <View style={{ backgroundColor: chipColour(palette, status), paddingHorizontal: 6, paddingVertical: 2,
                   borderRadius: palette.r.chip }}>
      <Text style={{ ...TYPE.pill, fontSize: 11, color: palette.onAccent }}>{label}</Text>
    </View>
  );
}

/** Each species' pictures — its ocean look and its spawning look side by side — and a link to its fish sheet. */
function Looks({ species, palette, actions, images }: { species: SalmonRowVM["species"]; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  return (
    <View testID="salmon-looks" style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 12, marginTop: 6 }}>
      {species.map((code) => {
        const looks = FISH[code] ?? [];
        const name = looks[0]?.caption ?? code;
        return (
          <RegsPress key={code} testID={`pic:${code}`} accessibilityRole="button" accessibilityLabel={WORDS.LOOKS_LIKE(name)}
                     box={{ w: 80, h: 44 }} onPress={() => actions?.onSheet(`sp:${code}`)} style={{ paddingVertical: 4, minHeight: 44 }}>
            <View style={{ flexDirection: "row", columnGap: 4 }}>
              {looks.map((l, i) => {
                const src = images?.[code]?.[i];
                return (
                  <View key={l.file} testID="salmon-look">
                    {src ? <Image source={src} style={{ width: 80, height: 40 }} resizeMode="contain" accessibilityIgnoresInvertColors />
                      : <View style={{ width: 80, height: 40, backgroundColor: palette.line2 }} />}
                    {l.phase && <Text style={[TYPE.micro, { color: palette.sub, textAlign: "center" }]}>{l.phase}</Text>}
                  </View>
                );
              })}
            </View>
            <Text style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.accent, marginTop: 2 }]}>{name} ›</Text>
          </RegsPress>
        );
      })}
    </View>
  );
}

/** One answer row of the table: its status rail and chip, DFO's words, the sizes, the species' pictures. `place`: a DFO
 *  place on this water not drawn yet (its words and the tag), said inside the row (T221: one table, owner 2026-10-28). */
function Row({ r, allClosed, first, place, palette, testID, actions, images }: {
  r: SalmonRowVM; allClosed: boolean; first: boolean; place?: { label: string; tag: string }; palette: Palette; testID: string;
  actions?: KeepActions; images?: FishImages }) {
  return (
    <View testID={`${testID}-row`}
          style={{ marginTop: first ? 8 : 0, paddingHorizontal: 10, paddingVertical: 8, borderWidth: 1, borderColor: palette.line2,
                   borderTopWidth: first ? 1 : 0, borderLeftWidth: 5, borderLeftColor: chipColour(palette, r.status) }}>
      <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "flex-start", gap: 10 }}>
        <Text style={{ ...TYPE.regsBodySemi, color: palette.ink, flex: 1 }}>
          {allClosed ? `${r.label} — ${r.statusLabel.toLowerCase()}` : r.label}
        </Text>
        <Chip status={r.status} label={r.statusLabel} palette={palette} />
      </View>
      {place && (
        <View testID="salmon-unmapped" style={{ marginTop: 2 }}>
          <Text style={{ ...TYPE.metaSemi, color: palette.ink }}>{place.label}</Text>
          <Text testID="salmon-unmapped-tag" style={{ ...TYPE.kicker, color: palette.sub }}>{place.tag}</Text>
        </View>
      )}
      {r.notes.map((n) => <Text key={n} style={{ ...TYPE.meta, color: palette.sub, marginTop: 2 }}>{n}</Text>)}
      {r.sizes && <Text testID={`${testID}-sizes`} style={{ ...TYPE.meta, color: palette.ink, marginTop: 2 }}>{r.sizes}</Text>}
      {(r.speciesNotes ?? []).map((n) => (
        <Text key={`sn:${n}`} testID={`${testID}-species-note`} style={{ ...TYPE.meta, color: palette.ink, marginTop: 2 }}>{n}</Text>
      ))}
      <Looks species={r.species} palette={palette} actions={actions} images={images} />
    </View>
  );
}

/**
 * (T221, owner 2026-10-28: "make salmon one table total") THE TABLE: the water's own rows, then each DFO place on it not
 * drawn yet, its rows in the SAME table (the place said in the row) — one table, never a second box under the first.
 * The blocks' gear lines follow it.
 */
function Table({ card, palette, actions, images }: { card: SalmonCardVM; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  // the partial-coverage line above is the table's lead row: the first answer row joins it
  let n = card.partial ? 1 : 0;
  return (
    <View testID="salmon-card-block">
      {card.rows.map((r) => (
        <Row key={r.key} r={r} allClosed={card.allClosed} first={n++ === 0} palette={palette} testID="salmon-card-block"
             actions={actions} images={images} />
      ))}
      {card.unmapped.map((u, i) => u.block.rows.map((r) => (
        <Row key={`${u.label}#${i}:${r.key}`} r={r} allClosed={u.block.allClosed} first={n++ === 0} place={{ label: u.label, tag: u.tag }}
             palette={palette} testID="salmon-unmapped-block" actions={actions} images={images} />
      )))}
      {card.gear.map((g) => (
        <Text key={g} testID="salmon-card-block-gear" style={{ ...TYPE.meta, color: palette.sub, paddingTop: 6 }}>{g}</Text>
      ))}
      {card.unmapped.flatMap((u) => u.block.gear).filter((g) => !card.gear.includes(g)).map((g) => (
        <Text key={`u:${g}`} testID="salmon-unmapped-block-gear" style={{ ...TYPE.meta, color: palette.sub, paddingTop: 6 }}>{g}</Text>
      ))}
    </View>
  );
}

export function SalmonCard({ card, palette, actions, images }: {
  card: SalmonCardVM & { readonly checked?: string | null }; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  // every outside picture is credited where shown (T43): the looks drawn here, once each
  const shown = [...new Set(card.rows.flatMap((r) => r.species).concat(card.unmapped.flatMap((u) => u.block.rows.flatMap((r) => r.species)))
    .flatMap((c) => (FISH[c] ?? []).map((l) => `${l.credit} (${l.source})`)))];
  return (
    <WaterInset style={{ paddingTop: SPACE.s4 }}>
      <View testID="salmon-card" accessibilityLabel={`${card.title}, ${card.badge}: ${card.headline}`}>
        <View style={{ paddingVertical: 8, borderTopWidth: 1.5, borderTopColor: palette.ink, backgroundColor: palette.card }}>
          <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
            <Text accessibilityRole="header" style={[TYPE.lead, { color: palette.ink, flex: 1 }]}>
              {card.title}<Text style={[TYPE.kicker, { color: palette.sub }]}>{`  ${card.badge}`}</Text>
            </Text>
            <Chip status={card.headlineStatus} label={card.headline} palette={palette} />
          </View>
        </View>
        {card.partial && (
          <View testID="salmon-partial" style={{ marginTop: 8, paddingHorizontal: 10, paddingVertical: 8, borderWidth: 1,
                                                  borderColor: palette.line2, borderLeftWidth: 5, borderLeftColor: palette.sub }}>
            <Text style={{ ...TYPE.regsBodySemi, color: palette.ink }}>{card.partial.text}</Text>
            <Text testID="salmon-partial-link" accessibilityRole="link" onPress={() => { void Linking.openURL(card.partial!.url); }}
                  style={{ ...TYPE.metaSemi, color: palette.accent, marginTop: 4, paddingVertical: 4 }}>{card.partial.link} ›</Text>
          </View>
        )}
        <Table card={card} palette={palette} actions={actions} images={images} />
        {card.gearProvincial && (
          <Text testID="salmon-gear-provincial" style={{ ...TYPE.metaSemi, color: palette.ink, paddingTop: 8 }}>{card.gearProvincial}</Text>
        )}
        {card.waterRules.length > 0 && (
          <View testID="salmon-water-rules" style={{ paddingTop: 8 }}>
            <Text style={{ ...TYPE.kicker, color: palette.sub }}>{SALMON_WORDS.waterRules}</Text>
            {card.waterRules.map((t) => <Text key={t} style={{ ...TYPE.meta, color: palette.ink, marginTop: 2 }}>{t}</Text>)}
          </View>
        )}
        {card.notListed && (
          <Text testID="salmon-not-listed" style={{ ...TYPE.meta, color: palette.ink, paddingTop: 8 }}>{card.notListed}</Text>
        )}
        {card.regionRules.map((g) => (
          <View key={g.label} testID="salmon-region-rules"
                style={{ marginTop: 12, paddingTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.kicker, color: palette.sub }}>{`${SALMON_WORDS.regionRules} · ${g.label}`}</Text>
            {g.notes.map((t) => (
              <Text key={t} style={{ ...TYPE.meta, color: palette.ink, marginTop: 2 }}>{`• ${t}`}</Text>
            ))}
          </View>
        ))}
        <View style={{ marginTop: 12, paddingTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
          <Text testID="salmon-source" style={{ ...TYPE.metaBold, color: palette.accent }}>{card.source}</Text>
          <Text testID="salmon-asof" style={{ ...TYPE.meta, color: palette.sub }}>{card.asOf}</Text>
          {card.checked && <Text testID="salmon-checked" style={{ ...TYPE.meta, color: palette.sub }}>{card.checked}</Text>}
          <Text testID="salmon-credit" style={{ ...TYPE.meta, color: palette.sub }}>{card.credit}</Text>
          {shown.length > 0 && <Text testID="salmon-pic-credit" style={{ ...TYPE.meta, color: palette.sub }}>{`${WORDS.FISH_PICTURES}: ${shown.join(" · ")}`}</Text>}
        </View>
        <Text testID="salmon-note" style={{ ...TYPE.metaSemi, color: palette.ink, paddingTop: SPACE.s2 }}>{card.note}</Text>
      </View>
    </WaterInset>
  );
}

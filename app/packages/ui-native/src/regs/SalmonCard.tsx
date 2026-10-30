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
import { useState } from "react";
import { FISH, SALMON_WORDS, WORDS, salmonTable, type SalmonCardVM, type SalmonPlaceTableVM, type SalmonRowVM, type SalmonSaysVM, type SalmonSpecies, type SalmonTableRowVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { WaterInset } from "../WaterFrame";
import { RegsPress } from "./RegsPress";
import { RegionRules } from "./RegionRules";
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

/** The species' OCEAN picture only (the spawning look, and the tips, are in its fish sheet); null when it has none. */
function oceanOf(code: SalmonSpecies, images?: FishImages) {
  const looks = FISH[code] ?? [];
  const i = Math.max(0, looks.findIndex((l) => l.phase === "ocean"));
  return { look: looks[i], src: images?.[code]?.[i], name: looks[i]?.caption ?? code };
}

/** One species of the table: its ocean picture (opens its fish sheet, both looks), its name, today's status, its sizes. */
function SpeciesRow({ r, palette, actions, images, pictures }: {
  r: SalmonTableRowVM; palette: Palette; actions?: KeepActions; images?: FishImages; pictures: boolean }) {
  const o = oceanOf(r.code, images);
  const body = (
    <View style={{ flexDirection: "row", alignItems: "center", columnGap: 10, flex: 1 }}>
      {pictures && (o.src ? <Image source={o.src} testID="salmon-look" style={{ width: 56, height: 30 }} resizeMode="contain" accessibilityIgnoresInvertColors />
        : <View testID="salmon-look" style={{ width: 56, height: 30, backgroundColor: palette.line2 }} />)}
      <View style={{ flex: 1 }}>
        <Text style={{ ...TYPE.regsBodySemi, color: palette.ink }}>{r.name}{pictures && <Text style={{ ...TYPE.metaSemi, color: palette.accent }}>{" ›"}</Text>}</Text>
        {r.limit && <Text selectable testID="salmon-limit" style={{ ...TYPE.meta, color: palette.sub }}>{r.limit}</Text>}
      </View>
    </View>
  );
  return (
    <View testID="salmon-species-row" style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 10, minHeight: 44,
                                               paddingVertical: 4, borderTopWidth: 1, borderTopColor: palette.line2 }}>
      {pictures ? (
        <RegsPress testID={`pic:${r.code}`} accessibilityRole="button" accessibilityLabel={WORDS.LOOKS_LIKE(o.name)} box={{ w: 120, h: 44 }}
                   onPress={() => actions?.onSheet(`sp:${r.code}`)} style={{ flex: 1, minHeight: 44, justifyContent: "center" }}>{body}</RegsPress>
      ) : body}
      <Text testID="salmon-species-status" style={{ ...TYPE.metaBold, color: chipColour(palette, r.status) }}>{r.statusLabel}</Text>
    </View>
  );
}

/** DFO's own words for a group of species, once each. */
function Says({ says, palette }: { says: readonly SalmonSaysVM[]; palette: Palette }) {
  if (!says.length) return null;
  return (
    <View testID="salmon-says" style={{ paddingTop: 8 }}>
      {says.map((x, i) => (
        <View key={i} style={{ marginTop: i ? 6 : 0 }}>
          {x.label && <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{x.label}</Text>}
          {x.lines.map((l) => <Text selectable key={l} testID="salmon-says-line" style={{ ...TYPE.meta, color: palette.sub, marginTop: 2 }}>{l}</Text>)}
        </View>
      ))}
    </View>
  );
}

/** A DFO place on this water not drawn yet, folded to one line (its place, the tag, what it says); opened: its species and DFO's words. */
function Place({ p, palette }: { p: SalmonPlaceTableVM; palette: Palette }) {
  const [open, setOpen] = useState(false);
  return (
    <View testID="salmon-place" style={{ borderTopWidth: 1, borderTopColor: palette.line2, borderLeftWidth: 5, borderLeftColor: chipColour(palette, p.status), paddingLeft: 10 }}>
      <RegsPress testID="salmon-place-fold" accessibilityRole="button" aria-expanded={open} box={{ w: 120, h: 44 }} onPress={() => setOpen(!open)}
                 style={{ minHeight: 44, justifyContent: "center", paddingVertical: 6 }}>
        <View testID="salmon-unmapped">
          <Text selectable style={{ ...TYPE.regsBodySemi, color: palette.ink }}>{p.label}</Text>
          <Text testID="salmon-unmapped-tag" style={{ ...TYPE.kicker, color: palette.sub }}>{p.tag}</Text>
          <Text testID="salmon-place-summary" style={{ ...TYPE.meta, color: palette.sub }}>{p.summary}</Text>
        </View>
        <Text style={{ ...TYPE.metaSemi, color: palette.accent }}>{open ? SALMON_WORDS.placeHide : SALMON_WORDS.placeShow} ›</Text>
      </RegsPress>
      {open && (
        <View testID="salmon-place-body" style={{ paddingBottom: 8 }}>
          {p.species.map((r) => <SpeciesRow key={r.code} r={r} palette={palette} pictures={false} />)}
          <Says says={p.says} palette={palette} />
          {p.gear.map((g) => <Text key={g} testID="salmon-unmapped-block-gear" style={{ ...TYPE.meta, color: palette.sub, paddingTop: 6 }}>{g}</Text>)}
        </View>
      )}
    </View>
  );
}

/**
 * (T318, owner 2026-10-31) THE TABLE: one row per species (its ocean picture, today's status, sizes), DFO's words said once
 * under it, the blocks' gear lines, then the places not drawn yet each folded to one line. Core `salmonTable`'s.
 */
function Table({ card, palette, actions, images }: { card: SalmonCardVM; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  const t = salmonTable(card);
  return (
    <View testID="salmon-card-block">
      <Text accessibilityRole="header" style={{ ...TYPE.kicker, color: palette.sub, marginTop: 10, marginBottom: 2 }}>{SALMON_WORDS.tableSpecies}</Text>
      {t.species.map((r) => <SpeciesRow key={r.code} r={r} palette={palette} actions={actions} images={images} pictures />)}
      <Says says={t.says} palette={palette} />
      {card.gear.map((g) => (
        <Text key={g} testID="salmon-card-block-gear" style={{ ...TYPE.meta, color: palette.sub, paddingTop: 6 }}>{g}</Text>
      ))}
      {t.places.length > 0 && (
        <View testID="salmon-places" style={{ marginTop: 12 }}>
          <Text accessibilityRole="header" style={{ ...TYPE.kicker, color: palette.sub, marginBottom: 2 }}>{SALMON_WORDS.placesHead}</Text>
          {t.places.map((p, i) => <Place key={`${p.label}#${i}`} p={p} palette={palette} />)}
        </View>
      )}
    </View>
  );
}

export function SalmonCard({ card, palette, actions, images }: {
  card: SalmonCardVM & { readonly checked?: string | null }; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  // every outside picture is credited where shown (T43): the looks drawn here, once each
  const shown = [...new Set(salmonTable(card).species.flatMap((r) => { const l = oceanOf(r.code).look; return l ? [`${l.credit} (${l.source})`] : []; }))];
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
            <Text selectable style={{ ...TYPE.regsBodySemi, color: palette.ink }}>{card.partial.text}</Text>
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
            {card.waterRules.map((t) => <Text selectable key={t} style={{ ...TYPE.meta, color: palette.ink, marginTop: 2 }}>{t}</Text>)}
          </View>
        )}
        {card.notListed && (
          <Text testID="salmon-not-listed" style={{ ...TYPE.meta, color: palette.ink, paddingTop: 8 }}>{card.notListed}</Text>
        )}
        {card.regionRules.map((g) => <RegionRules key={g.label} group={g} palette={palette} />)}
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

/**
 * (T211, restyled owner 2026-10-28) THE SALMON CARD — "Salmon · DFO", drawn like a Keep group (GroupCard): a head under
 * a heavy rule, then one boxed row per answer with a status rail and chip, the species' pictures (ocean AND spawning
 * looks) each opening its fish sheet (sp:CH … — the ID tips), then DFO's source and as-of block. Above the provincial
 * Keep tab. Every species closed: one "Salmon — closed" row. (T372, owner 2026-11-03: the trout way) DFO places on this
 * water not drawn yet are the trout card's "in part" caveat block (`CaveatBlock`, core `inPartBlock`), above the table;
 * the rows keep the drawn answer. Then (owner 2026-10-28) the provincial bait ban / fly only that day (it applies to salmon too), the
 * header notes naming this water, and the page's DFO regions' header rules in a compact "Region rules (DFO)" block; an
 * open row carries the header's sizes, and (T227) any row that may be fished carries the region notes about its species
 * (DFO's words: what an "adult chinook" is …), which the region block then leaves out. (T482, owner 2026-11-07) Sizes sit on
 * each species' row (closed rows too), the daylight rule is one `hours` line, and DFO's source block is a fold, closed by
 * default. No logic and no words of its own: everything is core `salmonCard`'s.
 */
import { useState } from "react";
import { Image, View } from "react-native";
import { Text } from "../Text";
import { FISH, SALMON_WORDS, WORDS, salmonTable, type SalmonCardVM, type SalmonRowVM, type SalmonSaysVM, type SalmonSpecies, type SalmonTableRowVM, type SalmonWordsRowVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { WaterInset } from "../WaterFrame";
import { RegsPress } from "./RegsPress";
import { RegionRules } from "./RegionRules";
import { LineText, NO_ACTIONS, type KeepActions } from "./LineText";
import { CaveatBlock } from "./KeepBlocks";
import type { FishImages } from "./types";
import { CoachSpot } from "../CoachMarks";
import { Icon } from "../icons";

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

/** The species' SPAWNING picture (T394, owner 2026-10-31 via T343 item 6: the look it has in a river; the ocean look and
 *  the tips are in its fish sheet); null when it has none. */
function spawningOf(code: SalmonSpecies, images?: FishImages) {
  const looks = FISH[code] ?? [];
  const i = Math.max(0, looks.findIndex((l) => l.phase === "spawning"));
  return { look: looks[i], src: images?.[code]?.[i]?.thumb, name: looks[i]?.caption ?? code };
}

/** One species of the table: its spawning picture (opens its fish sheet, both looks), its name, today's status, its sizes. */
function SpeciesRow({ r, palette, actions, images, pictures }: {
  r: SalmonTableRowVM; palette: Palette; actions?: KeepActions; images?: FishImages; pictures: boolean }) {
  const o = spawningOf(r.code, images);
  const body = (
    <View style={{ flexDirection: "row", alignItems: "center", columnGap: 10, flex: 1 }}>
      {pictures && (o.src ? <Image source={o.src} testID="salmon-look" style={{ width: 56, height: 30 }} resizeMode="contain" accessibilityIgnoresInvertColors />
        : <View testID="salmon-look" style={{ width: 56, height: 30, backgroundColor: palette.line2 }} />)}
      <View style={{ flex: 1 }}>
        <Text style={{ ...TYPE.regsBodySemi, color: palette.ink }}>{r.name}{pictures && <Text style={{ ...TYPE.metaSemi, color: palette.accent }}>{` ${SALMON_WORDS.more}`}</Text>}</Text>
        {/* (T332) the quota tree drawn as the trout card draws its tiers (core quotaLine: tierWords / kindWords) */}
        {r.quotaRuns && <LineText testID="salmon-quota" line={r.quotaRuns} style={TYPE.metaSemi} color={palette.ink} palette={palette}
                                  actions={actions ?? NO_ACTIONS} num={TYPE.metaBold} />}
        {r.window && <Text selectable testID="salmon-window" style={{ ...TYPE.meta, color: palette.sub }}>{r.window}</Text>}
        {r.quotaUnread && <Text testID="salmon-quota-unread" style={{ ...TYPE.meta, color: palette.sub }}>{SALMON_WORDS.quotaUnread}</Text>}
        {r.limit && <Text selectable testID="salmon-limit" style={{ ...TYPE.meta, color: palette.sub }}>{r.limit}</Text>}
        {/* (T668 fix 4) where today's answer comes from: "Region 6 · Skeena River · B(ii)" (core `placeFrom`) */}
        {r.from && <Text selectable testID="salmon-from" style={{ ...TYPE.meta, color: palette.sub }}>{r.from}</Text>}
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

/** (T321, owner 2026-10-31) DFO's rows in DFO's words: `strong` — the wording changed and the app reads no number for
 *  them (a boxed block above the table); else the rows whose words matter as printed. Species and dates, then DFO's text. */
function Words({ head, rows, palette, strong, testID }: {
  head: string; rows: readonly SalmonWordsRowVM[]; palette: Palette; strong?: boolean; testID: string }) {
  return (
    <View testID={testID} style={strong
      ? { marginTop: 8, paddingHorizontal: 10, paddingVertical: 8, borderWidth: 1, borderColor: palette.line2, borderLeftWidth: 5, borderLeftColor: palette.restricted }
      : { marginTop: 10 }}>
      <Text accessibilityRole="header" style={strong ? { ...TYPE.regsBodySemi, color: palette.ink } : { ...TYPE.kicker, color: palette.sub }}>{head}</Text>
      {rows.map((r, i) => (
        <View key={`${r.species}|${r.dates}|${r.words}`} testID="salmon-words-row" style={{ marginTop: i ? 6 : 4 }}>
          <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{`${r.species} · ${r.dates}`}</Text>
          <Text selectable testID="salmon-words-text" style={{ ...TYPE.meta, color: palette.ink, marginTop: 2 }}>{r.words}</Text>
        </View>
      ))}
    </View>
  );
}

/**
 * (T318, owner 2026-10-31) THE TABLE: one row per species (its spawning picture (T394), today's status, sizes), DFO's words said once
 * under it, the blocks' gear lines, then the places not drawn yet each folded to one line. Core `salmonTable`'s.
 */
function Table({ card, palette, actions, images }: { card: SalmonCardVM; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  const t = salmonTable(card);
  return (
    <View testID="salmon-card-block">
      <Text accessibilityRole="header" style={{ ...TYPE.kicker, color: palette.sub, marginTop: 10, marginBottom: 2 }}>{SALMON_WORDS.tableSpecies}</Text>
      {t.bucket && (
        <View testID="salmon-bucket" style={{ paddingVertical: 6 }}>
          {/* (T332) the bucket is the quota tree's total tier: drawn by the trout presenter (core tierWords) */}
          <LineText testID="salmon-bucket-line" line={t.bucket.runs ?? [{ t: "text", s: t.bucket.line }]} style={TYPE.regsBodySemi}
                    color={palette.ink} palette={palette} actions={actions ?? NO_ACTIONS} num={TYPE.regsBodyBold} />
        </View>
      )}
      {(t.waterBuckets ?? []).map((w) => (
        <View key={w.line} style={{ paddingBottom: 4 }}>
          <LineText testID="salmon-water-bucket" line={w.runs ?? [{ t: "text", s: w.line }]} style={TYPE.metaSemi}
                    color={palette.ink} palette={palette} actions={actions ?? NO_ACTIONS} num={TYPE.metaBold} />
        </View>
      ))}
      {t.species.map((r) => <SpeciesRow key={r.code} r={r} palette={palette} actions={actions} images={images} pictures />)}
      {/* (T343, T482) the page's salmon hours, one line (the daylight note folds into it) */}
      {card.hours && <Text selectable testID="salmon-hours" style={{ ...TYPE.metaSemi, color: palette.ink, paddingTop: 8 }}>{card.hours}</Text>}
      <Says says={t.says} palette={palette} />
      {card.gear.map((g) => (
        <Text key={g} testID="salmon-card-block-gear" style={{ ...TYPE.meta, color: palette.sub, paddingTop: 6 }}>{g}</Text>
      ))}
    </View>
  );
}

/**
 * (T482, owner 2026-11-07: "the source of the regs should be collapsible"; T343 item 7) DFO's source block — the
 * agency and region, the page's date, when it was checked, the credit, the pictures' credit, and the two-authorities
 * note — as ONE fold, closed by default, like the Methods tab's Reference fold and T479's examples fold. Its state is
 * local (it changes no answer). The walkthrough's "both rules hold" rings its summary row.
 */
function SourceFold({ card, shown, palette }: {
  card: SalmonCardVM & { readonly checked?: string | null }; shown: readonly string[]; palette: Palette }) {
  const [open, setOpen] = useState(false);
  return (
    <View testID="salmon-source-fold" style={{ marginTop: 12, borderTopWidth: 1, borderTopColor: palette.line }}>
      <CoachSpot k="regs:both">
        <RegsPress testID="salmon-source-summary" accessibilityRole="button" aria-expanded={open} onPress={() => setOpen(!open)} box={{ w: 44, h: 44 }}
                   style={{ minHeight: 44, flexDirection: "row", alignItems: "center", columnGap: 8, paddingVertical: 6 }}>
          <Text testID="kicker:salmon-source" style={[TYPE.kicker, { flex: 1, color: palette.sub }]}>{SALMON_WORDS.sourceFold}</Text>
          <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={16} />
        </RegsPress>
      </CoachSpot>
      {open && (
        <View testID="salmon-source-body" style={{ paddingBottom: 4 }}>
          <Text testID="salmon-source" style={{ ...TYPE.metaBold, color: palette.accent }}>{card.source}</Text>
          <Text testID="salmon-asof" style={{ ...TYPE.meta, color: palette.sub }}>{card.asOf}</Text>
          {card.checked && <Text testID="salmon-checked" style={{ ...TYPE.meta, color: palette.sub }}>{card.checked}</Text>}
          <Text testID="salmon-credit" style={{ ...TYPE.meta, color: palette.sub }}>{card.credit}</Text>
          {shown.length > 0 && <Text testID="salmon-pic-credit" style={{ ...TYPE.meta, color: palette.sub }}>{`${WORDS.FISH_PICTURES}: ${shown.join(" · ")}`}</Text>}
          <Text testID="salmon-note" style={{ ...TYPE.metaSemi, color: palette.ink, paddingTop: SPACE.s2 }}>{card.note}</Text>
        </View>
      )}
    </View>
  );
}

export function SalmonCard({ card, palette, actions, images }: {
  card: SalmonCardVM & { readonly checked?: string | null }; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  // every outside picture is credited where shown (T43): the looks drawn here, once each
  const shown = [...new Set(salmonTable(card).species.flatMap((r) => { const l = spawningOf(r.code).look; return l ? [`${l.credit} (${l.source})`] : []; }))];
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
        {card.caveat && <CaveatBlock caveat={card.caveat} palette={palette} actions={actions ?? NO_ACTIONS} inset={false} />}
        {card.dfoWords && card.dfoWords.changed.length > 0 && (
          <Words testID="salmon-wording-changed" head={card.dfoWords.changedHead} rows={card.dfoWords.changed} palette={palette} strong />
        )}
        <Table card={card} palette={palette} actions={actions} images={images} />
        {card.dfoWords && card.dfoWords.notes.length > 0 && (
          <Words testID="salmon-wording-notes" head={card.dfoWords.notesHead} rows={card.dfoWords.notes} palette={palette} />
        )}
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
        <SourceFold card={card} shown={shown} palette={palette} />
      </View>
    </WaterInset>
  );
}

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
 * default. (T796) Where the rule book closes the page's salmon today the head's badge, the fold's title and its first
 * lines name the book (page, row, its words and dates). (T828, owner 2026-11-16) DFO's rows as printed — the species' row
 * words, DFO's words for this water, the region rules — sit in that fold under the book's lines; the "DFO changed the
 * wording" warning stays above the table. (T829, owner 2026-11-16 "B: trout keep box") The species rows in the trout
 * table's look (`KeepBox`): the species open today in the tint box, each one's number in the white box on the right;
 * release only, then closed species and another fish's closure (the eulachon line) in the "✕ Closed" group under it.
 * No logic and no words of its own: everything is core `salmonCard`'s.
 */
import { useState } from "react";
import { View } from "react-native";
import { Text } from "../Text";
import { FISH, SALMON_WORDS, WORDS, salmonTable, type SalmonCardVM, type SalmonIssueVM, type SalmonRowVM, type SalmonSaysVM, type SalmonSourceVM, type SalmonSpecies, type SalmonTableRowVM, type SalmonWordsRowVM } from "@app/core";
import { TYPE } from "../type";
import { RADIUS, SPACE, type Palette } from "../theme";
import { WaterInset } from "../WaterFrame";
import { RegsPress } from "./RegsPress";
import { RegionRules } from "./RegionRules";
import { NO_ACTIONS, type KeepActions } from "./LineText";
import { KeepBox, ShutGroup, type KeepBandVM, type KeepRowVM } from "./KeepBox";
import { CaveatBlock } from "./KeepBlocks";
import type { FishImages } from "./types";
import { CoachSpot } from "../CoachMarks";
import { Icon } from "../icons";

const chipColour = (p: Palette, s: SalmonRowVM["status"]): string =>
  s === "open" ? p.salmon.open : s === "release" ? p.salmon.release : s === "closed" ? p.salmon.closed : p.sub;

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

/**
 * (T829) One species as a keep-box row: its spawning picture (opens its fish sheet, both looks), its name, then its quiet
 * lines — a number DFO's words would not give (`quotaUnread`), its sizes, where today's answer comes from (T668 fix 4,
 * core `placeFrom`). Its number today is the row's band (`speciesBand`), never a line.
 */
function speciesRow(r: SalmonTableRowVM, actions?: KeepActions, images?: FishImages): KeepRowVM {
  const o = spawningOf(r.code, images);
  return { key: r.code, name: r.name, label: null, sub: null, lines: [],
           quiet: [...(r.quotaUnread ? [SALMON_WORDS.quotaUnread] : []), ...(r.limit ? [r.limit] : []), ...(r.from ? [r.from] : [])],
           pic: { source: o.src, more: SALMON_WORDS.more, label: WORDS.LOOKS_LIKE(o.name), testID: `pic:${r.code}`,
                  onPress: () => actions?.onSheet(`sp:${r.code}`) } };
}

/** (T829) an open species' cell in the box's right band: its numbers today in the white box ("4 a day · only 2 over 50
 *  cm", the quota tree's words) with the dates they run small under them; no number of its own: its status word, free */
const speciesBand = (r: SalmonTableRowVM, i: number): KeepBandVM =>
  r.quota ? { from: i, to: i, kind: "own", text: r.quota, date: r.window ?? null }
    : { from: i, to: i, kind: "upto", text: r.statusLabel };

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
 * (T318, owner 2026-10-31) THE TABLE: one row per species (its spawning picture (T394), today's status, sizes), the
 * blocks' gear lines. Core `salmonTable`'s. (T829) Laid out as the trout table: open species in the keep box, the others
 * in their groups under it by status. (T770) Another fish's closure (core `closures`) is a closed row, never a gear line.
 */
function Table({ card, palette, actions, images }: { card: SalmonCardVM; palette: Palette; actions?: KeepActions; images?: FishImages }) {
  const t = salmonTable(card);
  const open = t.species.filter((r) => r.status === "open");
  const release = t.species.filter((r) => r.status === "release");
  const closed = t.species.filter((r) => r.status === "closed");
  const others = [...new Set(t.species.filter((r) => r.status === "check" || r.status === "varies").map((r) => r.statusLabel))]
    .map((label) => [label, t.species.filter((r) => r.statusLabel === label)] as const);
  return (
    <View testID="salmon-card-block">
      {/* (T829, owner 2026-11-16 "B: trout keep box") the species open today in the trout table's keep box: DFO's day
          total (the quota tree's total tier, T320/T332), its period limits (T671 D3) and a water system's own limit
          across the top, each species' number in the white box on the right */}
      {open.length > 0 && (
        <KeepBox testID="salmon-keep-box" head={t.bucket?.line ?? null} label={null} strip={null} tag={null}
                 notes={[...(t.bucket?.tiers ?? []).map((x) => x.line), ...(t.waterBuckets ?? []).map((w) => w.line)]}
                 rows={open.map((r) => speciesRow(r, actions, images))} bands={open.map(speciesBand)} palette={palette} />
      )}
      {/* the species that may be fished another way today ("Check DFO", "Varies along this water"), then release only */}
      {others.map(([label, rs]) => (
        <ShutGroup key={label} kind="other" head={label} testID="salmon-other-group" palette={palette}
                   rows={rs.map((r) => speciesRow(r, actions, images))} />
      ))}
      {release.length > 0 && (
        <ShutGroup kind="release" head={release[0]!.statusLabel} testID="salmon-release-group" palette={palette}
                   rows={release.map((r) => speciesRow(r, actions, images))} />
      )}
      {/* the trout table's "✕ Closed" group: the species closed today, then (T770, owner 2026-11-16 19:09: "As a closure
          line") another fish's closure ("Eulachon closed all year: No fishing for eulachon", Python's words) — what may
          not be fished here, read before how (the hours, the gear lines) */}
      {(closed.length > 0 || card.closures.length > 0) && (
        <ShutGroup kind="closed" head={closed[0]?.statusLabel ?? SALMON_WORDS.status.closed} testID="salmon-closed-group" palette={palette}
                   rows={[...closed.map((r) => speciesRow(r, actions, images)),
                          ...card.closures.map((l): KeepRowVM => ({ key: `closure:${l}`, name: l, label: null, sub: null, lines: [], quiet: [], pic: null }))]} />
      )}
      {/* (T343, T482) the page's salmon hours, one line (the daylight note folds into it) */}
      {card.hours && <Text selectable testID="salmon-hours" style={{ ...TYPE.metaSemi, color: palette.ink, paddingTop: 8 }}>{card.hours}</Text>}
      {/* (T828) DFO's rows as printed sit in the source fold; where every species is closed they ARE the card's one
          statement (the region's closure, the rule book's, "not opened"), so they stay here, said once */}
      {card.allClosed && <Says says={t.says} palette={palette} />}
      {/* (T870 item 2) DFO's gear lines as core says them: each names whose it is, a shared bait ban said once */}
      {(card.gearLines ?? card.gear).map((g) => (
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
  const says = card.allClosed ? [] : salmonTable(card).says;
  const [open, setOpen] = useState(false);
  return (
    <View testID="salmon-source-fold" style={{ marginTop: 12, borderTopWidth: 1, borderTopColor: palette.line }}>
      <CoachSpot k="regs:both">
        <RegsPress testID="salmon-source-summary" accessibilityRole="button" aria-expanded={open} onPress={() => setOpen(!open)} box={{ w: 44, h: 44 }}
                   style={{ minHeight: 44, flexDirection: "row", alignItems: "center", columnGap: 8, paddingVertical: 6 }}>
          <Text testID="kicker:salmon-source" style={[TYPE.kicker, { flex: 1, color: palette.sub }]}>{card.sourceTitle}</Text>
          <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={16} />
        </RegsPress>
      </CoachSpot>
      {open && (
        <View testID="salmon-source-body" style={{ paddingBottom: 4 }}>
          {/* (T796, owner 2026-11-16) the rule book's closures answering today: the book's page and row, its words */}
          {card.book.map((b) => (
            <View key={b.words} testID="salmon-book-source" style={{ paddingBottom: SPACE.s2 }}>
              <Text testID="salmon-book-from" style={{ ...TYPE.metaBold, color: palette.accent }}>{b.from}</Text>
              <Text selectable testID="salmon-book-words" style={{ ...TYPE.meta, color: palette.ink }}>{b.words}</Text>
            </View>
          ))}
          {/* (T828, owner 2026-11-16: "Fold into Source") DFO's rows as printed — each species' row words, in-season
              notice numbers, DFO's words for this water, the region's rules — closed by default; the wording DFO
              changed stays above the fold */}
          {/* (T870, owner 2026-11-18: "It should have all the rows that apply in source") DFO's rows as printed for each
              place of this water's chain, and the page header's notes the card's lines were read from */}
          <Sources sources={card.sources ?? []} palette={palette} />
          <Says says={says} palette={palette} />
          {card.dfoWords && card.dfoWords.notes.length > 0 && (
            <Words testID="salmon-wording-notes" head={card.dfoWords.notesHead} rows={card.dfoWords.notes} palette={palette} />
          )}
          {card.regionRules.map((g) => <RegionRules key={g.label} group={g} palette={palette} />)}
          <Text testID="salmon-source" style={{ ...TYPE.metaBold, color: palette.accent,
                                               ...(says.length || card.dfoWords?.notes.length || card.regionRules.length || card.sources?.length
                                                 ? { marginTop: SPACE.s3, paddingTop: SPACE.s2, borderTopWidth: 1, borderTopColor: palette.line } : {}) }}>{card.source}</Text>
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

/** (T870) the DFO rows and header notes behind the card's answers: a head, then per place / page header its name and
 *  DFO's words as printed. No words of its own (core `salmonCard` `sources`, SALMON_WORDS). */
function Sources({ sources, palette }: { sources: readonly SalmonSourceVM[]; palette: Palette }) {
  if (!sources.length) return null;
  return (
    <View testID="salmon-sources" style={{ paddingBottom: SPACE.s2 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.kicker, color: palette.sub }}>{SALMON_WORDS.sourcesHead}</Text>
      {sources.map((g) => (
        <View key={g.from} testID="salmon-sources-group" style={{ marginTop: 6 }}>
          <Text testID="salmon-sources-from" style={{ ...TYPE.metaBold, color: palette.ink }}>{g.from}</Text>
          {g.lines.map((l) => <Text selectable key={l} testID="salmon-sources-line" style={{ ...TYPE.meta, color: palette.ink, marginTop: 2 }}>{l}</Text>)}
        </View>
      ))}
    </View>
  );
}

/** (T759) ONE BANNER PER ISSUE the live feed lists on this water: the in-season banner's card (tint, the attention bar —
 *  not a water-status colour), its lead-in, then DFO's area and rows verbatim, then the closing line. Never folded and
 *  never a status: the rows and the chip above are what they are without it. Words are core's (`SALMON_ISSUE_WORDS`). */
function IssueBanners({ issues, palette }: { issues: readonly SalmonIssueVM[]; palette: Palette }) {
  if (!issues.length) return null;
  return (
    <View testID="salmon-issues">
      {issues.map((b) => (
        <View key={b.key} testID="salmon-issue"
              style={{ marginTop: 10, borderRadius: RADIUS.box, backgroundColor: palette.tint, borderLeftWidth: 6,
                       borderLeftColor: palette.attention, paddingVertical: 10, paddingLeft: 12, paddingRight: 14 }}>
          <Text accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub }]}>{b.title}</Text>
          <Text testID="salmon-issue-lead" style={[TYPE.regsBodySemi, { color: palette.ink, marginTop: 2 }]}>{b.lead}</Text>
          <Text selectable testID="salmon-issue-area" style={[TYPE.regsBody, { color: palette.ink, marginTop: 4 }]}>{b.area}</Text>
          {b.rows.map((r, i) => (
            <Text selectable key={i} testID="salmon-issue-row" style={[TYPE.regsBody, { color: palette.ink, marginTop: 2 }]}>{r}</Text>
          ))}
          <Text testID="salmon-issue-close" style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{b.close}</Text>
        </View>
      ))}
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
        <IssueBanners issues={card.issues ?? []} palette={palette} />
        {card.caveat && <CaveatBlock caveat={card.caveat} palette={palette} actions={actions ?? NO_ACTIONS} inset={false} />}
        {card.dfoWords && card.dfoWords.changed.length > 0 && (
          <Words testID="salmon-wording-changed" head={card.dfoWords.changedHead} rows={card.dfoWords.changed} palette={palette} strong />
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
        <SourceFold card={card} shown={shown} palette={palette} />
      </View>
    </WaterInset>
  );
}

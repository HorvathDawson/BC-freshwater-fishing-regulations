/**
 * The Keep tab ("What you can keep", T21): every block of `KeepVM.order`, in order, each drawn by its component. No
 * logic — which blocks, in which order, with which words, is core `keepOf`'s.
 *
 * (T221, owner 2026-10-28 — a try) `glance`: THE AT-A-GLANCE TABLE. The species list (Other fish) also carries Salmon and
 * each quota group as a folded row (`@app/core` `salmonGlance` / `groupGlance`), drawn where the first group or the list
 * would be; each opens its full card in place. Absent: every card in full, as before.
 *
 * (T227, owner 2026-10-28) `salmonStrip`: DFO's salmon year at the top of the tab, beside the provincial year bar (the
 * two named). A tab with no rows for the table (a closed water) draws the table after the leading blocks.
 *
 * (owner 2026-11-12, "Why is salmon under trout table now? Instead of on top when salmon rules exist") The Salmon (DFO)
 * block — its bar or bare status, and its rows — is drawn FIRST, where the game-fish calendar would start, whenever it is
 * drawn; the game-fish calendar and its rows follow. This is the order of the T531 mockup the owner approved 2026-11-09
 * (the owner's own sketch); cb1e01549 had kept game fish first, which put salmon under the trout table.
 *
 * (T531, owner 2026-11-09; replaced by T879) Each calendar owned its species' rows.
 *
 * (T879, owner 2026-11-18 ~20:53 "Two headers, one table", approved ~22:58) Both status blocks first — SALMON [DFO] (its big
 * word and year bar), then GAME FISH [B.C.] (its big word, year bar and timed lines) — then ONE species list under
 * "Species and limits": the Salmon fold first, then trout and char, the quota groups and the other fish. (T907, owner
 * 2026-11-19 ~06:50: "the bc/dfo tag in the table can go … the tag next to the calendar … are fine still") the rows wear
 * no tag; the two block heads keep theirs (`TaggedTitle`, `SourceTag`). The salmon card's extras (hours, gear lead, Source (DFO) fold) stay in the Salmon row's
 * fold. A page with no DFO block draws no tags and keeps its "Game fish (B.C. rules)" title.
 */
import { Fragment, type ReactNode } from "react";
import { View } from "react-native";
import { daylightOffsets, groupGlance, troutGlance, dfoBlockStatus, SALMON_WORDS, SOURCE_WORDS, WORDS, type CaveatVM, type DaylightAt, type GlanceRowVM, type KeepVM, type SalmonStripVM } from "@app/core";
import { GUTTER, type Palette } from "../theme";
import { Text } from "../Text";
import { TYPE } from "../type";
import { ClosedBanner, CaveatBlock, TimedLines, InheritedLine, OtherFish, OutsideFoot, PossessionRow, WhereNot, type GlanceFold } from "./KeepBlocks";
import { GroupCard } from "./GroupCard";
import { PlainRow } from "./PlainRow";
import { TroutCard } from "./TroutCard";
import { SalmonYearStrip, YearStrip } from "./YearStrip";
import { CoachSpot } from "../CoachMarks";
import { TaggedTitle } from "./SourceTag";
import type { KeepActions } from "./LineText";
import type { FishImages } from "./types";

export function KeepTab({ keep, dateLabel, palette, actions, images, glance = null, salmonStrip = null, daylight = null }: {
  keep: KeepVM; dateLabel: string; palette: Palette; actions: KeepActions; images?: FishImages;
  salmonStrip?: SalmonStripVM | null;
  /** (T473, T898) the open and closed hours of a daylight-only closure on the day at the water, from the rule's own
   *  offsets (`useDaylight`); null: no point */
  daylight?: DaylightAt | null;
  glance?: { salmon: { row: GlanceRowVM; card: () => ReactNode } | null } | null;
}) {
  // (T810 O, S) the trout and char table, as N built it (null: none drawn: no cell, a closed day, or a refused frame)
  const trout = keep.trout?.kind === "table" ? keep.trout : null;
  const troutCard = (kicker: boolean) => trout && (
    <TroutCard t={trout} palette={palette} actions={actions} images={images} kicker={kicker}
               todayAt={keep.strip?.todayAt ?? null} months={keep.strip?.months ?? []} />
  );
  const folds: GlanceFold[] = !glance ? [] : [
    ...(glance.salmon ? [{ key: "salmon", row: glance.salmon.row, body: glance.salmon.card, arrow: true }] : []),
    // (T810 O) ONE trout fold for the table (owner 15:47: the existing entry, its short line, the expand arrow); the other
    // fish's quota groups keep their own folds, unchanged
    ...keep.order.flatMap((b): GlanceFold[] => b === "trout" && trout ? [{ key: "trout", row: troutGlance(trout, keep.troutFrom), body: () => troutCard(false), arrow: true }]
      : typeof b === "object" && "group" in b ? [{
      key: `group:${b.group}`, row: groupGlance(keep.groups[b.group]!),
      body: () => <GroupCard g={keep.groups[b.group]!} palette={palette} actions={actions} images={images} />,
    }] : []),
  ];
  const home = !!(glance && keep.strip && keep.order.includes("strip"));   // a game-fish calendar to hang the B.C. species under
  const salmonHome = !!(glance && glance.salmon);   // a DFO block (with or without a calendar) to hang the DFO row under
  // (T879) a DFO block on the page: the two blocks are named with their tags (T907: the rows of the one list wear none)
  const tags = !!(salmonStrip || salmonHome);
  const at = keep.order.findIndex((b) => b === "otherFish" || (b === "trout" && !!trout) || (typeof b === "object" && "group" in b));
  // (T293) the walkthrough's "what you can keep" points at the species table
  const table = (key: number) => (
    <CoachSpot key={key} k="regs:keep">
      <OtherFish rows={keep.otherFish ?? []} keys={keep.otherFishKey} palette={palette} actions={actions} images={images}
                 folds={folds} />
    </CoachSpot>
  );
  // no row for the table: it goes after the leading blocks (strip, inherited line, banner, caveats), never above the bar
  const lead = keep.order.findIndex((b) => !(b === "strip" || b === "inherited" || b === "banner" || (typeof b === "object" && "caveat" in b)));
  const tableAt = at >= 0 ? at : lead >= 0 ? lead : keep.order.length;
  // (T318) the timed closures are lines under the game-fish bar (which they apply to), not a boxed note further down
  const timedAt = keep.order.findIndex((b) => typeof b === "object" && "caveat" in b && keep.caveats[b.caveat]?.kind === "timed");
  const timed = timedAt >= 0 && keep.strip && keep.order.includes("strip") ? keep.caveats[(keep.order[timedAt] as { caveat: number }).caveat]! : null;
  // (T473) a daylight-only closure said as the day's real hours with the status; its rule words stay as a small line
  const off = timed ? [...timed.items, ...(timed.later ?? [])].map(daylightOffsets).find((o) => o !== null) ?? null : null;
  const range = off && daylight ? daylight.open(off.after, off.before) : null;
  const hours = range ? WORDS.DAYLIGHT_HOURS(range) : null;
  // (T879) ONE list under both blocks: the Salmon fold first (DFO), then the B.C. rows, each tagged
  const oneList = home && (folds.length > 0 || (keep.otherFish ?? []).length > 0) && table(-2);
  const bareStatus = glance?.salmon ? dfoBlockStatus(glance.salmon.row) : null;
  const salmonBar = (salmonStrip || salmonHome) && (
    <View key="salmon-block" testID="salmon-block"
          style={keep.strip && glance ? { paddingBottom: 12, marginBottom: 2, borderBottomWidth: 1, borderBottomColor: palette.line2 } : undefined}>
      {salmonStrip ? (
        <SalmonYearStrip strip={salmonStrip} todayAt={keep.strip?.todayAt ?? 0} months={keep.strip?.months ?? []}
                         dateLabel={dateLabel} palette={palette} head={{ text: SOURCE_WORDS.salmon, tag: SOURCE_WORDS.salmonTag[salmonStrip.by] }} />
      ) : (
        <View testID="salmon-bare" style={{ paddingTop: 10, paddingHorizontal: GUTTER, paddingBottom: 2 }}>
          <TaggedTitle text={SOURCE_WORDS.salmon} tag={SOURCE_WORDS.tag.dfo} palette={palette} testID="salmon-strip-title" />
          {bareStatus && <Text testID={`salmon-bare-st:${glance!.salmon!.row.tone}`}
            style={[TYPE.answer, { color: glance!.salmon!.row.tone === "closed" ? palette.closed : glance!.salmon!.row.tone === "release" ? palette.restricted : palette.ink }]}>{bareStatus}</Text>}
        </View>
      )}
    </View>
  );
  return (
    <View testID="keep-tab" style={{ paddingBottom: 24 }}>
      {!keep.strip && salmonBar}
      {keep.order.map((b, i) => {
        const before = glance && !home && at < 0 && folds.length > 0 && i === tableAt ? table(-1) : null;
        const block = ((): ReactNode => {
        if (glance && (b === "otherFish" || b === "trout" || (typeof b === "object" && "group" in b))) return i === at && !home ? table(i) : null;
        if (b === "strip") return [salmonBar, <YearStrip key={i} strip={keep.strip!} dateLabel={dateLabel} palette={palette} title={tags ? SOURCE_WORDS.game : glance ? SALMON_WORDS.barGeneral : null} tag={tags ? SOURCE_WORDS.tag.bc : null} hours={hours} />,
          timed && <TimedLines key="timed" caveat={timed} palette={palette} actions={actions} hoursSaid={!!hours} daylight={daylight} />, oneList];
        if (b === "inherited") return <InheritedLine key={i} inherited={keep.inherited!} palette={palette} actions={actions} />;
        if (b === "banner") return <ClosedBanner key={i} banner={keep.banner!} palette={palette} actions={actions} />;
        if (b === "otherFish") return <OtherFish key={i} rows={keep.otherFish!} keys={keep.otherFishKey} palette={palette} actions={actions} images={images} />;
        if (b === "possession") return <PossessionRow key={i} possession={keep.possession!} palette={palette} actions={actions} />;
        if (b === "whereNot") return <WhereNot key={i} whereNot={keep.whereNot!} palette={palette} actions={actions} />;
        if (b === "outside") return <OutsideFoot key={i} text={keep.outside!} palette={palette} />;
        // (T810 O) no at-a-glance table: the trout table in full where its block is, named by its kicker
        if (b === "trout") return trout ? <Fragment key={i}>{troutCard(true)}</Fragment> : null;

        if ("caveat" in b && i === timedAt && timed) return null;
        if ("caveat" in b) return <CaveatBlock key={caveatKey(keep.caveats[b.caveat]!, i)} caveat={keep.caveats[b.caveat]!} palette={palette} actions={actions} />;
        if ("group" in b) return <GroupCard key={i} g={keep.groups[b.group]!} palette={palette} actions={actions} images={images} />;
        return <PlainRow key={i} row={keep.plainRows[b.plain]!} palette={palette} actions={actions} images={images} />;
        })();
        return before ? [before, block] : block;
      })}
      {glance && !home && at < 0 && folds.length > 0 && tableAt >= keep.order.length && table(-1)}
    </View>
  );
}

/** (T620 G20) A caveat block's identity, so a part step shows each part's own fold and not the last part's. */
const caveatKey = (c: CaveatVM, i: number) => `${i}:${c.kind}:${c.title}:${c.items.map((x) => x.rule ?? "").join(",")}`;

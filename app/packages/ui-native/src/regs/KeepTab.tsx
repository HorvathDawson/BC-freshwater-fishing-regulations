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
 * (T531, owner 2026-11-09) With the glance table, each calendar owns its species: heading, status, year bar and legend, then
 * ITS rows — Game fish (B.C. rules) the quota groups and Other fish, Salmon (DFO) the Salmon row. Which block a row sits
 * under is its rule source (core `speciesBlocks`), and the separate "Species and limits" list is drawn only where no
 * game-fish calendar is (a water the strip does not govern).
 */
import type { ReactNode } from "react";
import { View } from "react-native";
import { daylightOffsets, groupGlance, speciesBlocks, dfoBlockStatus, SALMON_WORDS, WORDS, type CaveatVM, type GlanceRowVM, type KeepVM, type SalmonStripVM } from "@app/core";
import { GUTTER, type Palette } from "../theme";
import { Text } from "../Text";
import { TYPE } from "../type";
import { ClosedBanner, CaveatBlock, TimedLines, InheritedLine, OtherFish, SpeciesRows, OutsideFoot, PossessionRow, WhereNot, type GlanceFold } from "./KeepBlocks";
import { GroupCard } from "./GroupCard";
import { PlainRow } from "./PlainRow";
import { SalmonYearStrip, YearStrip } from "./YearStrip";
import { CoachSpot } from "../CoachMarks";
import type { KeepActions } from "./LineText";
import type { FishImages } from "./types";

export function KeepTab({ keep, dateLabel, palette, actions, images, glance = null, salmonStrip = null, daylight = null }: {
  keep: KeepVM; dateLabel: string; palette: Palette; actions: KeepActions; images?: FishImages;
  salmonStrip?: SalmonStripVM | null;
  /** (T473) the open hours of a daylight-only closure on the day at the water, from the rule's own offsets; null: no point */
  daylight?: ((after: number, before: number) => string | null) | null;
  glance?: { salmon: { row: GlanceRowVM; card: () => ReactNode } | null } | null;
}) {
  const folds: GlanceFold[] = !glance ? [] : [
    ...(glance.salmon ? [{ key: "salmon", row: glance.salmon.row, body: glance.salmon.card }] : []),
    ...keep.order.flatMap((b) => typeof b === "object" && "group" in b ? [{
      key: `group:${b.group}`, row: groupGlance(keep.groups[b.group]!),
      body: () => <GroupCard g={keep.groups[b.group]!} palette={palette} actions={actions} images={images} />,
    }] : []),
  ];
  const home = !!(glance && keep.strip && keep.order.includes("strip"));   // a game-fish calendar to hang the B.C. species under
  const salmonHome = !!(glance && glance.salmon);   // a DFO block (with or without a calendar) to hang the DFO row under
  const split = speciesBlocks(folds, salmonHome);
  const at = keep.order.findIndex((b) => b === "otherFish" || (typeof b === "object" && "group" in b));
  // (T293) the walkthrough's "what you can keep" points at the species table
  const table = (key: number) => (
    <CoachSpot key={key} k="regs:keep">
      <OtherFish rows={keep.otherFish ?? []} keys={keep.otherFishKey} palette={palette} actions={actions} images={images}
                 folds={home ? [] : split.rest} />
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
  const range = off && daylight ? daylight(off.after, off.before) : null;
  const hours = range ? WORDS.DAYLIGHT_HOURS(range) : null;
  // (T531) the species under their calendar: the B.C. rows under the game-fish bar, the DFO row under the salmon bar
  const gameRows = home && (split.rest.length > 0 || (keep.otherFish ?? []).length > 0) && (
    <CoachSpot key="game-species" k="regs:keep">
      <SpeciesRows rows={keep.otherFish ?? []} keys={keep.otherFishKey} palette={palette} actions={actions} images={images} folds={split.rest} />
    </CoachSpot>
  );
  // (T531) a DFO row with no DFO calendar still sits under its own heading and status, just no bar or legend
  const dfoRows = salmonHome && split.dfo.length > 0 && <SpeciesRows rows={[]} keys={[]} palette={palette} actions={actions} images={images} folds={split.dfo} />;
  const bareStatus = glance?.salmon ? dfoBlockStatus(glance.salmon.row) : null;
  const salmonBar = (salmonStrip || salmonHome) && (
    <View key="salmon-block" testID="salmon-block"
          style={keep.strip && glance ? { paddingBottom: 14, marginBottom: 4, borderBottomWidth: 1.5, borderBottomColor: palette.ink } : undefined}>
      {salmonStrip ? (
        <SalmonYearStrip strip={salmonStrip} todayAt={keep.strip?.todayAt ?? 0} months={keep.strip?.months ?? []}
                         dateLabel={dateLabel} palette={palette} />
      ) : (
        <View testID="salmon-bare" style={{ paddingTop: 10, paddingHorizontal: GUTTER, paddingBottom: 2 }}>
          <Text testID="salmon-strip-title" accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub, marginBottom: 2 }]}>{SALMON_WORDS.barSalmon}</Text>
          {bareStatus && <Text testID={`salmon-bare-st:${glance!.salmon!.row.tone}`}
            style={[TYPE.answer, { color: glance!.salmon!.row.tone === "closed" ? palette.closed : glance!.salmon!.row.tone === "release" ? palette.restricted : palette.ink }]}>{bareStatus}</Text>}
        </View>
      )}
      {dfoRows}
    </View>
  );
  return (
    <View testID="keep-tab" style={{ paddingBottom: 24 }}>
      {!keep.strip && salmonBar}
      {keep.order.map((b, i) => {
        const before = glance && !home && at < 0 && split.rest.length > 0 && i === tableAt ? table(-1) : null;
        const block = ((): ReactNode => {
        if (glance && (b === "otherFish" || (typeof b === "object" && "group" in b))) return i === at && !home ? table(i) : null;
        if (b === "strip") return [salmonBar, <YearStrip key={i} strip={keep.strip!} dateLabel={dateLabel} palette={palette} title={salmonStrip ? salmonStrip.generalTitle : glance ? SALMON_WORDS.barGeneral : null} hours={hours} />,
          timed && <TimedLines key="timed" caveat={timed} palette={palette} actions={actions} hoursSaid={!!hours} />, gameRows];
        if (b === "inherited") return <InheritedLine key={i} inherited={keep.inherited!} palette={palette} actions={actions} />;
        if (b === "banner") return <ClosedBanner key={i} banner={keep.banner!} palette={palette} actions={actions} />;
        if (b === "otherFish") return <OtherFish key={i} rows={keep.otherFish!} keys={keep.otherFishKey} palette={palette} actions={actions} images={images} />;
        if (b === "possession") return <PossessionRow key={i} possession={keep.possession!} palette={palette} actions={actions} />;
        if (b === "whereNot") return <WhereNot key={i} whereNot={keep.whereNot!} palette={palette} actions={actions} />;
        if (b === "outside") return <OutsideFoot key={i} text={keep.outside!} palette={palette} />;
        if ("caveat" in b && i === timedAt && timed) return null;
        if ("caveat" in b) return <CaveatBlock key={caveatKey(keep.caveats[b.caveat]!, i)} caveat={keep.caveats[b.caveat]!} palette={palette} actions={actions} />;
        if ("group" in b) return <GroupCard key={i} g={keep.groups[b.group]!} palette={palette} actions={actions} images={images} />;
        return <PlainRow key={i} row={keep.plainRows[b.plain]!} palette={palette} actions={actions} images={images} />;
        })();
        return before ? [before, block] : block;
      })}
      {glance && !home && at < 0 && split.rest.length > 0 && tableAt >= keep.order.length && table(-1)}
    </View>
  );
}

/** (T620 G20) A caveat block's identity, so a part step shows each part's own fold and not the last part's. */
const caveatKey = (c: CaveatVM, i: number) => `${i}:${c.kind}:${c.title}:${c.items.map((x) => x.rule ?? "").join(",")}`;

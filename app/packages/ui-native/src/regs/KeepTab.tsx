/**
 * The Keep tab ("What you can keep", T21): every block of `KeepVM.order`, in order, each drawn by its component. No
 * logic — which blocks, in which order, with which words, is core `keepOf`'s.
 *
 * (T221, owner 2026-10-28 — a try) `glance`: THE AT-A-GLANCE TABLE. The species list (Other fish) also carries Salmon and
 * each quota group as a folded row (`@app/core` `salmonGlance` / `groupGlance`), drawn where the first group or the list
 * would be; each opens its full card in place. Absent: every card in full, as before.
 */
import type { ReactNode } from "react";
import { View } from "react-native";
import { groupGlance, type GlanceRowVM, type KeepVM } from "@app/core";
import type { Palette } from "../theme";
import { ClosedBanner, CaveatBlock, InheritedLine, OtherFish, OutsideFoot, PossessionRow, WhereNot, type GlanceFold } from "./KeepBlocks";
import { GroupCard } from "./GroupCard";
import { PlainRow } from "./PlainRow";
import { YearStrip } from "./YearStrip";
import type { KeepActions } from "./LineText";
import type { FishImages } from "./types";

export function KeepTab({ keep, dateLabel, palette, actions, images, glance = null }: {
  keep: KeepVM; dateLabel: string; palette: Palette; actions: KeepActions; images?: FishImages;
  glance?: { salmon: { row: GlanceRowVM; card: () => ReactNode } | null } | null;
}) {
  const folds: GlanceFold[] = !glance ? [] : [
    ...(glance.salmon ? [{ key: "salmon", row: glance.salmon.row, body: glance.salmon.card }] : []),
    ...keep.order.flatMap((b) => typeof b === "object" && "group" in b ? [{
      key: `group:${b.group}`, row: groupGlance(keep.groups[b.group]!),
      body: () => <GroupCard g={keep.groups[b.group]!} palette={palette} actions={actions} images={images} />,
    }] : []),
  ];
  const at = keep.order.findIndex((b) => b === "otherFish" || (typeof b === "object" && "group" in b));
  const table = (key: number) => (
    <OtherFish key={key} rows={keep.otherFish ?? []} keys={keep.otherFishKey} palette={palette} actions={actions} images={images}
               folds={folds} />
  );
  return (
    <View testID="keep-tab" style={{ paddingBottom: 24 }}>
      {glance && at < 0 && folds.length > 0 && table(-1)}
      {keep.order.map((b, i) => {
        if (glance && (b === "otherFish" || (typeof b === "object" && "group" in b))) return i === at ? table(i) : null;
        if (b === "strip") return <YearStrip key={i} strip={keep.strip!} dateLabel={dateLabel} palette={palette} />;
        if (b === "inherited") return <InheritedLine key={i} inherited={keep.inherited!} palette={palette} actions={actions} />;
        if (b === "banner") return <ClosedBanner key={i} banner={keep.banner!} palette={palette} actions={actions} />;
        if (b === "otherFish") return <OtherFish key={i} rows={keep.otherFish!} keys={keep.otherFishKey} palette={palette} actions={actions} images={images} />;
        if (b === "possession") return <PossessionRow key={i} possession={keep.possession!} palette={palette} actions={actions} />;
        if (b === "whereNot") return <WhereNot key={i} whereNot={keep.whereNot!} palette={palette} actions={actions} />;
        if (b === "outside") return <OutsideFoot key={i} text={keep.outside!} palette={palette} />;
        if ("caveat" in b) return <CaveatBlock key={i} caveat={keep.caveats[b.caveat]!} palette={palette} actions={actions} />;
        if ("group" in b) return <GroupCard key={i} g={keep.groups[b.group]!} palette={palette} actions={actions} images={images} />;
        return <PlainRow key={i} row={keep.plainRows[b.plain]!} palette={palette} actions={actions} images={images} />;
      })}
    </View>
  );
}

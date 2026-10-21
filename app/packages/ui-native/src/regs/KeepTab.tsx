/**
 * The Keep tab ("What you can keep", T21): every block of `KeepVM.order`, in order, each drawn by its component. No
 * logic — which blocks, in which order, with which words, is core `keepOf`'s.
 */
import { View } from "react-native";
import type { KeepVM } from "@app/core";
import type { Palette } from "../theme";
import { ClosedBanner, CaveatBlock, InheritedLine, OtherFish, OutsideFoot, PossessionRow, WhereNot } from "./KeepBlocks";
import { GroupCard } from "./GroupCard";
import { PlainRow } from "./PlainRow";
import { YearStrip } from "./YearStrip";
import type { KeepActions } from "./LineText";
import type { FishImages } from "./types";

export function KeepTab({ keep, dateLabel, palette, actions, images }: {
  keep: KeepVM; dateLabel: string; palette: Palette; actions: KeepActions; images?: FishImages;
}) {
  return (
    <View testID="keep-tab" style={{ paddingBottom: 24 }}>
      {keep.order.map((b, i) => {
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

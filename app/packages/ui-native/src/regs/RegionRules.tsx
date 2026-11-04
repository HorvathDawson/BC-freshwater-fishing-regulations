/**
 * (T309, owner 2026-10-31: "region salmon regs are walls of text") THE REGION RULES of the salmon card, drawn like the
 * game-fish rows: one row per DFO rule — its kind chip, the species it is about (small tags), one line — with DFO's whole
 * wording under a "Full wording" fold when the line is not all of it. Nothing dropped. (T342) No Copy button: the words
 * copy with the phone's own long-press, as every pane's do (`Text.tsx`). No logic and no
 * words of its own: core `salmonCard`'s `regionRules[].rules` and SALMON_WORDS.
 */
import { useState } from "react";
import { View } from "react-native";
import { Text } from "../Text";
import { SALMON_WORDS, type SalmonRuleVM } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { RegsPress } from "./RegsPress";

function Tag({ label, palette }: { label: string; palette: Palette }) {
  return (
    <View style={{ borderWidth: 1, borderColor: palette.line2, paddingHorizontal: 6, paddingVertical: 1, borderRadius: palette.r.chip }}>
      <Text style={{ ...TYPE.micro, color: palette.sub }}>{label}</Text>
    </View>
  );
}

function RuleRow({ r, palette }: { r: SalmonRuleVM; palette: Palette }) {
  const [open, setOpen] = useState(false);
  return (
    <View testID="salmon-rule" style={{ paddingVertical: 8, borderTopWidth: 1, borderTopColor: palette.line2 }}>
      <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 6 }}>
        <View style={{ backgroundColor: palette.ink, paddingHorizontal: 6, paddingVertical: 2, borderRadius: palette.r.chip }}>
          <Text testID="salmon-rule-kind" style={{ ...TYPE.pill, fontSize: 11, color: palette.card }}>{r.kindLabel}</Text>
        </View>
        {r.species.map((s) => <Tag key={s} label={s} palette={palette} />)}
      </View>
      <Text selectable testID="salmon-rule-line" style={{ ...TYPE.regsBodySemi, color: palette.ink, marginTop: 4 }}>{r.line}</Text>
      {r.full && open && <Text selectable testID="salmon-rule-full" style={{ ...TYPE.meta, color: palette.sub, marginTop: 4 }}>{r.full}</Text>}
      <View style={{ flexDirection: "row", columnGap: 16 }}>
        {r.full && (
          <RegsPress testID="salmon-rule-fold" accessibilityRole="button" aria-expanded={open} box={{ w: 44, h: 44 }}
                     onPress={() => setOpen(!open)} style={{ minHeight: 44, justifyContent: "center" }}>
            <Text style={{ ...TYPE.metaSemi, color: palette.accent }}>{open ? SALMON_WORDS.ruleHideWording : SALMON_WORDS.ruleFullWording} ›</Text>
          </RegsPress>
        )}
      </View>
    </View>
  );
}

export function RegionRules({ group, palette }: { group: { label: string; rules: readonly SalmonRuleVM[] }; palette: Palette }) {
  return (
    <View testID="salmon-region-rules" style={{ marginTop: 12, paddingTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
      <Text style={{ ...TYPE.kicker, color: palette.sub, marginBottom: 4 }}>{`${SALMON_WORDS.regionRules} · ${group.label}`}</Text>
      {group.rules.map((r) => <RuleRow key={r.id} r={r} palette={palette} />)}
    </View>
  );
}

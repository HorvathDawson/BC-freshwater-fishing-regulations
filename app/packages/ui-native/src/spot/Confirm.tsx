/** The spot screen's confirm: a destructive or lossy step, asked first (also SpotCapture's draft question). */
import { Text, View } from "react-native";
import { DRAFT_WORDS } from "@app/data/spots";
import { SPOT_CONFIRM_WORDS } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Button } from "../Button";

/**
 * A destructive or lossy step, asked before it happens. `draft`: leaving a spot being added that holds what the reader
 * put in (SpotCapture — a back or Cancel), in `DRAFT_WORDS`.
 */
export function Confirm({ palette, kind, onCancel, onConfirm, onSaveInstead }: {
  palette: Palette; kind: "delete" | "discard" | "draft";
  onCancel: () => void; onConfirm: () => void; onSaveInstead?: () => void;
}) {
  if (kind === "draft")
    return (
      <View accessibilityLabel={DRAFT_WORDS.label}
            style={{ marginBottom: 4, padding: 16, borderRadius: palette.r.box, borderWidth: 1,
                     borderColor: palette.line2, backgroundColor: palette.wash, gap: 12 }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{DRAFT_WORDS.title}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{DRAFT_WORDS.detail}</Text>
        <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap" }}>
          <Button palette={palette} kind="ghost" label={DRAFT_WORDS.keep} onPress={onCancel} />
          <Button palette={palette} kind="solid" label={DRAFT_WORDS.discard} onPress={onConfirm} />
        </View>
      </View>
    );
  const del = kind === "delete";
  const w = del ? SPOT_CONFIRM_WORDS.delete : SPOT_CONFIRM_WORDS.discard;
  return (
    <View accessibilityLabel={w.label}
          style={{ marginHorizontal: 18, marginBottom: 8, padding: 16, borderRadius: palette.r.box,
                   borderWidth: 1, borderColor: del ? palette.closed : palette.line2,
                   backgroundColor: palette.wash, gap: 12 }}>
      <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>
        {w.title}
      </Text>
      <Text style={{ ...TYPE.small, color: palette.sub }}>
        {w.detail}
      </Text>
      <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap" }}>
        <Button palette={palette} kind="ghost" label={w.keep}
                onPress={onCancel} />
        {/* NOT "Save changes" — the footer already has a button with that label, and two
            identically-named buttons on one screen is how somebody presses the wrong one.
            This one saves AND leaves, so it says so. */}
        {onSaveInstead && (
          <Button palette={palette} label={SPOT_CONFIRM_WORDS.saveAndClose} onPress={onSaveInstead} />
        )}
        <Button palette={palette} kind={del ? "danger" : "solid"}
                label={w.go} onPress={onConfirm} />
      </View>
    </View>
  );
}

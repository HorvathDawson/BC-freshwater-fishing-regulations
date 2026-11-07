/**
 * (T54, T147) THE ACCESS NOTICES of a Regulations page: the no-access banner and the gentle note (private land, a
 * reserve, permit-only land). No logic and no words of its own: core's `pageAccessNotices` says what, `@app/ui`
 * `useAccessNotices` says for which part (T387: the page's part, never the tapped section).
 *
 * (T367, owner 2026-11-03: "it should live in the stickied header since it is important info … make sure it doesnt make
 * screen size too small") Each notice is ONE slim strip pinned under the page's header, outside the scroll: its title in
 * bold, then what the land is by name, cut to one line; a tap opens the whole sentence under it, a second tap closes it.
 * The banner wears the WARNING amber (T165, `palette.warning`: a bar of it down the left, its tint as the ground,
 * `palette.noAccessBand`); the note wears the ACCENT, so the two never read as one fact. No dotted swatch before the
 * title: at strip size it read as an ellipsis (owner, T367 round 4). Every word in ink, which holds 7:1 on both tints. No map link (T367): a link that does
 * not highlight the area only moved the reader.
 *
 * (T480, owner 2026-11-07: "Maybe the access may be restricted should live in header (without making it taller, just
 * somewhere where there is whitespace already)?") On the Regulations page the GENTLE note is no longer a strip: it is a
 * compact chip in the header's right-hand whitespace beside the place and sun lines (`AccessChip`, its title only), and a
 * tap opens the same strip, open — title · what the land is, then the sentence — under the header (`AccessDetails`); a
 * second tap (on either) closes it. The no-access BANNER stays a strip: it is the warning, and it leads.
 */
import { useState } from "react";
import { View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { accessLabel, type AccessNotice } from "@app/core";
import { TYPE } from "../type";
import { NO_ACCESS_BAND, laid, type Palette } from "../theme";
import { Icon } from "../icons";

export function AccessNoticeBlock({ notices, palette, noteOpen }: { notices: readonly AccessNotice[]; palette: Palette;
  /** (T480) the gentle note is the header's chip (`AccessChip`): drawn here only while that chip holds it open, then
   *  open, its press closing it; absent: every notice a strip, as before (the Conditions view) */
  noteOpen?: { open: boolean; toggle: () => void } }) {
  const shown = noteOpen ? notices.filter((n) => n.tier === "no_access" || noteOpen.open) : notices;
  if (!shown.length) return null;
  return (
    <View testID="access-notices">
      {shown.map((n) => <Strip key={n.tier} notice={n} palette={palette}
                               held={noteOpen && n.tier !== "no_access" ? noteOpen : undefined} />)}
    </View>
  );
}

/**
 * (T480) The gentle note as a chip in the Regulations header: its title in a box of the note's colours (accent rail, its
 * tint), at most two short lines, a chevron; a press opens / closes its strip under the header. Sized to sit in the
 * whitespace beside the place and sun lines (`RegsHeader` `aside`), never taller than them.
 */
export function AccessChip({ notice, palette, open, onPress }: { notice: AccessNotice; palette: Palette; open: boolean; onPress: () => void }) {
  return (
    <Pressable testID="access-chip" onPress={onPress} accessibilityRole="button" aria-expanded={open}
               accessibilityLabel={accessLabel(notice)} hitSlop={6}
               style={{ backgroundColor: laid(palette.accent, palette.card, NO_ACCESS_BAND), borderLeftWidth: 3,
                        borderLeftColor: palette.accent, paddingVertical: 3, paddingLeft: 6, paddingRight: 4,
                        flexDirection: "row", alignItems: "center", gap: 2, maxWidth: CHIP_MAX_W }}>
      <Text testID="access-chip-title" numberOfLines={2} maxFontSizeMultiplier={1}
            style={[TYPE.metaBold, { fontSize: 11.5, lineHeight: 14, color: palette.ink, flexShrink: 1 }]}>{notice.title}</Text>
      <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={12} />
    </Pressable>
  );
}
/** (T480) the chip's widest: two lines of "Access may be restricted" at 11.5 px, so the place / sun lines keep the rest */
export const CHIP_MAX_W = 108;

function Strip({ notice, palette, held }: { notice: AccessNotice; palette: Palette; held?: { open: boolean; toggle: () => void } }) {
  const [own, setOwn] = useState(false);
  const open = held ? held.open : own;
  const setOpen = held ? () => held.toggle() : () => setOwn((o) => !o);
  const banner = notice.tier === "no_access";
  const [bar, ground] = banner ? [palette.warning, palette.noAccessBand]
    : [palette.accent, laid(palette.accent, palette.card, NO_ACCESS_BAND)];
  return (
    <Pressable testID={banner ? "access-banner" : "access-note"} onPress={setOpen}
               accessibilityRole="button" aria-expanded={open}
               accessibilityLabel={accessLabel(notice)}
               style={{ backgroundColor: ground, borderLeftWidth: 4, borderLeftColor: bar, borderBottomWidth: 1,
                        borderBottomColor: palette.line, minHeight: 36, paddingVertical: 8, paddingLeft: 12,
                        paddingRight: 14, justifyContent: "center" }}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
        <Text testID="access-title" numberOfLines={open ? undefined : 1} style={[TYPE.meta, { color: palette.ink, flex: 1 }]}>
          <Text style={TYPE.metaBold}>{notice.title}</Text>{` · ${notice.brief}`}
        </Text>
        <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={14} />
      </View>
      {open && (
        <Text testID="access-text" accessibilityRole={banner ? "alert" : undefined}
              style={[TYPE.meta, { color: palette.ink, marginTop: 4 }]}>{notice.text}</Text>
      )}
    </Pressable>
  );
}

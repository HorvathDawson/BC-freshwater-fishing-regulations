/**
 * (T54, T147) THE ACCESS NOTICES of a Regulations page: the no-access banner and the gentle note (private land, a
 * reserve, permit-only land). No logic and no words of its own: core's `accessNotices` / `waterAccessNotice` say what,
 * `@app/ui` `useAccessNotices` says for which section or part.
 *
 * (T367, owner 2026-11-03: "it should live in the stickied header since it is important info … make sure it doesnt make
 * screen size too small") Each notice is ONE slim strip pinned under the page's header, outside the scroll: its title in
 * bold, then what the land is by name, cut to one line; a tap opens the whole sentence under it, a second tap closes it.
 * The banner wears the WARNING amber (T165, `palette.warning`: a bar of it down the left, its tint as the ground,
 * `palette.noAccessBand`, and the legend's own swatch, the map's dotted grey line); the note wears the ACCENT, so the
 * two never read as one fact. Every word in ink, which holds 7:1 on both tints. No map link (T367): a link that does
 * not highlight the area only moved the reader.
 */
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import type { AccessNotice } from "@app/core";
import { TYPE } from "../type";
import { NO_ACCESS_BAND, laid, type Palette } from "../theme";
import { DottedSwatch } from "../Chrome";
import { Icon } from "../icons";

export function AccessNoticeBlock({ notices, palette }: { notices: readonly AccessNotice[]; palette: Palette }) {
  if (!notices.length) return null;
  return (
    <View testID="access-notices">
      {notices.map((n) => <Strip key={n.tier} notice={n} palette={palette} />)}
    </View>
  );
}

function Strip({ notice, palette }: { notice: AccessNotice; palette: Palette }) {
  const [open, setOpen] = useState(false);
  const banner = notice.tier === "no_access";
  const [bar, ground] = banner ? [palette.warning, palette.noAccessBand]
    : [palette.accent, laid(palette.accent, palette.card, NO_ACCESS_BAND)];
  return (
    <Pressable testID={banner ? "access-banner" : "access-note"} onPress={() => setOpen((o) => !o)}
               accessibilityRole="button" accessibilityState={{ expanded: open }}
               accessibilityLabel={`${notice.title}: ${notice.text}`}
               style={{ backgroundColor: ground, borderLeftWidth: 4, borderLeftColor: bar, borderBottomWidth: 1,
                        borderBottomColor: palette.line, minHeight: 36, paddingVertical: 8, paddingLeft: 12,
                        paddingRight: 14, justifyContent: "center" }}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
        {banner && <View testID="access-swatch"><DottedSwatch colour={palette.waterStatus.no_access} width={18} /></View>}
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

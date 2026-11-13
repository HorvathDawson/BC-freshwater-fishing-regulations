/**
 * (T700) THE IN-SEASON INBOX, the bell's list of in-season notices this device has met: unread ones under "New", read
 * ones under "Earlier", newest first, with "Mark all as read". Each row is a card (square corners): its mark (New /
 * Updated / Ended), the date, the water and region, the change's first lines, and "Not on the map yet" when it is not.
 * A tap opens it (`@app/ui` `useInSeasonInbox`: its water's page, or its text here — `InSeasonInboxDetail`).
 * Presentational: every word is core's (`inboxVM`, `inboxDetail`, `INBOX_WORDS`).
 */
import { View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { RegsPress } from "./regs/RegsPress";
import { INBOX_WORDS, INSEASON_WORDS, type InboxDetailVM, type InboxRowVM } from "@app/core";
import { useBackLayer, type InSeasonInboxState } from "@app/ui";
import { ErrorState } from "./ErrorState";
import { TYPE } from "./type";
import { round, type Palette } from "./theme";
import { linkTo } from "./openLink";

/** what the bell's sheet draws of the inbox: `@app/ui` `useInSeasonInbox`'s state */
export type InboxProps = Pick<InSeasonInboxState, "vm" | "detail" | "failure" | "pick" | "back" | "readAll">;

function Row({ r, palette, onPick }: { r: InboxRowVM; palette: Palette; onPick: (id: string) => void }) {
  return (
    <Pressable testID="inbox-row" onPress={() => onPick(r.id)} accessibilityRole="button" accessibilityLabel={r.label}
               style={({ pressed }) => ({ flexDirection: "row", gap: 10, paddingVertical: 10, paddingLeft: 10, paddingRight: 12,
                                          minHeight: 44, borderWidth: 1, borderColor: palette.line,
                                          borderLeftWidth: 4, borderLeftColor: r.unread ? palette.attention : palette.line,
                                          backgroundColor: pressed ? palette.wash : r.unread ? palette.tint : palette.card })}>
      <View style={{ flex: 1, gap: 2 }}>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
          {r.unread && <View testID="inbox-unread-dot"
                             style={{ width: 8, height: 8, borderRadius: round(8), backgroundColor: palette.attention }} />}
          <Text testID="inbox-tag" style={{ ...TYPE.metaBold, color: r.unread ? palette.ink : palette.sub }}>{r.tag}</Text>
          <Text style={{ ...TYPE.meta, color: palette.sub, flex: 1 }}>{r.date}</Text>
        </View>
        <Text style={{ ...(r.unread ? TYPE.bodyStrong : TYPE.body), color: palette.ink }}>
          {r.water}<Text style={{ ...TYPE.meta, color: palette.sub }}>{` · ${r.region}`}</Text>
        </Text>
        <Text numberOfLines={2} style={{ ...TYPE.small, color: palette.ink }}>{r.text}</Text>
        {r.note && <Text testID="inbox-note" style={{ ...TYPE.metaBold, color: palette.sub }}>{r.note}</Text>}
      </View>
    </Pressable>
  );
}

/** The list (or, a notice opened that has no one page to go to, its text). `header`: the section's title, when the
 *  bell holds other sections too. */
export function InSeasonInbox({ inbox, palette, header }: { inbox: InboxProps; palette: Palette; header: boolean }) {
  const { vm, failure } = inbox;
  return (
    <View testID="inbox" style={{ gap: 10 }}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: 8, minHeight: vm.unread ? 44 : undefined }}>
        {header && <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub, flex: 1 }}>{INBOX_WORDS.section}</Text>}
        {!header && <View style={{ flex: 1 }} />}
        {vm.unread > 0 && (
          <RegsPress testID="inbox-read-all" accessibilityRole="button" accessibilityLabel={INBOX_WORDS.markAll}
                     onPress={inbox.readAll} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center" }}>
            <Text style={{ ...TYPE.metaBold, color: palette.accent }}>{INBOX_WORDS.markAll}</Text>
          </RegsPress>
        )}
      </View>
      {failure && <ErrorState testID="inbox-failure" palette={palette} message={failure} />}
      {vm.groups.map((g) => (
        <View key={g.title} style={{ gap: 6 }}>
          <Text accessibilityRole="header" testID="inbox-group" style={{ ...TYPE.metaBold, color: palette.sub }}>{g.title}</Text>
          {g.rows.map((r) => <Row key={r.id} r={r} palette={palette} onPick={inbox.pick} />)}
        </View>
      ))}
    </View>
  );
}

/** One notice opened in the inbox: back to the list, its mark, water, dates, the change verbatim, a note, its source. */
export function InSeasonInboxDetail({ d, palette, onBack }: { d: InboxDetailVM; palette: Palette; onBack: () => void }) {
  // the platform's back steps back to the list before it closes the sheet
  useBackLayer(true, onBack);
  return (
    <View testID="inbox-detail" style={{ gap: 10 }}>
      <RegsPress testID="inbox-back" accessibilityRole="button" accessibilityLabel={INBOX_WORDS.backLabel} onPress={onBack}
                 box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center", alignSelf: "flex-start" }}>
        <Text style={{ ...TYPE.metaBold, color: palette.accent }}>{INBOX_WORDS.back}</Text>
      </RegsPress>
      <View style={{ borderWidth: 1, borderColor: palette.line, borderLeftWidth: 4, borderLeftColor: palette.attention,
                     padding: 12, gap: 6, backgroundColor: palette.card }}>
        <Text style={{ ...TYPE.metaBold, color: palette.sub }}>{d.tag}{d.dates ? ` · ${d.dates}` : ""}</Text>
        <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{d.head}</Text>
        <Text selectable testID="inbox-detail-text" style={{ ...TYPE.body, color: palette.ink }}>{d.text}</Text>
        {d.note && <Text testID="inbox-detail-note" style={{ ...TYPE.small, color: palette.sub }}>{d.note}</Text>}
        {!!d.url && (
          <RegsPress accessibilityRole="link" accessibilityLabel={INSEASON_WORDS.sourceLabel}
                     onPress={linkTo(d.url)} box={{ w: 44, h: 44 }}
                     style={{ minHeight: 44, justifyContent: "center", alignSelf: "flex-start" }}>
            <Text style={{ ...TYPE.metaBold, color: palette.accent }}>{INSEASON_WORDS.source}</Text>
          </RegsPress>
        )}
      </View>
    </View>
  );
}

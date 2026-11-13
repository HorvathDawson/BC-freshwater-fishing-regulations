/**
 * (T230) THE BELL'S SHEET: the app and data updates, the sources that failed, (T700) THE IN-SEASON INBOX — every in-season
 * notice this device has met, unread first (`InSeasonInbox`, `@app/ui` `useInSeasonInbox`) — and the DFO section. It
 * opens from the top bar's bell, and by itself once for an in-season notice not on the map yet (as T230's notice did).
 * A notice opened that has no one page to go to shows its text here, in place of the list. Words are core's. A
 * scrolling sheet at every width (a centred dialog on a wide screen).
 */
import { useState } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import { DFO_NOTICE_WORDS, INBOX_WORDS, INSEASON_WORDS, syncErrorVM, type DfoNoticeVM, UPDATE_WORDS, type AppUpdateVM, type ReleaseNoticeVM,
         type UpdateSourceInput, now } from "@app/core";
import { SyncFailures } from "./SyncErrorSheet";
import { Button } from "./Button";
import { InSeasonInbox, InSeasonInboxDetail, type InboxProps } from "./InSeasonInbox";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function InSeasonNotice({ open, inbox = null, palette, onClose, sync = null, release = null, app = null, dfo = null }: {
  open: boolean; palette: Palette; onClose: () => void;
  /** (T700) the in-season inbox (`@app/ui` `useInSeasonInbox`); null: no section */
  inbox?: InboxProps | null;
  /** (T521) THE BELL'S DFO SECTION, after the in-season rows: DFO rules on a water no curated binding knows (core
   *  `dfoNoticeRows`), and a list the feed could not read. Null or nothing: no section. */
  dfo?: DfoNoticeVM | null;
  /**
   * (T231, owner 2026-10-28) THE BELL'S SYNC SECTION: the update service's failed sources (what failed, when each last
   * succeeded) and Retry, above the in-season rows; null or none failed: no section.
   */
  sync?: { failed: readonly UpdateSourceInput[]; checking: boolean; onRetry: () => void } | null;
  /**
   * (T263) THE BELL'S "DATA UPDATE" SECTION, first: the release's state (core `releaseNoticeVM`) and its one button —
   * `onAction` is told which (download after its confirm, apply, update the app).
   */
  release?: { vm: ReleaseNoticeVM; onAction: (kind: "download" | "apply") => void } | null;
  /**
   * (T390) THE BELL'S "APP UPDATE" SECTION, first of all: a newer app (the gentle prompt) or new data that needs one
   * (the strong prompt), core `appUpdateVM`, and its one Update button (`@app/ui` `useAppUpdate`).
   */
  app?: { vm: AppUpdateVM; onUpdate: () => void } | null;
}) {
  const [asking, setAsking] = useState(false);
  const rel = release?.vm ?? null;
  const upd = app?.vm ?? null;
  const dfoOn = !!dfo && (dfo.rows.length > 0 || !!dfo.unread);
  // (T700) the inbox counts as a section while it holds a notice (read or not) or a line saying the feed failed
  const inboxOn = !!inbox && (!inbox.vm.empty || !!inbox.failure);
  const others = inboxOn || dfoOn || !!(sync && sync.failed.length);
  const failed = sync && sync.failed.length ? syncErrorVM(sync.failed, now()) : null;
  // (owner 2026-10-29) the bell is always drawn, so it can open on nothing: "No notifications"
  const empty = !upd && !rel && !failed && !inboxOn && !dfoOn;
  const title = empty ? UPDATE_WORDS.bell.noneTitle : upd && !rel && !others ? upd.title
    : upd ? UPDATE_WORDS.bell.title : rel && !others ? rel.title
    : failed && inboxOn || rel && others ? UPDATE_WORDS.bell.title
    : failed && dfoOn || inboxOn && dfoOn ? UPDATE_WORDS.bell.title
    : failed ? failed.title : inboxOn ? INBOX_WORDS.title : DFO_NOTICE_WORDS.title;
  // (T700) a notice opened from the list that has no one page to go to: its text, in the list's place
  if (inbox?.detail) return (
    <Sheet open={open} onClose={onClose} title={INBOX_WORDS.title} sub={null} palette={palette} scroll dialog>
      <View testID="inseason-notice" style={{ paddingHorizontal: 20, paddingTop: 8, paddingBottom: 12, gap: 14 }}>
        <InSeasonInboxDetail d={inbox.detail} palette={palette} onBack={inbox.back} />
        <Button palette={palette} label={INSEASON_WORDS.noticeClose} onPress={onClose} />
      </View>
    </Sheet>
  );
  return (
    <Sheet open={open} onClose={onClose} title={title} sub={null} palette={palette} scroll dialog>
      <View testID="inseason-notice" style={{ paddingHorizontal: 20, paddingTop: 8, paddingBottom: 12, gap: 14 }}>
        {empty && <Text testID="notice-empty" style={{ ...TYPE.body, color: palette.sub }}>{UPDATE_WORDS.bell.none}</Text>}
        {empty && inbox && <Text testID="notice-empty-sub" style={{ ...TYPE.small, color: palette.sub }}>{INBOX_WORDS.emptySub}</Text>}
        {upd && app && (
          <View testID="notice-app" style={{ gap: 8 }}>
            {(rel || others) && <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{upd.title}</Text>}
            <Text testID="notice-app-text"
                  style={{ ...(upd.strength === "strong" ? TYPE.bodyStrong : TYPE.body),
                           color: upd.failed && !upd.button && !upd.said ? palette.closed : palette.ink }}>
              {upd.text}
            </Text>
            {!!upd.sub && <Text style={{ ...TYPE.small, color: palette.sub }}>{upd.sub}</Text>}
            {/* (T234 v1.1) the incoming version's bullets, before it is installed; none published: said, never blank */}
            {upd.whatsNew && (
              <View testID="notice-app-whatsnew" style={{ gap: 4, borderLeftWidth: 2, borderLeftColor: palette.line2,
                                                          paddingLeft: 10 }}>
                <Text accessibilityRole="header" style={{ ...TYPE.metaBold, color: palette.ink }}>{upd.whatsNew.title}</Text>
                {upd.whatsNew.notes
                  ? upd.whatsNew.notes.map((n, i) => (
                    <Text key={i} testID="notice-app-note" style={{ ...TYPE.small, color: palette.ink }}>{`• ${n}`}</Text>
                  ))
                  : <Text testID="notice-app-notes-missing" style={{ ...TYPE.small, color: palette.sub }}>{upd.whatsNew.missing}</Text>}
              </View>
            )}
            {upd.button && (
              <Button palette={palette} label={upd.button.label} testID="notice-app-action" onPress={app.onUpdate} />
            )}
            {!!upd.said && (
              <Text testID="notice-app-said" accessibilityLiveRegion="polite"
                    style={{ ...TYPE.small, color: upd.failed ? palette.closed : palette.sub }}>{upd.said}</Text>
            )}
          </View>
        )}
        {upd && (rel || others) && <View style={{ height: 1, backgroundColor: palette.line }} />}
        {rel && release && (
          <View testID="notice-release" style={{ gap: 8 }}>
            {others && <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{rel.title}</Text>}
            <Text testID="notice-release-text" style={{ ...TYPE.body, color: palette.ink }}>
              {asking && rel.action?.confirm ? rel.action.confirm.text : rel.text}
            </Text>
            {rel.action && !asking && (
              <Button palette={palette} label={rel.action.label} testID="notice-release-action"
                      onPress={() => (rel.action!.confirm ? setAsking(true) : release.onAction(rel.action!.kind))} />
            )}
            {rel.action?.confirm && asking && (
              <View style={{ gap: 8 }}>
                <Button palette={palette} label={rel.action.confirm.yes} testID="notice-release-yes"
                        onPress={() => { setAsking(false); release.onAction(rel.action!.kind); }} />
                <Button palette={palette} label={rel.action.confirm.no} testID="notice-release-no" kind="ghost"
                        onPress={() => setAsking(false)} />
              </View>
            )}
          </View>
        )}
        {rel && others && <View style={{ height: 1, backgroundColor: palette.line }} />}
        {failed && (
          <View testID="notice-sync" style={{ gap: 8 }}>
            {(inboxOn || !!rel) && <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{failed.title}</Text>}
            <Text style={{ ...TYPE.small, color: palette.sub }}>{failed.sub}</Text>
            <SyncFailures vm={failed} palette={palette} checking={sync!.checking} onRetry={sync!.onRetry} />
          </View>
        )}
        {inboxOn && <InSeasonInbox inbox={inbox!} palette={palette} header={!!failed || !!rel || !!upd || dfoOn} />}
        {dfoOn && (
          <View testID="notice-dfo" style={{ gap: 8, paddingTop: inboxOn || failed ? 6 : 0 }}>
            {(inboxOn || !!failed || !!rel || !!upd) && (
              <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{DFO_NOTICE_WORDS.title}</Text>
            )}
            {dfo!.rows.length > 0 && <Text style={{ ...TYPE.body, color: palette.ink }}>{DFO_NOTICE_WORDS.intro}</Text>}
            {dfo!.rows.map((r) => (
              <View key={r.id} testID="dfo-notice-row"
                    style={{ borderLeftWidth: 4, borderLeftColor: palette.attention, paddingLeft: 10, gap: 2 }}>
                <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{r.where}</Text>
                <Text selectable style={{ ...TYPE.body, color: palette.ink }}>{r.text}</Text>
              </View>
            ))}
            {!!dfo!.unread && <Text testID="dfo-notice-unread" style={{ ...TYPE.small, color: palette.closed }}>{dfo!.unread}</Text>}
          </View>
        )}
        <Button palette={palette} label={INSEASON_WORDS.noticeClose} onPress={onClose} />
      </View>
    </Sheet>
  );
}

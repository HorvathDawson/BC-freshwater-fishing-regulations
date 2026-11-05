/**
 * (T230) THE IN-SEASON NOTICE: the provincial in-season changes in effect that are not placed on the map yet, as the
 * province printed them — region and water, the dates, the change verbatim, and its source. It opens by itself once per
 * row per device and from the top bar's bell (`@app/ui` `useInSeasonNotice`); words are core's (`INSEASON_WORDS`,
 * `inSeasonNoticeRows`). A scrolling sheet at every width (a centred dialog on a wide screen).
 */
import { useState } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import { INSEASON_WORDS, syncErrorVM, UPDATE_WORDS, type AppUpdateVM, type InSeasonNoticeRow, type ReleaseNoticeVM,
         type UpdateSourceInput, now } from "@app/core";
import { SyncFailures } from "./SyncErrorSheet";
import { Button } from "./Button";
import { RegsPress } from "./regs/RegsPress";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { linkTo } from "./openLink";

export function InSeasonNotice({ open, rows, palette, onClose, sync = null, release = null, app = null }: {
  open: boolean; rows: readonly InSeasonNoticeRow[]; palette: Palette; onClose: () => void;
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
  const others = rows.length > 0 || !!(sync && sync.failed.length);
  const failed = sync && sync.failed.length ? syncErrorVM(sync.failed, now()) : null;
  // (owner 2026-10-29) the bell is always drawn, so it can open on nothing: "No notifications"
  const empty = !upd && !rel && !failed && rows.length === 0;
  const title = empty ? UPDATE_WORDS.bell.noneTitle : upd && !rel && !others ? upd.title
    : upd ? UPDATE_WORDS.bell.title : rel && !others ? rel.title
    : failed && rows.length || rel && others ? UPDATE_WORDS.bell.title
    : failed ? failed.title : INSEASON_WORDS.noticeTitle;
  return (
    <Sheet open={open} onClose={onClose} title={title} sub={null} palette={palette} scroll dialog>
      <View testID="inseason-notice" style={{ paddingHorizontal: 20, paddingTop: 8, paddingBottom: 12, gap: 14 }}>
        {empty && <Text testID="notice-empty" style={{ ...TYPE.body, color: palette.sub }}>{UPDATE_WORDS.bell.none}</Text>}
        {upd && app && (
          <View testID="notice-app" style={{ gap: 8 }}>
            {(rel || others) && <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{upd.title}</Text>}
            <Text testID="notice-app-text"
                  style={{ ...(upd.strength === "strong" ? TYPE.bodyStrong : TYPE.body),
                           color: upd.failed && !upd.button && !upd.said ? palette.closed : palette.ink }}>
              {upd.text}
            </Text>
            {!!upd.sub && <Text style={{ ...TYPE.small, color: palette.sub }}>{upd.sub}</Text>}
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
            {(rows.length > 0 || !!rel) && <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{failed.title}</Text>}
            <Text style={{ ...TYPE.small, color: palette.sub }}>{failed.sub}</Text>
            <SyncFailures vm={failed} palette={palette} checking={sync!.checking} onRetry={sync!.onRetry} />
          </View>
        )}
        {failed && rows.length > 0 && (
          <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub, paddingTop: 6 }}>
            {INSEASON_WORDS.noticeTitle}
          </Text>
        )}
        {rows.length > 0 && <Text style={{ ...TYPE.body, color: palette.ink }}>{INSEASON_WORDS.noticeIntro}</Text>}
        {rows.map((r) => (
          <View key={r.id} testID="inseason-notice-row"
                style={{ borderLeftWidth: 4, borderLeftColor: palette.attention, paddingLeft: 10, gap: 2 }}>
            <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{r.where}{r.dates ? ` · ${r.dates}` : ""}</Text>
            <Text style={{ ...TYPE.body, color: palette.ink }}>{r.text}</Text>
            {!!r.url && (
              <RegsPress accessibilityRole="link" accessibilityLabel={INSEASON_WORDS.sourceLabel}
                         onPress={linkTo(r.url)} box={{ w: 44, h: 32 }}
                         style={{ minHeight: 32, justifyContent: "center", alignSelf: "flex-start" }}>
                <Text style={{ ...TYPE.metaBold, color: palette.accent }}>{INSEASON_WORDS.source}</Text>
              </RegsPress>
            )}
          </View>
        ))}
        <Button palette={palette} label={INSEASON_WORDS.noticeClose} onPress={onClose} />
      </View>
    </Sheet>
  );
}

/**
 * (T251) THE UPDATES LOG — a development and staging tool, reached from About: every check the update service made on
 * this device (per source: when, unchanged 304 / changed / failed and its class / offline, bytes, sha or release before
 * → after, how long), newest first, and the buttons a tester needs — check now, force re-download, simulate offline,
 * clear cache, apply a staged release — the update channel (live, or the staging test channel), and the whole log
 * copied for a bug report. Draws `updateLogVM` / `updateLogText` (core updateLog.ts); decides nothing.
 */
import { useState, useSyncExternalStore } from "react";
import { Pressable, Text, View } from "react-native";
import { UPDATE_LOG_CAP, UPDATE_LOG_WORDS, UPDATE_WORDS, updateLogHead, updateLogStatus, updateLogText, updateLogVM,
         type UpdateLogHeader } from "@app/core";
import type { UpdateLog, UpdateService } from "@app/data";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

export type UpdateChannelKey = "live" | "test";

/** What the app shell hands the page (App.tsx): the log, the service, and the tester's levers. */
export interface UpdatesLogControls {
  readonly log: UpdateLog;
  readonly updates: UpdateService;
  /** the build and data source, for the page's head and the export */
  header(): UpdateLogHeader;
  /** the update channel and its switch (kept; applied on the next start); null: the build cannot switch */
  readonly channel: { readonly current: UpdateChannelKey; pick(c: UpdateChannelKey): void } | null;
  /** the service is told the network is down (no request is made) while on */
  readonly offline: { get(): boolean; set(on: boolean): void };
  /** forget every validator, then check: each object downloaded whole */
  forceRedownload(): Promise<void>;
  /** forget validators and drop the platform's release cache (the web's Cache Storage) */
  clearCache(): Promise<void>;
  /** put a staged release in use now */
  apply(): void;
  /** the platform's copy (the web's clipboard, a phone's share sheet); false: it failed */
  copy(text: string): Promise<boolean>;
}

const W = UPDATE_LOG_WORDS;

function Act({ id, label, onPress, palette, on }: { id: string; label: string; onPress?: () => void; palette: Palette;
                                                    on?: boolean }) {
  const off = !onPress;
  return (
    <Pressable testID={`updates-log-${id}`} onPress={onPress} disabled={off} accessibilityRole="button"
               accessibilityLabel={label} aria-disabled={off}
               style={{ paddingHorizontal: SPACE.s3, paddingVertical: SPACE.s2, borderWidth: on ? 1.5 : 1,
                        borderColor: on ? palette.ink : palette.line2, backgroundColor: on ? palette.tint : "transparent",
                        opacity: off ? 0.45 : 1 }}>
      <Text style={{ ...TYPE.small, color: palette.ink }}>{label}</Text>
    </Pressable>
  );
}

export function UpdatesLogPage({ c, palette }: { c: UpdatesLogControls; palette: Palette }) {
  const events = useSyncExternalStore(c.log.subscribe, c.log.events, c.log.events);
  const snap = useSyncExternalStore(c.updates.subscribe, c.updates.snapshot, c.updates.snapshot);
  const [offline, setOffline] = useState(c.offline.get());
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState<boolean | null>(null);
  const rows = updateLogVM(events);
  const h = c.header();
  const run = (p: () => Promise<void>) => () => { setBusy(true); void p().finally(() => setBusy(false)); };
  const r = snap.release;
  const status = updateLogStatus([...snap.feeds, ...(r ? [r] : [])]);
  const [headApp, headData] = updateLogHead(h);

  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.sub }}>{W.intro}</Text>
      <View testID="updates-log-head" style={{ gap: 2, borderTopWidth: 1, borderTopColor: palette.line, paddingTop: SPACE.s2 }}>
        <Text selectable style={{ ...TYPE.figure, fontSize: 12, color: palette.ink }}>{headApp}</Text>
        <Text selectable style={{ ...TYPE.figure, fontSize: 12, color: palette.sub }}>{headData}</Text>
        <Text testID="updates-log-status" selectable style={{ ...TYPE.figure, fontSize: 12, color: palette.sub }}>
          {status.join("\n")}
        </Text>
        {r?.phase === "app-too-old" && (
          <Text testID="updates-log-app-too-old" style={{ ...TYPE.bodyStrong, color: palette.closed }}>
            {UPDATE_WORDS.row.appTooOld}
          </Text>
        )}
      </View>

      <View style={{ gap: SPACE.s1 }}>
        <Text style={{ ...TYPE.section, color: palette.sub }}>{W.channel.label}</Text>
        <View accessibilityRole="radiogroup" accessibilityLabel={W.channel.label} style={{ flexDirection: "row", gap: SPACE.s2 }}>
          {(["live", "test"] as const).map((k) => (
            <Act key={k} id={`channel-${k}`} label={W.channel[k]} palette={palette}
                 on={(c.channel?.current ?? "live") === k} onPress={c.channel ? () => c.channel!.pick(k) : undefined} />
          ))}
        </View>
      </View>

      <View style={{ flexDirection: "row", flexWrap: "wrap", gap: SPACE.s2 }}>
        <Act id="check" label={W.actions.check} palette={palette}
             onPress={busy || snap.checking ? undefined : run(() => c.updates.check({ manual: true }))} />
        <Act id="force" label={W.actions.force} palette={palette} onPress={busy ? undefined : run(c.forceRedownload)} />
        <Act id="offline" label={offline ? W.actions.offlineOn : W.actions.offlineOff} palette={palette} on={offline}
             onPress={() => { c.offline.set(!offline); setOffline(!offline); }} />
        <Act id="clear-cache" label={W.actions.clearCache} palette={palette} onPress={busy ? undefined : run(c.clearCache)} />
        <Act id="apply" label={W.actions.apply} palette={palette} onPress={r?.phase === "ready" ? c.apply : undefined} />
      </View>

      <View style={{ flexDirection: "row", gap: SPACE.s2, alignItems: "center" }}>
        <Act id="copy" label={W.actions.copy} palette={palette}
             onPress={() => { void c.copy(updateLogText(events, c.header(), Date.now())).then(setCopied); }} />
        <Act id="clear" label={W.actions.clear} palette={palette} onPress={events.length ? c.log.clear : undefined} />
        {copied !== null && (
          <Text testID="updates-log-copied" style={{ ...TYPE.small, color: copied ? palette.sub : palette.closed }}>
            {copied ? W.actions.copied : W.actions.copyFailed}
          </Text>
        )}
      </View>

      <View testID="updates-log-rows" style={{ borderBottomWidth: 1, borderBottomColor: palette.line }}>
        <Text style={{ ...TYPE.small, color: palette.sub, paddingBottom: SPACE.s1 }}>{W.count(events.length, UPDATE_LOG_CAP)}</Text>
        {rows.length === 0 && <Text style={{ ...TYPE.small, color: palette.sub }}>{W.empty}</Text>}
        {rows.map((row) => (
          <View key={row.key} testID="updates-log-row" accessibilityLabel={row.label}
                style={{ paddingVertical: SPACE.s2 - 2, borderTopWidth: 1, borderTopColor: palette.line, gap: 1 }}>
            <View style={{ flexDirection: "row", gap: SPACE.s2 }}>
              <Text style={{ ...TYPE.figure, fontSize: 12, color: palette.sub, width: 64 }}>{row.time}</Text>
              <Text style={{ ...TYPE.bodyStrong, fontSize: 13, color: palette.ink, width: 84 }}>{row.source}</Text>
              <Text style={{ ...TYPE.bodyStrong, fontSize: 13, flex: 1,
                             color: row.kind === "failed" || row.kind === "app-too-old" ? palette.closed : palette.ink }}>
                {row.result}
              </Text>
            </View>
            <Text selectable style={{ ...TYPE.figure, fontSize: 11.5, color: palette.sub }}>
              {[row.detail, row.change].filter(Boolean).join("   ")}
            </Text>
            {row.problem && (
              <Text selectable style={{ ...TYPE.figure, fontSize: 11.5, color: palette.closed }}>{row.problem}</Text>
            )}
          </View>
        ))}
      </View>
    </>
  );
}

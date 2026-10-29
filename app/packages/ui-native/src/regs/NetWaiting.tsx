/**
 * (T254) WAITING ON A POOR LINE, IN WORDS: under the Regulations page's spinner, what it is waiting for ("Loading the
 * regulations… 1.2 of 3.4 MB", "Offline — this will load when you're back online", "Connection dropped — trying again");
 * and where a part of a page is not there yet, a line in its place that says whether it is still coming or cannot come
 * ("Salmon rules: still loading (slow connection)", "…: not available offline") — never a silent gap. Words: core's.
 */
import { useEffect, useState } from "react";
import { Pressable, Text, View } from "react-native";
import { NET_WORDS, partLine, waitingLine } from "@app/core";
import { pendingOf, PRIORITY } from "@app/data";
import { useNet, useNetActivity } from "@app/ui";
import { SPACE, type Palette } from "../theme";
import { TYPE } from "../type";
import { MIN_HIT } from "./hit";

/** On a good line the spinner alone says enough for this long; a slow or missing line is said at once. */
export const SAY_AFTER_MS = 2500;

function useSayAfter(now: boolean): boolean {
  const [late, setLate] = useState(false);
  useEffect(() => { const t = setTimeout(() => setLate(true), SAY_AFTER_MS); return () => clearTimeout(t); }, []);
  return now || late;
}

/** Under a Regulations spinner: how far the files the page is built from have come. Nothing while nothing is coming. */
export function RegsWaiting({ palette }: { palette: Palette }) {
  const { net, mode } = useNet();
  const act = useNetActivity(net);
  const say = useSayAfter(mode.low || mode.offline);
  const answer = (k: string) => act.downloads.some((d) => d.key === k && d.priority === PRIORITY.answer);
  const p = pendingOf(act, answer);
  const attempt = Math.max(1, ...act.downloads.filter((d) => answer(d.key)).map((d) => d.attempt));
  if (!say || (!p.files && !mode.offline)) return null;
  return (
    <Text testID="regs-slow" accessibilityLiveRegion="polite"
          style={{ ...TYPE.small, color: palette.sub, textAlign: "center", marginTop: SPACE.s3, paddingHorizontal: SPACE.s4 }}>
      {waitingLine({ loaded: p.loaded, total: p.total, offline: mode.offline, attempt })}
    </Text>
  );
}

/**
 * A part of the page that is not there: still loading (said once the line is known slow, or after SAY_AFTER_MS), or
 * failed — "not available offline" when the device is offline, else "couldn't load" with Try again.
 */
export function PartPending({ palette, what, state, onRetry, testID }: {
  palette: Palette; what: string; state: "loading" | "failed"; onRetry?: () => void; testID?: string;
}) {
  const { mode } = useNet();
  const say = useSayAfter(mode.low || mode.offline || state === "failed");
  const line = partLine(what, state, mode.offline);
  if (!say || !line) return null;
  return (
    <View testID={testID} accessible accessibilityLabel={line}
          style={{ flexDirection: "row", alignItems: "center", flexWrap: "wrap", gap: SPACE.s2, marginVertical: SPACE.s2,
                   paddingVertical: SPACE.s2, paddingHorizontal: SPACE.s3, borderLeftWidth: 3, borderLeftColor: palette.line,
                   backgroundColor: palette.tint }}>
      <Text style={{ ...TYPE.small, color: palette.sub, flex: 1, minWidth: 160 }}>{line}</Text>
      {state === "failed" && !mode.offline && onRetry && (
        <Pressable onPress={onRetry} accessibilityRole="button" accessibilityLabel={NET_WORDS.part.retry} hitSlop={6}
                   style={{ minHeight: MIN_HIT, justifyContent: "center" }}>
          <Text style={{ ...TYPE.metaBold, color: palette.accent }}>{NET_WORDS.part.retry}</Text>
        </Pressable>
      )}
    </View>
  );
}

/**
 * T215 — the synopsis's loading card (owner 2026-10-28): a fish spinner and "Loading the synopsis… N%" (indeterminate when the
 * download's size is unknown) with Cancel; on a failure the words, Retry and the plain gov.bc.ca link. Drawn by the Shell
 * (floating over the app) and inside the sheet host (a sheet's Modal covers the Shell). Nothing while idle, and nothing in
 * the first SHOW_AFTER_MS of a load (a cached copy opens without a flash). The app's tokens only.
 */
import { View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { NET_WORDS, SYNOPSIS_PDF } from "@app/core";
import { useBackLayer } from "@app/ui";
import { LoadingState } from "../LoadingState";
import { BottomInset } from "../WindowInset";
import { SPACE, type Palette } from "../theme";
import { TYPE } from "../type";
import { MIN_HIT } from "./hit";
import { openBook } from "./openBook";
import { cancelSynopsisLoad, declineSynopsisOffer, dismissSynopsisError, percentOf, SYNOPSIS_WORDS as W, useSynopsisLoad } from "./synopsisLoad";
import { openLink } from "../openLink";

/** The link's own words while its page is loading (null: it is not loading). */
export function useBookLoadingWords(pdf: number | null): string | null {
  const s = useSynopsisLoad();
  if (pdf === null || s.phase !== "loading" || s.pdf !== pdf || !s.visible) return null;
  const n = percentOf(s);
  return n === null ? W.loading : W.percent(n);
}

export function SynopsisLoading({ palette, floating = false }: { palette: Palette; floating?: boolean }) {
  const s = useSynopsisLoad();
  // (T285) the failed card closes without opening anything: its Close, the Android back button, a tap outside (floating)
  const failing = s.phase === "error";
  // (one mechanism, tools/back-layers.test.ts) the failed card is a back layer: system back dismisses it
  useBackLayer(failing, dismissSynopsisError);
  if (s.phase === "idle" || (s.phase === "loading" && !s.visible)) return null;
  const offer = s.phase === "offer" ? s : null;
  const err = s.phase === "error";
  const failed = s.phase === "error" ? s : null;
  const n = s.phase === "loading" ? percentOf(s) : null;
  const act = (label: string, onPress: () => void, testID: string) => (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label} testID={testID} hitSlop={4}
               style={({ pressed }) => ({ minHeight: MIN_HIT, minWidth: MIN_HIT, justifyContent: "center", paddingHorizontal: SPACE.s3, opacity: pressed ? 0.6 : 1 })}>
      <Text style={[TYPE.bodyStrong, { color: palette.accent }]}>{label}</Text>
    </Pressable>);
  // a snackbar: a bar the width of its host, fixed at the foot (the Shell's window, a sheet's foot), a 1.5 px ink rule above it
  // like the sheet's own, above the system bar's inset
  const bar = (
    <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", paddingLeft: SPACE.s4, minHeight: 56,
                   backgroundColor: palette.card, borderTopWidth: 1.5, borderTopColor: err ? palette.warning : palette.ink }}>
      {/* the bar itself waited before it showed (`visible`): its fish is there at once */}
      {!err && !offer && <View style={{ marginRight: SPACE.s2 }}><LoadingState palette={palette} size={36} label={W.loading} immediate /></View>}
      <Text accessibilityLiveRegion="polite" style={[TYPE.bodyStrong, { flex: 1, minWidth: 140, color: palette.ink, paddingVertical: SPACE.s3 }]}>
        {offer ? NET_WORDS.pdf.offer(offer.bytes, offer.seconds) : err ? W.failed : n === null ? W.loading : W.percent(n)}
      </Text>
      {/* (T254) on a slow line the rule book is offered with its size before a byte of it is fetched */}
      {offer ? (
        <>
          {act(NET_WORDS.pdf.download, offer.go, "synopsis-download")}
          {act(NET_WORDS.pdf.cancel, declineSynopsisOffer, "synopsis-decline")}
        </>
      ) : err ? (
        <>
          {act(W.retry, () => { if (!failed) return; dismissSynopsisError(); void openBook(failed.pdf, failed.boxes).catch(() => undefined); }, "synopsis-retry")}
          {act(W.close, dismissSynopsisError, "synopsis-close")}
          {act(W.fallback, () => { dismissSynopsisError(); void openLink(SYNOPSIS_PDF); }, "synopsis-fallback")}
        </>
      ) : act(W.cancel, cancelSynopsisLoad, "synopsis-cancel")}
    </View>);
  const id = offer ? "synopsis-offer" : "synopsis-loading";
  return floating
    ? <>
        {err && <Pressable testID="synopsis-scrim" accessibilityLabel={W.close} onPress={dismissSynopsisError}
                           style={{ position: "absolute", left: 0, right: 0, top: 0, bottom: 0 }} />}
        <View testID={id} style={{ position: "absolute", left: 0, right: 0, bottom: 0, backgroundColor: palette.card }}>{bar}<BottomInset min={0} /></View>
      </>
    : <View testID={id}>{bar}</View>;
}

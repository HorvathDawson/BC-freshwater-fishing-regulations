/**
 * T354 (NATIVE) — the rule book inside the app (owner 2026-11-03): react-native-pdf (pdfium on Android, PDFKit on iOS)
 * opens the local file at the cited page, the highlight drawn on the page in its place (`appendPdf.ts`). Full screen over
 * everything (a Modal: a sheet's Modal would cover the Shell); Close and the one back close it. A file it cannot draw is
 * SAID on its face (`failPdf`), never handed off quietly. `PdfViewer.web.tsx` draws nothing: the browser's tab is the
 * web's viewer.
 */
import { useState } from "react";
import { Modal, Pressable, Text, View } from "react-native";
import Pdf from "react-native-pdf";
import { reportFailure, useBackLayer } from "@app/ui";
import { SPACE, type Palette } from "../theme";
import { TYPE } from "../type";
import { WindowInsets } from "../WindowInset";
import { MIN_HIT } from "./hit";
import { closePdf, failPdf, usePdfView } from "./pdfView";
import { SYNOPSIS_WORDS as W } from "./synopsisLoad";

export function PdfViewer({ palette }: { palette: Palette }) {
  const v = usePdfView();
  const [at, setAt] = useState<{ page: number; of: number } | null>(null);
  const close = () => { setAt(null); closePdf(); };
  const requestClose = useBackLayer(v !== null, close);
  if (!v) return null;
  return (
    <Modal visible animationType="slide" onRequestClose={requestClose} statusBarTranslucent={false}>
      {/* the status bar and the gesture bar are the window's, as on a menu page (an Android 15+ Modal draws under them) */}
      <WindowInsets style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ flex: 1, backgroundColor: palette.page }} testID="pdf-viewer">
        <View style={{ flexDirection: "row", alignItems: "center", paddingHorizontal: SPACE.s3, borderBottomWidth: 1,
                       borderBottomColor: palette.line, backgroundColor: palette.card }}>
          <Text style={[TYPE.bodyStrong, { color: palette.ink, flex: 1 }]} accessibilityRole="header">{W.title}</Text>
          {at && <Text style={[TYPE.small, { color: palette.sub, marginRight: SPACE.s3 }]}>{W.pageOf(at.page, at.of)}</Text>}
          <Pressable onPress={requestClose} accessibilityRole="button" accessibilityLabel={W.close} testID="pdf-viewer-close"
                     hitSlop={4} style={({ pressed }) => ({ minHeight: MIN_HIT, minWidth: MIN_HIT, justifyContent: "center",
                                                           alignItems: "flex-end", opacity: pressed ? 0.6 : 1 })}>
            <Text style={[TYPE.bodyStrong, { color: palette.accent }]}>{W.close}</Text>
          </Pressable>
        </View>
        {v.failed ? (
          <View style={{ flex: 1, padding: SPACE.s4, gap: SPACE.s2 }} testID="pdf-viewer-failed">
            <Text style={[TYPE.bodyStrong, { color: palette.ink }]}>{W.cantShow}</Text>
            <Text style={[TYPE.small, { color: palette.sub }]} selectable>{v.failed}</Text>
          </View>
        ) : (
          <Pdf source={{ uri: v.uri }} page={v.page} enableAnnotationRendering trustAllCerts={false}
               style={{ flex: 1, backgroundColor: palette.wash }}
               onLoadComplete={(of) => { setAt({ page: v.page, of }); console.info(`[synopsis] drawn p.${v.page} of ${of} in ${Date.now() - v.at} ms`); }}
               onPageChanged={(page, of) => setAt({ page, of })}
               onError={(e) => {
                 // said on its face AND reported (owner 2026-11-03: every shown failure reaches us)
                 console.warn("synopsis: the in-app viewer could not draw the PDF", e);
                 reportFailure(e);
                 failPdf(String((e as Error)?.message ?? e));
               }} />
        )}
      </View>
      </WindowInsets>
    </Modal>
  );
}

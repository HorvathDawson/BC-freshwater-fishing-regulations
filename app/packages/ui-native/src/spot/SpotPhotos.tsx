/** S1: a spot's photographs — drawn from the store; in draft and edit, with add and remove (`usePhotoEdit`). */
import { ErrorState } from "../ErrorState";
import { LoadingState } from "../LoadingState";
import { Image, Pressable, Text, View } from "react-native";
import { PHOTO_WORDS, photoLabel, removePhotoLabel } from "@app/data/spots";
import type { PhotoEdit } from "@app/ui";
import { TYPE } from "../type";
import { round, type Palette } from "../theme";
import { Button } from "../Button";
import { Icon } from "../icons";
import { Block } from "./parts";

export function SpotPhotos({ photos, uris, pe, palette }: {
  photos: readonly string[]; uris: ReturnType<typeof import("@app/ui").usePhotoUris>; pe: PhotoEdit | undefined;
  palette: Palette;
}) {
  return (
    <Block palette={palette} title={PHOTO_WORDS.heading}>
      <View testID="spot-photo-grid" style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
        {photos.map((ref, i) => {
          const uri = uris.get(ref);
          return (
            <View key={ref} style={{ flexGrow: 1, flexBasis: 104, maxWidth: 220, aspectRatio: 1, borderRadius: palette.r.box,
                                     overflow: "hidden", backgroundColor: palette.tint,
                                     alignItems: "center", justifyContent: "center" }}>
              {uri ? (
                <Image source={{ uri }} accessibilityRole="image" accessibilityLabel={photoLabel(i + 1, photos.length)}
                       style={{ width: "100%", height: "100%" }} resizeMode="cover" />
              ) : uri === null ? (
                <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint, padding: 8,
                               textAlign: "center" }}>{PHOTO_WORDS.missing}</Text>
              ) : null}
              {pe && (
                <Pressable onPress={() => { void pe.remove(ref); }} accessibilityRole="button"
                           accessibilityLabel={removePhotoLabel(i + 1)}
                           style={{ position: "absolute", top: 4, right: 4, width: 28, height: 28,
                                    borderRadius: round(28), alignItems: "center", justifyContent: "center",
                                    backgroundColor: palette.card }}>
                  <Icon name="close" colour={palette.ink} size={16} />
                </Pressable>
              )}
            </View>
          );
        })}
      </View>
      {pe && pe.sources.length > 0 && (
        <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap" }}>
          {pe.sources.map((from) => (
            <Button key={from} palette={palette} kind="ghost" grow={false}
                    label={from === "camera" ? PHOTO_WORDS.camera : PHOTO_WORDS.library}
                    onPress={pe.busy ? undefined : () => { void pe.add(from); }} />
          ))}
        </View>
      )}
      {pe?.busy ? (
        <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
          <LoadingState palette={palette} size={22} label={PHOTO_WORDS.adding} />
          <Text style={{ ...TYPE.small, color: palette.sub }}>{PHOTO_WORDS.adding}</Text>
        </View>
      ) : null}
      {pe?.error ? (
        <ErrorState palette={palette} message={pe.error} />
      ) : null}
    </Block>
  );
}

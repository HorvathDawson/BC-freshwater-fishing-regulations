/** S1: a spot's photographs — drawn from the store; in draft and edit, with add and remove (`usePhotoEdit`). */
import { Image, Pressable, Text, View } from "react-native";
import { PHOTO_WORDS, removePhotoLabel } from "@app/data/spots";
import type { PhotoEdit } from "@app/ui";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Button } from "../Button";
import { Icon } from "../icons";
import { Block } from "./parts";

export function SpotPhotos({ photos, uris, pe, palette }: {
  photos: readonly string[]; uris: ReturnType<typeof import("@app/ui").usePhotoUris>; pe: PhotoEdit | undefined;
  palette: Palette;
}) {
  return (
    <Block palette={palette} title={PHOTO_WORDS.heading}>
      <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
        {photos.map((ref, i) => {
          const uri = uris.get(ref);
          return (
            <View key={ref} style={{ width: 104, height: 104, borderRadius: palette.r.box,
                                     overflow: "hidden", backgroundColor: palette.tint,
                                     alignItems: "center", justifyContent: "center" }}>
              {uri ? (
                <Image source={{ uri }} accessibilityLabel=""
                       style={{ width: 104, height: 104 }} resizeMode="cover" />
              ) : uri === null ? (
                <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint, padding: 8,
                               textAlign: "center" }}>{PHOTO_WORDS.missing}</Text>
              ) : null}
              {pe && (
                <Pressable onPress={() => { void pe.remove(ref); }} accessibilityRole="button"
                           accessibilityLabel={removePhotoLabel(i + 1)}
                           style={{ position: "absolute", top: 4, right: 4, width: 28, height: 28,
                                    borderRadius: 14, alignItems: "center", justifyContent: "center",
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
      {pe?.busy ? <Text style={{ ...TYPE.small, color: palette.sub }}>{PHOTO_WORDS.adding}</Text> : null}
      {pe?.error ? (
        <Text accessibilityRole="alert" style={{ ...TYPE.small, color: palette.closed }}>{pe.error}</Text>
      ) : null}
    </Block>
  );
}

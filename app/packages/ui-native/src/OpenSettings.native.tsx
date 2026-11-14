/** (T723) The phone's: a link-button to this app's page in Settings. See `OpenSettings.d.ts`. */
import type { ReactNode } from "react";
import { View } from "react-native";
import { needsSettings, OPEN_SETTINGS } from "@app/core";
import { Button } from "./Button";
import { openAppSettings } from "./openLink";
import type { Palette } from "./theme";

export function OpenSettings({ palette, line, inset = 20 }: { palette: Palette; line: string | null | undefined; inset?: number }): ReactNode {
  if (!needsSettings(line)) return null;
  // (the first phone proof) the button's own words are its accessible name, and it sits where the line above it does
  return (
    <View style={{ paddingHorizontal: inset, paddingBottom: inset > 0 ? 12 : 0, alignItems: "flex-start" }}>
      <Button palette={palette} kind="link" size="meta" label={OPEN_SETTINGS.label} testID="open-settings"
              onPress={() => { void openAppSettings(); }} />
    </View>
  );
}

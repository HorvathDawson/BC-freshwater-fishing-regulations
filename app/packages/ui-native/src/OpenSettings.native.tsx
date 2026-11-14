/** (T723 G6) The phone's: a link-button to this app's page in Settings. See `OpenSettings.d.ts`. */
import type { ReactNode } from "react";
import { needsSettings, OPEN_SETTINGS } from "@app/core";
import { Button } from "./Button";
import { openAppSettings } from "./openLink";
import type { Palette } from "./theme";

export function OpenSettings({ palette, line }: { palette: Palette; line: string | null | undefined }): ReactNode {
  if (!needsSettings(line)) return null;
  return (
    <Button palette={palette} kind="link" size="meta" label={OPEN_SETTINGS.label} a11yLabel={OPEN_SETTINGS.a11y}
            testID="open-settings" onPress={() => { void openAppSettings(); }} />
  );
}

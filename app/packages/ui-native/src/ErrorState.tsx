/**
 * (T257) THE ERROR LINE: what went wrong, in `closed` (the app's error red), announced (`alert`), and — where asking again
 * can help — "Try again" beside it as the link kind of the one `Button`. Extracted unchanged from the copies the screens
 * had typed out (a page's: body words; a block's: small words), so a screen's error reads like every other screen's.
 * `tools/one-button.test.ts` refuses an error line or a retry link typed out again.
 */
import { Text, type TextStyle } from "react-native";
import { Button } from "./Button";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function ErrorState({ palette, message, size = "small", onRetry, retryLabel, retryTestID, style, testID }: {
  palette: Palette;
  message: string;
  /** "body" on a page of its own; "small" inside a block (the default) */
  size?: "body" | "small";
  /** ask again; absent: no button */
  onRetry?: () => void;
  retryLabel?: string;
  retryTestID?: string;
  /** the line's own room (padding) */
  style?: TextStyle;
  testID?: string;
}) {
  const line = (
    <Text testID={testID} accessibilityRole="alert" style={{ ...(size === "body" ? TYPE.body : TYPE.small), color: palette.closed, ...style }}>
      {message}
    </Text>
  );
  if (!onRetry || !retryLabel) return line;
  return (
    <>
      {line}
      <Button palette={palette} kind="link" size={size === "body" ? "body" : "meta"} label={retryLabel} onPress={onRetry}
              testID={retryTestID} style={{ alignSelf: "flex-start" }} />
    </>
  );
}

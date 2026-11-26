/**
 * (T1164) THE ERROR BOUNDARY OF A SCREEN. A throw while one screen is drawn (a render, a hook, a lifecycle) used to end the
 * app; now it ends that screen only: the screen's own line says so, with "Try again" (draw it again) and, where a
 * screen has a place to go back to, "Go back", and the fault goes through core's one reporter (`reportFailure`).
 * The shell around it (the tab bar, the rail, the sheets) is outside and stays whole, so another tab still works.
 *
 * It resets by itself when `resetKey` changes (a different tab, a different water), so leaving the broken screen and
 * coming back draws it afresh. It is a class because React has no hook for this (no dependency: `deps.md`).
 * Every screen is mounted through it in `Shell.tsx`; `tools/screen-boundary.test.ts` fails a screen that is not.
 * A boundary catches only what is thrown while drawing: an event handler's or a promise's failure is reported where
 * it is caught (AGENTS W46), and `tools/check-no-render-throw.mjs` keeps new render-path code from throwing.
 */
import { Component, type ErrorInfo, type ReactNode } from "react";
import { View } from "react-native";
import { SCREEN_FAULT_ACTIONS, reportFailure, screenFaultLine, type ScreenName } from "@app/core";
import { ErrorState } from "./ErrorState";
import { Button } from "./Button";
import type { Palette } from "./theme";

type Props = {
  screen: ScreenName;
  palette: Palette;
  /** a change of this draws the screen afresh (the tab, the water) */
  resetKey: string;
  /** the way back to where the reader came from; absent on a tab (the tab bar is the way out) */
  goBack?: () => void;
  children: ReactNode;
};

export class ScreenBoundary extends Component<Props, { faulted: boolean; key: string }> {
  override state = { faulted: false, key: this.props.resetKey };

  /** a new key draws the screen afresh, BEFORE it renders (resetting after a fault would draw the fault's own key twice) */
  static getDerivedStateFromProps(props: Props, state: { faulted: boolean; key: string }): { faulted: boolean; key: string } | null {
    return props.resetKey !== state.key ? { faulted: false, key: props.resetKey } : null;
  }

  static getDerivedStateFromError(): { faulted: boolean } { return { faulted: true }; }

  override componentDidCatch(error: unknown, _info: ErrorInfo): void { reportFailure(error); }

  private retry = (): void => { this.setState({ faulted: false }); };

  override render(): ReactNode {
    if (!this.state.faulted) return this.props.children;
    const { screen, palette, goBack } = this.props;
    return (
      <View testID={`screen-fault-${screen}`} style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: 24,
                                                    backgroundColor: palette.card }}>
        <ErrorState palette={palette} size="body" align="center" message={screenFaultLine(screen)}
                    onRetry={this.retry} retryLabel={SCREEN_FAULT_ACTIONS.retry} retryTestID="screen-fault-retry" />
        {goBack && <Button palette={palette} kind="link" size="body" label={SCREEN_FAULT_ACTIONS.back} onPress={goBack}
                           testID="screen-fault-back" style={{ alignSelf: "center" }} />}
      </View>
    );
  }
}

/** No wheel on a phone: the pinch is react-native-gesture-handler's. See `photoInput.web.ts`. */
import type { RefObject } from "react";

export function usePhotoWheel(_ref: RefObject<unknown>, _onWheel: (x: number, y: number, deltaY: number) => void,
                              _enabled: boolean): void {}

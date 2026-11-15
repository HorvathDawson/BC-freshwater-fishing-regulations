/** (T723 F7) The browser's: no edge swipe of ours. See `ModalBack.d.ts`. */
import type { ReactNode } from "react";

export function ModalBack({ children }: { onBack: () => void; children: ReactNode }): ReactNode {
  return <>{children}</>;
}

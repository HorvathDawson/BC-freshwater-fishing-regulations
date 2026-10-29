/**
 * (T231) THE SYNC ICON'S PROGRESS RING: how much of an update has downloaded, drawn round the sync glyph — a faint full
 * track and an arc from twelve o'clock, clockwise, in the accent. A picture of a number (data), not a glyph: the one
 * icon set's guard lists it with the other data drawings (tools/one-icon-set.test.ts).
 */
import Svg, { Circle } from "react-native-svg";

export function SyncRing({ progress, size, track, colour }: {
  /** 0–1 */
  progress: number; size: number; track: string; colour: string;
}) {
  const w = 2, r = (size - w) / 2, c = 2 * Math.PI * r;
  const p = Math.max(0, Math.min(1, progress));
  return (
    <Svg testID="sync-ring" width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
      <Circle cx={size / 2} cy={size / 2} r={r} stroke={track} strokeWidth={w} fill="none" />
      <Circle cx={size / 2} cy={size / 2} r={r} stroke={colour} strokeWidth={w} fill="none" strokeLinecap="round"
              strokeDasharray={`${c * p} ${c}`} transform={`rotate(-90 ${size / 2} ${size / 2})`} />
    </Svg>
  );
}

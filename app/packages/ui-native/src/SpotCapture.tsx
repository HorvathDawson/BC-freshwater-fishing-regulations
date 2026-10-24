/**
 * Adding a spot. Four steps, each confirmed before the next.
 *
 *   1. PICK THE WATER    tap a stream or lake; it highlights; Next
 *   2. PICK THE POINT    tap the exact place on it; Next
 *   3. WHEN WERE YOU     the date and hour you were there; Next
 *   4. WHAT WAS TRUE     gauge, how the gauge reaches this spot, weather that day — then
 *                        notes and photographs, then Save
 *
 * STEP 3 COMES BEFORE ANYTHING IS FETCHED, and that is the whole reason it exists as a
 * step rather than a field on the last screen. Weather and the gauge reading are fetched
 * FOR A DATE; asking afterwards would mean fetching twice, or
 * — worse — showing a person Tuesday's sky over the Sunday they actually fished.
 *
 * WHY THE CONFIRM STEPS. The first version tapped once and saved. On a phone, over a
 * one-pixel river, that is a coin toss: you get whichever water your thumb happened to
 * cover and no chance to see it was wrong before it is a permanent record. Highlighting
 * the choice and asking is the difference between picking and guessing.
 *
 * STARTED AT A POINT (owner 2026-10-21): a long press on the map, the Spots tab's "Pick on the map" (a tap in pick mode)
 * or "At my location" hands the point in (`at`). The pin is then already down: the map opens on it, step 1 picks the
 * water near it, and Next goes straight to step 3 — step 2 is still there, one ‹ Back away, to move the pin.

 */
import { SPOT_CAPTURE_AT, SPOT_CAPTURE_WORDS as CW, VISIT_QUICK_DAYS, type LatLon, type SectionKey, type VisitSteps } from "@app/core";
import { useRef, useState } from "react";
import { Pressable, Text, View } from "react-native";
import type { WaterSource, SectionId } from "@app/data";
import type { HydatDaily, Spot, WeatherSource } from "@app/data/spots";
import { dayFlowView, useBackLayer, useCaptureSpot, type CaptureWater, type SpotPhotos } from "@app/ui";
import type { Camera, TileEndpoints } from "@app/map";
import { FishSpinner } from "./FishSpinner";
import { MapScreen } from "./MapScreen";
import { SpotScreen } from "./SpotScreen";
import { Confirm } from "./spot/Confirm";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { Button } from "./Button";
import { Icon, IconText, type IconName } from "./icons";

type Step = "water" | "point" | "when" | "details" | "saving";
/** Where one back goes from each step: the step before it (a read in flight goes back to the hour it was asked for). */
const PREV: Record<Step, Step | null> = { water: null, point: "water", when: "point", details: "when", saving: "when" };

/** How close the map opens on a spot started at a point: near enough to pick one channel from the next. */
const AT_ZOOM = 14;

export interface SpotCaptureProps {
  /**
   * The live index, so the ESTIMATE is frozen as the reader saw it.
   *
   * Without it a spot keeps only the matched station's own reading — true, but not what
   * the app was showing, which was a panel of up to four gauges and an interval.
   */
  feed?: { index(): Promise<any> };

  source: WaterSource;
  weather?: WeatherSource;
  tiles: TileEndpoints;
  palette: Palette;
  theme: string;
  camera: Camera;
  /** The point the spot was started at (a long press, a tap in pick mode, the reader's location); absent: picked here. */
  at?: LatLon | null;
  onCancel: () => void;
  /** S1: the photo store and the platform's picker. Absent -> the draft offers no photos. */
  photos?: SpotPhotos;
  /**
   * Q161: HYDAT and the device's online word, so a trip on an earlier day is read from that day's record. Absent: the
   * feed's daily series only, and older days wait for a refresh.
   */
  history?: { hydat?: HydatDaily; online?: () => boolean };
  /** Awaited: a rejection is shown and the sheet stays open (P01). */
  onSaved: (spot: Spot) => void | Promise<void>;
}

export function SpotCapture(props: SpotCaptureProps) {
  const { palette, tiles, theme, camera, source, feed } = props;
  const [step, setStep] = useState<Step>("water");
  const [water, setWater] = useState<CaptureWater | null>(null);
  const [point, setPoint] = useState<{ lat: number; lon: number } | null>(props.at ?? null);
  // a spot started at a point opens the map ON it, close enough to tell one channel from the next
  const [start] = useState<Camera>(() => props.at
    ? { lat: props.at.lat, lon: props.at.lon, zoom: Math.max(camera.zoom, AT_ZOOM) } : camera);
  const preset = props.at != null;
  // The visit's day and hour, the capture and the save: `useCaptureSpot` (P12b).
  const cap = useCaptureSpot({ source, weather: props.weather, feed, photos: props.photos, history: props.history });
  // Leaving without saving deletes the photos the draft added (S1): nothing else would ever hold them.
  const cancel = () => { void cap.cancel(); props.onCancel(); };
  const draft = cap.draft;
  const [title, setTitle] = useState("");
  const [notes, setNotes] = useState("");
  /*
   * BACK, WHILE ADDING A SPOT (app/HANDOFF-ui.md "Back, everywhere"): one back is one step back — the details to the
   * hour, the hour to the point, the point to the water — exactly what each step's own ‹ / Back does. On the first
   * step back leaves, as Cancel does: at once when nothing was put in, else only after asking (`DRAFT_WORDS`), so
   * no back ever throws away a name, notes or photographs unasked. `asking` is answered "keep" by a back.
   */
  const [asking, setAsking] = useState(false);
  const held = notes.trim() !== "" || cap.photos.refs.length > 0
    || (title.trim() !== "" && title !== (water?.name ?? ""));
  const leave = () => (held ? setAsking(true) : cancel());
  const stepNow = useRef(step);
  stepNow.current = step;
  useBackLayer(step !== "water" || held, () => {
    if (asking) setAsking(false);
    else if (PREV[step] !== null) setStep(PREV[step]!);
    else leave();
  });

  const pickWater = async (_layer: string, featureId: SectionKey) => {
    // The tile's feature id IS the section handle — see SectionId in @app/data.
    const section = Number(featureId) as SectionId;
    // One read for the id and the name: `waterFor` is the reach-to-water lookup.
    const found = await source.waterFor(section);
    setWater({ section, item: found?.item ?? null, name: found?.name ?? null });
  };

  const toDetails = async () => {
    if (!water || !point) return;
    setStep("saving");
    // never left on "saving": a failure goes back to the step that asked, with the reason (P01)
    const ok = await cap.capture(water, point, title);
    if (stepNow.current !== "saving") return;            // a back while it read: the reader has moved on
    if (!ok) { setStep("when"); return; }
    setTitle(title || water.name || "");
    setStep("details");
  };

  if (step === "when") {
    return (
      <WhenScreen palette={palette} steps={cap.steps} onShiftDay={cap.shiftDay} onDay={cap.toDay}
                  onHour={cap.setHour} water={water?.name ?? null} error={cap.error}
                  onBack={() => setStep("point")} onNext={toDetails} />
    );
  }

  if (step === "details" || step === "saving") {
    if (!draft) return <Busy palette={palette} label={CW.reading} />;
    // The SAME component that shows a saved spot, in draft mode. What you confirm here is
    // exactly what you get afterwards — which was not true when this screen had its own
    // renderer and quietly showed less.
    return (
      <SpotScreen spot={draft} palette={palette} mode="draft"
                  // an earlier trip day: the day's flow as read from its record, said before it is saved (Q161)
                  dayFlow={{ view: dayFlowView(draft, "idle"), onAsk: null }}
                  photoStore={props.photos?.store} photoEdit={cap.photos}
                  title={title} notes={notes} onTitle={setTitle} onNotes={setNotes}
                  onBack={() => setStep("when")}
                  error={cap.error}
                  // a failure is said and the sheet stays open with the draft in it
                  onSave={() => cap.save(title, notes, props.onSaved)} />
    );
  }

  const ready = step === "water" ? water !== null : point !== null;
  return (
    <View style={{ flex: 1 }}>
      <MapScreen at={tiles} palette={palette} theme={theme} camera={start}
                 view="plain"
                 onPressFeature={step === "water"
                   ? pickWater
                   : () => { /* step 2 takes a POINT, handled by onMapPoint below */ }}
                 onMapPoint={step === "point" ? (lat, lon) => setPoint({ lat, lon }) : undefined}
                 highlight={water ? [water.section] : []}
                 marker={point} />

      {asking && (
        <View style={{ backgroundColor: palette.card, paddingHorizontal: 16, paddingTop: 12 }}>
          <Confirm palette={palette} kind="draft" onCancel={() => setAsking(false)}
                   onConfirm={() => { setAsking(false); cancel(); }} />
        </View>
      )}
      <Coach palette={palette}
             title={step === "water" ? (preset ? SPOT_CAPTURE_AT.title : CW.pickWater) : CW.exactSpot}
             detail={step === "water"
               ? (preset ? SPOT_CAPTURE_AT.detail : CW.pickWaterDetail)
               : CW.tapPoint(water?.name ?? null)}
             chose={step === "water" ? water?.name ?? null
                                     : point && `${point.lat.toFixed(4)}, ${point.lon.toFixed(4)}`}
             // Cancel leaves the whole capture: asks first when the draft holds what the reader put in
             onCancel={leave}
             // the pin is already down when the spot was started at a point: on to when you were there
             onNext={ready ? () => setStep(step === "water" && !preset ? "point" : "when") : undefined} />
    </View>
  );
}

/**
 * When were you there.
 *
 * Deliberately NOT a calendar. Almost every spot is today, yesterday or the day before —
 * the drive home, the evening after, the next morning — so those are one tap each, and the
 * arrows handle the rest without a date picker that has to be styled twice and behaves
 * differently on two platforms.
 *
 * The hour matters as much as the day: cloud cover and flow at 6 a.m. are not cloud cover
 * and flow at 4 p.m., and the whole record is fetched against this instant.
 *
 * A future date is refused rather than allowed and then quietly failing at the fetch. The
 * archive has nothing for tomorrow, and a spot that records a visit that has not happened
 * is not a spot.
 */
function WhenScreen({ palette, steps, onShiftDay, onDay, onHour, water, onBack, onNext, error }: {
  palette: Palette; steps: VisitSteps;
  onShiftDay: (n: number) => void; onDay: (offset: number) => void; onHour: (h: number) => void;
  water: string | null; onBack: () => void; onNext: () => void; error?: string | null;
}) {
  const { dayOffset } = steps;
  const { hour } = steps;

  return (
    <View style={{ flex: 1, backgroundColor: palette.card, padding: 20, gap: 26 }}>
      <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel={CW.back}>
        <IconText name="back" text={CW.back} style={{ ...TYPE.micro, fontSize: 13, color: palette.accent }} />
      </Pressable>
      {error ? <Text accessibilityRole="alert" style={{ ...TYPE.small, color: palette.closed }}>{error}</Text> : null}

      <View style={{ gap: 6 }}>
        <Text style={{ ...TYPE.title, color: palette.ink }}>{CW.when}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          The weather, the flow and the rules are all recorded for this moment
          {water ? ` on ${water}` : ""} — so it is worth getting right.
        </Text>
      </View>

      <View style={{ gap: 10 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.5,
                       color: palette.faint }}>DAY</Text>
        <View style={{ flexDirection: "row", gap: 8, flexWrap: "wrap" }}>
          {VISIT_QUICK_DAYS.map(([label, off]) => (
            <Pressable key={label} accessibilityRole="button" accessibilityLabel={label}
                       onPress={() => onDay(off)}
                       style={{ paddingVertical: 9, paddingHorizontal: 14, borderRadius: palette.r.pill,
                                borderWidth: 1,
                                borderColor: dayOffset === off ? palette.accent : palette.line2,
                                backgroundColor: dayOffset === off ? palette.accent : "transparent" }}>
              <Text style={{ ...TYPE.micro, fontSize: 12.5,
                             color: dayOffset === off ? palette.onAccent : palette.ink }}>
                {label}
              </Text>
            </Pressable>
          ))}
        </View>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 14, paddingTop: 4 }}>
          <Step palette={palette} label={CW.earlierDay} icon="back" onPress={() => onShiftDay(-1)} />
          <Text style={{ ...TYPE.bodyStrong, flex: 1, textAlign: "center", color: palette.ink }}>
            {steps.dayText}
          </Text>
          <Step palette={palette} label={CW.laterDay} icon="next" onPress={() => onShiftDay(1)}
                disabled={!steps.laterDay} />
        </View>
      </View>

      <View style={{ gap: 10 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.5,
                       color: palette.faint }}>HOUR</Text>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 14 }}>
          <Step palette={palette} label={CW.earlierHour} icon="back"
                onPress={() => onHour(hour - 1)} disabled={!steps.earlierHour} />
          <Text style={{ ...TYPE.figureBig, fontSize: 30, flex: 1, textAlign: "center",
                         color: palette.ink }}>
            {steps.hourLabel}
          </Text>
          <Step palette={palette} label={CW.laterHour} icon="next"
                onPress={() => onHour(hour + 1)} disabled={!steps.laterHour} />
        </View>
      </View>

      <View style={{ flex: 1 }} />
      <Pressable onPress={onNext} accessibilityRole="button" accessibilityLabel={CW.next}
                 style={{ borderRadius: palette.r.box, paddingVertical: 15, alignItems: "center",
                          backgroundColor: palette.accent }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.onAccent }}>{CW.next}</Text>
      </Pressable>
    </View>
  );
}

function Step({ palette, label, icon, onPress, disabled }: {
  palette: Palette; label: string; icon: IconName; onPress: () => void; disabled?: boolean;
}) {
  return (
    <Pressable onPress={onPress} disabled={disabled} accessibilityRole="button"
               accessibilityLabel={label} aria-disabled={!!disabled}
               style={{ width: 44, height: 44, borderRadius: palette.r.box, alignItems: "center",
                        justifyContent: "center", borderWidth: 1, borderColor: palette.line2,
                        opacity: disabled ? 0.35 : 1 }}>
      <Icon name={icon} colour={palette.ink} size={22} />
    </Pressable>
  );
}

/** The banner: what to do, what you chose, and the way forward. */
function Coach({ palette, title, detail, chose, onCancel, onNext, onNextAsync }: {
  palette: Palette; title: string; detail: string; chose: string | null;
  onCancel: () => void; onNext?: () => void; onNextAsync?: () => void;
}) {
  const go = onNextAsync ?? onNext;
  return (
    <View style={{ backgroundColor: palette.card, borderTopWidth: 1,
                   borderTopColor: palette.line, padding: 16, gap: 12 }}>
      <View style={{ flexDirection: "row", alignItems: "flex-start", gap: 12 }}>
        <View style={{ flex: 1, gap: 3 }}>
          <Text style={{ ...TYPE.name, fontSize: 17, color: palette.ink }}>{title}</Text>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{detail}</Text>
          {/* What you picked, before it becomes permanent. */}
          {chose && (
            <Text style={{ ...TYPE.bodyStrong, fontSize: 13.5, color: palette.accent,
                           marginTop: 3 }}>{chose}</Text>
          )}
        </View>
      </View>
      <View style={{ flexDirection: "row", gap: 10 }}>
        <Button palette={palette} label={CW.cancel} onPress={onCancel} kind="ghost" />
        <Button palette={palette} label={CW.next} onPress={go} />
      </View>
    </View>
  );
}


function Busy({ palette, label }: { palette: Palette; label: string }) {
  return (
    <View style={{ flex: 1, alignItems: "center", justifyContent: "center", gap: 14,
                   backgroundColor: palette.card }}>
      <FishSpinner palette={palette} size={96} label={label} />
      <Text style={{ ...TYPE.small, color: palette.sub }}>{label}…</Text>
    </View>
  );
}

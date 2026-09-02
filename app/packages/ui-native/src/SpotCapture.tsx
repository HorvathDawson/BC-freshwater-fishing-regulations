/**
 * Adding a spot. Four steps, each confirmed before the next.
 *
 *   1. PICK THE WATER    tap a stream or lake; it highlights; Next
 *   2. PICK THE POINT    tap the exact place on it; Next
 *   3. WHEN WERE YOU     the date and hour you were there; Next
 *   4. WHAT WAS TRUE     gauge, how the gauge reaches this spot, weather, the regulation
 *                        in force that day — then notes and photographs, then Save
 *
 * STEP 3 COMES BEFORE ANYTHING IS FETCHED, and that is the whole reason it exists as a
 * step rather than a field on the last screen. Weather, the gauge reading and the
 * regulation are all fetched FOR A DATE; asking afterwards would mean fetching twice, or
 * — worse — showing a person Tuesday's sky over the Sunday they actually fished.
 *
 * WHY THE CONFIRM STEPS. The first version tapped once and saved. On a phone, over a
 * one-pixel river, that is a coin toss: you get whichever water your thumb happened to
 * cover and no chance to see it was wrong before it is a permanent record. Highlighting
 * the choice and asking is the difference between picking and guessing.
 *
 * The satellite toggle is on screen during step 2 for the same reason. You are choosing a
 * point you can find again — a gravel bar, a seam below a bend — and the base map draws a
 * river as a blue line where the imagery shows you the actual bar. Satellite drops the
 * overlay entirely while picking, because a coloured line over the imagery hides the very
 * thing you came to look at.
 */
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import type { PlainDate, SpeciesGroup } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { Spot, WeatherSource } from "@app/data/spots";
import { captureSpot } from "@app/data/spots";
import type { Camera, TileEndpoints } from "@app/map";
import { Pill } from "./Chrome";
import { FishSpinner } from "./FishSpinner";
import { MapScreen } from "./MapScreen";
import { SpotScreen } from "./SpotScreen";
import { TYPE } from "./type";
import type { Palette } from "./theme";

type Step = "water" | "point" | "when" | "details" | "saving";

export interface SpotCaptureProps {
  source: RegsSource;
  weather?: WeatherSource;
  tiles: TileEndpoints;
  palette: Palette;
  theme: string;
  camera: Camera;
  on: PlainDate;
  group: SpeciesGroup;
  onCancel: () => void;
  onSaved: (spot: Spot) => void;
}

export function SpotCapture(props: SpotCaptureProps) {
  const { palette, tiles, theme, camera, source, on, group } = props;
  const [step, setStep] = useState<Step>("water");
  const [satellite, setSatellite] = useState(false);
  const [water, setWater] = useState<{ section: SectionId; item: ItemId | null;
                                       name: string | null } | null>(null);
  const [point, setPoint] = useState<{ lat: number; lon: number } | null>(null);
  // Defaults to now, because that is the common case and a default nobody has to touch is
  // worth more than a blank that everybody must.
  const [visitedAt, setVisitedAt] = useState(() => Date.now());
  const [draft, setDraft] = useState<Spot | null>(null);
  const [title, setTitle] = useState("");
  const [notes, setNotes] = useState("");

  const pickWater = async (_layer: string, featureId: string) => {
    const section = featureId as SectionId;
    const item = await source.itemForSection(section);
    const sheet = item ? await source.regsForItem(item, on, group) : null;
    setWater({ section, item, name: sheet?.name ?? null });
  };

  const toDetails = async () => {
    if (!water || !point) return;
    setStep("saving");
    const spot = await captureSpot({
      source, weather: props.weather, at: point, visitedAt,
      item: water.item, section: water.section, waterName: water.name,
      group, title: title || water.name || "",
    });
    setDraft(spot);
    setTitle(spot.title);
    setStep("details");
  };

  if (step === "when") {
    return (
      <WhenScreen palette={palette} at={visitedAt} onChange={setVisitedAt}
                  water={water?.name ?? null}
                  onBack={() => setStep("when")} onNext={toDetails} />
    );
  }

  if (step === "details" || step === "saving") {
    if (!draft) return <Busy palette={palette} label="Reading the water" />;
    // The SAME component that shows a saved spot, in draft mode. What you confirm here is
    // exactly what you get afterwards — which was not true when this screen had its own
    // renderer and quietly showed less.
    return (
      <SpotScreen spot={draft} palette={palette} mode="draft"
                  title={title} notes={notes} onTitle={setTitle} onNotes={setNotes}
                  onBack={() => setStep("when")}
                  onSave={() => props.onSaved({ ...draft, title: title || draft.title, notes,
                                                updatedAt: Date.now() })} />
    );
  }

  const ready = step === "water" ? water !== null : point !== null;
  return (
    <View style={{ flex: 1 }}>
      <MapScreen at={tiles} palette={palette} theme={theme} camera={camera} on={on}
                 // Satellite drops the overlay: you are looking for a gravel bar, and a
                 // coloured line drawn over it hides exactly what you came to see.
                 view={satellite ? "plain" : "regulations"}
                 modes={{ stream: satellite ? "plain" : "closure",
                          lake: satellite ? "plain" : "closure" }}
                 onPressFeature={step === "water"
                   ? pickWater
                   : () => { /* step 2 takes a POINT, handled by onMapPoint below */ }}
                 onMapPoint={step === "point" ? (lat, lon) => setPoint({ lat, lon }) : undefined}
                 highlight={water ? [water.section] : []}
                 marker={point} />

      <Coach palette={palette}
             title={step === "water" ? "Pick the water" : "Now the exact spot"}
             detail={step === "water"
               ? "Tap the stream or lake your spot is on."
               : `Tap the point on ${water?.name ?? "this reach"}.`}
             chose={step === "water" ? water?.name ?? null
                                     : point && `${point.lat.toFixed(4)}, ${point.lon.toFixed(4)}`}
             satellite={satellite} onSatellite={setSatellite}
             onCancel={props.onCancel}
             onNext={ready ? () => setStep(step === "water" ? "point" : "when") : undefined} />
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
function WhenScreen({ palette, at, onChange, water, onBack, onNext }: {
  palette: Palette; at: number; onChange: (t: number) => void;
  water: string | null; onBack: () => void; onNext: () => void;
}) {
  const d = new Date(at);
  const startOfToday = new Date(); startOfToday.setHours(0, 0, 0, 0);
  const dayOffset = Math.round(
    (new Date(at).setHours(0, 0, 0, 0) - startOfToday.getTime()) / 86_400_000);

  const shiftDay = (n: number) => {
    const next = new Date(at); next.setDate(next.getDate() + n);
    if (next.getTime() > Date.now()) return;      // never ahead of now
    onChange(next.getTime());
  };
  const setHour = (h: number) => {
    const next = new Date(at); next.setHours(h, 0, 0, 0);
    onChange(Math.min(next.getTime(), Date.now()));
  };

  const quick: [string, number][] = [["Today", 0], ["Yesterday", -1], ["2 days ago", -2]];

  return (
    <View style={{ flex: 1, backgroundColor: palette.card, padding: 20, gap: 26 }}>
      <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel="Back">
        <Text style={{ ...TYPE.micro, fontSize: 13, color: palette.accent }}>‹  Back</Text>
      </Pressable>

      <View style={{ gap: 6 }}>
        <Text style={{ ...TYPE.title, color: palette.ink }}>When were you there?</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          The weather, the flow and the rules are all recorded for this moment
          {water ? ` on ${water}` : ""} — so it is worth getting right.
        </Text>
      </View>

      <View style={{ gap: 10 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.5,
                       color: palette.faint }}>DAY</Text>
        <View style={{ flexDirection: "row", gap: 8, flexWrap: "wrap" }}>
          {quick.map(([label, off]) => (
            <Pressable key={label} accessibilityRole="button" accessibilityLabel={label}
                       onPress={() => shiftDay(off - dayOffset)}
                       style={{ paddingVertical: 9, paddingHorizontal: 14, borderRadius: 999,
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
          <Step palette={palette} label="Earlier day" glyph="‹" onPress={() => shiftDay(-1)} />
          <Text style={{ ...TYPE.bodyStrong, flex: 1, textAlign: "center", color: palette.ink }}>
            {d.toDateString()}
          </Text>
          <Step palette={palette} label="Later day" glyph="›" onPress={() => shiftDay(1)}
                disabled={dayOffset >= 0} />
        </View>
      </View>

      <View style={{ gap: 10 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.5,
                       color: palette.faint }}>HOUR</Text>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 14 }}>
          <Step palette={palette} label="Earlier hour" glyph="‹"
                onPress={() => setHour(d.getHours() - 1)} disabled={d.getHours() <= 0} />
          <Text style={{ ...TYPE.figureBig, fontSize: 30, flex: 1, textAlign: "center",
                         color: palette.ink }}>
            {String(d.getHours()).padStart(2, "0")}:00
          </Text>
          <Step palette={palette} label="Later hour" glyph="›"
                onPress={() => setHour(d.getHours() + 1)}
                disabled={d.getHours() >= 23
                          || new Date(at).setHours(d.getHours() + 1, 0, 0, 0) > Date.now()} />
        </View>
      </View>

      <View style={{ flex: 1 }} />
      <Pressable onPress={onNext} accessibilityRole="button" accessibilityLabel="Next"
                 style={{ borderRadius: 14, paddingVertical: 15, alignItems: "center",
                          backgroundColor: palette.accent }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.onAccent }}>Next</Text>
      </Pressable>
    </View>
  );
}

function Step({ palette, label, glyph, onPress, disabled }: {
  palette: Palette; label: string; glyph: string; onPress: () => void; disabled?: boolean;
}) {
  return (
    <Pressable onPress={onPress} disabled={disabled} accessibilityRole="button"
               accessibilityLabel={label} accessibilityState={{ disabled: !!disabled }}
               style={{ width: 44, height: 44, borderRadius: 22, alignItems: "center",
                        justifyContent: "center", borderWidth: 1, borderColor: palette.line2,
                        opacity: disabled ? 0.35 : 1 }}>
      <Text style={{ ...TYPE.bodyStrong, fontSize: 19, color: palette.ink }}>{glyph}</Text>
    </Pressable>
  );
}

/** The banner: what to do, what you chose, and the way forward. */
function Coach({ palette, title, detail, chose, satellite, onSatellite, onCancel, onNext,
                 onNextAsync }: {
  palette: Palette; title: string; detail: string; chose: string | null;
  satellite: boolean; onSatellite: (on: boolean) => void;
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
        <Pressable onPress={() => onSatellite(!satellite)} accessibilityRole="switch"
                   accessibilityState={{ checked: satellite }}
                   accessibilityLabel="Satellite imagery">
          <Pill palette={palette}>
            <Text style={{ ...TYPE.micro, fontSize: 12,
                           color: satellite ? palette.accent : palette.sub }}>
              {satellite ? "Satellite" : "Map"}
            </Text>
          </Pill>
        </Pressable>
      </View>
      <View style={{ flexDirection: "row", gap: 10 }}>
        <Action palette={palette} label="Cancel" onPress={onCancel} ghost />
        <Action palette={palette} label="Next" onPress={go} />
      </View>
    </View>
  );
}

function Action({ palette, label, onPress, ghost = false }: {
  palette: Palette; label: string; onPress?: () => void; ghost?: boolean;
}) {
  const off = onPress === undefined;
  return (
    <Pressable onPress={onPress} disabled={off} accessibilityRole="button"
               accessibilityLabel={label} accessibilityState={{ disabled: off }}
               style={{ flex: 1, alignItems: "center", borderRadius: 13, paddingVertical: 14,
                        opacity: off ? 0.4 : 1,
                        backgroundColor: ghost ? "transparent" : palette.accent,
                        borderWidth: ghost ? 1 : 0, borderColor: palette.line2 }}>
      <Text style={{ ...TYPE.bodyStrong, fontSize: 15, fontWeight: "700",
                     color: ghost ? palette.accent : palette.onAccent }}>{label}</Text>
    </Pressable>
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

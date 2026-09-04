/**
 * One spot — the ONLY thing that renders one, in every state it has.
 *
 *   draft   before it is saved: title and notes editable, Back and Save
 *   view    saved: read-only, Edit and Delete
 *   edit    saved and being changed: editable, Discard and Save changes
 *
 * ONE COMPONENT ON PURPOSE. There used to be a second, `Details`, inside the capture flow,
 * and it drifted exactly the way rule 23 predicts: it showed a temperature and nothing else
 * while this screen showed wind, rain, the cloud window and the observation time. A person
 * saved a spot, opened it, and found more than they had been shown. What you confirm before
 * saving must be what you get.
 *
 * EVERYTHING HERE IS A RECORD, NOT A READING. The gauge figure, the weather and the
 * regulation were copied in when the spot was made and are never recomputed, so nothing on
 * this screen may imply they are current. Each carries its own date, and a value filled in
 * after the fact says so.
 *
 * The trace panel is the same component the Conditions tab shows, fed from the spot's
 * frozen copy instead of a live query. A gauge's representativeness is the most misread
 * number in the app; it may not have two explanations.
 */
import { useState } from "react";
import { Image, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import { isUntitled, needsRefresh, refreshReason, spotLabel, type Spot,
  type SpotWeather, type WeatherSample } from "@app/data/spots";
import { GaugeTrace } from "./GaugeTrace";
import { StatusPill } from "./StatusPill";
import { TYPE } from "./type";
import { outcomeColour, type Palette } from "./theme";
import { Button } from "./Button";

export type SpotMode = "draft" | "view" | "edit";

export interface SpotScreenProps {
  spot: Spot;
  palette: Palette;
  mode?: SpotMode;
  /** Draft and edit only. Absent means the fields are read-only. */
  title?: string;
  notes?: string;
  onTitle?: (s: string) => void;
  onNotes?: (s: string) => void;
  /** view: leave the screen. draft: back to picking. edit: handled by onDiscard. */
  onBack?: () => void;
  onEdit?: () => void;
  onSave?: () => void;
  onDiscard?: () => void;
  onDelete?: () => void;
  onRefresh?: () => void;
  refreshing?: boolean;
  /** Centre the map here and go to it. Absent in the draft, where there is nothing to go to. */
  onShowOnMap?: () => void;
  /** True in edit mode once something has actually changed. Gates the discard prompt. */
  dirty?: boolean;
}

export function SpotScreen(p: SpotScreenProps) {
  const { spot, palette, mode = "view" } = p;
  const editable = mode === "draft" || mode === "edit";
  const [confirm, setConfirm] = useState<"delete" | "discard" | null>(null);

  const r = spot.reading;
  const w = spot.weather;
  const when = new Date(spot.visitedAt ?? spot.createdAt);

  // Edit with nothing changed is not a decision worth interrupting someone for.
  const leaveEdit = () => (p.dirty ? setConfirm("discard") : p.onDiscard?.());

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ flexDirection: "row", alignItems: "center",
                     justifyContent: "space-between",
                     paddingHorizontal: 18, paddingTop: 14, paddingBottom: 6 }}>
        {mode === "edit" ? (
          <Link palette={palette} label="Cancel editing" text="Cancel" onPress={leaveEdit} />
        ) : p.onBack ? (
          <Link palette={palette} label="Back"
                text={mode === "draft" ? "‹  Back" : "‹  Spots"} onPress={p.onBack} />
        ) : <View />}

        <View style={{ flexDirection: "row", gap: 18 }}>
          {mode === "view" && p.onEdit && (
            <Link palette={palette} label="Edit spot" text="Edit" onPress={p.onEdit} />
          )}
          {mode === "view" && p.onDelete && (
            <Link palette={palette} label="Delete spot" text="Delete" tone={palette.closed}
                  onPress={() => setConfirm("delete")} />
          )}
        </View>
      </View>

      {confirm && (
        <Confirm palette={palette}
                 kind={confirm}
                 onCancel={() => setConfirm(null)}
                 onConfirm={() => {
                   setConfirm(null);
                   if (confirm === "delete") p.onDelete?.();
                   else p.onDiscard?.();
                 }}
                 onSaveInstead={confirm === "discard" ? p.onSave : undefined} />
      )}

      <ScrollView contentContainerStyle={{ padding: 18, paddingTop: 6, gap: 22,
                                           paddingBottom: 40 }}>
        <View style={{ gap: 4 }}>
          {editable && p.onTitle ? (
            <TextInput value={p.title ?? ""} onChangeText={p.onTitle}
                       placeholder="Name this spot" placeholderTextColor={palette.faint}
                       accessibilityLabel="Name this spot"
                       style={{ ...TYPE.title, color: palette.ink }} />
          ) : (
            <Text style={{ ...TYPE.title, color: palette.ink }}>{spotLabel(spot)}</Text>
          )}
          {/* A title is optional, so the prompt is an offer rather than a warning — but it
              is worth offering, because the list shows this and a coordinate is hard to
              pick out of twenty. */}
          {!editable && isUntitled(spot) && p.onEdit && (
            <Pressable onPress={p.onEdit} accessibilityRole="button"
                       accessibilityLabel="Add a title">
              <Text style={{ ...TYPE.small, color: palette.accent }}>Add a title</Text>
            </Pressable>
          )}
          <Text style={{ ...TYPE.small, color: palette.sub }}>
            {when.toISOString().slice(0, 10)} at {when.toISOString().slice(11, 16)} ·{" "}
            {spot.lat.toFixed(4)}, {spot.lon.toFixed(4)}
          </Text>
          {spot.waterName && (
            <Text style={{ ...TYPE.small, color: palette.faint }}>
              {/* The name it had when it was pinned, even if the registry has renamed it. */}
              on {spot.waterName}
            </Text>
          )}
        </View>

        {p.onRefresh && needsRefresh(spot) && (
          <View style={{ flexDirection: "row", alignItems: "center", gap: 12, padding: 14,
                         borderRadius: 12, backgroundColor: palette.tint }}>
            <Text style={{ ...TYPE.small, flex: 1, color: palette.sub }}>
              {refreshReason(spot)}
            </Text>
            <Pressable onPress={p.onRefresh} disabled={p.refreshing}
                       accessibilityRole="button" accessibilityLabel="Refresh this spot"
                       style={{ borderRadius: 999, paddingVertical: 8, paddingHorizontal: 14,
                                backgroundColor: palette.card, borderWidth: 1,
                                borderColor: palette.line2, opacity: p.refreshing ? 0.5 : 1 }}>
              <Text style={{ ...TYPE.micro, fontSize: 12.5, color: palette.accent }}>
                {p.refreshing ? "…" : "Refresh"}
              </Text>
            </Pressable>
          </View>
        )}

        <Block palette={palette} title="What the water was doing">
          {r?.discharge != null ? (
            <>
              <View style={{ flexDirection: "row", alignItems: "baseline", gap: 8,
                             flexWrap: "wrap" }}>
                <Text style={{ ...TYPE.figureBig, fontSize: 32, color: palette.live }}>
                  {r.discharge}
                </Text>
                <Text style={{ ...TYPE.figure, color: palette.sub }}>m³/s</Text>
                {r.percentile != null && (
                  <Text style={{ ...TYPE.figure, color: palette.sub }}>
                    · p{Math.round(r.percentile * 100)} against the record
                  </Text>
                )}
              </View>
              <Meta palette={palette} at={r.at} backfilled={r.backfilled} station={r.station} />
            </>
          ) : (
            <Text style={{ ...TYPE.small, color: palette.sub }}>
              No gauge is entitled to speak for this water, so no reading was recorded. A
              station draining a far larger watershed would have given a number, and the
              number would have been wrong.
            </Text>
          )}
        </Block>

        {p.onShowOnMap && (
          <Pressable onPress={p.onShowOnMap} accessibilityRole="button"
                     accessibilityLabel="Show on map"
                     style={{ flexDirection: "row", alignItems: "center", gap: 10,
                              paddingVertical: 12, paddingHorizontal: 14, borderRadius: 12,
                              borderWidth: 1, borderColor: palette.line2 }}>
            <Text style={{ ...TYPE.bodyStrong, flex: 1, color: palette.ink }}>
              Show on map
            </Text>
            <Text style={{ ...TYPE.micro, fontSize: 13, color: palette.accent }}>›</Text>
          </Pressable>
        )}

        {spot.trace && <GaugeTrace trace={spot.trace} palette={palette} />}

        <Block palette={palette} title="Weather">
          {w?.tempC != null ? (
            <>
              <View style={{ flexDirection: "row", alignItems: "baseline", gap: 12,
                             flexWrap: "wrap" }}>
                <Text style={{ ...TYPE.figureBig, fontSize: 32, color: palette.ink }}>
                  {w.tempC}°C
                </Text>
                {w.windKph != null && (
                  <Text style={{ ...TYPE.figure, color: palette.sub }}>
                    wind {Math.round(w.windKph)} km/h
                  </Text>
                )}
                {w.rain3h != null && (
                  <Text style={{ ...TYPE.figure, color: palette.sub }}>
                    rain {w.rain3h} mm
                  </Text>
                )}
              </View>
              <Window weather={w} palette={palette} />
              <Meta palette={palette} at={w.at} backfilled={w.backfilled} />
            </>
          ) : (
            <Text style={{ ...TYPE.small, color: palette.sub }}>
              No weather was recorded for this spot.
            </Text>
          )}
        </Block>

        {spot.regulation && (
          <Block palette={palette} title={`In force on ${spot.regulation.on}`}>
            <View style={{ flexDirection: "row", alignItems: "center", gap: 10 }}>
              <View style={{ width: 4, height: 22, borderRadius: 2,
                             backgroundColor: outcomeColour(palette, spot.regulation.outcome) }} />
              <StatusPill palette={palette}
                          status={{ outcome: spot.regulation.outcome,
                                    provenance: spot.regulation.provenance, from: [] }} />
            </View>
            <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
              What was true that day. Half of these rules are seasonal, so this is recorded
              rather than recalculated — otherwise a river you fished in a closure would
              quietly start reading as open.
            </Text>
          </Block>
        )}

        <Block palette={palette} title="Notes">
          {editable && p.onNotes ? (
            <TextInput value={p.notes ?? ""} onChangeText={p.onNotes} multiline
                       placeholder="What happened, what you used, what the water looked like"
                       placeholderTextColor={palette.faint} accessibilityLabel="Notes"
                       style={{ ...TYPE.body, color: palette.ink, minHeight: 96,
                                textAlignVertical: "top", borderWidth: 1,
                                borderColor: palette.line, borderRadius: 12, padding: 12 }} />
          ) : spot.notes.trim() !== "" ? (
            <Text style={{ ...TYPE.body, color: palette.ink }}>{spot.notes}</Text>
          ) : (
            <Text style={{ ...TYPE.small, color: palette.faint }}>None.</Text>
          )}
        </Block>

        {spot.photos.length > 0 && (
          <Block palette={palette} title="Photographs">
            <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
              {spot.photos.map((uri) => (
                <Image key={uri} source={{ uri }} accessibilityLabel=""
                       style={{ width: 104, height: 104, borderRadius: 12 }}
                       resizeMode="cover" />
              ))}
            </View>
          </Block>
        )}
      </ScrollView>

      {editable && p.onSave && (
        <View style={{ flexDirection: "row", gap: 10, padding: 16, borderTopWidth: 1,
                       borderTopColor: palette.line }}>
          <Button palette={palette} kind="ghost" label={mode === "draft" ? "Back" : "Cancel"}
                  onPress={mode === "draft" ? p.onBack : leaveEdit} />
          <Button palette={palette}
                  label={mode === "draft" ? "Save spot" : "Save changes"} onPress={p.onSave} />
        </View>
      )}
    </View>
  );
}

/**
 * The three hours around the visit.
 *
 * A TABLE, not a sentence, because four measures across three hours is a shape the eye
 * reads and prose does not. The trend line under it names the two changes that actually
 * decide an afternoon — the sky and the barometer — and only when both ends are known.
 *
 * Renders nothing when there is no window. An empty grid of dashes would imply we looked.
 */
function Window({ weather, palette }: { weather: SpotWeather; palette: Palette }) {
  const win = weather.window;
  if (!win || (!win.before && !win.at && !win.after)) return null;

  const cols: [string, WeatherSample | null][] =
    [["−1 h", win.before], ["visit", win.at], ["+1 h", win.after]];
  const rows: [string, (s: WeatherSample) => number | null, string][] = [
    ["Cloud", (s) => s.cloudPct, "%"],
    ["Pressure", (s) => s.pressureHpa, ""],
    ["Humidity", (s) => s.humidityPct, "%"],
    ["Temp", (s) => s.tempC, "°"],
  ];

  const fig = (v: number | null, unit: string) =>
    v == null ? "—" : `${Math.round(v)}${unit}`;

  return (
    <View style={{ gap: 8, paddingTop: 4 }}>
      <View style={{ flexDirection: "row" }}>
        <View style={{ width: 76 }} />
        {cols.map(([label]) => (
          <Text key={label}
                style={{ ...TYPE.micro, flex: 1, fontSize: 11, textAlign: "right",
                         color: label === "visit" ? palette.ink : palette.faint }}>
            {label}
          </Text>
        ))}
      </View>
      {rows.map(([label, get, unit]) => (
        <View key={label} style={{ flexDirection: "row", alignItems: "baseline" }}>
          <Text style={{ ...TYPE.small, width: 76, fontSize: 12, color: palette.sub }}>
            {label}
          </Text>
          {cols.map(([col, s]) => (
            <Text key={col} accessibilityLabel={`${label} ${col}`}
                  style={{ ...TYPE.figure, flex: 1, fontSize: 13, textAlign: "right",
                           color: col === "visit" ? palette.ink : palette.sub }}>
              {s ? fig(get(s), unit) : "—"}
            </Text>
          ))}
        </View>
      ))}
      <Trend win={win} palette={palette} />
    </View>
  );
}

/**
 * What changed across the window, in words.
 *
 * Thresholds are the width of the models' own disagreement, so a value below one is noise
 * rather than a trend: 10 points of cloud, 1 hPa of pressure. Below both, the honest
 * sentence is that it was holding — which is itself information.
 */
function Trend({ win, palette }: {
  win: NonNullable<SpotWeather["window"]>; palette: Palette;
}) {
  const b = win.before, a = win.after;
  if (!b || !a) return null;

  const bits: string[] = [];
  if (b.cloudPct != null && a.cloudPct != null) {
    const d = a.cloudPct - b.cloudPct;
    bits.push(d >= 10 ? "clouding over" : d <= -10 ? "clearing" : "cloud holding");
  }
  if (b.pressureHpa != null && a.pressureHpa != null) {
    const d = a.pressureHpa - b.pressureHpa;
    bits.push(d >= 1 ? "pressure rising" : d <= -1 ? "pressure falling" : "pressure steady");
  }
  if (!bits.length) return null;

  return (
    <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
      {bits.join(" · ")} — across the hour either side
    </Text>
  );
}

/** A destructive or lossy step, asked before it happens. */
function Confirm({ palette, kind, onCancel, onConfirm, onSaveInstead }: {
  palette: Palette; kind: "delete" | "discard";
  onCancel: () => void; onConfirm: () => void; onSaveInstead?: () => void;
}) {
  const del = kind === "delete";
  return (
    <View accessibilityLabel={del ? "Confirm delete" : "Unsaved changes"}
          style={{ marginHorizontal: 18, marginBottom: 8, padding: 16, borderRadius: 12,
                   borderWidth: 1, borderColor: del ? palette.closed : palette.line2,
                   backgroundColor: palette.wash, gap: 12 }}>
      <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>
        {del ? "Delete this spot?" : "You have unsaved changes."}
      </Text>
      <Text style={{ ...TYPE.small, color: palette.sub }}>
        {del
          ? "The gauge reading, the weather and the rules that were in force are recorded " +
            "nowhere else. This cannot be undone."
          : "Keep them, or throw them away and go back to what was saved."}
      </Text>
      <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap" }}>
        <Button palette={palette} kind="ghost" label={del ? "Keep it" : "Keep editing"}
                onPress={onCancel} />
        {/* NOT "Save changes" — the footer already has a button with that label, and two
            identically-named buttons on one screen is how somebody presses the wrong one.
            This one saves AND leaves, so it says so. */}
        {onSaveInstead && (
          <Button palette={palette} label="Save and close" onPress={onSaveInstead} />
        )}
        <Button palette={palette} kind={del ? "danger" : "solid"}
                label={del ? "Delete" : "Discard"} onPress={onConfirm} />
      </View>
    </View>
  );
}

/** When a value was observed, and whether it was filled in after the fact. */
function Meta({ palette, at, backfilled, station }: {
  palette: Palette; at: string | null; backfilled?: boolean; station?: string | null;
}) {
  const bits = [
    station ?? null,
    at ? `observed ${at.replace("T", " ").slice(0, 16)}` : "no observation time recorded",
    backfilled ? "filled in afterwards" : null,
  ].filter(Boolean);
  return (
    <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
      {bits.join(" · ")}
    </Text>
  );
}

function Block({ palette, title, children }: {
  palette: Palette; title: string; children: React.ReactNode;
}) {
  return (
    <View style={{ gap: 8 }}>
      <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                     color: palette.faint }}>{title.toUpperCase()}</Text>
      {children}
    </View>
  );
}

function Link({ palette, label, text, onPress, tone }: {
  palette: Palette; label: string; text: string; onPress: () => void; tone?: string;
}) {
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label}>
      <Text style={{ ...TYPE.micro, fontSize: 13, color: tone ?? palette.accent }}>
        {text}
      </Text>
    </Pressable>
  );
}


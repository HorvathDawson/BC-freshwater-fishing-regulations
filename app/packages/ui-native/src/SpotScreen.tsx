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
 * EVERYTHING HERE IS A RECORD, NOT A READING. The gauge figure and the weather were copied
 * in when the spot was made and are never recomputed, so nothing on this screen may imply
 * they are current. Each carries its own date, and a value filled in
 * after the fact says so.
 *
 * The trace panel is the same component the Conditions tab shows, fed from the spot's
 * frozen copy instead of a live query. A gauge's representativeness is the most misread
 * number in the app; it may not have two explanations.
 */
import { useState } from "react";
import { Image, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import { DRAFT_WORDS, isUntitled, needsRefresh, observedLine, PHOTO_WORDS, refreshReason, removePhotoLabel, spotHeadline,
  spotLabel, weatherTrend, type PhotoStore, type Spot, type SpotWeather, type WeatherSample } from "@app/data/spots";
import { leadUpView, useBackLayer, usePhotoUris, type LikeVisitView, type PhotoEdit } from "@app/ui";
import { LeadUpStrip } from "./LeadUpStrip";
import { LikeVisitStrip } from "./LikeVisitStrip";
import { DonorPanel } from "./DonorPanel";
import { GaugeTrace } from "./GaugeTrace";
import { percentileLabel } from "@app/core";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { Button } from "./Button";
import { Icon, IconText, type IconName } from "./icons";

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
  /** (T131) view: where back goes, in words (`@app/core` `spotBack`: the tab it is over, or "Back"). Absent: Spots. */
  back?: string;
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
  /** A save or delete that failed, said in full (P01); the screen stays open. */
  error?: string | null;
  /** The shell asks before dropping an unsaved edit (a tab change, P01); `onKeepEditing` answers "keep". */
  askToDiscard?: boolean;
  onKeepEditing?: () => void;
  /** S1: where the photos are drawn from (their references resolved). Absent -> a reference cannot be drawn. */
  photoStore?: Pick<PhotoStore, "uri">;
  /** S1, draft and edit: the photos being edited, with add and remove (`usePhotoEdit`). */
  photoEdit?: PhotoEdit;
  /**
   * T51, a saved spot only: its conditions then against today and the next four days (`useLikeVisit`). Absent or null:
   * no strip (a draft has no visit to compare with yet).
   */
  likeVisit?: LikeVisitView | null;
}

/**
 * A SPOT READS AS ONE COLUMN AT EVERY WIDTH (owner 2026-10-21: "desktop falls apart"). It is a record — a title, the
 * water's answer that day, the weather, the notes, the photographs — not a dashboard, so on a desktop it keeps a
 * reading measure and centres, with its header over the same column. Its donor panel stays one column inside it.
 */
const MEASURE = { width: "100%", maxWidth: 760, alignSelf: "center" } as const;

export function SpotScreen(p: SpotScreenProps) {
  const { spot, palette, mode = "view" } = p;
  const editable = mode === "draft" || mode === "edit";
  const [confirm, setConfirm] = useState<"delete" | "discard" | null>(null);

  const r = spot.reading;
  const w = spot.weather;
  // S1: the week before the visit, decided in @app/ui; and the photos — the edit's while editing, else the spot's
  const lead = leadUpView(spot.leadUp);
  const pe = editable ? p.photoEdit : undefined;
  const photos = pe ? pe.refs : spot.photos;
  const uris = usePhotoUris(p.photoStore, photos);

  // Edit with nothing changed is not a decision worth interrupting someone for.
  const leaveEdit = () => (p.dirty ? setConfirm("discard") : p.onDiscard?.());
  const shown = confirm ?? (p.askToDiscard ? "discard" : null);
  // "Keep it" / "Keep editing": nothing deleted, nothing dropped
  const keep = () => { setConfirm(null); if (!confirm) p.onKeepEditing?.(); };
  /*
   * BACK, ON A SPOT (app/HANDOFF-ui.md "Back, everywhere"): an open question is answered "keep"; an edit is left the
   * way Cancel leaves it — asking first when something changed, so no back ever drops an unsaved edit unasked; a spot
   * only viewed is left by the route (to the list). A draft's own steps are the capture's (SpotCapture).
   */
  useBackLayer(shown !== null || mode === "edit", () => (shown ? keep() : leaveEdit()));

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ flexDirection: "row", alignItems: "center",
                     justifyContent: "space-between",
                     paddingHorizontal: 18, paddingTop: 14, paddingBottom: 6, ...MEASURE }}>
        {mode === "edit" ? (
          <Link palette={palette} label="Cancel editing" text="Cancel" onPress={leaveEdit} />
        ) : p.onBack ? (
          <Link palette={palette} label="Back" icon="back"
                text={mode === "draft" ? "Back" : p.back ?? "Spots"} onPress={p.onBack} />
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

      {p.error ? (
        <Text accessibilityRole="alert" style={{ ...TYPE.small, color: palette.closed, paddingHorizontal: 18 }}>
          {p.error}
        </Text>
      ) : null}

      {shown && (
        <Confirm palette={palette}
                 kind={shown}
                 onCancel={keep}
                 onConfirm={() => {
                   setConfirm(null);
                   if (shown === "delete") p.onDelete?.();
                   else p.onDiscard?.();
                 }}
                 onSaveInstead={shown === "discard" ? p.onSave : undefined} />
      )}

      <ScrollView contentContainerStyle={{ padding: 18, paddingTop: 6, gap: 22,
                                           paddingBottom: 40, ...MEASURE }}>
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
          {/* The visit, in the reader's own time (P12b) — the day they were at the water. */}
          <Text testID="spot-when" style={{ ...TYPE.small, color: palette.sub }}>
            {spotHeadline(spot)}
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
                         borderRadius: palette.r.box, backgroundColor: palette.tint }}>
            <Text style={{ ...TYPE.small, flex: 1, color: palette.sub }}>
              {refreshReason(spot)}
            </Text>
            <Pressable onPress={p.onRefresh} disabled={p.refreshing}
                       accessibilityRole="button" accessibilityLabel="Refresh this spot"
                       style={{ borderRadius: palette.r.pill, paddingVertical: 8, paddingHorizontal: 14,
                                backgroundColor: palette.card, borderWidth: 1,
                                borderColor: palette.line2, opacity: p.refreshing ? 0.5 : 1 }}>
              <Text style={{ ...TYPE.micro, fontSize: 12.5, color: palette.accent }}>
                {p.refreshing ? "…" : "Refresh"}
              </Text>
            </Pressable>
          </View>
        )}

        <Block palette={palette} title="What the water was doing">
          {/*
            THE SAME COMPONENT THE CONDITIONS SCREEN USES, on frozen data.
            
            This was a hand-rolled copy: a big number, a unit, and a percentile spelled
            "p4" where the map said "p4th" and the sheet said "p0.4th". It also showed one
            station where the app had shown a panel of four, so a spot recorded what the
            app never said.
            
            `spot.panel` is the answer `answerFrom` produced at capture time, stored
            verbatim. `DonorPanel` renders it exactly as it rendered it that day — the
            range, the words, the donors and what each contributed — with no map, because
            a saved spot carries no tiles, and no horizon chips, because you cannot
            re-forecast the past.
          */}
          {spot.panel ? (
            <>
              <DonorPanel palette={palette} value={spot.panel}
                          day={new Date(spot.visitedAt)} />
              {r && <Meta palette={palette} at={r.at} backfilled={r.backfilled}
                          station={r.station} />}
            </>
          ) : r?.discharge != null ? (
            // A spot saved before the estimate was frozen. Its own reading is all there is,
            // and it is still a true record of that day.
            <>
              <View style={{ flexDirection: "row", alignItems: "baseline", gap: 8,
                             flexWrap: "wrap" }}>
                <Text style={{ ...TYPE.figureBig, fontSize: 32, color: palette.live }}>
                  {r.discharge}
                </Text>
                <Text style={{ ...TYPE.figure, color: palette.sub }}>m³/s</Text>
                {r.percentile != null && (
                  <Text style={{ ...TYPE.figure, color: palette.sub }}>
                    · {percentileLabel(r.percentile)} against the record
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
                              paddingVertical: 12, paddingHorizontal: 14, borderRadius: palette.r.box,
                              borderWidth: 1, borderColor: palette.line2 }}>
            <Text style={{ ...TYPE.bodyStrong, flex: 1, color: palette.ink }}>
              Show on map
            </Text>
            <Icon name="next" colour={palette.accent} size={18} />
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


        {lead && (
          <Block palette={palette} title={lead.heading}>
            <LeadUpStrip view={lead} palette={palette} />
          </Block>
        )}

        {mode !== "draft" && p.likeVisit && (
          <Block palette={palette} title={p.likeVisit.heading}>
            <LikeVisitStrip view={p.likeVisit} palette={palette} />
          </Block>
        )}

        <Block palette={palette} title="Notes">
          {editable && p.onNotes ? (
            <TextInput value={p.notes ?? ""} onChangeText={p.onNotes} multiline
                       placeholder="What happened, what you used, what the water looked like"
                       placeholderTextColor={palette.faint} accessibilityLabel="Notes"
                       style={{ ...TYPE.body, color: palette.ink, minHeight: 96,
                                textAlignVertical: "top", borderWidth: 1,
                                borderColor: palette.line, borderRadius: palette.r.box, padding: 12 }} />
          ) : spot.notes.trim() !== "" ? (
            <Text style={{ ...TYPE.body, color: palette.ink }}>{spot.notes}</Text>
          ) : (
            <Text style={{ ...TYPE.small, color: palette.faint }}>None.</Text>
          )}
        </Block>

        {(photos.length > 0 || (pe && pe.sources.length > 0)) && (
          <Block palette={palette} title={PHOTO_WORDS.heading}>
            <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
              {photos.map((ref, i) => {
                const uri = uris.get(ref);
                return (
                  <View key={ref} style={{ width: 104, height: 104, borderRadius: palette.r.box,
                                           overflow: "hidden", backgroundColor: palette.tint,
                                           alignItems: "center", justifyContent: "center" }}>
                    {uri ? (
                      <Image source={{ uri }} accessibilityLabel=""
                             style={{ width: 104, height: 104 }} resizeMode="cover" />
                    ) : uri === null ? (
                      <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint, padding: 8,
                                     textAlign: "center" }}>{PHOTO_WORDS.missing}</Text>
                    ) : null}
                    {pe && (
                      <Pressable onPress={() => { void pe.remove(ref); }} accessibilityRole="button"
                                 accessibilityLabel={removePhotoLabel(i + 1)}
                                 style={{ position: "absolute", top: 4, right: 4, width: 28, height: 28,
                                          borderRadius: 14, alignItems: "center", justifyContent: "center",
                                          backgroundColor: palette.card }}>
                        <Icon name="close" colour={palette.ink} size={16} />
                      </Pressable>
                    )}
                  </View>
                );
              })}
            </View>
            {pe && pe.sources.length > 0 && (
              <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap" }}>
                {pe.sources.map((from) => (
                  <Button key={from} palette={palette} kind="ghost" grow={false}
                          label={from === "camera" ? PHOTO_WORDS.camera : PHOTO_WORDS.library}
                          onPress={pe.busy ? undefined : () => { void pe.add(from); }} />
                ))}
              </View>
            )}
            {pe?.busy ? <Text style={{ ...TYPE.small, color: palette.sub }}>{PHOTO_WORDS.adding}</Text> : null}
            {pe?.error ? (
              <Text accessibilityRole="alert" style={{ ...TYPE.small, color: palette.closed }}>{pe.error}</Text>
            ) : null}
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

/** What changed across the window, in words — `weatherTrend` in @app/data/spots says it, or says nothing. */
function Trend({ win, palette }: {
  win: NonNullable<SpotWeather["window"]>; palette: Palette;
}) {
  const said = weatherTrend(win);
  if (said === null) return null;
  return (
    <Text testID="spot-trend" style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
      {said}
    </Text>
  );
}

/**
 * A destructive or lossy step, asked before it happens. `draft`: leaving a spot being added that holds what the reader
 * put in (SpotCapture — a back or Cancel), in `DRAFT_WORDS`.
 */
export function Confirm({ palette, kind, onCancel, onConfirm, onSaveInstead }: {
  palette: Palette; kind: "delete" | "discard" | "draft";
  onCancel: () => void; onConfirm: () => void; onSaveInstead?: () => void;
}) {
  if (kind === "draft")
    return (
      <View accessibilityLabel={DRAFT_WORDS.label}
            style={{ marginBottom: 4, padding: 16, borderRadius: palette.r.box, borderWidth: 1,
                     borderColor: palette.line2, backgroundColor: palette.wash, gap: 12 }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{DRAFT_WORDS.title}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{DRAFT_WORDS.detail}</Text>
        <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap" }}>
          <Button palette={palette} kind="ghost" label={DRAFT_WORDS.keep} onPress={onCancel} />
          <Button palette={palette} kind="solid" label={DRAFT_WORDS.discard} onPress={onConfirm} />
        </View>
      </View>
    );
  const del = kind === "delete";
  return (
    <View accessibilityLabel={del ? "Confirm delete" : "Unsaved changes"}
          style={{ marginHorizontal: 18, marginBottom: 8, padding: 16, borderRadius: palette.r.box,
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
  return (
    <Text testID="spot-observed" style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
      {observedLine(at, backfilled, station)}
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

function Link({ palette, label, text, onPress, tone, icon }: {
  palette: Palette; label: string; text: string; onPress: () => void; tone?: string; icon?: IconName;
}) {
  const style = { ...TYPE.micro, fontSize: 13, color: tone ?? palette.accent };
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label}>
      {icon ? <IconText name={icon} text={text} style={style} /> : <Text style={style}>{text}</Text>}
    </Pressable>
  );
}


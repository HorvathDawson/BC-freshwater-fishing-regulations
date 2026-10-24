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
import { Pressable, ScrollView, Text, TextInput, View } from "react-native";
import { isUntitled, needsRefresh, refreshReason, spotHeadline, spotLabel, type PhotoStore, type Spot } from "@app/data/spots";
import { leadUpView, useBackLayer, usePhotoUris, type DayFlowView, type LikeVisitView, type PhotoEdit } from "@app/ui";
import { LeadUpStrip } from "./LeadUpStrip";
import { LikeVisitStrip } from "./LikeVisitStrip";
import { GaugeTrace } from "./GaugeTrace";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { Button } from "./Button";
import { Icon } from "./icons";
import { Confirm } from "./spot/Confirm";
import { DayFlowBlock } from "./spot/DayFlow";
import { Block, Link } from "./spot/parts";
import { WaterRecord, WeatherRecord } from "./spot/Record";
import { SpotPhotos } from "./spot/SpotPhotos";


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
  /**
   * T170, a saved spot only: the visit day's own flow from the day's record, and "Get the flow for that day"
   * (`useDayFlow`). Absent, or a null view: nothing to say (no station, or the reading is the day's own).
   */
  dayFlow?: { view: DayFlowView | null; onAsk: (() => void) | null };
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

  // S1: the week before the visit, decided in @app/ui; and the photos — the edit's while editing, else the spot's
  const lead = leadUpView(spot.leadUp, spot.reading);
  const pe = editable ? p.photoEdit : undefined;
  const photos = pe ? pe.refs : spot.photos;
  const uris = usePhotoUris(p.photoStore, photos);
  const df = p.dayFlow?.view ?? null;

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

        <WaterRecord spot={spot} palette={palette} />

        {/* the trip day's own flow; in the draft too, where a spot added for an earlier day shows what it read */}
        {df && (mode !== "draft" || df.line || df.status) && (
          <DayFlowBlock df={df} onAsk={p.dayFlow?.onAsk} palette={palette} />
        )}

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

        <WeatherRecord spot={spot} palette={palette} />

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
          <SpotPhotos photos={photos} uris={uris} pe={pe} palette={palette} />
        )}
      </ScrollView>

      {editable && p.onSave && (
        // ONE WAY OUT (T60, one surface per action): the way back / out of the edit is the ‹ / Cancel at the top, as on
        // every screen; this row had a second Back / Cancel beside Save, the same action offered twice
        <View style={{ flexDirection: "row", gap: 10, padding: 16, borderTopWidth: 1,
                       borderTopColor: palette.line }}>
          <Button palette={palette}
                  label={mode === "draft" ? "Save spot" : "Save changes"} onPress={p.onSave} />
        </View>
      )}
    </View>
  );
}

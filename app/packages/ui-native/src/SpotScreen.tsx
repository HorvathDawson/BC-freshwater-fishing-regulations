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
import { ErrorState } from "./ErrorState";
import { useState } from "react";
import { Pressable, ScrollView, Text, TextInput, View, useWindowDimensions } from "react-native";
import { isUntitled, needsRefresh, refreshReason, spotHeadline, spotLabel, type PhotoStore, type Spot } from "@app/data/spots";
import { SPOT_WORDS as W } from "@app/core";
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
import { Block, Link, Section } from "./spot/parts";
import { WIDE_PANEL } from "./DonorPanel";
import { WaterRecord, WeatherRecord } from "./spot/Record";
import { SpotPhotos } from "./spot/SpotPhotos";
import { hitProps } from "./regs/hit";


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
 * THREE HEADED BLOCKS (T284, owner 2026-10-29): That day (the record: water, weather, the week before), What you added
 * (notes, photographs), The next few days (how alike each looks to that day, and why). A phone reads them in that order
 * in ONE column; from `WIDE_PANEL` up the first two stack in the left column and the third sits beside them, so the
 * comparison is on screen with the day it compares to. The page keeps a reading measure and centres, its header over
 * the same width. The donor panel measures its own column and stays one column inside it.
 */
const MEASURE = { width: "100%", maxWidth: 760, alignSelf: "center" } as const;
const MEASURE_WIDE = { width: "100%", maxWidth: 1120, alignSelf: "center" } as const;

export function SpotScreen(p: SpotScreenProps) {
  const { spot, palette, mode = "view" } = p;
  const wide = useWindowDimensions().width >= WIDE_PANEL;
  const MEASURE_NOW = wide ? MEASURE_WIDE : MEASURE;
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

  const thatDay = (
    <Section palette={palette} title={W.thatDay} testID="spot-that-day">
    <WaterRecord spot={spot} palette={palette} />

    {/* the trip day's own flow; in the draft too, where a spot added for an earlier day shows what it read */}
    {df && (mode !== "draft" || df.line || df.status) && (
      <DayFlowBlock df={df} onAsk={p.dayFlow?.onAsk} palette={palette} />
    )}

    {spot.trace && <GaugeTrace trace={spot.trace} palette={palette} />}

    <WeatherRecord spot={spot} palette={palette} />

    {lead && (
      <Block palette={palette} title={lead.heading}>
        <LeadUpStrip view={lead} palette={palette} />
      </Block>
    )}

    </Section>
  );
  const added = (
    <Section palette={palette} title={W.added} testID="spot-added">
    <Block palette={palette} title={W.notes}>
      {editable && p.onNotes ? (
        <TextInput value={p.notes ?? ""} onChangeText={p.onNotes} multiline
                   placeholder={W.notesHint}
                   placeholderTextColor={palette.faint} accessibilityLabel={W.notes}
                   style={{ ...TYPE.body, color: palette.ink, minHeight: 96,
                            textAlignVertical: "top", borderWidth: 1,
                            borderColor: palette.line, borderRadius: palette.r.box, padding: 12 }} />
      ) : spot.notes.trim() !== "" ? (
        <Text style={{ ...TYPE.body, color: palette.ink }}>{spot.notes}</Text>
      ) : (
        <Text style={{ ...TYPE.small, color: palette.faint }}>{W.noNotes}</Text>
      )}
    </Block>

    {(photos.length > 0 || (pe && pe.sources.length > 0)) && (
      <SpotPhotos photos={photos} uris={uris} pe={pe} palette={palette} />
    )}
    </Section>
  );
  const nextDays = mode !== "draft" && p.likeVisit && (
      <Section palette={palette} title={W.nextDays} testID="spot-next-days">
        <LikeVisitStrip view={p.likeVisit} palette={palette} />
      </Section>
  );

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ flexDirection: "row", alignItems: "center",
                     justifyContent: "space-between",
                     paddingHorizontal: 18, paddingTop: 14, paddingBottom: 6, ...MEASURE_NOW }}>
        {mode === "edit" ? (
          <Link palette={palette} label={W.cancelEditing} text={W.cancel} onPress={leaveEdit} />
        ) : p.onBack ? (
          <Link palette={palette} label={W.back} icon="back"
                text={mode === "draft" ? W.back : p.back ?? W.spots} onPress={p.onBack} />
        ) : <View />}

        <View style={{ flexDirection: "row", gap: 18 }}>
          {mode === "view" && p.onEdit && (
            <Link palette={palette} label={W.editSpot} text={W.edit} onPress={p.onEdit} />
          )}
          {mode === "view" && p.onDelete && (
            <Link palette={palette} label={W.deleteSpot} text={W.delete} tone={palette.closed}
                  onPress={() => setConfirm("delete")} />
          )}
        </View>
      </View>

      {p.error ? (
        <ErrorState palette={palette} message={p.error} style={{ paddingHorizontal: 18 }} />
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
                                           paddingBottom: 40, ...MEASURE_NOW }}>
        <View style={{ gap: 4 }}>
          {editable && p.onTitle ? (
            <TextInput value={p.title ?? ""} onChangeText={p.onTitle}
                       placeholder={W.name} placeholderTextColor={palette.faint}
                       accessibilityLabel={W.name}
                       style={{ ...TYPE.title, color: palette.ink }} />
          ) : (
            <Text style={{ ...TYPE.title, color: palette.ink }}>{spotLabel(spot)}</Text>
          )}
          {/* A title is optional, so the prompt is an offer rather than a warning — but it
              is worth offering, because the list shows this and a coordinate is hard to
              pick out of twenty. */}
          {!editable && isUntitled(spot) && p.onEdit && (
            <Pressable {...hitProps({ w: 80, h: 44 })} onPress={p.onEdit} accessibilityRole="button"
                       accessibilityLabel={W.addTitle} style={{ minHeight: 44, justifyContent: "center" }}>
              <Text style={{ ...TYPE.small, color: palette.accent }}>{W.addTitle}</Text>
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
            <Button palette={palette} kind="small" label={W.refresh} a11yLabel={W.refreshSpot} onPress={p.onRefresh} busy={p.refreshing} />
          </View>
        )}

        {p.onShowOnMap && (
          <Pressable {...hitProps({ w: 120, h: 44 })} onPress={p.onShowOnMap} accessibilityRole="button"
                     accessibilityLabel={W.showOnMap}
                     style={{ flexDirection: "row", alignItems: "center", gap: 10,
                              paddingVertical: 12, paddingHorizontal: 14, borderRadius: palette.r.box,
                              borderWidth: 1, borderColor: palette.line2 }}>
            <Text style={{ ...TYPE.bodyStrong, flex: 1, color: palette.ink }}>
              {W.showOnMap}
            </Text>
            <Icon name="next" colour={palette.accent} size={18} />
          </Pressable>
        )}

        {wide ? (
          <View style={{ flexDirection: "row", gap: 36, alignItems: "flex-start" }}>
            <View style={{ flex: 1, minWidth: 0, gap: 28 }}>{thatDay}{added}</View>
            <View style={{ flex: 1, minWidth: 0 }}>{nextDays}</View>
          </View>
        ) : (<>{thatDay}{added}{nextDays}</>)}
      </ScrollView>

      {editable && p.onSave && (
        // ONE WAY OUT (T60, one surface per action): the way back / out of the edit is the ‹ / Cancel at the top, as on
        // every screen; this row had a second Back / Cancel beside Save, the same action offered twice
        <View style={{ flexDirection: "row", gap: 10, padding: 16, borderTopWidth: 1,
                       borderTopColor: palette.line }}>
          <Button palette={palette}
                  label={mode === "draft" ? W.save : W.saveChanges} onPress={p.onSave} />
        </View>
      )}
    </View>
  );
}

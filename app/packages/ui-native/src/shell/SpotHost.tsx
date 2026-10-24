/**
 * AN OPEN SPOT, over the map it was opened from (or, T131, over a water): its screen, its edit and the tab bar under it
 * (T62 3.4: out of the Shell, unchanged). The edit draft is held by the Shell (`useSpotEdit`), so leaving the screen
 * cannot strand half an edit inside an unmounted component.
 */
import { spotBack } from "@app/core";
import { useState, type MutableRefObject } from "react";
import { View } from "react-native";
import { spotSaveFailed, useDayFlow, useLikeVisit, useOnLeave, usePhotoEdit, type SpotPhotos } from "@app/ui";
import type { Spot } from "@app/data/spots";
import type { Camera } from "@app/map";
import { SpotScreen } from "../SpotScreen";
import { TABS, TabBar, type TabKey } from "../TabBar";
import type { Palette } from "../theme";
import type { ShellNav } from "./useShellNav";

/** The open spot's visit-day flow block and its action (`useDayFlow`). */
type DayFlowBlock = ReturnType<typeof useDayFlow>;

/** The spot edit's state, held across the spot screen's life. */
export function useSpotEdit(photos: SpotPhotos | undefined, openSpot: string | null) {
  // Non-null only while editing. The draft lives here rather than in SpotScreen so that
  // leaving the screen cannot strand half an edit inside an unmounted component.
  const [edit, setEdit] = useState<{ title: string; notes: string } | null>(null);
  // S1: the photos of the spot being edited; files added in an edit that is not saved are deleted with it
  const photoEdit = usePhotoEdit(photos);
  // P01: a failed save or delete, said on the spot's screen; and a tab change asked for while an edit is unsaved
  const [spotError, setSpotError] = useState<string | null>(null);
  const [pendingTab, setPendingTab] = useState<TabKey | null>(null);
  // G1: however the spot is left (‹, Android's back, the browser's back, a tab), its edit ends with it — else the next
  // spot opens in edit mode with this one's text and Save writes it there
  useOnLeave(openSpot, () => { if (edit) void photoEdit.discard(); setEdit(null); setSpotError(null); setPendingTab(null); });
  return { edit, setEdit, photoEdit, spotError, setSpotError, pendingTab, setPendingTab };
}

export function SpotHost({ viewing, spotEdit, nav, palette, camera, like, dayFlow, photos, onSaveSpot, onDeleteSpot,
                           onRefreshSpots, refreshing, show }: {
  viewing: Spot; spotEdit: ReturnType<typeof useSpotEdit>; nav: ShellNav; palette: Palette;
  camera: MutableRefObject<Camera>;
  like: ReturnType<typeof useLikeVisit>; dayFlow: DayFlowBlock;
  photos?: SpotPhotos;
  onSaveSpot?: (spot: Spot) => void | Promise<void>;
  onDeleteSpot?: (id: string) => void | Promise<void>;
  onRefreshSpots?: () => void | Promise<void>;
  refreshing?: boolean;
  /** THE ONE SHOW (the rail's): the Map tab, framed on the spot with its pin. */
  show: (s: Spot) => void;
}) {
  const { edit, setEdit, photoEdit, spotError, setSpotError, pendingTab, setPendingTab } = spotEdit;
  const { route, tab, back, toTab } = nav;
  const dirty = edit !== null
    && (edit.title !== viewing.title || edit.notes !== viewing.notes
        || photoEdit.refs.join("\n") !== viewing.photos.join("\n"));
  // an unsaved edit's added photos go with it; after a save there are none left to delete
  const endEdit = () => { if (edit) void photoEdit.discard(); setEdit(null); };
  const close = () => { endEdit(); setSpotError(null); setPendingTab(null); back(); };
  const goTab = (k: TabKey) => { endEdit(); setSpotError(null); setPendingTab(null); toTab(k); };
  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ flex: 1 }}>
        <SpotScreen spot={viewing} palette={palette}
                    mode={edit ? "edit" : "view"} likeVisit={like} dayFlow={dayFlow}
                    photoStore={photos?.store} photoEdit={edit ? photoEdit : undefined}
                    title={edit?.title} notes={edit?.notes}
                    onTitle={edit ? (t) => setEdit({ ...edit, title: t }) : undefined}
                    onNotes={edit ? (n) => setEdit({ ...edit, notes: n }) : undefined}
                    dirty={dirty}
                    onBack={close}
                    // (T131) the ‹ names where back goes: the tab the spot is over, or Back over a water
                    back={spotBack(route.mode === "water" ? null : TABS.find((t) => t.k === tab)!.t)}
                    onEdit={onSaveSpot && (() => {
                      setEdit({ title: viewing.title, notes: viewing.notes });
                      photoEdit.begin(viewing.id, viewing.photos);
                    })}
                    onDiscard={() => { endEdit(); if (pendingTab) goTab(pendingTab); }}
                    error={spotError}
                    askToDiscard={pendingTab !== null}
                    onKeepEditing={() => setPendingTab(null)}
                    onSave={edit && onSaveSpot ? async () => {
                      try {
                        await onSaveSpot({ ...viewing, title: edit.title || viewing.title,
                                           notes: edit.notes, photos: [...photoEdit.refs],
                                           updatedAt: Date.now() });
                        photoEdit.committed();
                        setEdit(null);
                        setSpotError(null);
                        if (pendingTab) goTab(pendingTab);
                      } catch (e) {
                        setPendingTab(null);
                        setSpotError(spotSaveFailed(e));    // the edit stays open, nothing dropped
                      }
                    } : undefined}
                    onDelete={onDeleteSpot && (async () => {
                      try { await onDeleteSpot(viewing.id); close(); }
                      catch (e) { setSpotError(spotSaveFailed(e)); }
                    })}
                    onShowOnMap={() => {
                      // THE ONE SHOW (the Spots tab's row does the same): the Map tab, framed on the spot with its
                      // pin. The camera is a ref, so setting it first means the map mounts there already.
                      camera.current = { lon: viewing.lon, lat: viewing.lat, zoom: 14 };
                      endEdit(); setSpotError(null); setPendingTab(null);
                      show(viewing);
                    }}
                    onRefresh={onRefreshSpots && (() => onRefreshSpots())}
                    refreshing={refreshing} />
      </View>
      <TabBar active={tab} palette={palette}
              // an unsaved edit is never dropped by a tab change without asking (P01)
              onChange={(k) => (dirty ? setPendingTab(k) : goTab(k))} />
    </View>
  );
}

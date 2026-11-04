/**
 * AN OPEN SPOT, over the map it was opened from (or, T131, over a water): its screen, its edit and the tab bar under it
 * (T62 3.4: out of the Shell, unchanged). The edit draft is held by the Shell (`useSpotEdit`), so leaving the screen
 * cannot strand half an edit inside an unmounted component.
 */
import { SPOT_WORDS, spotBack } from "@app/core";
import type { MutableRefObject } from "react";
import { useState } from "react";
import { View } from "react-native";
import { nextRating, spotSaveFailed, useAddVisit, useBackLayer, useDayFlow, useLikeVisit, visitsView,
         reportFailure, type AddVisitOptions, type SpotPhotos } from "@app/ui";
import { photoOwner, rateVisit, removeVisit, visitSpot, withVisit, type Spot } from "@app/data/spots";
import { VISIT_WORDS } from "@app/core";
import { Busy, WhenScreen } from "../SpotCapture";
import type { Camera, TileEndpoints } from "@app/map";
import { ParkingPick } from "../spot/ParkingPick";
import type { Locate } from "@app/ui";
import { SpotScreen } from "../SpotScreen";
import { TABS, TabBar, type TabKey } from "../TabBar";
import type { Palette } from "../theme";
import type { ShellNav } from "./useShellNav";
import type { useSpotEdit } from "./useSpotEdit";
import { openLink } from "../openLink";

/** The open spot's visit-day flow block and its action (`useDayFlow`). */
type DayFlowBlock = ReturnType<typeof useDayFlow>;

export function SpotHost({ viewing, spotEdit, nav, palette, camera, like, dayFlow, photos, onSaveSpot, onDeleteSpot,
                           onRefreshSpots, refreshing, show, visitSources, tiles, theme, locate }: {
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
  /** (T294) what "Add a visit" reads the water with; absent: no Add a visit. */
  visitSources?: AddVisitOptions | null;
  /** (T310) the parking picker's map, and the phone's location for "Use my location" */
  tiles?: TileEndpoints; theme?: string; locate?: Locate | null;
}) {
  const { edit, setEdit, photoEdit, spotError, setSpotError, pendingTab, setPendingTab, shownVisit, setShownVisit,
          addingVisit, setAddingVisit } = spotEdit;
  const { route, tab, back, toTab } = nav;
  // (T294) the page shows ONE visit: the spot read as it (its record, notes, photos, rating); the spot's place and name
  // are every visit's. An edit or a rating is written back to that visit alone (`withVisit`).
  const shown = visitSpot(viewing, shownVisit);
  const visits = visitsView(viewing, shown.visitId, Date.now());
  const add = useAddVisit(visitSources ?? null);
  const [pickingParking, setPickingParking] = useState<{ lat: number; lon: number } | null | false>(false);
  const [parkingNotice, setParkingNotice] = useState<string | null>(null);
  const same = (a?: { lat: number; lon: number } | null, b?: { lat: number; lon: number } | null) =>
    (a?.lat ?? null) === (b?.lat ?? null) && (a?.lon ?? null) === (b?.lon ?? null);
  const dirty = edit !== null
    && (edit.title !== viewing.title || edit.notes !== shown.notes
        || photoEdit.refs.join("\n") !== shown.photos.join("\n")
        || !same(edit.parking, viewing.parking));
  const saveOr = async (next: Spot) => {
    try { await onSaveSpot!(next); setSpotError(null); }
    catch (e) { reportFailure(e); setSpotError(spotSaveFailed(e)); }
  };
  // (T294) a back on "Add a visit" goes back to the spot, as its ‹ does
  useBackLayer(addingVisit, () => setAddingVisit(false));
  if (pickingParking !== false && edit && tiles && theme) {
    return (
      <ParkingPick palette={palette} tiles={tiles} theme={theme} point={pickingParking}
                   camera={edit.parking ? { ...edit.parking, zoom: 15 } : { lat: viewing.lat, lon: viewing.lon, zoom: 15 }}
                   onPoint={setPickingParking} onCancel={() => setPickingParking(false)}
                   onUse={() => { if (pickingParking) setEdit({ ...edit, parking: pickingParking }); setPickingParking(false); }} />
    );
  }
  if (addingVisit && visitSources && onSaveSpot) {
    if (add.busy) return <Busy palette={palette} label={VISIT_WORDS.reading} />;
    return (
      <WhenScreen palette={palette} steps={add.steps} onShiftDay={add.shiftDay} onDay={add.toDay} onHour={add.setHour}
                  water={viewing.waterName} error={add.error} title={VISIT_WORDS.addTitle}
                  onBack={() => setAddingVisit(false)}
                  onNext={async () => {
                    const id = await add.record(viewing, onSaveSpot);
                    if (id) { setShownVisit(id); setAddingVisit(false); }
                  }} />
    );
  }
  // an unsaved edit's added photos go with it; after a save there are none left to delete
  const endEdit = () => { if (edit) void photoEdit.discard(); setEdit(null); };
  const close = () => { endEdit(); setSpotError(null); setPendingTab(null); back(); };
  const goTab = (k: TabKey) => { endEdit(); setSpotError(null); setPendingTab(null); toTab(k); };
  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ flex: 1 }}>
        <SpotScreen spot={shown} palette={palette}
                    visits={visits}
                    onPickVisit={(id) => { if (!edit) setShownVisit(id); }}
                    onRate={onSaveSpot && ((n) => void saveOr(rateVisit(viewing, shown.visitId, nextRating(visits.rate.value, n))))}
                    onAddVisit={visitSources && onSaveSpot ? () => { add.reset(); setAddingVisit(true); } : undefined}
                    onRemoveVisit={onSaveSpot && visits.canRemove ? async () => {
                      await saveOr(removeVisit(viewing, shown.visitId));
                      setShownVisit(null);
                    } : undefined}
                    mode={edit ? "edit" : "view"} likeVisit={like} dayFlow={dayFlow}
                    photoStore={photos?.store} photoEdit={edit ? photoEdit : undefined}
                    title={edit?.title} notes={edit?.notes}
                    parking={edit ? edit.parking : viewing.parking ?? null} parkingNotice={parkingNotice}
                    onDirections={() => { const q = viewing.parking; if (q) void openLink(`https://www.google.com/maps/dir/?api=1&destination=${q.lat},${q.lon}`); }}
                    onParkingPick={edit && tiles && theme ? () => setPickingParking(edit.parking ?? null) : undefined}
                    onParkingHere={edit && locate ? () => {
                      setParkingNotice(null);
                      locate.here().then((f) => setEdit({ ...edit, parking: { lat: f.lat, lon: f.lon } }),
                                         () => setParkingNotice(SPOT_WORDS.parkingNoFix));
                    } : undefined}
                    onParkingRemove={edit ? () => setEdit({ ...edit, parking: null }) : undefined}
                    onTitle={edit ? (t) => setEdit({ ...edit, title: t }) : undefined}
                    onNotes={edit ? (n) => setEdit({ ...edit, notes: n }) : undefined}
                    dirty={dirty}
                    onBack={close}
                    // (T131) the ‹ names where back goes: the tab the spot is over, or Back over a water
                    back={spotBack(route.mode === "water" ? null : TABS.find((t) => t.k === tab)!.t)}
                    onEdit={onSaveSpot && (() => {
                      setEdit({ title: viewing.title, notes: shown.notes, parking: viewing.parking ?? null });
                      setParkingNotice(null);
                      // a later visit's new photos are named after the visit, the first's after the spot (as always)
                      photoEdit.begin(photoOwner(viewing, shown.visitId), shown.photos);
                    })}
                    onDiscard={() => { endEdit(); if (pendingTab) goTab(pendingTab); }}
                    error={spotError}
                    askToDiscard={pendingTab !== null}
                    onKeepEditing={() => setPendingTab(null)}
                    onSave={edit && onSaveSpot ? async () => {
                      try {
                        const { parking: _was, ...rest } = viewing;
                        await onSaveSpot(withVisit({ ...rest, ...(edit.parking ? { parking: edit.parking } : {}),
                                                     title: edit.title || viewing.title, updatedAt: Date.now() },
                                                   shown.visitId,
                                                   { ...shown, notes: edit.notes, photos: [...photoEdit.refs] }));
                        photoEdit.committed();
                        setEdit(null);
                        setSpotError(null);
                        if (pendingTab) goTab(pendingTab);
                      } catch (e) {
                        setPendingTab(null);
                        reportFailure(e);
                        setSpotError(spotSaveFailed(e));    // the edit stays open, nothing dropped
                      }
                    } : undefined}
                    onDelete={onDeleteSpot && (async () => {
                      try { await onDeleteSpot(viewing.id); close(); }
                      catch (e) { reportFailure(e); setSpotError(spotSaveFailed(e)); }
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

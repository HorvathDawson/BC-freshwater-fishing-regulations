# (T749 TRIAL) R8 keep rules for Can I Fish This?, appended to android/app/proguard-rules.pro by plugins/with-no-minify.js
# ONLY when CANIFISHTHIS_R8=1 at prebuild. Each rule names what reaches the kept code by name (reflection, JNI, a class
# name compared as a string) and what breaks without it. The libraries' own consumer rules (expo-modules-core, expo,
# expo-background-task, expo-task-manager, expo-notifications, expo-location, react-native-svg, react-android, the
# MapLibre AAR, pdfiumandroid, WorkManager, Room, Gson, OkHttp) still apply; these cover what they miss.
# AGP 8+ runs R8 in FULL mode: a `-keep class X` keeps X's name but NOT its no-argument constructor, which is the T250 crash.
#
# Resource shrinking (android.enableShrinkResourcesInReleaseBuilds) stays OFF: React Native resolves the JS bundle's images,
# the notification icon and the splash by resource NAME (getIdentifier), which the shrinker cannot see from the dex.

# 1. THE T250 CRASH. WorkManager (expo-background-task) opens its Room database at start (androidx.startup's
#    InitializationProvider -> WorkManagerInitializer -> WorkDatabase). Room makes `WorkDatabase_Impl` by reflection
#    (Class.forName(...).newInstance()). Room 2.5's own rule `-keep class * extends androidx.room.RoomDatabase` keeps the
#    class but, in R8 full mode, not its constructor: newInstance() throws InstantiationException, which Room reports as
#    "Failed to create an instance of class androidx.work.impl.WorkDatabase" and the app dies before the first frame.
-keep class * extends androidx.room.RoomDatabase { <init>(); }

# 2. MemoryTrim (modules/memory-trim, T360) finds MapLibre's map views by comparing class NAMES with
#    "org.maplibre.android.maps.MapView" and calls `onLowMemory` by name (getMethod). Renamed: no map is told to drop its
#    tiles on a trim (silent: the trim event says `maps: 0`).
-keep class org.maplibre.android.maps.MapView { public void onLowMemory(); }

# 3. MemoryTrim's `purge()` is a JNI method: the C++ symbol is Java_ca_canifishthis_memorytrim_MemoryTrimModule_purge, so
#    the class and method names are part of the link. The default proguard-android-optimize.txt already keeps native
#    method names with their classes; pinned here so a template change cannot drop it (UnsatisfiedLinkError on a trim).
-keepclasseswithmembernames class ca.canifishthis.memorytrim.MemoryTrimModule { native <methods>; }

# 4. maplibre-react-native's MLRNMapView installs a surface-view detach guard by reflection on MapLibreSurfaceView's
#    private fields `detachedListener` and `renderThread` (getDeclaredField). Renamed: the reflection fails, is caught and
#    logged, and the guard against the render-thread crash on detach is silently not installed.
-keepclassmembers class org.maplibre.android.maps.renderer.surfaceview.MapLibreSurfaceView {
  *** detachedListener;
  *** renderThread;
}

# 5. react-native-gesture-handler looks up com.facebook.react.uimanager.HasChildPressedStateDelay and its setter by name
#    (Class.forName + getMethod) to keep a scroll view's press-state delay. Renamed: caught, and a pressed row inside a
#    scroll view highlights at once on a scroll start (silent behaviour change).
-keep interface com.facebook.react.uimanager.HasChildPressedStateDelay { *; }

# 6. React Native (CoreReactPackage) and gesture-handler look up `<Package>$$ReactModuleInfoProvider` by name and, when it
#    is absent, read @ReactModule from the module class at run time (getAnnotation(...)!!: a stripped annotation is a
#    NullPointerException at start). These two rules are in React Native's own reactnative.pro, which the react-android
#    AAR does not ship as a consumer rule.
-keep class **$$ReactModuleInfoProvider
-keep @interface com.facebook.react.module.annotations.ReactModule { *; }

# 7. Readable native stack traces: keep line numbers (R8's mapping.txt, kept beside the build, maps names back).
-keepattributes SourceFile,LineNumberTable

# (T749, T1003) R8 keep rules for Can I Fish This?, appended to android/app/proguard-rules.pro by plugins/with-r8.js for
# every release build (R8 is always on since T1003). Each rule names what reaches the kept code by name (reflection, JNI, a class
# name compared as a string) and what breaks without it. The libraries' own consumer rules (expo-modules-core, expo,
# expo-location, react-native-svg, react-android, the MapLibre AAR, WorkManager, Room, Gson, OkHttp) still apply; these
# cover what they miss. (T1027) expo-notifications, expo-task-manager, expo-background-task and pdfiumandroid are the
# exception: R8 skips their own rules (whole-package keeps) and rules 8-10 below restate what they need.
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

# ---- (T1027) LIBRARIES WHOSE OWN KEEP RULES R8 DOES NOT READ, restated narrowly ----
# plugins/with-r8.js lists them (IGNORED_RULES) and writes AGP's `optimization { keepRules { ignoreFrom(...) } }` into
# the release build type, so R8 skips that library's consumer rules (each was one `-keep class <its package>.** { *; }`,
# which froze every class and member: no shrinking, optimization or renaming of the whole library). What the library
# reaches by name (reflection, JNI, a class name stored on the phone or in an Intent the system keeps) is restated here,
# each with its reason; everything else in it is shrunk, optimized and renamed like our own code. Only an EXTERNAL
# dependency (an AAR) can be dropped this way; a library built from node_modules (expo-modules-core,
# react-native-svg) keeps its rules (AGP's ignoreFrom does not filter project dependencies, measured 2026-11-24).
# A library update can need a new name: its walk is in the T1027 ticket; the floors in r8-report.mjs catch a
# library whose coordinates moved (its broad rule would come back and the scores fall).

# 8. expo-notifications (was `-keep class expo.modules.notifications.** {*;}`, 279 classes, 2,028 members).
#    a. Its Parcelable classes (Notification, NotificationRequest, NotificationResponse, triggers, ...) go into the
#       PendingIntent of every notification it raises; a Parcel holds the class NAME (writeParcelable), and Android keeps
#       that PendingIntent across an app update: renamed differently by the next build, a tap on a notification raised
#       before the update could not be read back. Names only: Parcel reads its fields by position (CREATOR is AGP's
#       default rule).
-keepnames class expo.modules.notifications.** implements android.os.Parcelable
#    b. Its Serializable classes (NotificationRequest, NotificationContent, the triggers, categories, actions) are
#       stored in SharedPreferences as Java serialization (Base64Serialization: scheduled requests, categories), which
#       reads class names, field NAMES and serialVersionUID back, also after an update. Kept: the names, every
#       instance field (not removed, not renamed, and the classes of their types named) and the serialization hooks.
-keepnames class expo.modules.notifications.** implements java.io.Serializable
-keepclassmembers,includedescriptorclasses class expo.modules.notifications.** implements java.io.Serializable {
  static final long serialVersionUID;
  !static !transient <fields>;
  private void writeObject(java.io.ObjectOutputStream);
  private void readObject(java.io.ObjectInputStream);
  java.lang.Object writeReplace();
  java.lang.Object readResolve();
}
#    Not restated (expo-modules-core's own rules hold them for every module): its Expo modules, Records (@Field, read by
#    reflection) and Enumerable enums; its manifest components (NotificationsService, NotificationForwarderActivity,
#    ExpoFirebaseMessagingService) are kept by AGP's aapt rules, whose names its request codes hash.

# 9. expo-task-manager + expo-background-task (were `-keep class expo.modules.taskmanager.** {*;}` and
#    `-keep class expo.modules.backgroundtask.** { *; }`, 75 classes, 490 members). TaskService stores each task's
#    consumer CLASS NAME in SharedPreferences and, at every start, boot and app update (restoreTasks), loads it with
#    Class.forName, reads its static `VERSION` by getDeclaredField and makes it with the (Context,
#    TaskManagerUtilsInterface) constructor by reflection. Renamed: the background check would not come back after the
#    next update. (expo-location's consumers keep their own rule, `expo.modules.location.taskConsumers.**`.)
#    WorkManager's BackgroundTaskWork is held by WorkManager's own rule (a ListenableWorker's constructor and name).
-keep class * implements expo.modules.interfaces.taskManager.TaskConsumerInterface {
  <init>(android.content.Context, expo.modules.interfaces.taskManager.TaskManagerUtilsInterface);
  static int VERSION;
}

# 10. pdfiumandroid (react-native-pdf's renderer; was `-keep class io.legere.pdfiumandroid.** { *; }` and two more,
#     110 classes, 1,320 members). libpdfiumandroid.so reaches Java through JNI. Its native methods bind by class and
#     method name, which AGP's default `-keepclasseswithmembernames class * { native <methods>; }` already keeps (the
#     classes it looks up with FindClass, PdfiumCore, PdfDocument, PdfPage, PdfTextPage, PdfPageLink and FindResult, all
#     hold native methods). What the C++ calls or reads by name, from the strings in the .so:
#     - ThrowNew(PdfPasswordException): the class name and its (String) constructor (a password-protected PDF);
#     - PdfiumNativeSourceBridge: `read(long, long)` and the field `buffer` (a document opened from a custom source);
#     - PdfWriteCallback.WriteBlock(byte[]) on whatever implements it (saveAsCopy).
-keep class io.legere.pdfiumandroid.PdfPasswordException { <init>(java.lang.String); }
-keep class io.legere.pdfiumandroid.util.PdfiumNativeSourceBridge { byte[] buffer; int read(long, long); }
-keep interface io.legere.pdfiumandroid.PdfWriteCallback { int WriteBlock(byte[]); }
-keepclassmembers class * implements io.legere.pdfiumandroid.PdfWriteCallback { int WriteBlock(byte[]); }

package ca.canifishthis.memorytrim

import android.content.ComponentCallbacks2
import android.content.Context
import android.content.res.Configuration
import android.view.View
import android.view.ViewGroup
import android.util.Log
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition

/**
 * (T360) WHEN ANDROID ASKS FOR MEMORY BACK (`onTrimMemory`), nothing in the app answered: React Native only runs a Hermes
 * GC, and maplibre-react-native never forwards the call to its MapView. On a trim this module (1) calls `onLowMemory()` on
 * every MapLibre MapView in the activity (MapLibre drops the tiles and resources the view is not drawing), (2) asks the
 * allocator to give freed pages back to the OS (`memorytrim.cpp`, mallopt M_PURGE_ALL / M_PURGE) and (3) sends the level to JS as a `trim` event (`src/memoryTrim.ts`: the release's SQLite page caches are shrunk). The MapView is found
 * by its class name, so this module needs no MapLibre dependency of its own.
 */
class MemoryTrimModule : Module() {
  private var callbacks: ComponentCallbacks2? = null

  /** 2: everything purged (M_PURGE_ALL), 1: the larger part (M_PURGE), 0: the allocator refused. */
  private external fun purge(): Int

  companion object {
    /** The library loaded (a missing one is said in every trim event as `purged: -1`, never a crash) */
    private val loaded: Boolean = try { System.loadLibrary("memorytrim"); true }
      catch (e: UnsatisfiedLinkError) { Log.w("MemoryTrim", "libmemorytrim did not load", e); false }
  }

  override fun definition() = ModuleDefinition {
    Name("MemoryTrim")
    Events("trim")

    OnCreate {
      val ctx = appContext.reactContext?.applicationContext ?: return@OnCreate
      val cb = object : ComponentCallbacks2 {
        override fun onTrimMemory(level: Int) {
          // RUNNING_MODERATE (5) and up, foreground or not; UI_HIDDEN (20) and the background levels too
          if (level < ComponentCallbacks2.TRIM_MEMORY_RUNNING_MODERATE) return
          val maps = trimMaps()
          val purged = if (loaded) purge() else -1
          Log.i("MemoryTrim", "trim level $level: $maps map view(s) told, purge $purged")
          sendEvent("trim", mapOf("level" to level, "maps" to maps, "purged" to purged))
        }
        override fun onConfigurationChanged(newConfig: Configuration) {}
        @Deprecated("onTrimMemory") override fun onLowMemory() {}
      }
      ctx.registerComponentCallbacks(cb)
      callbacks = cb
    }

    OnDestroy {
      val ctx = appContext.reactContext?.applicationContext
      callbacks?.let { ctx?.unregisterComponentCallbacks(it) }
      callbacks = null
    }
  }

  /** `onLowMemory()` on every MapLibre MapView under the activity's window (called on the main thread, as Android calls us). */
  private fun trimMaps(): Int {
    val root = appContext.currentActivity?.window?.decorView ?: return 0
    var n = 0
    fun walk(v: View) {
      if (isMapView(v.javaClass)) {
        try { v.javaClass.getMethod("onLowMemory").invoke(v); n++ }
        catch (e: Exception) { Log.w("MemoryTrim", "MapView.onLowMemory failed", e) }
      }
      if (v is ViewGroup) for (i in 0 until v.childCount) walk(v.getChildAt(i))
    }
    walk(root)
    return n
  }

  private fun isMapView(c: Class<*>?): Boolean {
    var k = c
    while (k != null) { if (k.name == "org.maplibre.android.maps.MapView") return true; k = k.superclass }
    return false
  }
}

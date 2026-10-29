package ca.canifishthis.exitinfo

import android.app.ActivityManager
import android.app.ApplicationExitInfo
import android.content.Context
import android.os.Build
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition

/**
 * (T242) WHY THE LAST RUN OF THE APP ENDED, from Android itself (ApplicationExitInfo, API 30+): a native crash, an ANR, the system
 * killing the app for memory, a Java crash the JS handler never saw (an OutOfMemoryError). Read on the next start and handed to
 * JS, which scrubs it and sends it to our own server under the "Send crash reports" switch. Reads only this app's main process.
 * Nothing is read from the traces (they hold paths and memory); only the reason, importance, signal/status and memory size.
 */
class ExitInfoModule : Module() {
  override fun definition() = ModuleDefinition {
    Name("ExitInfo")

    // exits of this app after `since` (epoch ms), newest first, at most 8
    AsyncFunction("recent") { since: Double ->
      val out = ArrayList<Map<String, Any?>>()
      val ctx = appContext.reactContext
      if (ctx == null || Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return@AsyncFunction out
      val am = ctx.getSystemService(Context.ACTIVITY_SERVICE) as ActivityManager
      for (e in am.getHistoricalProcessExitReasons(ctx.packageName, 0, 8)) {
        if (e.timestamp <= since.toLong() || e.processName != ctx.packageName) continue
        out.add(mapOf(
          "timestamp" to e.timestamp.toDouble(),
          "reason" to reasonName(e.reason),
          "importance" to importanceName(e.importance),
          "status" to e.status,
          "rssKb" to e.rss,
          "description" to (e.description ?: ""),
        ))
      }
      out
    }
  }

  private fun reasonName(r: Int): String = when (r) {
    ApplicationExitInfo.REASON_CRASH -> "CRASH"
    ApplicationExitInfo.REASON_CRASH_NATIVE -> "CRASH_NATIVE"
    ApplicationExitInfo.REASON_ANR -> "ANR"
    ApplicationExitInfo.REASON_LOW_MEMORY -> "LOW_MEMORY"
    ApplicationExitInfo.REASON_EXCESSIVE_RESOURCE_USAGE -> "EXCESSIVE_RESOURCE_USAGE"
    ApplicationExitInfo.REASON_INITIALIZATION_FAILURE -> "INITIALIZATION_FAILURE"
    ApplicationExitInfo.REASON_SIGNALED -> "SIGNALED"
    ApplicationExitInfo.REASON_DEPENDENCY_DIED -> "DEPENDENCY_DIED"
    ApplicationExitInfo.REASON_PERMISSION_CHANGE -> "PERMISSION_CHANGE"
    ApplicationExitInfo.REASON_EXIT_SELF -> "EXIT_SELF"
    ApplicationExitInfo.REASON_USER_REQUESTED -> "USER_REQUESTED"
    ApplicationExitInfo.REASON_USER_STOPPED -> "USER_STOPPED"
    else -> "OTHER_$r"
  }

  private fun importanceName(i: Int): String = when {
    i <= ActivityManager.RunningAppProcessInfo.IMPORTANCE_FOREGROUND -> "FOREGROUND"
    i <= ActivityManager.RunningAppProcessInfo.IMPORTANCE_VISIBLE -> "VISIBLE"
    i <= ActivityManager.RunningAppProcessInfo.IMPORTANCE_SERVICE -> "SERVICE"
    else -> "BACKGROUND"
  }
}

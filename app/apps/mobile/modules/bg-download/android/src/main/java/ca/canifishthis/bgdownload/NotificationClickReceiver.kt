package ca.canifishthis.bgdownload

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/** (T298) A tap on the first install's download notification: the app opens (where it left off: setup picks up). */
class NotificationClickReceiver : BroadcastReceiver() {
  override fun onReceive(context: Context, intent: Intent) {
    val launch = context.packageManager.getLaunchIntentForPackage(context.packageName) ?: return
    launch.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_RESET_TASK_IF_NEEDED)
    try { context.startActivity(launch) } catch (_: Exception) { /* a background start the system refused */ }
  }
}

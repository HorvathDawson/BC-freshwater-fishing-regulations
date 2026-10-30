package ca.canifishthis.bgdownload

import android.app.DownloadManager
import android.content.Context
import android.net.Uri
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import java.io.File
import java.io.FileInputStream
import java.security.MessageDigest

/**
 * (T298) THE FIRST INSTALL'S BIG FILES, CARRIED BY ANDROID'S OWN DOWNLOAD MANAGER: the tiles, mask, basemap and rule
 * book are handed to the system (`DownloadManager`), which downloads them in its own process — on after the app is
 * swiped away or the screen is off, with its own progress notification, resuming dropped lines, and holding a job
 * "Wi-Fi only" (`setAllowedOverMetered(false)`) until an unmetered network is up. No foreground service, no
 * notification permission (the notification is the download provider's), nothing to declare to Play.
 *
 * JS (`@app/data` install.native.ts) owns the policy: it enqueues, polls `jobs()`, and once a job is done `claim`s its
 * file into the install root (a rename, or a copy across file systems) and checks it with `sha256` (native: ~1 s for
 * the 274 MB basemap, where JS took 23 s) before anything points at it. Files land in the app's own external files
 * folder (`bg-download/`), which the system's downloader may write to without any storage permission.
 */
class BgDownloadModule : Module() {
  private val ctx: Context get() = appContext.reactContext ?: throw IllegalStateException("no context")
  private val dm: DownloadManager get() = ctx.getSystemService(Context.DOWNLOAD_SERVICE) as DownloadManager
  private val prefs get() = ctx.getSharedPreferences("bg-download", Context.MODE_PRIVATE)

  override fun definition() = ModuleDefinition {
    Name("BgDownload")

    // every download this app has with the system (the download manager answers each app for its own only)
    AsyncFunction("jobs") {
      val out = ArrayList<Map<String, Any?>>()
      dm.query(DownloadManager.Query()).use { c ->
        while (c.moveToNext()) {
          val id = c.getLong(c.getColumnIndexOrThrow(DownloadManager.COLUMN_ID))
          out.add(mapOf(
            "id" to id.toDouble(),
            "url" to c.getString(c.getColumnIndexOrThrow(DownloadManager.COLUMN_URI)),
            "status" to statusName(c.getInt(c.getColumnIndexOrThrow(DownloadManager.COLUMN_STATUS))),
            "reason" to c.getInt(c.getColumnIndexOrThrow(DownloadManager.COLUMN_REASON)),
            "bytes" to c.getLong(c.getColumnIndexOrThrow(DownloadManager.COLUMN_BYTES_DOWNLOADED_SO_FAR)).toDouble(),
            "total" to c.getLong(c.getColumnIndexOrThrow(DownloadManager.COLUMN_TOTAL_SIZE_BYTES)).toDouble(),
            "metered" to prefs.getBoolean("metered:$id", true),
          ))
        }
      }
      out
    }

    // a new job: `name` is its file in bg-download/ (one left there by an earlier job is replaced, not renamed beside)
    AsyncFunction("enqueue") { url: String, name: String, title: String, text: String, metered: Boolean ->
      val dir = ctx.getExternalFilesDir("bg-download") ?: throw IllegalStateException("no external files folder")
      File(dir, name).delete()
      val req = DownloadManager.Request(Uri.parse(url))
        .setTitle(title)
        .setDescription(text)
        .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE)
        .setAllowedOverMetered(metered)
        .setAllowedOverRoaming(metered)
        .setDestinationInExternalFilesDir(ctx, "bg-download", name)
      val id = dm.enqueue(req)
      prefs.edit().putBoolean("metered:$id", metered).apply()
      id.toDouble()
    }

    AsyncFunction("remove") { id: Double ->
      dm.remove(id.toLong())
      prefs.edit().remove("metered:${id.toLong()}").apply()
    }

    // a finished job's file onto `toUri` (replacing it): a rename where it can be, else a copy; then the job is forgotten
    AsyncFunction("claim") { id: Double, toUri: String ->
      val src = localFile(id.toLong()) ?: throw IllegalStateException("download $id has no file")
      val dest = File(Uri.parse(toUri).path!!)
      dest.parentFile?.mkdirs()
      if (dest.exists()) dest.delete()
      if (!src.renameTo(dest)) {
        src.inputStream().use { i -> dest.outputStream().use { o -> i.copyTo(o, 1 shl 20) } }
        src.delete()
      }
      dm.remove(id.toLong())
      prefs.edit().remove("metered:${id.toLong()}").apply()
    }

    // the sha256 of a file, hex
    AsyncFunction("sha256") { uri: String ->
      val md = MessageDigest.getInstance("SHA-256")
      val buf = ByteArray(1 shl 20)
      FileInputStream(File(Uri.parse(uri).path!!)).use { s ->
        while (true) { val n = s.read(buf); if (n < 0) break; md.update(buf, 0, n) }
      }
      md.digest().joinToString("") { "%02x".format(it) }
    }
  }

  private fun localFile(id: Long): File? {
    dm.query(DownloadManager.Query().setFilterById(id)).use { c ->
      if (!c.moveToFirst()) return null
      val local = c.getString(c.getColumnIndexOrThrow(DownloadManager.COLUMN_LOCAL_URI)) ?: return null
      return Uri.parse(local).path?.let { File(it) }
    }
  }

  private fun statusName(s: Int): String = when (s) {
    DownloadManager.STATUS_PENDING -> "pending"
    DownloadManager.STATUS_RUNNING -> "running"
    DownloadManager.STATUS_PAUSED -> "paused"
    DownloadManager.STATUS_SUCCESSFUL -> "done"
    else -> "failed"
  }
}

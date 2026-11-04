// (T360) Give freed native memory back to the OS. A screen that churns (Conditions: ~250 MB allocated and freed by
// MapLibre in 45 s, heapprofd) leaves the allocator holding the pages: PSS stays ~140 MB up though only ~28 MB is live.
// M_PURGE_ALL (API 34+) releases every cached page; M_PURGE (API 28+) the larger part. `mallopt` is declared only from
// API 26 and the module builds for 24: it is looked up in libc when called (none: 0, nothing purged).
#include <jni.h>
#include <dlfcn.h>
#include <android/api-level.h>

#define M_PURGE (-101)
#define M_PURGE_ALL (-104)

extern "C" JNIEXPORT jint JNICALL
Java_ca_canifishthis_memorytrim_MemoryTrimModule_purge(JNIEnv*, jobject) {
  using Mallopt = int (*)(int, int);
  static const Mallopt mallopt_ = reinterpret_cast<Mallopt>(dlsym(RTLD_DEFAULT, "mallopt"));
  if (!mallopt_) return 0;
  if (android_get_device_api_level() >= 34 && mallopt_(M_PURGE_ALL, 0) == 1) return 2;
  return mallopt_(M_PURGE, 0) == 1 ? 1 : 0;
}

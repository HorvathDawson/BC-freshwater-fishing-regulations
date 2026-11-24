/**
 * (T1058) THE GRADLE DAEMON'S MEMORY, written into android/gradle.properties at `expo prebuild` (android/ is generated,
 * never edited), so CI, a fresh machine and every worktree get it; any template value is replaced, never duplicated.
 *
 * WHY. Expo's template writes `-Xmx2048m -XX:MaxMetaspaceSize=512m`. A release build of this app (R8 on, ~700 tasks)
 * fills ~80 % of a fresh daemon's 512 MiB of Metaspace (the daemon logs read 80-82 % at the end of every build that hit
 * it, 2026-11-19 to 11-24), past Gradle's health line, so the daemon "expires" as the build ends. On 2026-11-24 09:14
 * (the 0.9.0 production build) that expiry raced the result: the daemon stopped BEFORE it sent the client the build's
 * result, the client printed BUILD SUCCESSFUL and then waited on the closed connection for over an hour
 * (~/.gradle/daemon/9.4.1/daemon-53034.out.log: "stop() called" before "dispatching the build result"; "Timed out
 * waiting for finished message from client"). deploy/lib/gradle-watch.sh now ends such a wait; this removes its cause.
 *
 * THE NUMBERS. Metaspace 1024m: twice what a build uses, so the health line is never reached; a cap, not an allocation
 * (the JVM commits only what the loaded classes take). Heap stays 2048m (22 % used at the end of that build). The most
 * the daemon may take is about 3.3 GiB (2 GiB heap + 1 GiB Metaspace + the JVM's 240 MiB code cache); the Kotlin
 * compile daemons are separate processes (their own lines in the T1058 proof).
 *
 * NOTE: `~/.gradle/gradle.properties` OVERRIDES this file (Gradle's order: command line, then the user home, then the
 * project), so a machine-wide `org.gradle.jvmargs` there wins; build-apk.sh says so when it finds one.
 */
const { withGradleProperties } = require("expo/config-plugins");

const KEY = "org.gradle.jvmargs";
const JVMARGS = "-Xmx2048m -XX:MaxMetaspaceSize=1024m";

/** gradle.properties items with the daemon's jvmargs set to JVMARGS (any template value replaced, never duplicated) */
function setJvmargs(items) {
  const out = items.filter((p) => !(p.type === "property" && p.key === KEY));
  out.push({ type: "property", key: KEY, value: JVMARGS });
  return out;
}

module.exports = function withGradleMemory(config) {
  return withGradleProperties(config, (c) => {
    c.modResults = setJvmargs(c.modResults);
    return c;
  });
};
Object.assign(module.exports, { setJvmargs, KEY, JVMARGS });

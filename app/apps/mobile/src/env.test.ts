/** (T232) The environment a build reads, resolved from its build variables, platform and development switch. */
import { describe, expect, it } from "vitest";
import { endpointsFor, envOfOrigin, ORIGINS, resolveEnv } from "./env";

const STAGING = "https://data-staging.canifishthis.ca";

describe("resolveEnv", () => {
  it("unset: the web is local, a phone is staging — never production by default", () => {
    expect(resolveEnv({ web: true, switchable: false })).toEqual(
      { env: "local", built: "local", origin: ORIGINS.local, switchable: false });
    expect(resolveEnv({ web: false, switchable: true })).toEqual(
      { env: "staging", built: "staging", origin: STAGING, switchable: true });
  });

  it("EXPO_PUBLIC_APP_ENV picks the environment; production is used only when named", () => {
    expect(resolveEnv({ web: true, appEnv: "staging", switchable: false }).origin).toBe(STAGING);
    expect(resolveEnv({ web: false, appEnv: " production ", switchable: false })).toEqual(
      { env: "production", built: "production", origin: "https://data.canifishthis.ca", switchable: false });
  });

  it("a misspelt environment fails at start instead of reading some other data", () => {
    expect(() => resolveEnv({ web: false, appEnv: "stage", switchable: false }))
      .toThrow('EXPO_PUBLIC_APP_ENV="stage" is not one of local, staging, production');
  });

  it("EXPO_PUBLIC_DATA_BASE: a known origin names its environment; any other is the build's own origin", () => {
    expect(resolveEnv({ web: true, dataBase: `${STAGING}/`, switchable: false }))
      .toEqual({ env: "staging", built: "staging", origin: STAGING, switchable: false });
    expect(resolveEnv({ web: false, appEnv: "local", dataBase: "http://192.168.1.20:39217/", switchable: false }).origin)
      .toBe("http://192.168.1.20:39217");
  });

  it("the development switch wins only in a switchable build, and switching away leaves the build's base behind", () => {
    const lan = { web: false, appEnv: "local", dataBase: "http://192.168.1.20:39217", override: "staging" };
    expect(resolveEnv({ ...lan, switchable: false })).toMatchObject({ env: "local", origin: "http://192.168.1.20:39217" });
    expect(resolveEnv({ ...lan, switchable: true })).toEqual(
      { env: "staging", built: "local", origin: STAGING, switchable: true });
    // back to the build's own environment: its explicit base again
    expect(resolveEnv({ ...lan, override: "local", switchable: true }).origin).toBe("http://192.168.1.20:39217");
    // a stored value that is no environment (an older build's, a corrupt file) is ignored
    expect(resolveEnv({ web: false, override: "qa", switchable: true }).env).toBe("staging");
  });

  it("envOfOrigin knows the three origins, slash or not", () => {
    expect([envOfOrigin(`${STAGING}/`), envOfOrigin(ORIGINS.production), envOfOrigin("https://x.test"), envOfOrigin("")])
      .toEqual(["staging", "production", null, null]);
  });
});

describe("endpointsFor", () => {
  it("hangs every URL the app reads off one origin", () => {
    const e = endpointsFor(STAGING);
    expect([e.manifest, e.release, e.tiles.atlas, e.feed, e.dfoFeed, e.inSeason, e.sqlWasm]).toEqual([
      `${STAGING}/release/manifest.json`, `${STAGING}/release/`, `${STAGING}/atlas.pmtiles`, `${STAGING}/feeds/live`,
      `${STAGING}/feeds/dfo`, `${STAGING}/feeds/inseason`, `${STAGING}/`]);
  });
});

import { describe, expect, it } from "vitest";
import { formFactorFor } from "./index.js";

describe("formFactorFor", () => {
  it("is decided in exactly one place", () => {
    expect(formFactorFor(390)).toBe("phone");
    expect(formFactorFor(899)).toBe("phone");
    expect(formFactorFor(900)).toBe("desktop");
    expect(formFactorFor(1440)).toBe("desktop");
  });
});

import assert from "node:assert/strict";
import { test } from "node:test";
import { translate, translateDiagnostic } from "./i18n";

test("demo copy interpolates field labels", () => {
  assert.equal(translate("en", "Fold line {line}", { line: 3 }), "Fold line 3");
  assert.equal(
    translate("en", "{count} lines folded", { count: 2 }),
    "2 lines folded",
  );
});

test("external diagnostics remain verbatim", () => {
  const external =
    'Error from server (Forbidden): pods "actual-pod" cannot be listed';
  assert.equal(translateDiagnostic(external, "en"), external);
});

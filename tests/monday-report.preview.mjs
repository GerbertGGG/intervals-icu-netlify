import assert from "node:assert/strict";
import { buildMondayReportPreview } from "../src/index.js";

const output = buildMondayReportPreview();

assert.ok(output.includes("🏗️ BLOCK-STATUS"));
assert.ok(output.includes("📊 WOCHENURTEIL (Trainer)"));
assert.ok(output.includes("🧠 LEARNINGS (nur das Relevante)"));
assert.ok(output.includes("🎯 ENTSCHEIDUNG & WOCHENZIEL"));

console.log("--- Montags-Report Preview ---\n" + output);

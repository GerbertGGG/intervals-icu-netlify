import assert from "node:assert/strict";
import { buildMondayReportPreview } from "../src/index.js";

const output = buildMondayReportPreview();

assert.ok(output.includes("🏗️ BLOCK-STATUS"));
assert.ok(output.includes("🧠 WOCHENFAZIT (Trainer)"));
assert.ok(output.includes("📐 PLANABWEICHUNG (Soll vs Ist)"));
assert.ok(output.includes("🎯 WOCHENZIEL (1 Fokus)"));

console.log("--- Montags-Report Preview ---\n" + output);

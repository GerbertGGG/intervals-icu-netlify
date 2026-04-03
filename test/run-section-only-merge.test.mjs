import assert from "node:assert/strict";
import { __test } from "../src/index.js";

const existing = [
  "🏃 HEUTE",
  "Longrun 64′.",
  "⸻",
  "",
  "🧠 COACH-ANALYSE",
  "Alter Coachtext.",
  "⸻",
  "",
  "🧩 WARUM",
  "Bestehende Begründung.",
  "⸻",
  "",
  "🧾 BOTTOM LINE",
  "Heute sauber laufen.",
  "⸻",
  "",
].join("\n");

const fresh = [
  "🏃 HEUTIGER LAUF",
  "Drift: 6,9 %.",
  "⸻",
  "",
  "🏃 HEUTE",
  "Longrun 64′.",
  "⸻",
  "",
  "🧠 COACH-ANALYSE",
  "Neu generierter Coachtext, der im run-only Modus NICHT übernommen werden darf.",
  "⸻",
  "",
].join("\n");

const merged = __test.mergeTodayRunSection(existing, fresh);

assert.match(merged, /🏃 HEUTIGER LAUF\nDrift: 6,9 %\./);
assert.match(merged, /🧠 COACH-ANALYSE\nAlter Coachtext\./);
assert.match(merged, /🧩 WARUM\nBestehende Begründung\./);
assert.doesNotMatch(merged, /Neu generierter Coachtext/);
assert.equal(
  merged.split("🧠 COACH-ANALYSE")[1],
  existing.split("🧠 COACH-ANALYSE")[1],
  "all content after HEUTIGER LAUF/HEUTE boundary should remain byte-identical"
);

console.log("run-section-only merge ok");

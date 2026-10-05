import assert from "node:assert/strict";
import { resolveSeasonBlock, parseBlockWeeks } from "../src/season-block.js";

const ev = { category: "SEASON_START", name: "Block Grundlage (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: "Ziel ist es Decoupling unter 8% zu druecken\n\nCheckpoint 3 / Blockabschluss nach Woche 12" };
assert.equal(parseBlockWeeks(ev.name), 12);
let b = resolveSeasonBlock([ev], "2026-10-05");
assert.equal(b.status, "upcoming"); assert.equal(b.daysToStart, 14); assert.equal(b.end, "2027-01-10");
assert.equal(b.name, "Block Grundlage"); assert.match(b.goal, /Decoupling/); assert.equal(b.notes.length, 1);
b = resolveSeasonBlock([ev], "2026-11-02");
assert.equal(b.status, "active"); assert.equal(b.weekNo, 3);
assert.equal(resolveSeasonBlock([{ category: "WORKOUT", name: "Block x" }], "2026-10-05"), null);
console.log("season-block ok");

import { buildBlockGoals, parseBlockTargets } from "../src/season-block.js";
const text = "Ziel ist es bei langen Läufen das Decoupling im Mittel unter 8% zu drücken\n\nCheckpoint 3: Decoupling ≤8% Kraft-Konsistenz ACWR-Korridor gehalten marathonShape ≥32-35%";
const t = parseBlockTargets(text);
assert.equal(t.decoupling.max, 8); assert.equal(t.marathonShape.min, 32); assert.equal(t.marathonShape.stretch, 35); assert.ok(t.strength && t.acwr);
const blk = resolveSeasonBlock([{ ...ev, description: text }], "2026-11-16"); // Woche 5
const input = {
  longRuns: [{ date: "2026-10-25", decoupling: 9.5 }, { date: "2026-11-08", decoupling: 6 }, { date: "2026-10-01", decoupling: 20 }],
  strength: [{ date: "2026-10-21", minutes: 65 }, { date: "2026-11-03", minutes: 30 }, { date: "2026-11-04", minutes: 35 }],
  acwrDays: [{ date: "2026-10-20", acwr: 1.0 }, { date: "2026-10-21", acwr: 1.5 }, { date: "2026-10-22", acwr: 0.9 }, { date: "2026-10-23", acwr: 1.1 }],
  marathonShape: { now: 28, history: [{ date: "2026-10-19", value: 23 }] },
};
const g = Object.fromEntries(buildBlockGoals(blk, input, "2026-11-16").map((x) => [x.key, x]));
assert.equal(g.decoupling.value, 7.8); assert.equal(g.decoupling.count, 2); assert.equal(g.decoupling.status, "ok"); // Lauf vor Blockstart zählt nicht
assert.equal(g.strength.of, 4); assert.equal(g.strength.value, 2); // Woche 1 (65) und Woche 3 (65)
assert.equal(g.acwr.value, 75); assert.equal(g.acwr.status, "warn");
assert.equal(g.marathonShape.value, 28);
const base = Object.fromEntries(buildBlockGoals(resolveSeasonBlock([{ ...ev, description: text }], "2026-10-05"), input, "2026-10-05").map((x) => [x.key, x]));
assert.equal(base.decoupling.status, "base"); assert.equal(base.decoupling.value, 20);
console.log("block goals ok");

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

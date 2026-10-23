import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { RUN_RULES, analyzeRunStreams, cleanRunDistance, runMovingAxis } from "../src/block-inputs.js";

// Echte Zeitreihen aus Runalyze (Zeitachse = Bewegungszeit, Distanz in m)
const load = (name) => JSON.parse(readFileSync(new URL(`./fixtures/${name}`, import.meta.url)));
const run23 = load("run-2026-10-23.json"); // Basistest 23.10.2026 mit GPS-Sprung zwischen 730 und 783 s
const run20 = load("run-2026-10-20.json"); // Grundlagenlauf 20.10.2026 ohne Ausreißer
const RAW = { ...RUN_RULES, GPS_MAX_RUN_SPEED: Infinity, GPS_MAX_JUMP_M: Infinity }; // Filter aus
const OLD = { ...RAW, DECOUPLING_SKIP_START_S: 0 }; // alte Rechnung: Fenster ab 0, ohne Filter
const at = (f, dist, sec) => dist[f.time.indexOf(sec)];

// ---------- GPS-Filter: Basistest 23.10. ----------
{
  const mt = runMovingAxis(run23.time, run23.distance);
  assert.deepEqual(mt, run23.time); // Runalyze liefert schon Bewegungszeit: Achse bleibt
  const { dist, gps } = cleanRunDistance(mt, run23.distance);
  // Sprung 730–783 s: 733–750 s (Lücken ≤ 5 s überbrückt) und 782–784 s; dazu ein Einzelwert bei 2890 s (4,8 m/s)
  assert.equal(gps.segments, 3);
  assert.ok(gps.correctedM > 300 && gps.correctedM < 350, `korrigiert ${gps.correctedM} m`);
  const jump = (d) => at(run23, d, 783) - at(run23, d, 730);
  assert.ok(jump(run23.distance) > 430); // roh: 440 m in 53 s
  assert.ok(jump(dist) > 100 && jump(dist) < 140, `bereinigt ${jump(dist)} m`); // realistisch ~120 m
  const d3600 = at(run23, dist, 3600) / 1000;
  assert.ok(Math.abs(d3600 - 8.0) < 0.05, `Distanz bei 3600 s: ${d3600} km`);
  console.log(`23.10.: ${gps.segments} Segmente, −${gps.correctedM} m, Distanz bei 3600 s ${d3600.toFixed(3)} km (roh ${(at(run23, run23.distance, 3600) / 1000).toFixed(3)} km)`);
}

// ---------- Decoupling 5–60 und Pace Basis: Basistest 23.10. ----------
{
  const a = analyzeRunStreams(run23, { capHr: 128 });
  const old = analyzeRunStreams(run23, { capHr: 128, rules: OLD });
  assert.equal(a.fromMin, 5);
  assert.deepEqual(a.gps, { segments: 3, correctedM: a.gps.correctedM });
  assert.ok(a.dec60 > 0 && a.dec60 < 12, `Decoupling 5–60 ${a.dec60} %`); // einstellig bis knapp zweistellig
  assert.equal(old.dec60, 18); // ohne Filter und ab Minute 0
  // Pace Basis = bereinigte Distanz 5–60 / 3300 s Bewegungszeit
  const { dist } = cleanRunDistance(run23.time, run23.distance);
  assert.equal(a.pace60, Math.round(3300 / ((at(run23, dist, 3600) - at(run23, dist, 300)) / 1000)));
  assert.deepEqual([a.durationMin, a.distKm], [60, Math.round(dist.at(-1)) / 1000]);
  console.log(`23.10.: Decoupling 5–60 ${a.dec60} % (alt 0–60 ungefiltert ${old.dec60} %), Pace 5–60 ${Math.floor(a.pace60 / 60)}:${String(a.pace60 % 60).padStart(2, "0")} /km, stabile Dauer ${a.stableMin} min`);
}

// ---------- Pace auf der Bewegungszeit: dieselbe Einheit mit vergangener Zeit (Intervals) ----------
// 60 s Stehen bei 1800 s (Proben ohne Distanzzuwachs) und eine Auto-Pause (Lücke von 30 s) bei 2400 s
{
  const time = [], distance = [], heartrate = [];
  let shift = 0;
  for (let i = 0; i < run23.time.length; i++) {
    const t = run23.time[i];
    if (t === 1800) for (let k = 0; k < 60; k++) { time.push(t + shift); distance.push(run23.distance[i - 1]); heartrate.push(run23.heartrate[i]); shift++; }
    if (t === 2400) shift += 30;
    time.push(t + shift); distance.push(run23.distance[i]); heartrate.push(run23.heartrate[i]);
  }
  const elapsed = { time, distance, heartrate };
  assert.equal(time.at(-1) - run23.time.at(-1), 90);
  const mt = runMovingAxis(time, distance);
  assert.equal(mt.at(-1), run23.time.at(-1)); // Stehen zählt nicht, die Pause wie die Probe davor 1 s
  const a = analyzeRunStreams(run23, { capHr: 128 }), b = analyzeRunStreams(elapsed, { capHr: 128 });
  assert.equal(b.durationMin, a.durationMin);
  assert.ok(Math.abs(b.dec60 - a.dec60) <= 0.2 && Math.abs(b.pace60 - a.pace60) <= 1, `${b.dec60}/${b.pace60} statt ${a.dec60}/${a.pace60}`);
  // Auf der vergangenen Zeit (ohne Bewegungsachse) wäre die Pace langsamer
  const naive = analyzeRunStreams(elapsed, { capHr: 128, rules: { ...RUN_RULES, PAUSE_GAP_S: Infinity, STOP_SPEED: 0 } });
  assert.ok(naive.pace60 > a.pace60 + 5);
}

// ---------- Normaler Lauf ohne Ausreißer: Filter ändert nichts ----------
{
  const mt = runMovingAxis(run20.time, run20.distance);
  assert.deepEqual(mt, run20.time);
  const { dist, gps } = cleanRunDistance(mt, run20.distance);
  assert.deepEqual(gps, { segments: 0, correctedM: 0 });
  assert.deepEqual(dist.map((x) => Math.round(x * 100)), run20.distance.map((x) => Math.round(x * 100)));
  // 34 min: kein Fenster bis 60, EF Minute 5–34 unverändert
  const a = analyzeRunStreams(run20, { capHr: 128 }), raw = analyzeRunStreams(run20, { capHr: 128, rules: RAW });
  assert.deepEqual(a, raw);
  assert.deepEqual([a.dec60, a.stableMin, a.pace60, a.efToMin], [null, null, null, 34]);
}
// Synthetischer 90-min-Lauf mit schwankendem Tempo (2,2–2,8 m/s) und Pulsanstieg aus dem Stand: Filter ändert nichts,
// das Decoupling ändert sich nur durch die Startausblendung
{
  const n = 90 * 60, time = Array.from({ length: n }, (_, i) => i);
  let seed = 7; const rnd = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
  const distance = [0];
  for (let i = 1; i < n; i++) distance.push(distance[i - 1] + 2.2 + rnd() * 0.6);
  const heartrate = time.map((t) => (t < 120 ? 80 + t / 3 : 121 + t / 1800));
  const st = { time, distance, heartrate };
  const a = analyzeRunStreams(st), raw = analyzeRunStreams(st, { rules: RAW }), old = analyzeRunStreams(st, { rules: OLD });
  assert.deepEqual(a.gps, { segments: 0, correctedM: 0 });
  assert.deepEqual(a, raw);
  assert.deepEqual(a.windows.map((x) => x.min), [60, 75, 90]);
  assert.ok(old.dec60 > a.dec60 + 1 && Math.abs(a.dec60) < 1.5, `0–60 ${old.dec60} %, 5–60 ${a.dec60} %`); // Kaltstart: niedriger Puls in Hälfte 1 treibt 0–60 hoch
}
console.log("run analysis ok");

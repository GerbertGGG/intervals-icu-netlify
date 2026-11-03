// Spalten und D1-Schema des Exports (DOKUMENTATION.md, Teil B 13). Gemeinsam fuer den Worker (export.js, liest aus D1) und
// das Skript in GitHub Actions (scripts/export-sync.mjs, rechnet und schreibt nach D1).

// Methode der Zeitreihen-Kennzahlen. Aendert sich die Rechnung, Zahl erhoehen: das Skript rechnet dann jede Zeile mit
// aelterer methodVersion neu.
// 4 (03.11.2026): Tabellen rides und swims, Koppel-Spalten in runs (brick, brickRideId, brickGapMin, brickRideMin, brickRideLoad)
// 3 (03.11.2026): neue Tagesspalte calendar (Kalender-Eintraege), sick auch bei OP/Operation/Verletzung im Namen
// 2 (03.11.2026): Aufzeichnungsluecken bis 12 s als Bewegung (intelligente Aufzeichnung, 2022), Schutzregel GPS > 10 %,
// Rennen ueber jeden Renn-Eintrag im Kalender am selben Tag ohne Distanzabgleich
export const METHOD_VERSION = 4;

export const METRIC_COLUMNS = [
  "paceBand125_135", "secBand125_135", "paceBand120_128", "secBand120_128", "paceBand125_135Flat", "paceBand120_128Flat",
  "effAt130", "effSlope", "effR2", "effN", "decoup", "hrDriftBpm", "gradeShareFlat", "gpsCorrectedM",
];

// Spalten der Ausgabe (Reihenfolge = columns bei format=table und csv). methodVersion je Zeile steht am Ende.
export const RUN_COLUMNS = [
  "id", "date", "startLocal", "type", "name", "tags", "treadmill", "race", "interval", "runClass", "distKm", "movingMin",
  "elevPerKm", "avgHr", "maxHr", "rpe", "feel", "load", "brick", "brickRideId", "brickGapMin", "brickRideMin", "brickRideLoad",
  "tempC", "humidity", "windMs", "weatherSrc", "streamStatus", "streamSkip",
  ...METRIC_COLUMNS, "methodVersion",
];

// Radfahrten (export-tri.js)
export const RIDE_METRIC_COLUMNS = ["hrAt160_170", "secAt160_170", "powerAt120", "powerAt130", "pwSlope", "pwR2", "pwN", "ef", "decoup", "decoup60", "mainHr"];
export const RIDE_COLUMNS = [
  "id", "date", "startLocal", "endLocal", "type", "name", "tags", "trainer", "movingMin", "distKm", "elevPerKm", "avgPower", "np", "if", "vi",
  "ftp", "avgHr", "maxHr", "avgCadence", "load", "rpe", "feel", "warmupS", "intensity", "testKind",
  "tempC", "humidity", "windMs", "weatherSrc", "streamStatus", "streamSkip", ...RIDE_METRIC_COLUMNS, "methodVersion",
];

// Schwimmeinheiten (export-tri.js)
export const SWIM_COLUMNS = [
  "id", "date", "startLocal", "type", "name", "pool", "poolLengthM", "distanceM", "movingMin", "pace100", "avgHr", "load", "rpe", "feel",
  "test500a", "test500b", "testPauseS", "testPassed", "lapsStatus", "methodVersion",
];

export const DAY_COLUMNS = [
  "date", "ctl", "atl", "rampRate", "loadRun", "loadBike", "loadSwim", "loadStrength", "minRun", "minBike", "minSwim", "strengthSets",
  "hrv", "restHr", "sleepH", "sleepScore", "sleepQuality", "fatigue", "soreness", "mood", "motivation", "sick", "calendar", "weightKg",
  "kcal", "kcalGoal", "protein", "carbs", "methodVersion",
];

// SQLite-Typen; alles andere REAL. Wahrheitswerte als 0/1, tags als JSON-Text.
const TEXT = new Set(["id", "date", "startLocal", "endLocal", "type", "name", "tags", "runClass", "calendar", "weatherSrc", "streamStatus", "streamSkip",
  "brickRideId", "intensity", "testKind", "pool", "lapsStatus"]);
const BOOL = new Set(["treadmill", "race", "interval", "sick", "brick", "trainer", "testPassed"]);
const INT = new Set(["methodVersion"]);
const typeOf = (c) => (TEXT.has(c) ? "TEXT" : BOOL.has(c) || INT.has(c) ? "INTEGER" : "REAL");

// Neben den Ausgabespalten: hash (Zeileninhalt, fuer "nur geaenderte Zeilen schreiben") und syncedAt
export const TABLES = {
  runs: { key: "id", columns: RUN_COLUMNS },
  days: { key: "date", columns: DAY_COLUMNS },
  rides: { key: "id", columns: RIDE_COLUMNS },
  swims: { key: "id", columns: SWIM_COLUMNS },
};
const q = (c) => `"${c}"`;
export function schemaStatements() {
  const out = [];
  for (const [name, t] of Object.entries(TABLES)) {
    const cols = t.columns.map((c) => `${q(c)} ${typeOf(c)}${c === t.key ? " PRIMARY KEY" : ""}`);
    out.push(`CREATE TABLE IF NOT EXISTS ${name} (${[...cols, `"hash" TEXT`, `"syncedAt" TEXT`].join(", ")})`);
  }
  out.push(`CREATE INDEX IF NOT EXISTS runs_start ON runs ("startLocal", "id")`);
  out.push(`CREATE INDEX IF NOT EXISTS runs_date ON runs ("date")`);
  for (const t of ["rides", "swims"]) {
    out.push(`CREATE INDEX IF NOT EXISTS ${t}_start ON ${t} ("startLocal", "id")`);
    out.push(`CREATE INDEX IF NOT EXISTS ${t}_date ON ${t} ("date")`);
  }
  out.push(`CREATE TABLE IF NOT EXISTS export_meta ("key" TEXT PRIMARY KEY, "value" TEXT)`);
  return out;
}
// Spalten, die eine aeltere Tabelle noch nicht hat (neue Kennzahl nach methodVersion-Wechsel): ALTER TABLE ... ADD COLUMN
export function missingColumnStatements(name, existing) {
  const have = new Set(existing);
  return [...TABLES[name].columns, "hash", "syncedAt"].filter((c) => !have.has(c)).map((c) => `ALTER TABLE ${name} ADD COLUMN ${q(c)} ${c === "hash" || c === "syncedAt" ? "TEXT" : typeOf(c)}`);
}

// Zeile -> SQL-Werte (bool 0/1, tags JSON) und zurueck
export function encodeRow(row, columns) {
  return columns.map((c) => {
    const v = row[c];
    if (v == null) return null;
    if (BOOL.has(c)) return v ? 1 : 0;
    if (c === "tags") return JSON.stringify(Array.isArray(v) ? v : []);
    return v;
  });
}
export function decodeRow(dbRow, columns) {
  const out = {};
  for (const c of columns) {
    const v = dbRow?.[c];
    if (v == null) out[c] = c === "tags" ? [] : null;
    else if (BOOL.has(c)) out[c] = v === 1 || v === true || v === "1";
    else if (c === "tags") { try { out[c] = JSON.parse(v); } catch { out[c] = []; } }
    else out[c] = v;
  }
  return out;
}

// Trainings-Widget fuer Scriptable (iOS) - gedacht fuer das grosse Widget (Large).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfuegen.
//  2. Skript einmal in Scriptable oeffnen und ausfuehren: Es fragt nach der Adresse deines Workers
//     (https://....workers.dev, ohne Pfad) und dem Dashboard-Token. Beides landet im Schluesselbund
//     dieses iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Groesse "Gross", Skript auswaehlen.
//     Kleine Widgets (Groesse "Klein"): Fitness (CTL, Standard), Ernaehrung (Parameter  ernaehrung)
//     Bereitschaft mit Ringen (Parameter  bereit)
//     sowie Schlaf und Erholung (Parameter  schlaf).
//     Mittleres Widget (Groesse "Mittel"): Ernaehrung heute (Kalorienring, Protein, Kohlenhydrate, Fett). Mit dem Parameter
//     form  zeigt es stattdessen Form, Halbmarathon-Zeiten vs. Ziel, VDOT und Paces, Schwellen. Mit dem Parameter
//     training  zeigt es je Disziplin (Schwimmen, Rad, Lauf) Wochenvolumen gegen Plan und die Verteilung der Zeit.
// Schluesseleinheiten kennzeichnest du im Intervals-Kalender mit dem Stichwort (Tag) #key am Workout.
// CTL, TSB und TSS sind sportartuebergreifend.
// Zugangsdaten spaeter aendern: Skript in Scriptable ausfuehren und "Zugangsdaten setzen" waehlen.
//
// Gross zeigt: Rennen-Countdown, Bereitschaft (Koerper und Gefuehl), heutige Einheit mit Warum und die naechsten
// drei Tage. Fehlende Werte stehen als "fehlt", nie als 0.

const KEY_URL = "training-dashboard-url";
const KEY_TOKEN = "training-dashboard-token";
// Groesse bestimmt die Ansicht: Gross = Hauptansicht, Mittel = Details (Form, Zeiten vs. Ziel, Paces ...)
// Ansicht nach Widget-Groesse: Gross = Hauptansicht, Mittel = Details, Klein = Schlaf oder Ernaehrung
// (Klein: im Widget unter "Parameter"  ernaehrung  oder  schlaf  eintragen, sonst Fitness).
const WIDGET_PARAM = String((typeof args !== "undefined" && args.widgetParameter) || "").trim().toLowerCase();
let VIEW = (typeof config !== "undefined" && config.runsInWidget)
  ? (config.widgetFamily === "medium" ? (/^(form|zeit|hm|detail)/.test(WIDGET_PARAM) ? "detail" : /^(train|tri)/.test(WIDGET_PARAM) ? "training" : "sleepfood") : config.widgetFamily === "small" ? (/^(ern|food|essen|kcal)/.test(WIDGET_PARAM) ? "food" : /^(schlaf|sleep|erhol)/.test(WIDGET_PARAM) ? "sleep" : /^(bereit|ready)/.test(WIDGET_PARAM) ? "ready" : "fitness") : "main")
  : "main";
const endpointView = () => (VIEW === "detail" ? "detail" : VIEW === "training" ? "training" : VIEW === "sleep" || VIEW === "food" || VIEW === "fitness" || VIEW === "sleepfood" ? "small" : "");
const cacheFile = () => `training-widget${endpointView() ? "-" + endpointView() : ""}-cache.json`;
const SPORTS = [["run", "Laufen", "#5b8fd6"], ["bike", "Rad", "#a283d8"], ["swim", "Schwimmen", "#3fb6cc"], ["strength", "Kraft", "#d0a03c"], ["other", "Sonst.", "#9aa5b4"]];

const sportHex = (k) => (SPORTS.find(([s]) => s === k) || [])[2] || "#9aa5b4";

const dyn = (l, d) => Color.dynamic(new Color(l), new Color(d));
const COL = {
  bg: dyn("#f2f4f7", "#0f1318"), card: dyn("#ffffff", "#1a2029"), text: dyn("#1c2430", "#e6eaf0"), muted: dyn("#5d6877", "#9aa5b4"),
  ok: dyn("#1f7a4d", "#6fd4a0"), warn: dyn("#9a6400", "#f0c060"), bad: dyn("#b3261e", "#f2a29c"), none: dyn("#5d6877", "#9aa5b4"),
  accent: dyn("#3b6ea8", "#7ba3dc"), race: dyn("#c2410c", "#fb923c"),
  tile: dyn("#eef1f5", "#1a2029"), line: dyn("#d6dbe3", "#2b323d"),
};
// Groesse des Widgets in Punkten nach Bildschirmbreite (Apple-Sollwerte fuer Mittel/Gross); Innenbreite = ohne Rand
const WIDGET_SIZES = [[430, 364, 170], [428, 364, 170], [414, 360, 169], [393, 338, 158], [390, 338, 158], [375, 321, 148]];
function widgetSize() {
  const sw = Math.min(Device.screenSize().width, Device.screenSize().height);
  const hit = WIDGET_SIZES.find(([w]) => sw >= w) || WIDGET_SIZES[WIDGET_SIZES.length - 1];
  return { w: hit[1], h: hit[2] };
}
const widgetInnerWidth = () => widgetSize().w - 26;
const ZONE_RGB = { ok: "#2f9d64", warn: "#d99a1a", bad: "#d0453b", none: "#8a94a3" };
const baseUrl = () => (Keychain.get(KEY_URL).match(/^https:\/\/[^\/\s?#]+/) || [Keychain.get(KEY_URL)])[0];

/* ---------- Konfiguration ---------- */
async function askConfig() {
  const a = new Alert();
  a.title = "Trainings-Widget einrichten";
  a.message = "Adresse deines Workers (nur https://\u2026.workers.dev) und Dashboard-Token. Beides bleibt im Schl\u00fcsselbund dieses iPhones.";
  a.addTextField("https://\u2026.workers.dev", Keychain.contains(KEY_URL) ? Keychain.get(KEY_URL) : "");
  a.addSecureTextField("Token", "");
  a.addAction("Speichern");
  a.addCancelAction("Abbrechen");
  if ((await a.presentAlert()) === -1) return false;
  // Nur Protokoll und Host zaehlen: Ein angehaengter Pfad wie /dashboard/ wuerde /api/widget verfaelschen.
  const url = (a.textFieldValue(0).trim().match(/^https:\/\/[^\/\s?#]+/) || [""])[0];
  const token = a.textFieldValue(1).trim();
  if (!url || !token) return false;
  Keychain.set(KEY_URL, url);
  Keychain.set(KEY_TOKEN, token);
  return true;
}

/* ---------- Daten laden (mit Cache f\u00fcr Offline) ---------- */
function cachePath() { const fm = FileManager.local(); return { fm, path: fm.joinPath(fm.documentsDirectory(), cacheFile()) }; }

async function fetchJson(base, path) {
  const req = new Request(`${base}${path}`);
  req.headers = { Authorization: `Bearer ${Keychain.get(KEY_TOKEN)}` };
  req.timeoutInterval = 25;
  const body = await req.loadString();
  const status = req.response ? req.response.statusCode : 0;
  if (status === 401) throw new Error("Token wurde abgelehnt");
  if (status === 503) throw new Error("Worker: DASHBOARD_TOKEN nicht gesetzt");
  if (status === 404) throw new Error(`404 bei ${base}${path} \u2013 falsche Adresse oder Deploy noch nicht durch`);
  if (status !== 200) throw new Error(`Worker antwortet mit ${status}`);
  return JSON.parse(body);
}
const widgetPath = (view) => `/api/widget${view ? "?view=" + view : ""}`;

// Geplante Einheiten ab heute aus dem Dashboard-Kalender (fuer die Kacheln "Naechste Tage"). Vergangene Tage
// stehen dort nicht mehr drin.
async function fetchUpcoming(base) {
  const dash = await fetchJson(base, "/api/dashboard");
  return (dash.planned || []).map((e) => ({
    date: e.date,
    name: String(e.name || "").replace(/[ \t]*#key\b/gi, "").trim() || null,
    sport: e.sport,
    durationMin: e.durationMin,
    key: (e.tags || []).some((t) => /^#?key$/i.test(String(t).trim())) || /#key\b/i.test(`${e.name || ""}\n${e.description || ""}`),
  }));
}

// Gross: Hauptdaten plus Plan der naechsten Tage (data.upcoming). Klein/Mittel (Schlaf, Ernaehrung): diese Daten plus das
// Rennziel (data.goal), damit die Phase ueberall aus demselben Renndatum kommt. Zusatzabfragen duerfen scheitern.
async function loadData() {
  const base = baseUrl();
  const { fm, path } = cachePath();
  try {
    const view = endpointView();
    const data = await fetchJson(base, widgetPath(view));
    if (view === "") data.upcoming = await fetchUpcoming(base).catch(() => null);
    else if (view === "small") data.goal = (await fetchJson(base, widgetPath("")).catch(() => null))?.goal ?? null;
    fm.writeString(path, JSON.stringify(data));
    return { data, stale: false, error: null };
  } catch (e) {
    if (fm.fileExists(path)) return { data: JSON.parse(fm.readString(path)), stale: true, error: String(e.message || e) };
    throw e;
  }
}

/* ---------- Format ---------- */
const fmt = (n, d = 0) => (n == null || !Number.isFinite(Number(n)) ? "\u2013" : Number(n).toLocaleString("de-DE", { minimumFractionDigits: d, maximumFractionDigits: d }));
const fmtTime = (s) => { const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = Math.round(s % 60); return h ? `${h}:${String(m).padStart(2, "0")}:${String(x).padStart(2, "0")}` : `${m}:${String(x).padStart(2, "0")}`; };
const fmtPace = (s) => `${Math.floor(s / 60)}:${String(Math.round(s % 60)).padStart(2, "0")}`;
const dateShort = (iso) => { const [, m, d] = iso.split("-"); return `${d}.${m}.`; };
const colorFor = (cls) => COL[cls] || COL.none;
const WEEKDAYS = ["SO", "MO", "DI", "MI", "DO", "FR", "SA"];
const weekdayOf = (iso) => WEEKDAYS[new Date(iso + "T12:00:00").getDay()];

/* ---------- Rennphase (zentral, eine Funktion) ---------- */
// Schwellen in Tagen bis zum Rennen (Renntag = 0, danach negativ)
const PHASE_DAYS = { taperFrom: 7, recoveryDays: 3 };
const REDUCED_PHASES = ["taper"]; // bewusst reduzierte Trainingswoche
const PHASE_LABEL = { taper: "Taper", recovery: "Regeneration" };
// Renndistanz aus dem Namen des Ziels (Renndatum und Zielzeit kommen aus den Daten selbst)
const raceKm = (name) => (/halb/i.test(name) ? 21.0975 : /marathon/i.test(name) ? 42.195 : /10\s*k/i.test(name) ? 10 : /5\s*k/i.test(name) ? 5 : 21.0975);
function racePhase(daysToGo) {
  let name = "normal";
  if (daysToGo != null) {
    if (daysToGo > PHASE_DAYS.taperFrom) name = "normal";
    else if (daysToGo >= 0) name = "taper";
    else if (daysToGo >= -PHASE_DAYS.recoveryDays) name = "recovery";
  }
  return { name, reduced: REDUCED_PHASES.includes(name), label: PHASE_LABEL[name] || null };
}
// Bereitschaftskreise: Skala ab 1 = bestmoeglich, niedrig ist gut. Farbe allein aus dem Wert.

/* ---------- Bausteine ---------- */
function text(parent, str, size, { bold = false, color = COL.text, lines = 1, align = "left", opacity = 1, minScale = 0.75 } = {}) {
  const t = parent.addText(String(str));
  t.font = bold ? Font.boldSystemFont(size) : Font.systemFont(size);
  t.textColor = color;
  t.lineLimit = lines;
  t.minimumScaleFactor = minScale;
  t.textOpacity = opacity;
  if (align === "right") t.rightAlignText(); else if (align === "center") t.centerAlignText();
  return t;
}

// Karte mit fester Breite, damit die Zeilen die ganze Widget-Breite fuellen
function card(parent, width, pad = 8, radius = 12) {
  const s = parent.addStack();
  s.layoutVertically();
  s.backgroundColor = COL.card;
  s.cornerRadius = radius;
  s.setPadding(pad, pad + 3, pad, pad + 3);
  s.size = new Size(width, 0);
  return s;
}

function newCtx(w, h) {
  const dc = new DrawContext();
  w = Math.max(1, Math.round(w) || 1); h = Math.max(1, Math.round(h) || 1);
  dc.size = new Size(w, h);
  dc.opaque = false;
  dc.respectScreenScale = true;
  // Ein Kontext ohne jede Zeichnung liefert in Scriptable kein Bild, sondern null: fast unsichtbare Flaeche vorab
  dc.setFillColor(new Color("#000000", 0.004));
  dc.fillRect(new Rect(0, 0, w, h));
  return dc;
}

/* ---------- Widget ---------- */
const DAY_FULL = ["Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"];
const signed = (n, d = 0) => (n == null ? "–" : `${n > 0 ? "+" : n < 0 ? "−" : ""}${fmt(Math.abs(n), d)}`);

// Mini-Kurve ueber die Breite: Lücken (null) bleiben offen, optional Referenzlinie (gestrichelt oder fest) und Punkt am Ende.
function sparkImage(w, h, values, hex, { ref = null, dashed = false, dot = false } = {}) {
  const dc = newCtx(w, h), pts = values.map((v, i) => [i, v]).filter(([, v]) => v != null);
  if (pts.length < 2) {
    // Zu wenig Werte: nur eine blasse Grundlinie (ein leerer DrawContext liefert in Scriptable kein Bild, sondern null)
    dc.setFillColor(new Color("#8a94a3", 0.3));
    dc.fillRect(new Rect(0, h - 3, w, 1.5));
    return dc.getImage();
  }
  const all = pts.map(([, v]) => v).concat(ref != null ? [ref] : []);
  const lo = Math.min(...all), hi = Math.max(...all), span = hi - lo || 1, pad = 4;
  const X = (i) => pad + (i / (values.length - 1)) * (w - 2 * pad), Y = (v) => h - pad - ((v - lo) / span) * (h - 2 * pad);
  if (ref != null) {
    dc.setFillColor(new Color("#8a94a3", 0.55));
    if (dashed) for (let x = 0; x < w; x += 7) dc.fillRect(new Rect(x, Y(ref) - 0.5, 4, 1));
    else dc.fillRect(new Rect(0, Y(ref) - 0.5, w, 1));
  }
  dc.setStrokeColor(new Color(hex));
  dc.setLineWidth(2.2);
  let p = new Path(), pen = false;
  for (const [i, v] of pts) {
    const pt = new Point(X(i), Y(v));
    if (pen && values[i - 1] != null) p.addLine(pt); else p.move(pt);
    pen = true;
  }
  dc.addPath(p);
  dc.strokePath();
  if (dot) { const [i, v] = pts[pts.length - 1]; dc.setFillColor(new Color(hex)); dc.fillEllipse(new Rect(X(i) - 3.5, Y(v) - 3.5, 7, 7)); }
  return dc.getImage();
}

// Kleine Kapsel mit getoenter Flaeche (Status, Rennen, Hinweise)
function chip(parent, str, hex, size = 11, textColor = null) {
  const s = parent.addStack();
  s.backgroundColor = new Color(hex, 0.2);
  s.cornerRadius = 9;
  s.setPadding(2, 9, 2, 9);
  text(s, str, size, { bold: false, color: textColor || new Color(hex) });
  return s;
}

// Bereitschaft: Koerper (objektiv, gegen die eigenen Mediane) und Gefuehl (subjektiv, Skala 1 = bestmoeglich)
const sleepCls = (h) => (h == null ? "none" : h >= 7 ? "ok" : h >= 6 ? "warn" : "bad");
function hrvState(hrv, med) {
  if (hrv == null || med == null || med <= 0) return { arrow: "", cls: "none" };
  const q = hrv / med;
  return q < 0.85 ? { arrow: " ↓", cls: "bad" } : q < 0.95 ? { arrow: " ↓", cls: "warn" } : q > 1.05 ? { arrow: " ↑", cls: "ok" } : { arrow: " →", cls: "ok" };
}
const restCls = (v, med) => (v == null || med == null ? "none" : v - med >= 6 ? "bad" : v - med >= 3 ? "warn" : "ok");
// Gefuehl nur so einstufen wie das Dashboard: gegen den eigenen ueblichen Bereich (Skala 1 = bestmoeglich).
// Ohne genug Vergleichswerte (cls "none") gibt es kein Urteil, der Wert allein sagt nichts.
function feelWord(it) {
  if (!it || it.v == null) return { txt: "fehlt", cls: "none" };
  if (it.cls === "ok") return { txt: "gut", cls: "ok" };
  if (it.cls === "warn") return { txt: "mittel", cls: "warn" };
  if (it.cls === "bad") return { txt: "schlecht", cls: "bad" };
  return { txt: "kein Vergleich", cls: "none" };
}
const FEEL = [["Muskelkater", "Muskeln"], ["Stimmung", "Stimmung"], ["Motivation", "Motivation"]];

// Satz unter dem Urteil
function verdictLine(v, p) {
  if (v.cls === "ok") return p ? "Körper und Gefühl passen: trainieren wie geplant." : "Körper und Gefühl passen.";
  return v.sub || "";
}

// Zweck aus dem Plan nur, wenn er ein Satz ist und keine Intervall-/Zonenzeile
function usablePurpose(s) {
  const t = String(s || "").replace(/\bRPE\s*:?\s*\d[\d\s–-]*/gi, "").trim();
  if (t.length < 25 || t.split(/\s+/).length < 4) return null;
  if (/^[\d\-•*]/.test(t) || /\d\s*(%|x\b)|\bz[1-7]\b|\bbpm\b|\d+\s*min\b/i.test(t)) return null;
  return t;
}
// Warum: Plan-Zweck, sonst regelbasiert aus Phase, Bereitschaft und Einheit
function whyText(d, p, ph, v) {
  const pu = p && usablePurpose(p.purpose);
  if (pu) return pu;
  if (d.goal && d.goal.daysToGo === 0) return "Renntag: gleichmäßig starten und das Tempo nicht überziehen.";
  if (ph.name === "recovery") return "Nach dem Rennen zählt Erholung. Nur locker bewegen, nichts erzwingen.";
  if (ph.name === "taper") return "Der Umfang sinkt vor dem Rennen. Die Beine sollen frisch werden, nicht müde. Locker bleiben.";
  if (!p) return "Ruhetag: Erholung gehört zum Training. So kann der Reiz der letzten Tage wirken.";
  if (v.cls === "bad") return "Die Werte liegen schlechter als üblich. Die Einheit kürzen oder locker halten.";
  if (p.key) return "Schlüsseleinheit: Hier entsteht der Trainingsreiz. Nur durchziehen, wenn Körper und Gefühl passen.";
  if (p.sport === "strength") return "Kraft stützt Hüfte und Laufwirtschaftlichkeit. Sauber ausführen statt schwer.";
  if (/locker|easy|ruhig|regen|recovery|grundlagen/i.test(p.name || "")) return "Locker halten: Der Reiz kommt aus dem Umfang, nicht aus dem Tempo.";
  return "Gleichmäßig trainieren. Regelmäßigkeit bringt mehr als einzelne harte Tage.";
}

// Kurzlabel unter der Dauer in den Tagekacheln
function shortLabel(e) {
  const n = e.name || "";
  if (e.key) return "Schlüssel";
  if (/locker|easy|ruhig|regen|recovery|grundlagen/i.test(n)) return "locker";
  if (/intervall|tempo|schwelle|threshold|interval/i.test(n)) return "intensiv";
  if (e.sport === "strength") return "Kraft";
  if (e.sport === "bike") return "Rad";
  if (e.sport === "swim") return "Schwimmen";
  return n || "Einheit";
}
const addIso = (iso, n) => new Date(Date.parse(iso + "T12:00:00Z") + n * 86400000).toISOString().slice(0, 10);

function dayTile(row, TW, iso, g, upcoming) {
  const race = !!g && g.date === iso;
  const items = upcoming ? upcoming.filter((e) => e.date === iso) : null;
  const t = row.addStack();
  t.layoutVertically();
  t.size = new Size(TW, 0);
  t.cornerRadius = 12;
  t.setPadding(6, 9, 6, 9);
  t.backgroundColor = race ? new Color("#fb923c", 0.16) : COL.tile;
  let main, sub;
  if (race) { main = "Rennen"; sub = g.name; }
  else if (!items) { main = "–"; sub = "fehlt"; }
  else if (!items.length) { main = "Pause"; sub = "frei"; }
  else {
    const mins = items.reduce((a, e) => a + (e.durationMin || 0), 0);
    main = mins ? `${mins} min` : items[0].name || "Einheit";
    sub = shortLabel(items[0]);
  }
  text(t, dayShort(iso), 11, { color: race ? COL.race : COL.muted, minScale: 1 });
  text(t, main, 16, { bold: true, color: race ? COL.race : COL.text });
  text(t, sub, 11, { color: race ? COL.race : COL.muted, minScale: 0.9 });
}

function buildWidget(res) {
  const d = res.data;
  const W = widgetInnerWidth();
  const g = d.goal, ph = racePhase(g ? g.daysToGo : null);
  const r = d.readiness, v = r.verdict, p = d.plan.today[0], tr = d.trend || { hrvMedian: null, restingMedian: null };
  const w = new ListWidget();
  w.backgroundColor = COL.card;
  w.setPadding(10, 13, 8, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);
  const rule = () => { const s = w.addStack(); s.size = new Size(W, 0.5); s.backgroundColor = COL.line; };

  // 1. Kopf: Wochentag und Datum links, Rennen-Countdown als Kapsel rechts (nur bis zum Renntag)
  const head = w.addStack(); head.centerAlignContent(); head.size = new Size(W, 0);
  const dt = new Date(d.today + "T12:00:00");
  text(head, `${DAY_FULL[dt.getDay()]} ${dateShort(d.today)}`, 13, { color: COL.muted });
  head.addSpacer();
  if (g && g.daysToGo != null && g.daysToGo >= 0) chip(head, g.daysToGo === 0 ? `${g.name} heute` : `${g.name} in ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "en"}`, "#fb923c", 13, COL.race);
  w.addSpacer(4);

  // 2. Bereitschaft: Urteil gross, ein Satz, darunter Koerper (objektiv) und Gefuehl (subjektiv)
  text(w, v.text, 34, { bold: true, color: v.cls === "none" ? COL.muted : colorFor(v.cls), minScale: 0.6 });
  text(w, verdictLine(v, p), 13, { color: COL.muted, lines: 2 });
  w.addSpacer(8);
  const CW = Math.floor((W - 16) / 2);
  const cols = w.addStack(); cols.size = new Size(W, 0); cols.spacing = 16;
  const col = (title) => { const c = cols.addStack(); c.layoutVertically(); c.size = new Size(CW, 0); text(c, title, 11, { color: COL.muted, minScale: 1 }); c.addSpacer(2); return c; };
  const line = (c, label, val, cls) => {
    const l = c.addStack(); l.centerAlignContent(); l.size = new Size(CW, 0);
    text(l, "●", 11, { color: colorFor(cls || "none"), minScale: 1 });
    l.addSpacer(6);
    text(l, label, 14, { minScale: 0.8 });
    l.addSpacer();
    text(l, val, 14, { bold: true, color: cls === "none" && val === "fehlt" ? COL.muted : COL.text, align: "right", minScale: 0.8 });
    c.addSpacer(3);
  };
  const body = col("Körper"), feel = col("Gefühl");
  const hs = hrvState(r.hrv, tr.hrvMedian);
  line(body, "Schlaf", r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h` : "fehlt", sleepCls(r.sleepHours));
  line(body, "HRV", r.hrv != null ? `${fmt(r.hrv)}${hs.arrow}` : "fehlt", r.hrv != null ? hs.cls : "none");
  line(body, "Ruhepuls", r.restingHR != null ? fmt(r.restingHR) : "fehlt", restCls(r.restingHR, tr.restingMedian));
  for (const [lab, short] of FEEL) {
    const f = feelWord((r.items || []).find((i) => i.label === lab));
    line(feel, short, f.txt, f.cls);
  }
  w.addSpacer(4);
  rule();
  w.addSpacer(7);

  // 3. Heute: Einheitenname gross, darunter Dauer, Distanz und RPE aus dem Plan
  text(w, "Heute", 11, { color: COL.muted, minScale: 1 });
  if (p) {
    text(w, p.name || "Einheit", 24, { bold: true, minScale: 0.7 });
    const meta = [p.durationMin && `${p.durationMin} min`, p.distanceKm && `${fmt(p.distanceKm, 1)} km`].filter(Boolean).join(" · ");
    if (meta || p.rpe) {
      const m = w.addStack(); m.bottomAlignContent();
      if (meta) text(m, meta, 16, { bold: true });
      if (p.rpe) text(m, `${meta ? " · " : ""}RPE ${p.rpe}`, 16, { color: COL.muted });
    }
  } else {
    text(w, "Keine Einheit geplant", 24, { bold: true, minScale: 0.7 });
    if (d.plan.next) text(w, `Nächste: ${weekdayOf(d.plan.next.date)} ${dateShort(d.plan.next.date)} ${d.plan.next.name || "Einheit"}`, 13, { color: COL.muted });
  }
  w.addSpacer(6);

  // 4. Warum: Akzentstrich links, 1 bis 2 Saetze
  const why = whyText(d, p, ph, v);
  const perLine = Math.max(20, Math.floor((W - 14) / 6.6));
  const nLines = Math.min(3, Math.max(1, Math.ceil(why.length / perLine)));
  const wr = w.addStack(); wr.size = new Size(W, 16 + nLines * 16 + 2);
  const bar = wr.addStack(); bar.size = new Size(3, 16 + nLines * 16 + 2); bar.backgroundColor = COL.accent; bar.cornerRadius = 0;
  wr.addSpacer(10);
  const wt = wr.addStack(); wt.layoutVertically();
  text(wt, "Warum", 13, { color: COL.accent });
  text(wt, why, 14, { lines: 3, minScale: 0.85 });
  w.addSpacer(8);

  // 5. Naechste Tage: drei Kacheln
  const TW = Math.floor((W - 12) / 3);
  const tiles = w.addStack(); tiles.size = new Size(W, 0); tiles.spacing = 6;
  for (let i = 1; i <= 3; i++) dayTile(tiles, TW, addIso(d.today, i), g, d.upcoming || null);
  if (res.stale) { w.addSpacer(3); text(w, "veraltet, keine Verbindung", 11, { color: COL.warn, minScale: 1 }); }
  w.addSpacer();
  return w;
}

/* ---------- Zweite Ansicht: Details (Widget-Parameter "detail") ---------- */
function formImage(w, h, form, zones, raceDay) {
  const dc = newCtx(w, h);
  const n = form.length, x = (i) => (i / (n - 1)) * w;
  const splitY = Math.round(h * 0.64);                         // oben CTL/ATL, unten TSB
  const vals = form.flatMap((f) => [f.ctl, f.atl]).filter((v) => v != null);
  const max = Math.max(10, ...vals), yTop = (v) => splitY - 4 - (v / max) * (splitY - 10);
  const line = (key, hex) => {
    const p = new Path();
    let pen = false;
    form.forEach((f, i) => {
      if (f[key] == null) { pen = false; return; }
      const pt = new Point(x(i), yTop(f[key]));
      if (!pen) { p.move(pt); pen = true; } else p.addLine(pt);
    });
    dc.addPath(p);
    dc.setStrokeColor(new Color(hex));
    dc.setLineWidth(2);
    dc.strokePath();
  };
  line("atl", "#8a94a3");
  line("ctl", "#5b8fd0");
  const tsbs = form.map((f) => (f.ctl != null && f.atl != null ? f.ctl - f.atl : null));
  const tv = tsbs.filter((v) => v != null);
  const lo = Math.min(-15, ...tv), hi = Math.max(15, ...tv), bandTop = splitY + 6, bandH = h - bandTop - 1;
  const yz = (v) => bandTop + (1 - (v - lo) / (hi - lo)) * bandH;
  dc.setFillColor(new Color("#8a94a3", 0.35));
  dc.fillRect(new Rect(0, yz(0), w, 1));
  const bw = Math.max(2, (w / n) * 0.7);
  tsbs.forEach((v, i) => {
    if (v == null) return;
    const cls = v >= zones.ok ? "ok" : v >= zones.warn ? "warn" : "bad";
    dc.setFillColor(new Color(ZONE_RGB[cls], 0.85));
    dc.fillRect(new Rect(x(i) - bw / 2, Math.min(yz(v), yz(0)), bw, Math.max(1, Math.abs(yz(v) - yz(0)))));
  });
  return dc.getImage();
}

// Mittleres Widget (halb so gross): Form links, Halbmarathon-Zeiten gegen das Ziel rechts,
// darunter VDOT/Paces, Schwellen sowie Ernaehrung und Heisshunger.
function buildMedium(res) {
  const d = res.data;
  const W = widgetInnerWidth();
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(8, 13, 6, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);

  const row = w.addStack(); row.spacing = 7;
  const LW = Math.floor((W - 7) * 0.52), RW = W - 7 - LW;

  // Links: Form der letzten 28 Tage
  const L = d.load, fc = card(row, LW, 7);
  text(fc, "FORM \u00b7 28 TAGE", 10, { bold: true, color: COL.muted });
  fc.addSpacer(2);
  if (d.form.some((f) => f.ctl != null)) { const im = fc.addImage(formImage(LW - 22, 52, d.form, d.tsbZones)); im.imageSize = new Size(LW - 22, 52); }
  else text(fc, "Keine CTL/ATL-Werte", 10, { color: COL.muted });
  fc.addSpacer(2);
  const nums = fc.addStack(); nums.centerAlignContent();
  text(nums, `CTL ${fmt(L.ctl, 1)}`, 10, { bold: true, color: COL.accent });
  nums.addSpacer(5);
  text(nums, `ATL ${fmt(L.atl, 1)}`, 10, { bold: true, color: COL.muted });
  nums.addSpacer();
  text(nums, `TSB ${fmt(L.tsb, 1)}`, 10, { bold: true, color: colorFor(L.tsbCls) });

  // Rechts: Halbmarathon-Zeiten gegen das Ziel
  const rc = card(row, RW, 7), goal = d.goal.targetTimeSecs;
  const rh = rc.addStack(); rh.centerAlignContent();
  text(rh, "HALBMARATHON", 10, { bold: true, color: COL.muted });
  rh.addSpacer();
  text(rh, `${d.goal.daysToGo > 0 ? d.goal.daysToGo + " Tg" : ""}`, 10, { bold: true, color: COL.muted });
  rc.addSpacer(2);
  const line = (label, secs, bold, tone) => {
    const r = rc.addStack(); r.centerAlignContent();
    text(r, label, 10, { bold, color: bold ? COL.text : COL.muted });
    r.addSpacer();
    text(r, fmtTime(secs), 11, { bold: true, color: tone || COL.text });
    if (secs !== goal) { const diff = secs - goal; r.addSpacer(4); text(r, `${diff > 0 ? "+" : "\u2212"}${fmtTime(Math.abs(diff)).replace(/^0:/, "")}`, 9, { color: diff <= 0 ? COL.ok : COL.muted }); }
  };
  line("Ziel", goal, true, COL.accent);
  if (d.hm && d.hm.estimates.length) {
    // Platz fuer drei Zeilen: Runalyze-Prognose, VDOT-Rechnung und die schnellste Bestzeit-Rechnung
    const est = d.hm.estimates, best = est.filter((e) => e.key.startsWith("best-")).sort((a, b) => a.seconds - b.seconds)[0];
    const pick = [est.find((e) => e.kind === "prognosis"), est.find((e) => e.key === "vdot"), best].filter(Boolean).sort((a, b) => a.seconds - b.seconds);
    for (const e of pick) {
      const name = e.kind === "prognosis" ? "Runalyze" : e.key === "vdot" ? "VDOT" : e.label.replace("aus ", "").replace("-Bestzeit", "");
      line(name, e.seconds, false);
    }
    text(rc, "Rechnung nach Daniels, keine Vorhersage", 8, { color: COL.muted, lines: 1 });
  } else text(rc, "Noch kein Runalyze-Snapshot eingespielt", 9, { color: COL.muted, lines: 2 });
  w.addSpacer(5);

  // Unten: VDOT und Paces, Schwellen, Ernaehrung, Heisshunger
  const bc = card(w, W, 6), T = d.thresholds;
  const pz = d.vdot && d.vdot.paces ? Object.fromEntries(d.vdot.paces.map((p) => [p.key, p.pace.replace("/km", "")])) : null;
  text(bc, pz ? `VDOT ${fmt(d.vdot.value, 1)} \u00b7 Easy ${pz.easy} \u00b7 Marathon ${pz.marathon} \u00b7 Schwelle ${pz.threshold} \u00b7 Ziel ${fmtPace(goal / raceKm(d.goal.name))}` : "VDOT und Paces: noch kein Runalyze-Snapshot", 9, { color: COL.text });
  text(bc, `Schwellen: Lauf ${T.run.thresholdPaceSecPerKm ? fmtPace(T.run.thresholdPaceSecPerKm) + "/km" : "fehlt"} \u00b7 FTP ${T.bike.ftp ? T.bike.ftp + " W" : "fehlt"} \u00b7 Schwimmen ${T.swim.thresholdPaceSecPer100m ? fmtPace(T.swim.thresholdPaceSecPer100m) + "/100 m" : "fehlt"}`, 9, { color: COL.text });
  const last = [...d.nutrition.days].reverse().find((x) => x.calories != null);
  const nut = d.nutrition.hasData && last ? `Kalorien ${fmt(last.calories)}${last.goal ? " / " + fmt(last.goal) : ""} kcal (${dateShort(last.date)})` : "Ern\u00e4hrung: noch keine Daten";
  const cr = d.cravings.count ? `Hei\u00dfhunger 7 Tage: ${d.cravings.count}\u00d7${d.cravings.strongest ? `, st\u00e4rkster ${d.cravings.strongest.strength}` : ""}` : "Hei\u00dfhunger 7 Tage: keiner";
  const time = new Date(d.generatedAt).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
  text(bc, `${nut} \u00b7 ${cr} \u00b7 ${res.stale ? "veraltet" : time}`, 9, { color: res.stale ? COL.warn : COL.muted });
  return w;
}

/* ---------- Schlaf und Erholung, Ernaehrung (klein, mittel und unten im grossen Widget) ---------- */
const DAY_SHORT = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"];
const dayShort = (iso) => DAY_SHORT[new Date(iso + "T12:00:00").getDay()];

// Saeulen der letzten 7 Tage; fehlende Tage sind nur ein kurzer Strich, nie ein Wert. Optional Ziel-Marken.
const DAY_LABEL_H = 15;
function smallBarsImage(w, h, values, todayIso, dates, goals, refLine) {
  h -= DAY_LABEL_H; // Platz fuer die Wochentags-Kuerzel darunter
  const dc = newCtx(w, h + DAY_LABEL_H), slot = w / values.length, bw = slot * 0.6;
  dc.setFont(Font.systemFont(11));
  dc.setTextAlignedCenter();
  dates.forEach((dt, i) => {
    dc.setTextColor(dt === todayIso ? new Color("#e6eaf0") : new Color("#9aa5b4"));
    dc.setFont(dt === todayIso ? Font.boldSystemFont(11) : Font.systemFont(11));
    dc.drawTextInRect(dayShort(dt), new Rect(i * slot, h + 2, slot, DAY_LABEL_H - 2));
  });
  const max = Math.max(1, ...values.filter((v) => v != null), ...(goals || []).filter((v) => v != null), refLine ?? 0);
  values.forEach((v, i) => {
    const cx = i * slot + slot / 2;
    if (v == null) { dc.setFillColor(new Color("#8a94a3", 0.3)); dc.fillRect(new Rect(cx - bw / 2, h - 2, bw, 1.5)); return; }
    const bh = Math.max(2, (v / max) * (h - 3)), p = new Path();
    p.addRoundedRect(new Rect(cx - bw / 2, h - bh, bw, bh), 2.5, 2.5);
    dc.addPath(p);
    dc.setFillColor(new Color(dates[i] === todayIso ? "#7db0f5" : "#4f7fbf"));
    dc.fillPath();
    if (goals && goals[i] != null) { dc.setFillColor(new Color("#ffffff", 0.85)); dc.fillRect(new Rect(cx - bw / 2 - 1, h - (goals[i] / max) * (h - 3) - 1, bw + 2, 1.5)); }
  });
  if (refLine) {
    // Referenzlinie (z. B. 8 h Schlaf) als gestrichelte Linie ueber den Saeulen
    const y = h - (refLine / max) * (h - 3);
    dc.setFillColor(new Color("#ffffff", 0.6));
    for (let x = 0; x < w; x += 6) dc.fillRect(new Rect(x, y, 3, 1));
  }
  return dc.getImage();
}

function smallWidget() {
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(10, 11, 8, 11);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);
  return w;
}

// Inhalt "Schlaf und Erholung" in einen beliebigen Container (Widget oder Karte); compact = im grossen Widget
function fillSleep(w, d, IW, compact) {
  const s = d.sleep;
  text(w, "SCHLAF UND ERHOLUNG", 9, { bold: true, color: COL.muted });
  if (!compact) w.addSpacer(2);
  const last = s.latest, isToday = last && last.date === d.today;
  text(w, last ? `${fmt(last.hours, 1)} h` : "fehlt", compact ? 22 : 28, { bold: true, color: last ? COL.text : COL.muted });
  if (!compact) text(w, last ? (isToday ? "Schlaf heute" : `Schlaf am ${dateShort(last.date)}`) : "keine Schlafdauer in 7 Tagen", 9, { color: COL.muted });
  const t = s.days[s.days.length - 1];
  const cmp = (v, med) => (v == null || med == null ? "" : ` (\u00d8 ${fmt(med)})`);
  text(w, `HRV ${t.hrv != null ? fmt(t.hrv) : "fehlt"}${cmp(t.hrv, s.medianHrv)}`, compact ? 9 : 10, { bold: true });
  text(w, `Ruhepuls ${t.restingHR != null ? fmt(t.restingHR) : "fehlt"}${cmp(t.restingHR, s.medianRestingHR)}`, compact ? 9 : 10, { bold: true });
  w.addSpacer(compact ? 3 : 4);
  const missing = s.days.filter((x) => x.hours == null);
  if (missing.length >= 3 && s.days.length - missing.length <= 1) {
    // Mehrere Tage ohne Daten: eine Zeile statt lauter leerer Striche
    const cap = (iso) => { const n = weekdayOf(iso); return n[0] + n[1].toLowerCase(); };
    text(w, `keine Daten ${cap(missing[0].date)}\u2013${cap(missing[missing.length - 1].date)}`, 10, { bold: true, color: COL.warn });
  } else {
    const bh = (compact ? 20 : 30) + DAY_LABEL_H;
    const im = w.addImage(smallBarsImage(IW, bh, s.days.map((x) => x.hours), d.today, s.days.map((x) => x.date), null, 8));
    im.imageSize = new Size(IW, bh);
    if (!compact) text(w, "--- 8 h", 7, { color: COL.muted });
  }
  if (!compact) { w.addSpacer(); text(w, "Ruhepuls = Tageswert, \u00d8 = letzte 14 Tage", 7, { color: COL.muted }); }
}

function progressBar(w, IW, ratio, hex, alpha = 1) {
  const dc = newCtx(IW, 6), track = new Path();
  track.addRoundedRect(new Rect(0, 0, IW, 6), 3, 3);
  dc.addPath(track); dc.setFillColor(new Color("#8a94a3", 0.25)); dc.fillPath();
  if (ratio > 0) {
    const bar = new Path();
    bar.addRoundedRect(new Rect(0, 0, Math.max(6, Math.min(1, ratio) * IW), 6), 3, 3);
    dc.addPath(bar); dc.setFillColor(new Color(hex, alpha)); dc.fillPath();
  }
  const im = w.addImage(dc.getImage()); im.imageSize = new Size(IW, 6);
}

// Ring in Ampelfarbe; die Zahl kommt als Text darueber, damit sie in Hell und Dunkel lesbar ist
function ringImage(size, cls) {
  const dc = newCtx(size, size), rgb = ZONE_RGB[cls] || ZONE_RGB.none;
  dc.setFillColor(new Color(rgb, cls === "none" ? 0.12 : 0.24));
  dc.fillEllipse(new Rect(2, 2, size - 4, size - 4));
  dc.setStrokeColor(new Color(rgb));
  dc.setLineWidth(3);
  dc.strokeEllipse(new Rect(2, 2, size - 4, size - 4));
  return dc.getImage();
}

// Kleines Widget im Kartenlook (Karte = Widget-Flaeche)
function cardWidget() {
  const w = smallWidget();
  w.backgroundColor = COL.card;
  w.setPadding(14, 14, 12, 14);
  return w;
}

// Bereitschaft (klein): Urteil als Kapsel, Schlafdauer, die fuenf Skalen als Ringe (3 + 2). Daten wie im grossen Widget.
function buildReady(res) {
  const d = res.data, r = d.readiness, v = r.verdict, g = d.goal, IW = 134;
  const w = cardWidget();
  const head = w.addStack(); head.centerAlignContent();
  chip(head, v.text, ZONE_RGB[v.cls] || ZONE_RGB.none, 16, colorFor(v.cls));
  head.addSpacer();
  if (g && g.daysToGo >= 0) text(head, g.daysToGo === 0 ? "heute" : `${g.daysToGo} T`, 12, { bold: true, color: COL.race });
  w.addSpacer(2);
  text(w, r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h Schlaf` : "Schlafdauer fehlt", 11, { color: COL.muted });
  w.addSpacer(6);
  const short = { Muskelkater: "Muskeln", Motivation: "Motiv.", "Erm\u00fcdung": "Erm\u00fcdung" };
  const ring = (parent, it, cw) => {
    const col = parent.addStack(); col.layoutVertically(); col.size = new Size(cw, 0);
    const c = col.addStack(); c.addSpacer();
    const rg = c.addStack(); rg.size = new Size(26, 26); rg.backgroundImage = ringImage(26, it.cls); rg.centerAlignContent();
    rg.addSpacer(); text(rg, it.v == null ? "\u2013" : it.v, 12, { bold: true, align: "center" }); rg.addSpacer();
    c.addSpacer();
    const l = col.addStack(); l.addSpacer(); text(l, short[it.label] || it.label, 8, { color: COL.muted, align: "center" }); l.addSpacer();
  };
  const rows = [r.items.slice(0, 3), r.items.slice(3)];
  rows.forEach((items, ri) => {
    if (ri) w.addSpacer(5);
    const row = w.addStack(); row.size = new Size(IW, 0);
    if (ri) row.addSpacer(IW / 6);
    items.forEach((it) => ring(row, it, IW / 3));
  });
  if (v.cls === "none") { w.addSpacer(3); text(w, v.sub, 8, { color: COL.muted, lines: 2 }); }
  w.addSpacer();
  if (res.stale) text(w, "veraltet", 8, { color: COL.warn });
  return w;
}

// Fitness (CTL): Wert gross, Veraenderung gegen vor 6 Wochen als Kapsel, Wochenkurve mit Punkt am Ende
function buildFitness(res) {
  const d = res.data, f = d.fitness || { ctl: null, delta: null, weekly: [] }, IW = 134;
  const w = cardWidget();
  text(w, "Fitness (CTL)", 13, { color: COL.muted });
  w.addSpacer(2);
  const row = w.addStack(); row.centerAlignContent();
  text(row, f.ctl != null ? fmt(f.ctl) : "fehlt", 40, { bold: true, color: f.ctl != null ? COL.accent : COL.muted });
  row.addSpacer();
  if (f.delta != null) chip(row, signed(f.delta), f.delta > 0 ? ZONE_RGB.ok : f.delta < 0 ? ZONE_RGB.warn : ZONE_RGB.none, 14, f.delta > 0 ? COL.ok : f.delta < 0 ? COL.warn : COL.muted);
  w.addSpacer(4);
  const im = w.addImage(sparkImage(IW, 44, f.weekly, "#6da2dc", { dot: true })); im.imageSize = new Size(IW, 44);
  w.addSpacer();
  text(w, "6 Wochen", 13, { color: COL.muted });
  if (res.stale) text(w, "veraltet", 8, { color: COL.warn });
  return w;
}

// Ernaehrung (klein): Protein gross, darunter Energie mit Balken. Ziele immer aus Yazio.
function buildFoodCard(res) {
  const d = res.data, f = d.food, IW = 134;
  const hasVals = (x) => x && [x.calories, x.protein, x.carbs, x.fat].some((v) => v != null);
  let td = f.days[f.days.length - 1] || {};
  const has = hasVals(td), prev = !has ? f.days.slice(0, -1).reverse().find(hasVals) : null;
  if (prev) td = prev;
  const show = has || !!prev, yg = f.goals || {};
  const big = { label: "Protein", v: td.protein, goal: yg.proteinG ?? null };
  const kcalGoal = td.goal ?? yg.kcal ?? null;
  const w = cardWidget(), dim = !has;
  const head = w.addStack(); head.centerAlignContent();
  text(head, "Ern\u00e4hrung", 13, { color: COL.muted });
  head.addSpacer();
  if (prev) text(head, prev === f.days[f.days.length - 2] ? "gestern" : dateShort(prev.date), 11, { bold: true, color: COL.muted });
  w.addSpacer(1);
  text(w, show && big.v != null ? `${fmt(big.v)} g` : "\u2013", 36, { bold: true, color: dim ? COL.muted : COL.text });
  text(w, `${big.label}${big.goal ? ` \u00b7 Ziel ${fmt(big.goal)}` : ""}`, 13, { color: COL.muted });
  w.addSpacer();
  text(w, show && td.calories != null ? `${fmt(td.calories)}${kcalGoal ? ` / ${fmt(kcalGoal)}` : ""} kcal` : kcalGoal ? `Ziel ${fmt(kcalGoal)} kcal` : "kcal \u2013", 15, { bold: true, color: dim ? COL.muted : COL.text });
  w.addSpacer(4);
  progressBar(w, IW, show && td.calories != null && kcalGoal ? td.calories / kcalGoal : 0, "#5b82c0", dim ? 0.5 : 1);
  if (!has) { w.addSpacer(3); text(w, prev ? "Yazio heute noch nicht synchron" : "Yazio noch nicht synchron", 8, { bold: true, color: COL.warn }); }
  return w;
}

function buildSleep(res) { const w = smallWidget(); fillSleep(w, res.data, 134); return w; }

/* ---------- Mittleres Widget: Ernaehrung heute ---------- */
// Fortschrittsring (Start oben, im Uhrzeigersinn); ratio wird auf 0..1 begrenzt
function ringProgressImage(size, ratio, hex, alpha = 1) {
  const dc = newCtx(size, size), lw = 10, r = (size - lw) / 2, c = size / 2;
  dc.setStrokeColor(new Color("#8a94a3", 0.25));
  dc.setLineWidth(lw);
  dc.strokeEllipse(new Rect(lw / 2, lw / 2, size - lw, size - lw));
  const q = Math.max(0, Math.min(1, ratio || 0));
  if (q > 0) {
    const pt = (a) => new Point(c + r * Math.cos(a), c + r * Math.sin(a)), a0 = -Math.PI / 2, steps = Math.max(2, Math.ceil(q * 120));
    const p = new Path();
    p.move(pt(a0));
    for (let i = 1; i <= steps; i++) p.addLine(pt(a0 + (i / steps) * q * 2 * Math.PI));
    dc.addPath(p);
    dc.setStrokeColor(new Color(hex, alpha));
    dc.setLineWidth(lw);
    dc.strokePath();
    dc.setFillColor(new Color(hex, alpha));
    for (const a of [a0, a0 + q * 2 * Math.PI]) dc.fillEllipse(new Rect(pt(a).x - lw / 2, pt(a).y - lw / 2, lw, lw));
  }
  return dc.getImage();
}

function buildFoodMedium(res) {
  const d = res.data, f = d.food, W = widgetInnerWidth();
  const hasVals = (x) => x && [x.calories, x.protein, x.carbs, x.fat].some((v) => v != null);
  let td = f.days[f.days.length - 1] || {};
  const has = hasVals(td);
  // Heute noch keine Yazio-Werte: gestern abgedunkelt zeigen, sonst nur der Hinweis (keine leeren Balken)
  const yesterday = !has ? f.days[f.days.length - 2] : null;
  const stale = hasVals(yesterday);
  if (stale) td = yesterday;
  const show = has || stale, op = stale ? 0.55 : 1;
  const w = new ListWidget();
  w.backgroundColor = COL.card;
  w.setPadding(10, 13, 8, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);

  const head = w.addStack(); head.centerAlignContent(); head.size = new Size(W, 0);
  text(head, stale ? "Ernährung gestern" : "Ernährung heute", 13, { color: COL.muted });
  head.addSpacer();
  w.addSpacer(6);

  if (!show) {
    w.addSpacer();
    const m = w.addStack(); m.size = new Size(W, 0); m.addSpacer();
    text(m, "Yazio nicht synchron", 16, { bold: true, color: COL.warn });
    m.addSpacer();
    w.addSpacer();
  } else {
    // Ziele immer aus Yazio, keine eigenen Platzhalter
    const yg = f.goals || {};
    const kcalGoal = td.goal ?? yg.kcal ?? null;
    const RS = 92, GAP = 14, RW = W - RS - GAP;
    const row = w.addStack(); row.centerAlignContent(); row.size = new Size(W, 0); row.spacing = GAP;
    // Links: Kalorienring, Zahl und Ziel darin
    const ring = row.addStack(); ring.size = new Size(RS, RS); ring.layoutVertically(); ring.centerAlignContent();
    ring.backgroundImage = ringProgressImage(RS, td.calories != null && kcalGoal ? td.calories / kcalGoal : 0, "#7ba3dc", stale ? 0.5 : 1);
    const cen = (str, size, o) => { const s = ring.addStack(); s.addSpacer(); text(s, str, size, { align: "center", minScale: 0.6, ...o }); s.addSpacer(); };
    cen(td.calories != null ? fmt(td.calories) : "fehlt", td.calories != null ? 24 : 16, { bold: false, color: td.calories != null ? COL.text : COL.muted, opacity: op });
    if (kcalGoal) cen(`von ${fmt(kcalGoal)}`, 12, { color: COL.muted, minScale: 0.85 });
    // Rechts: Protein, Kohlenhydrate, Fett als Balken (g / Ziel g); fehlende Werte als Text ohne Balken
    const col = row.addStack(); col.layoutVertically(); col.size = new Size(RW, 0);
    const macros = [
      { label: "Protein", v: td.protein, goal: yg.proteinG ?? null, hex: "#7ba3dc" },
      { label: "Kohlenhydrate", v: td.carbs, goal: yg.carbsG ?? null, hex: "#7ba3dc" },
      { label: "Fett", v: td.fat, goal: yg.fatG ?? null, hex: "#7ba3dc" },
    ];
    macros.forEach((m, i) => {
      if (i) col.addSpacer(7);
      const l = col.addStack(); l.centerAlignContent(); l.size = new Size(RW, 0);
      text(l, m.label, 15, { minScale: 0.8 });
      l.addSpacer();
      if (m.v == null) text(l, "fehlt", 14, { color: COL.muted, minScale: 1 });
      else {
        text(l, fmt(m.v), 17, { bold: true, opacity: op, minScale: 0.8 });
        if (m.goal) text(l, ` / ${fmt(m.goal)} g`, 13, { color: COL.muted, minScale: 0.8 });
        else text(l, " g", 13, { color: COL.muted });
      }
      if (m.v != null && m.goal) { col.addSpacer(3); progressBar(col, RW, m.v / m.goal, m.hex, stale ? 0.5 : 1); }
    });
  }
  if (res.stale || d.sourcesFailed.length) {
    w.addSpacer(3);
    text(w, res.stale ? "veraltet, keine Verbindung" : `Quelle fehlt: ${d.sourcesFailed.join(", ")}`, 11, { color: res.stale ? COL.warn : COL.bad, minScale: 1 });
  }
  w.addSpacer();
  return w;
}

/* ---------- Mittleres Widget "training": Disziplinen ---------- */
// Anteile der Sportarten an der Trainingszeit als ein Balken mit Prozentzahl im Segment; Soll als helle Striche
function splitImage(w, h, share, target) {
  const dc = newCtx(w, h), keys = ["swim", "bike", "run"], tot = keys.reduce((a, k) => a + share[k], 0) || 1;
  let acc = 0;
  dc.setFont(Font.boldSystemFont(8));
  dc.setTextAlignedCenter();
  for (const k of keys) {
    const a = (acc / tot) * w, b = ((acc + share[k]) / tot) * w;
    if (b - a > 1) {
      const p = new Path(); p.addRoundedRect(new Rect(a + 0.5, 0, b - a - 1, h), 3, 3);
      dc.addPath(p); dc.setFillColor(new Color(sportHex(k))); dc.fillPath();
      if (b - a > 26) { dc.setTextColor(new Color("#ffffff")); dc.drawTextInRect(`${share[k]} %`, new Rect(a, 2, b - a, h - 2)); }
    }
    acc += share[k];
  }
  if (target) {
    dc.setFillColor(new Color("#ffffff", 0.95));
    for (const at of [target.swim, target.swim + target.bike]) dc.fillRect(new Rect((at / 100) * w - 0.5, -1, 1.5, h + 2));
  }
  return dc.getImage();
}

function buildTraining(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(9, 13, 6, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);
  const head = w.addStack(); head.centerAlignContent(); head.size = new Size(W, 0);
  text(head, "TRAINING · DISZIPLINEN", 10, { bold: true, color: COL.muted });
  head.addSpacer();
  text(head, res.stale ? "veraltet, keine Verbindung" : d.sourcesFailed.length ? `Quelle fehlt: ${d.sourcesFailed.join(", ")}` : "diese Woche", 9, { color: res.stale ? COL.warn : d.sourcesFailed.length ? COL.bad : COL.muted });
  w.addSpacer(3);
  const row = w.addStack(); row.spacing = 7;
  const CW = Math.floor((W - 14) / 3), IW = CW - 20;
  for (const [k, name] of [["swim", "SCHWIMMEN"], ["bike", "RAD"], ["run", "LAUF"]]) {
    const s = d.sports[k], hex = sportHex(k);
    const c = card(row, CW, 5);
    const nl = c.addStack(); nl.centerAlignContent(); nl.spacing = 3;
    const dot = nl.addText("●"); dot.font = Font.systemFont(7); dot.textColor = new Color(hex);
    text(nl, name, 9, { bold: true, color: COL.muted });
    nl.addSpacer();
    const since = s.daysSince == null ? "–" : s.daysSince === 0 ? "heute" : `vor ${s.daysSince} T`;
    text(nl, since, 8, { bold: s.daysSince != null && s.daysSince >= 7, color: s.daysSince != null && s.daysSince >= 7 ? COL.warn : COL.muted });
    // Wochenvolumen: Kilometer gegen Plan (Plan nur, wenn im Kalender Distanzen stehen) und Trainingszeit
    const dec = (v) => fmt(v, v < 10 ? 1 : 0);
    const vl = c.addStack(); vl.bottomAlignContent(); vl.spacing = 3;
    text(vl, `${dec(s.weekKm)}`, 20, { bold: true });
    text(vl, s.plannedKm ? `/ ${dec(s.plannedKm)} km` : "km", 10, { color: COL.muted });
    c.addSpacer(4);
    progressBar(c, IW, s.plannedKm ? s.weekKm / s.plannedKm : 0, hex);
    c.addSpacer(4);
    const mins = Math.round(s.weekMinutes || 0);
    text(c, mins ? `${Math.floor(mins / 60)}:${String(mins % 60).padStart(2, "0")} h Training` : "noch kein Training", 9, { color: COL.muted });
  }
  w.addSpacer(4);
  const sc = card(w, W, 5), sp = d.split;
  const sh = sc.addStack(); sh.centerAlignContent();
  text(sh, `ZEITVERTEILUNG · ${sp.weeks} WOCHEN`, 9, { bold: true, color: COL.muted });
  sh.addSpacer();
  if (sp.target) text(sh, `Soll S ${sp.target.swim} · R ${sp.target.bike} · L ${sp.target.run} %`, 9, { color: COL.muted });
  if (sp.share) {
    sc.addSpacer(3);
    const im = sc.addImage(splitImage(W - 22, 14, sp.share, sp.target)); im.imageSize = new Size(W - 22, 14);
  } else text(sc, "Noch keine abgeschlossene Woche mit Training.", 9, { color: COL.muted });
  return w;
}

function messageWidget(msg) {
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  text(w, "Trainings-Widget", 14, { bold: true });
  text(w, msg, 12, { color: COL.muted, lines: 6 });
  return w;
}

/* ---------- Start ---------- */
async function main() {
  // Widget und Siri/Kurzbefehle koennen keine Dialoge zeigen: dort direkt die Ansicht bauen, ohne Menue
  const inWidget = config.runsInWidget || config.runsWithSiri;
  if (!inWidget) {
    const menu = new Alert();
    menu.title = "Trainings-Widget";
    menu.addAction("Vorschau (gro\u00df)");
    menu.addAction("Vorschau mittel: Form und Halbmarathon-Zeiten");
    menu.addAction("Vorschau mittel: Training (Disziplinen)");
    menu.addAction("Vorschau mittel: Ern\u00e4hrung heute");
    menu.addAction("Vorschau klein: Schlaf");
    menu.addAction("Vorschau klein: Ern\u00e4hrung");
    menu.addAction("Vorschau klein: Fitness (CTL)");
    menu.addAction("Vorschau klein: Bereitschaft");
    menu.addAction("Zugangsdaten setzen / zur\u00fccksetzen");
    menu.addCancelAction("Schlie\u00dfen");
    const choice = await menu.presentAlert();
    if (choice === -1) return;
    if (choice === 1) VIEW = "detail";
    if (choice === 2) VIEW = "training";
    if (choice === 3) VIEW = "sleepfood";
    if (choice === 4) VIEW = "sleep";
    if (choice === 5) VIEW = "food";
    if (choice === 6) VIEW = "fitness";
    if (choice === 7) VIEW = "ready";
    if (choice === 8 || !Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) { if (!(await askConfig())) return; if (choice === 8) return; }
  } else if (!Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) {
    Script.setWidget(messageWidget("Bitte das Skript einmal in Scriptable \u00f6ffnen und die Zugangsdaten eingeben."));
    return;
  }
  let widget;
  try { const res = await loadData(); widget = VIEW === "detail" ? buildMedium(res) : VIEW === "training" ? buildTraining(res) : VIEW === "sleepfood" ? buildFoodMedium(res) : VIEW === "sleep" ? buildSleep(res) : VIEW === "food" ? buildFoodCard(res) : VIEW === "fitness" ? buildFitness(res) : VIEW === "ready" ? buildReady(res) : buildWidget(res); }
  catch (e) { widget = messageWidget(`Keine Daten (${VIEW}${e.line ? `, Zeile ${e.line}` : ""}): ${String(e.message || e)}`); }
  if (inWidget) Script.setWidget(widget); else if (VIEW === "detail" || VIEW === "training" || VIEW === "sleepfood") await widget.presentMedium(); else if (VIEW === "sleep" || VIEW === "food" || VIEW === "fitness" || VIEW === "ready") await widget.presentSmall(); else await widget.presentLarge();
}
await main();
Script.complete();

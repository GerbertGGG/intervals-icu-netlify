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
//     Mittleres Widget (Groesse "Mittel"): Schlaf und Erholung links, Ernaehrung rechts. Mit dem Parameter
//     form  zeigt es stattdessen Form, Halbmarathon-Zeiten vs. Ziel, VDOT und Paces, Schwellen. Mit dem Parameter
//     training  zeigt es je Disziplin (Schwimmen, Rad, Lauf) Wochenvolumen gegen Plan und die Verteilung der Zeit.
// Schluesseleinheiten kennzeichnest du im Intervals-Kalender mit dem Stichwort (Tag) #key am Workout.
// CTL, TSB und TSS sind sportartuebergreifend.
// Zugangsdaten spaeter aendern: Skript in Scriptable ausfuehren und "Zugangsdaten setzen" waehlen.
//
// Es zeigt: heutige Einheit mit Bereitschaft, Countdown, Belastung (Frische, ACWR), Erholung (Schlaf, HRV),
// Wochenbelastung je Tag und Sportart, Kraft und Hueft-Hinweis. Fehlende Werte stehen als "fehlt", nie als 0.

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
  accent: dyn("#3b6ea8", "#6da2dc"), race: dyn("#c2410c", "#fb923c"),
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

// Geplante TSS der laufenden Woche (gesamt und je Tag) aus dem Dashboard-Kalender. Nur heute und kuenftige Tage
// haben einen Plan in den Daten; fuer vergangene Tage ist er dort nicht mehr enthalten.
async function fetchWeekPlan(base, week) {
  const dash = await fetchJson(base, "/api/dashboard");
  const wk = dash.weeks[dash.weeks.length - 1];
  const dates = week.days.map((x) => x.date), byDate = {};
  for (const e of dash.planned || []) if (dates.includes(e.date) && e.load != null) byDate[e.date] = (byDate[e.date] || 0) + e.load;
  return { total: wk && wk.plannedLoad != null ? wk.plannedLoad : null, byDate };
}

// Gross: Hauptdaten plus Wochenplan (data.weekPlan). Klein/Mittel (Schlaf, Ernaehrung): diese Daten plus das
// Rennziel (data.goal), damit die Phase ueberall aus demselben Renndatum kommt. Zusatzabfragen duerfen scheitern.
async function loadData() {
  const base = baseUrl();
  const { fm, path } = cachePath();
  try {
    const view = endpointView();
    const data = await fetchJson(base, widgetPath(view));
    if (view === "") data.weekPlan = await fetchWeekPlan(base, data.week).catch(() => null);
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
const PHASE_DAYS = { taperFrom: 7, carbloadFrom: 2, recoveryDays: 3 };
const REDUCED_PHASES = ["taper", "carbload"]; // bewusst reduzierte Trainingswoche
// TODO: Platzhalter, noch nicht festgelegt - bitte durch die echten Tagesziele ersetzen.
const NUTRITION_TARGETS = {
  normal: { proteinG: 130, kcal: 2400 },                // TODO Protein/Kalorien
  taper: { proteinG: 130, kcal: 2400 },                 // TODO wie normal
  carbload: { carbsG: 600, proteinG: 100, kcal: 3200 }, // TODO Carb-Loading (g Kohlenhydrate/Tag)
  recovery: { proteinG: 140, kcal: 2400, carbsG: 350 }, // TODO Protein/Kalorien/Kohlenhydrate
};
const PHASE_LABEL = { taper: "Taper", carbload: "Carb-Loading", recovery: "Regeneration" };
// Renndistanz aus dem Namen des Ziels (Renndatum und Zielzeit kommen aus den Daten selbst)
const raceKm = (name) => (/halb/i.test(name) ? 21.0975 : /marathon/i.test(name) ? 42.195 : /10\s*k/i.test(name) ? 10 : /5\s*k/i.test(name) ? 5 : 21.0975);
function racePhase(daysToGo) {
  let name = "normal";
  if (daysToGo != null) {
    if (daysToGo > PHASE_DAYS.taperFrom) name = "normal";
    else if (daysToGo > PHASE_DAYS.carbloadFrom) name = "taper";
    else if (daysToGo >= 0) name = "carbload";
    else if (daysToGo >= -PHASE_DAYS.recoveryDays) name = "recovery";
  }
  return { name, reduced: REDUCED_PHASES.includes(name), targets: NUTRITION_TARGETS[name], label: PHASE_LABEL[name] || null };
}
// Bereitschaftskreise: Skala ab 1 = bestmoeglich, niedrig ist gut. Farbe allein aus dem Wert.

/* ---------- Bausteine ---------- */
function text(parent, str, size, { bold = false, color = COL.text, lines = 1, align = "left", opacity = 1 } = {}) {
  const t = parent.addText(String(str));
  t.font = bold ? Font.boldSystemFont(size) : Font.systemFont(size);
  t.textColor = color;
  t.lineLimit = lines;
  t.minimumScaleFactor = 0.75;
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
  dc.size = new Size(w, h);
  dc.opaque = false;
  dc.respectScreenScale = true;
  return dc;
}

// Saeulen je Wochentag (Mo-So): erledigt als Saeule, geplant als helle Kontur dahinter. Zahlen stehen als Text darunter.
function dayBarsImage(w, h, days, todayIso) {
  const dc = newCtx(w, h);
  const max = Math.max(1, ...days.filter((x) => !x.isRace).map((x) => Math.max(x.load ?? 0, x.planned ?? 0))), slot = w / 7, bw = slot * 0.56, top = 2, base = h - 1;
  const bar = (cx, v, hex, alpha) => {
    const bh = v ? Math.max(3, (v / max) * (base - top)) : 1.5, p = new Path();
    p.addRoundedRect(new Rect(cx - bw / 2, base - bh, bw, bh), 2.5, 2.5);
    dc.addPath(p);
    dc.setFillColor(new Color(hex, alpha));
    dc.fillPath();
  };
  days.forEach((x, i) => {
    const cx = i * slot + slot / 2;
    if (x.isRace) {
      // Renntag: nur orange Spalte mit Marker, kein Balken (wuerde sonst die Skala der anderen Tage stauchen)
      dc.setFillColor(new Color("#fb923c", 0.18)); const q = new Path(); q.addRoundedRect(new Rect(i * slot + 1, 0, slot - 2, h), 4, 4); dc.addPath(q); dc.fillPath();
      dc.setFillColor(new Color("#fb923c", 0.95)); dc.fillEllipse(new Rect(cx - 3, h / 2 - 3, 6, 6));
      return;
    }
    if (x.planned) bar(cx, x.planned, "#8a94a3", 0.35);
    if (x.load == null) { if (!x.planned) { dc.setFillColor(new Color(x.isRace ? "#fb923c" : "#8a94a3", x.isRace ? 0.9 : 0.25)); dc.fillRect(new Rect(cx - bw / 2, base - 1.5, bw, 1.5)); } return; }
    // Erledigt nach Sportart gestapelt (Lauf unten); Renntag bleibt orange, heute bekommt einen hellen Rand
    const sp = x.sports ? SPORTS.filter(([k]) => x.sports[k] > 0) : [];
    if (!x.isRace && x.load && sp.length) {
      const total = sp.reduce((a, [k]) => a + x.sports[k], 0), bh = Math.max(3, (x.load / max) * (base - top));
      let y = base;
      for (const [k, , hex] of sp) {
        const sh = (x.sports[k] / total) * bh, p = new Path();
        p.addRoundedRect(new Rect(cx - bw / 2, y - sh + 0.5, bw, Math.max(1.5, sh - 1)), 1.5, 1.5);
        dc.addPath(p);
        dc.setFillColor(new Color(hex));
        dc.fillPath();
        y -= sh;
      }
      if (x.date === todayIso) { dc.setStrokeColor(new Color("#ffffff", 0.9)); dc.setLineWidth(1); dc.strokeRect(new Rect(cx - bw / 2 - 0.5, base - bh - 0.5, bw + 1, bh + 1)); }
    } else bar(cx, x.load, x.isRace ? "#fb923c" : x.date === todayIso ? "#7db0f5" : "#4f7fbf", x.load ? 1 : 0.35);
  });
  return dc.getImage();
}

// Fortschritt zur Wochen-TSS: Segmente nach Sportart, skaliert auf das Ziel (Rest = noch offen).
// Ohne Ziel fuellt der Anteil der Sportarten die ganze Breite.
function goalBarImage(w, h, week, goal) {
  const dc = newCtx(w, h);
  const total = SPORTS.reduce((a, [k]) => a + (week.bySport[k] ? week.bySport[k].load : 0), 0);
  const track = new Path();
  track.addRoundedRect(new Rect(0, 0, w, h), h / 2, h / 2);
  dc.addPath(track);
  dc.setFillColor(new Color("#8a94a3", 0.25));
  dc.fillPath();
  const scale = goal ? goal : total;
  let acc = 0;
  for (const [k, , hex] of SPORTS) {
    const v = week.bySport[k] ? week.bySport[k].load : 0;
    if (!v || !scale) continue;
    const a = Math.min(w, (acc / scale) * w), b = Math.min(w, ((acc + v) / scale) * w);
    dc.setFillColor(new Color(hex));
    dc.fillRect(new Rect(a, 0, Math.max(0, b - a), h));
    acc += v;
  }
  return dc.getImage();
}

/* ---------- Widget ---------- */
const DAY_FULL = ["Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"];
const TSB_TEXT = { ok: "frisch", warn: "belastet", bad: "stark ermüdet", none: "" };
const ACWR_TEXT = { ok: "im Korridor", warn: "niedrig", bad: "hoch", none: "" };
const signed = (n, d = 0) => (n == null ? "–" : `${n > 0 ? "+" : n < 0 ? "−" : ""}${fmt(Math.abs(n), d)}`);

// Mini-Kurve ueber die Breite: Lücken (null) bleiben offen, optional Referenzlinie (gestrichelt oder fest) und Punkt am Ende.
function sparkImage(w, h, values, hex, { ref = null, dashed = false, dot = false } = {}) {
  const dc = newCtx(w, h), pts = values.map((v, i) => [i, v]).filter(([, v]) => v != null);
  if (pts.length < 2) return dc.getImage();
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

// Trend der HRV: letzte drei Tage gegen die ersten drei der Kurve
function hrvTrend(series) {
  const v = series.filter((x) => x != null);
  if (v.length < 4) return { txt: "", cls: "none" };
  const avg = (a) => a.reduce((x, y) => x + y, 0) / a.length, diff = avg(v.slice(-3)) / avg(v.slice(0, 3)) - 1;
  return diff <= -0.04 ? { txt: "fällt", cls: "warn" } : diff >= 0.04 ? { txt: "steigt", cls: "ok" } : { txt: "stabil", cls: "ok" };
}

function buildWidget(res) {
  const d = res.data;
  const W = widgetInnerWidth(); // Innenbreite des großen Widgets
  const g = d.goal, ph = racePhase(g.daysToGo);
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(8, 13, 8, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);
  const GAP = 6, PAD = 9;

  // Kopf: Wochentag und Datum links, Rennen-Countdown als orange Kapsel rechts
  const head = w.addStack(); head.centerAlignContent(); head.size = new Size(W, 0);
  const dt = new Date(d.today + "T12:00:00");
  text(head, `${DAY_FULL[dt.getDay()]} ${dateShort(d.today)}`, 12, { color: COL.muted });
  head.addSpacer();
  if (g.daysToGo >= 0) chip(head, g.daysToGo === 0 ? `${g.name} heute` : `${g.name} in ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "en"}`, "#fb923c", 12);
  w.addSpacer(GAP - 1);

  // Heute: Einheit gross, Kapsel mit der Bereitschaft, Zweck darunter
  const r = d.readiness, v = r.verdict, p = d.plan.today[0], kp = d.plan.key;
  const hc = card(w, W, PAD, 16);
  const ht = hc.addStack(); ht.centerAlignContent();
  text(ht, "Heute", 12, { color: COL.muted });
  ht.addSpacer();
  chip(ht, v.cls === "ok" && p ? "Bereit, wie geplant" : v.text, ZONE_RGB[v.cls] || ZONE_RGB.none, 11, colorFor(v.cls));
  hc.addSpacer(2);
  if (p) {
    text(hc, p.name || "Einheit", 20, { bold: true });
    const meta = [p.durationMin && `${p.durationMin} min`, p.distanceKm && `${fmt(p.distanceKm, 1)} km`].filter(Boolean).join(" · ");
    if (meta) text(hc, meta, 13, { bold: true });
    if (p.purpose) text(hc, p.purpose, 11, { color: COL.muted, lines: 2 });
  } else {
    text(hc, "Keine Einheit geplant", 20, { bold: true });
    if (d.plan.next) text(hc, `Nächste: ${weekdayOf(d.plan.next.date)} ${dateShort(d.plan.next.date)} ${d.plan.next.name || "Einheit"}`, 11, { color: COL.muted });
    else if (v.cls === "none") text(hc, v.sub, 11, { color: COL.muted });
  }
  if (kp && !(p && p.key)) text(hc, `Schlüssel ${weekdayOf(kp.date)}: ${kp.name || "Einheit"}${kp.durationMin ? ` · ${kp.durationMin} min` : ""}`, 10, { bold: true, color: COL.accent });
  w.addSpacer(GAP);

  // Belastung (Frische, ACWR) und Erholung (Schlaf, HRV), je mit Mini-Kurve
  const L = d.load, tr = d.trend || { tsb14: [], hrv7: [], hrvMedian: null };
  const row = w.addStack(); row.spacing = GAP;
  const CW = Math.floor((W - GAP) / 2), IW = CW - 2 * (PAD + 3);
  const metric = (c, label, valTxt, cls) => {
    const l = c.addStack(); l.centerAlignContent();
    text(l, label, 12, { color: COL.muted });
    l.addSpacer();
    text(l, valTxt, 12, { bold: true, color: cls ? colorFor(cls) : COL.text });
  };
  const lc = card(row, CW, PAD, 16);
  text(lc, "Belastung", 12, { color: COL.muted });
  lc.addSpacer(2);
  metric(lc, "Frische", L.tsb == null ? "fehlt" : `${signed(L.tsb, 1)} ${TSB_TEXT[L.tsbCls]}`.trim(), L.tsbCls);
  metric(lc, "ACWR", L.acwr == null ? "fehlt" : `${fmt(L.acwr, 2)} ${ACWR_TEXT[L.acwrCls]}`.trim(), L.acwrCls === "ok" ? "none" : L.acwrCls);
  lc.addSpacer(2);
  const sp1 = lc.addImage(sparkImage(IW, 30, tr.tsb14, ZONE_RGB[L.tsbCls], { ref: 0 })); sp1.imageSize = new Size(IW, 30);
  lc.addSpacer(1);
  text(lc, "Frische, 14 Tage", 11, { color: COL.muted });
  const ec = card(row, CW, PAD, 16);
  text(ec, "Erholung", 12, { color: COL.muted });
  ec.addSpacer(2);
  const ht2 = hrvTrend(tr.hrv7);
  metric(ec, "Schlaf", r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h` : "fehlt");
  metric(ec, "HRV", r.hrv != null ? `${fmt(r.hrv)} ${ht2.txt}`.trim() : "fehlt", ht2.cls === "warn" ? "warn" : null);
  ec.addSpacer(2);
  const sp2 = ec.addImage(sparkImage(IW, 30, tr.hrv7, ht2.cls === "warn" ? "#e8c468" : "#6da2dc", { ref: tr.hrvMedian, dashed: true })); sp2.imageSize = new Size(IW, 30);
  ec.addSpacer(1);
  text(ec, `HRV, 7 Tage${tr.hrvMedian != null ? ` · Ø ${fmt(tr.hrvMedian)}` : ""}`, 11, { color: COL.muted });
  w.addSpacer(GAP);

  // Diese Woche: TSS erledigt von geplant, Fortschrittsbalken nach Sportart, Tage, Kraft und Hinweis
  const wc = card(w, W, PAD, 16), IW2 = W - 2 * (PAD + 3), wp = d.weekPlan || { total: null, byDate: {} };
  const days = d.week.days.map((x) => ({ ...x, isRace: x.date === g.date, planned: wp.byDate[x.date] ?? null }));
  const hasDayPlan = days.some((x) => x.planned != null);
  // Ziel = Erledigtes vergangener Tage + das Groessere aus Ist und Plan fuer heute und danach (Renntag zaehlt nicht)
  const goalTss = hasDayPlan
    ? Math.round(days.filter((x) => !x.isRace).reduce((a, x) => a + (x.date < d.today ? x.load ?? 0 : Math.max(x.load ?? 0, x.planned ?? 0)), 0))
    : wp.total != null ? wp.total : d.week.goal;
  const wl = wc.addStack(); wl.centerAlignContent();
  text(wl, "Diese Woche", 12, { color: COL.muted });
  wl.addSpacer();
  const left = d.week.plannedSessions;
  if (left != null) text(wl, left ? `noch ${left} Einheit${left === 1 ? "" : "en"}` : "alles erledigt", 12, { color: COL.muted });
  wc.addSpacer(2);
  const sums = wc.addStack(); sums.bottomAlignContent();
  text(sums, fmt(d.week.total), 20, { bold: true });
  text(sums, goalTss != null ? ` von ${fmt(goalTss)}` : "", 20, { bold: true });
  text(sums, " TSS", 13, { color: COL.muted });
  const run = d.week.bySport && d.week.bySport.run;
  if (run && run.km > 0) text(sums, ` · Laufen ${fmt(run.km, 1)} km`, 13, { color: COL.muted });
  wc.addSpacer(4);
  const gb = wc.addImage(goalBarImage(IW2, 6, { bySport: d.week.bySport || {} }, goalTss)); gb.imageSize = new Size(IW2, 6);
  wc.addSpacer(5);
  const bars = wc.addImage(dayBarsImage(IW2, 38, days, d.today)); bars.imageSize = new Size(IW2, 38);
  const lab = wc.addStack(); lab.size = new Size(IW2, 0);
  ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"].forEach((n, i) => {
    const x = days[i], isToday = x.date === d.today;
    const c = lab.addStack(); c.size = new Size(IW2 / 7, 0);
    c.addSpacer();
    text(c, n, 12, { bold: isToday || x.isRace, color: x.isRace ? COL.race : isToday ? COL.text : COL.muted });
    c.addSpacer();
  });
  wc.addSpacer(3);
  const fl = wc.addStack(); fl.centerAlignContent();
  if (ph.reduced) chip(fl, `${ph.label}: bewusst reduziert`, ZONE_RGB.ok, 10, COL.ok);
  else {
    // Ziel: 1 Stunde Krafttraining pro Woche
    const sm = d.week.strengthMinutes, done60 = sm != null && sm >= 60;
    text(fl, `Kraft ${sm != null ? fmt(Math.round(sm)) : "–"} / 60 min`, 12, { bold: done60, color: done60 ? COL.ok : COL.muted });
  }
  fl.addSpacer();
  if (d.hip.recent > 0) chip(fl, `Hüfte ${d.hip.recent}× erwähnt`, "#e8c468", 11, COL.warn);
  else if (d.sourcesFailed.length) text(fl, `Quelle fehlt: ${d.sourcesFailed.join(", ")}`, 10, { color: COL.bad });
  if (res.stale) { w.addSpacer(2); text(w, "veraltet, keine Verbindung", 9, { color: COL.warn }); }
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

// Inhalt "Ernaehrung", nach Rennphase: normal/taper Protein gross und darunter Kalorien, recovery zusaetzlich
// Kohlenhydrate, carbload Kohlenhydrate gross und Protein klein daneben. Ohne heutige Yazio-Werte steht das
// Tagesziel der Phase mit leerem Balken da.
function fillFood(w, d, IW, roomy, compact) {
  const f = d.food, ph = racePhase(d.goal ? d.goal.daysToGo : null), t = ph.targets;
  const hasVals = (x) => x && [x.calories, x.protein, x.carbs, x.fat].some((v) => v != null);
  let td = f.days[f.days.length - 1] || {};
  const has = hasVals(td);
  // Heute noch keine Yazio-Werte: letzte vorhandene Werte (gestern) abgedunkelt zeigen
  const prev = !has ? f.days.slice(0, -1).reverse().find(hasVals) : null;
  const stale = !!prev;
  if (stale) td = prev;
  const showVals = has || stale;
  const carb = ph.name === "carbload";
  const craving = d.cravings ? (d.cravings.count ? `Hei\u00dfhunger 7 Tage: ${d.cravings.count}\u00d7${d.cravings.strongest ? `, st\u00e4rkster ${d.cravings.strongest.strength}` : ""}` : "Hei\u00dfhunger 7 Tage: keiner") : null;
  const head = w.addStack(); head.centerAlignContent();
  text(head, "ERN\u00c4HRUNG", 9, { bold: true, color: COL.muted });
  head.addSpacer();
  if (stale) text(head, prev === f.days[f.days.length - 2] ? "gestern" : dateShort(prev.date), 8, { bold: true, color: COL.muted });
  else if (ph.name === "taper" && d.goal && d.goal.daysToGo === PHASE_DAYS.carbloadFrom + 1) text(head, "Carb-Loading ab morgen", 8, { bold: true, color: COL.race });
  else if (ph.label) text(head, ph.label, 8, { bold: true, color: COL.race });
  if (!compact) w.addSpacer(2);
  // Vier Balken: Protein, Kohlenhydrate, Fett, Energie. Ein Ziel gibt es nur, wenn Yazio oder die Phase eines liefert
  // (sonst nur der Wert, ohne Balken). Ohne heutige Yazio-Werte steht das Tagesziel mit leerem Balken da.
  // Ziele kommen aus Yazio (f.goals; Energie zusaetzlich als Tagesbudget td.goal inkl. Training). Nur beim
  // Carb-Loading gelten die Werte der Phase; fehlt Yazio, stehen die Phasen-Werte (Platzhalter) da.
  const yg = f.goals || {}, phaseWins = ph.name === "carbload";
  const pick = (yazio, phaseVal) => (phaseWins ? phaseVal ?? yazio : yazio ?? phaseVal);
  const macros = [
    { label: "Protein", v: td.protein, goal: pick(yg.proteinG, t.proteinG), unit: "g", hex: "#6da2dc" },
    { label: "Kohlenhydrate", v: td.carbs, goal: pick(yg.carbsG, t.carbsG), unit: "g", hex: carb ? "#fb923c" : "#f0a24a" },
    { label: "Fett", v: td.fat, goal: pick(yg.fatG, t.fatG), unit: "g", hex: "#e8cf6a" },
    { label: "Energie", v: td.calories, goal: phaseWins ? t.kcal : td.goal ?? yg.kcal ?? t.kcal, unit: "kcal", hex: "#8fa0b8" },
  ];
  macros.forEach((m, i) => {
    w.addSpacer(i === 0 ? (compact ? 2 : 4) : compact ? 3 : 5);
    if (!has && !stale) { /* keine Werte: Zielzeilen wie bisher */ } else if (m.v == null) return;
    const have = showVals && m.v != null, row = w.addStack(); row.centerAlignContent();
    text(row, m.label, 9, { color: COL.muted });
    row.addSpacer();
    const shown = have ? `${fmt(m.v)}${m.goal ? ` / ${fmt(m.goal)}` : ""} ${m.unit}` : m.goal ? `Ziel ${fmt(m.goal)} ${m.unit}` : "\u2013";
    text(row, shown, 11, { bold: true, color: have && !stale ? COL.text : COL.muted });
    if (m.goal && have) { w.addSpacer(2); progressBar(w, IW, m.v / m.goal, m.hex, stale ? 0.45 : 1); }
  });
  if (compact) w.addSpacer(2); else w.addSpacer();
  if (!has) text(w, stale ? "Yazio heute noch nicht synchron" : "Yazio noch nicht synchron", 8, { bold: true, color: COL.warn });
  else if (roomy && craving) text(w, craving, 9, { bold: true, color: COL.text });
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

// Ernaehrung (klein): normal/taper Protein, recovery Protein, carbload Kohlenhydrate gross; darunter Energie mit Balken
function buildFoodCard(res) {
  const d = res.data, f = d.food, ph = racePhase(d.goal ? d.goal.daysToGo : null), t = ph.targets, IW = 134;
  const hasVals = (x) => x && [x.calories, x.protein, x.carbs, x.fat].some((v) => v != null);
  let td = f.days[f.days.length - 1] || {};
  const has = hasVals(td), prev = !has ? f.days.slice(0, -1).reverse().find(hasVals) : null;
  if (prev) td = prev;
  const show = has || !!prev, yg = f.goals || {}, carb = ph.name === "carbload";
  const pick = (yazio, phaseVal) => (carb ? phaseVal ?? yazio : yazio ?? phaseVal);
  const big = carb ? { label: "Kohlenhydrate", v: td.carbs, goal: pick(yg.carbsG, t.carbsG) } : { label: "Protein", v: td.protein, goal: pick(yg.proteinG, t.proteinG) };
  const kcalGoal = carb ? t.kcal : td.goal ?? yg.kcal ?? t.kcal;
  const w = cardWidget(), dim = !has;
  const head = w.addStack(); head.centerAlignContent();
  text(head, "Ern\u00e4hrung", 13, { color: COL.muted });
  head.addSpacer();
  if (prev) text(head, prev === f.days[f.days.length - 2] ? "gestern" : dateShort(prev.date), 11, { bold: true, color: COL.muted });
  else if (ph.name === "taper" && d.goal && d.goal.daysToGo === PHASE_DAYS.carbloadFrom + 1) text(head, "Carbs ab morgen", 11, { bold: true, color: COL.race });
  else if (ph.label) text(head, ph.label, 14, { bold: true, color: COL.race });
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

// Mittleres Widget: Schlaf und Erholung links, Ernaehrung rechts
function buildSleepFood(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(9, 13, 8, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);
  const row = w.addStack(); row.spacing = 7;
  const CW = Math.floor((W - 7) / 2), IW = CW - 24;
  const CH = widgetSize().h - 9 - 8 - (res.stale || d.sourcesFailed.length ? 12 : 0);
  const cs = card(row, CW, 8), cf = card(row, CW, 8);
  cs.size = new Size(CW, CH);
  cf.size = new Size(CW, CH);
  fillSleep(cs, d, IW);
  fillFood(cf, d, IW, true);
  if (res.stale || d.sourcesFailed.length) {
    w.addSpacer(3);
    text(w, res.stale ? "veraltet, keine Verbindung" : `Quelle fehlt: ${d.sourcesFailed.join(", ")}`, 8, { color: res.stale ? COL.warn : COL.bad });
  }
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
  const inWidget = config.runsInWidget;
  if (!inWidget) {
    const menu = new Alert();
    menu.title = "Trainings-Widget";
    menu.addAction("Vorschau (gro\u00df)");
    menu.addAction("Vorschau mittel: Form und Halbmarathon-Zeiten");
    menu.addAction("Vorschau mittel: Training (Disziplinen)");
    menu.addAction("Vorschau mittel: Schlaf und Ern\u00e4hrung");
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
  try { const res = await loadData(); widget = VIEW === "detail" ? buildMedium(res) : VIEW === "training" ? buildTraining(res) : VIEW === "sleepfood" ? buildSleepFood(res) : VIEW === "sleep" ? buildSleep(res) : VIEW === "food" ? buildFoodCard(res) : VIEW === "fitness" ? buildFitness(res) : VIEW === "ready" ? buildReady(res) : buildWidget(res); }
  catch (e) { widget = messageWidget(`Keine Daten: ${String(e.message || e)}`); }
  if (inWidget) Script.setWidget(widget); else if (VIEW === "detail" || VIEW === "training" || VIEW === "sleepfood") await widget.presentMedium(); else if (VIEW === "sleep" || VIEW === "food" || VIEW === "fitness" || VIEW === "ready") await widget.presentSmall(); else await widget.presentLarge();
}
await main();
Script.complete();

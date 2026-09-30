// Trainings-Widget fuer Scriptable (iOS) - gedacht fuer das grosse Widget (Large).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfuegen.
//  2. Skript einmal in Scriptable oeffnen und ausfuehren: Es fragt nach der Adresse deines Workers
//     (https://....workers.dev, ohne Pfad) und dem Dashboard-Token. Beides landet im Schluesselbund
//     dieses iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Groesse "Gross", Skript auswaehlen.
//     Kleine Widgets (Groesse "Klein"): Schlaf und Erholung (Standard) sowie Ernaehrung, dafuer im Widget
//     unter "Parameter" das Wort  ernaehrung  eintragen.
//     Mittleres Widget (Groesse "Mittel"): Schlaf und Erholung links, Ernaehrung rechts. Mit dem Parameter
//     form  zeigt es stattdessen Form, Halbmarathon-Zeiten vs. Ziel, VDOT und Paces, Schwellen. Mit dem Parameter
//     training  zeigt es je Disziplin (Schwimmen, Rad, Lauf) Wochenvolumen gegen Plan und die Verteilung der Zeit.
// Schluesseleinheiten kennzeichnest du im Intervals-Kalender mit dem Stichwort (Tag) #key am Workout.
// CTL, TSB und TSS sind sportartuebergreifend.
// Zugangsdaten spaeter aendern: Skript in Scriptable ausfuehren und "Zugangsdaten setzen" waehlen.
//
// Es zeigt: Bereitschaft heute, Countdown, Frische (TSB), ACWR, heutige Einheit, Wochenbelastung
// je Tag und Sportart, Kraft und Hueft-Warnung. Fehlende Werte stehen als "fehlt", nie als 0.

const KEY_URL = "training-dashboard-url";
const KEY_TOKEN = "training-dashboard-token";
// Groesse bestimmt die Ansicht: Gross = Hauptansicht, Mittel = Details (Form, Zeiten vs. Ziel, Paces ...)
// Ansicht nach Widget-Groesse: Gross = Hauptansicht, Mittel = Details, Klein = Schlaf oder Ernaehrung
// (Klein: im Widget unter "Parameter"  ernaehrung  eintragen, sonst Schlaf).
const WIDGET_PARAM = String((typeof args !== "undefined" && args.widgetParameter) || "").trim().toLowerCase();
let VIEW = (typeof config !== "undefined" && config.runsInWidget)
  ? (config.widgetFamily === "medium" ? (/^(form|zeit|hm|detail)/.test(WIDGET_PARAM) ? "detail" : /^(train|tri)/.test(WIDGET_PARAM) ? "training" : "sleepfood") : config.widgetFamily === "small" ? (/^(ern|food|essen|kcal)/.test(WIDGET_PARAM) ? "food" : "sleep") : "main")
  : "main";
const endpointView = () => (VIEW === "detail" ? "detail" : VIEW === "training" ? "training" : VIEW === "sleep" || VIEW === "food" || VIEW === "sleepfood" ? "small" : "");
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
function card(parent, width, pad = 8) {
  const s = parent.addStack();
  s.layoutVertically();
  s.backgroundColor = COL.card;
  s.cornerRadius = 12;
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

// Skala mit Ampelzonen (abgerundete Segmente) und Marker mit Rand
const GAUGE_H = 16, GAUGE_TICK_H = 10;
function gaugeImage(w, min, max, zones, value, ticks = [], tickDigits = 0) {
  const h = GAUGE_H + (ticks.length ? GAUGE_TICK_H : 0), dc = newCtx(w, h);
  const x = (v) => ((Math.min(max, Math.max(min, v)) - min) / (max - min)) * w;
  for (const z of zones) {
    const a = x(z.from) + 1, b = x(z.to) - 1;
    const p = new Path();
    p.addRoundedRect(new Rect(a, 4, Math.max(2, b - a), 8), 4, 4);
    dc.addPath(p);
    dc.setFillColor(new Color(ZONE_RGB[z.cls], 0.92));
    dc.fillPath();
  }
  if (value != null) {
    const cx = Math.min(w - 6, Math.max(6, x(value)));
    // Weisser Marker mit dunklem Rand: in Hell und Dunkel lesbar, ohne auf den Darstellungsmodus zu bauen
    dc.setFillColor(new Color("#ffffff"));
    dc.fillEllipse(new Rect(cx - 6, 2, 12, 12));
    dc.setStrokeColor(new Color("#0f1318"));
    dc.setLineWidth(2);
    dc.strokeEllipse(new Rect(cx - 6, 2, 12, 12));
  }
  // Grenzwerte an den Segmentuebergaengen
  dc.setFont(Font.systemFont(8));
  dc.setTextColor(new Color("#8a94a3"));
  dc.setTextAlignedCenter();
  for (const t of ticks) {
    const cx = Math.min(w - 14, Math.max(14, x(t)));
    dc.drawTextInRect(fmt(t, tickDigits), new Rect(cx - 14, GAUGE_H - 1, 28, GAUGE_TICK_H));
  }
  return dc.getImage();
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

// Saeulen je Wochentag (Mo-So): erledigt als Saeule, geplant als helle Kontur dahinter. Zahlen stehen als Text darunter.
function dayBarsImage(w, h, days, todayIso) {
  const dc = newCtx(w, h);
  const max = Math.max(1, ...days.map((x) => Math.max(x.load ?? 0, x.planned ?? 0))), slot = w / 7, bw = slot * 0.56, top = 2, base = h - 1;
  const bar = (cx, v, hex, alpha) => {
    const bh = v ? Math.max(3, (v / max) * (base - top)) : 1.5, p = new Path();
    p.addRoundedRect(new Rect(cx - bw / 2, base - bh, bw, bh), 2.5, 2.5);
    dc.addPath(p);
    dc.setFillColor(new Color(hex, alpha));
    dc.fillPath();
  };
  days.forEach((x, i) => {
    const cx = i * slot + slot / 2;
    if (x.isRace) { dc.setFillColor(new Color("#fb923c", 0.18)); const q = new Path(); q.addRoundedRect(new Rect(i * slot + 1, 0, slot - 2, h), 4, 4); dc.addPath(q); dc.fillPath(); }
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
const TSB_TEXT = { ok: "frisch", warn: "belastet", bad: "stark erm\u00fcdet", none: "keine Daten" };
const ACWR_TEXT = { ok: "im Korridor", warn: "zu niedrig", bad: "zu hoch", none: "keine Daten" };

function buildWidget(res) {
  const d = res.data;
  const large = config.widgetFamily === "large" || !config.runsInWidget;
  const W = widgetInnerWidth(); // Innenbreite des gro\u00dfen Widgets
  const g = d.goal, ph = racePhase(g.daysToGo);
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(8, 13, 6, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);

  // Kopf: Rennen (dynamisch aus dem Renndatum), Ziel und Countdown
  const head = w.addStack();
  head.centerAlignContent();
  head.size = new Size(W, 0);
  const left = head.addStack(); left.layoutVertically();
  text(left, `${g.name.toUpperCase()} \u00b7 ${weekdayOf(g.date)} ${dateShort(g.date)}`, 10, { bold: true, color: COL.muted });
  text(left, `Ziel ${fmtTime(g.targetTimeSecs)} \u00b7 ${fmtPace(g.targetTimeSecs / raceKm(g.name))}/km`, 10, { color: COL.muted });
  head.addSpacer();
  text(head, g.daysToGo > 0 ? `noch ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "e"}` : g.daysToGo === 0 ? "Heute!" : "vorbei", 22, { bold: true });
  w.addSpacer(4);

  // Bereitschaft: alle Kreise nach demselben Ampelschema, Farbe aus dem Wert
  const r = d.readiness, v = r.verdict;
  const rc = card(w, W, 6);
  const top = rc.addStack(); top.centerAlignContent();
  const pill = top.addStack();
  pill.backgroundColor = new Color(ZONE_RGB[v.cls] || ZONE_RGB.none, 0.2);
  pill.cornerRadius = 9;
  pill.setPadding(1, 9, 1, 9);
  text(pill, v.text, 14, { bold: true, color: colorFor(v.cls) });
  top.addSpacer();
  text(top, r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h Schlaf` : "Schlafdauer fehlt", 11, { color: COL.muted });
  rc.addSpacer(3);
  const rings = rc.addStack();
  for (const [i, it] of r.items.entries()) {
    const col = rings.addStack(); col.layoutVertically(); col.centerAlignContent();
    const ring = col.addStack();
    ring.size = new Size(26, 26);
    ring.backgroundImage = ringImage(26, it.cls);
    ring.centerAlignContent();
    ring.addSpacer();
    text(ring, it.v == null ? "\u2013" : it.v, 12, { bold: true, align: "center" });
    ring.addSpacer();
    text(col, it.label.replace("Muskelkater", "Muskeln").replace("Motivation", "Motiv."), 9, { color: COL.muted, align: "center" });
    if (i < r.items.length - 1) rings.addSpacer();
  }
  if (v.cls === "none") text(rc, v.sub, 10, { color: COL.muted });
  w.addSpacer(4);

  // Frische und ACWR, Grenzwerte unter den Skalen
  const L = d.load, T = d.thresholds;
  const row = w.addStack(); row.spacing = 7;
  const CW = Math.floor((W - 7) / 2), IW = CW - 22;
  const gcol = (title, valueTxt, cls, statusTxt, img) => {
    const c = card(row, CW, 6);
    text(c, title, 10, { bold: true, color: COL.muted });
    const line = c.addStack(); line.centerAlignContent();
    text(line, valueTxt, 17, { bold: true, color: colorFor(cls) });
    line.addSpacer();
    text(line, statusTxt, 10, { color: colorFor(cls) });
    const im = c.addImage(img); im.imageSize = new Size(IW, GAUGE_H + GAUGE_TICK_H);
  };
  gcol("FRISCHE (TSB)", L.tsb == null ? "fehlt" : fmt(L.tsb, 1), L.tsbCls, TSB_TEXT[L.tsbCls], gaugeImage(IW, -40, 30, [{ from: -40, to: T.tsb.warn, cls: "bad" }, { from: T.tsb.warn, to: T.tsb.ok, cls: "warn" }, { from: T.tsb.ok, to: 30, cls: "ok" }], L.tsb, [T.tsb.warn, T.tsb.ok], 0));
  gcol("ACWR (ATL/CTL)", L.acwr == null ? "fehlt" : fmt(L.acwr, 2), L.acwrCls, ACWR_TEXT[L.acwrCls], gaugeImage(IW, 0.4, 1.8, [{ from: 0.4, to: T.acwr.lo, cls: "warn" }, { from: T.acwr.lo, to: T.acwr.hi, cls: "ok" }, { from: T.acwr.hi, to: 1.8, cls: "bad" }], L.acwr, [T.acwr.lo, T.acwr.hi], 1));
  w.addSpacer(4);

  if (!large) { footer(w, res, d, W); return w; }

  // Einheiten: heute und die naechste Schluesseleinheit (im Kalender mit #key gekennzeichnet), je mit Sportfarbe
  const pc = card(w, W, 6);
  const p = d.plan.today[0], kp = d.plan.key;
  const sessionLine = (tag, x, isKey) => {
    const line = pc.addStack(); line.centerAlignContent(); line.spacing = 5;
    const tg = line.addStack(); tg.size = new Size(28, 0);
    text(tg, tag, 9, { bold: true, color: COL.muted });
    const dot = line.addText("\u25cf"); dot.font = Font.systemFont(8); dot.textColor = new Color(sportHex(x.sport));
    const title = /marathon/i.test(x.name || "") && !/halb/i.test(x.name || "") ? `Vorbereitung ${g.name}` : x.name || "Einheit";
    text(line, `${isKey ? "Schl\u00fcssel: " : ""}${title}`, 13, { bold: true });
    line.addSpacer();
    const meta = [x.durationMin && `${x.durationMin} min`, x.distanceKm && `${fmt(x.distanceKm, 1)} km`].filter(Boolean).join(" \u00b7 ");
    if (meta) text(line, meta, 11, { color: COL.muted });
  };
  if (p) sessionLine("HEUTE", p, p.key);
  else {
    const line = pc.addStack(); line.centerAlignContent();
    text(line, "Heute keine Einheit geplant", 13, { bold: true });
    if (!kp && d.plan.next) { line.addSpacer(); text(line, `N\u00e4chste: ${dateShort(d.plan.next.date)} ${d.plan.next.name || "Einheit"}`, 10, { color: COL.muted }); }
  }
  if (kp) { pc.addSpacer(4); sessionLine(weekdayOf(kp.date), kp, true); }
  w.addSpacer(4);

  // Woche: TSS je Tag und fuer die Woche, erledigt und geplant. Taper/Carb-Loading: Badge, keine Kraft-Zeile.
  const wc = card(w, W, 6), IW2 = W - 22, wp = d.weekPlan || { total: null, byDate: {} };
  const days = d.week.days.map((x) => ({ ...x, isRace: x.date === g.date, planned: wp.byDate[x.date] ?? null }));
  const plannedTotal = wp.total != null ? wp.total : d.week.goal;
  const wl = wc.addStack(); wl.centerAlignContent();
  text(wl, ph.reduced ? `WOCHE \u00b7 ${ph.label.toUpperCase()}` : "WOCHE \u00b7 TSS", 10, { bold: true, color: COL.muted });
  wl.addSpacer();
  if (ph.reduced) {
    const badge = wl.addStack();
    badge.backgroundColor = new Color(ZONE_RGB.ok, 0.2);
    badge.cornerRadius = 7;
    badge.setPadding(1, 7, 1, 7);
    text(badge, "bewusst reduziert", 10, { bold: true, color: COL.ok });
  } else {
    const sc = d.week.strengthCount;
    text(wl, `Kraft ${sc}\u00d7 (Ziel 2\u20133)`, 10, { bold: sc >= 2, color: sc >= 2 ? COL.ok : COL.muted });
  }
  const sums = wc.addStack(); sums.centerAlignContent();
  text(sums, `erledigt ${fmt(d.week.total)}`, 13, { bold: true });
  sums.addSpacer(8);
  text(sums, `geplant ${plannedTotal != null ? fmt(plannedTotal) : "\u2013"}`, 13, { bold: true, color: COL.muted });
  sums.addSpacer(4);
  text(sums, "TSS", 10, { color: COL.muted });
  wc.addSpacer(3);
  const bars = wc.addImage(dayBarsImage(IW2, 30, days, d.today)); bars.imageSize = new Size(IW2, 30);
  const lab = wc.addStack(); lab.size = new Size(IW2, 0);
  ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"].forEach((n, i) => {
    const x = days[i], isToday = x.date === d.today;
    const c = lab.addStack(); c.layoutVertically(); c.size = new Size(IW2 / 7, 0);
    const row = (str, size, opts) => { const r = c.addStack(); r.addSpacer(); text(r, str, size, opts); r.addSpacer(); };
    row(x.isRace ? "Rennen" : n, 9, { bold: isToday || x.isRace, color: x.isRace ? COL.race : isToday ? COL.text : COL.muted });
    row(x.load == null ? " " : fmt(x.load), 10, { bold: true, color: x.load == null ? COL.muted : COL.text });
    row(x.planned != null ? `/${fmt(x.planned)}` : " ", 9, { color: COL.muted });
  });
  // Wochenvolumen je Disziplin: gelaufen/gefahren/geschwommen gegen Plan (Plan nur, wenn im Kalender Distanzen stehen)
  const bs = d.week.bySport || {};
  const tri = [["swim", "S"], ["bike", "R"], ["run", "L"]].filter(([k]) => bs[k] && (bs[k].km > 0 || bs[k].plannedKm));
  if (tri.length) {
    wc.addSpacer(3);
    const vr = wc.addStack(); vr.centerAlignContent();
    tri.forEach(([k, l], i) => {
      if (i) vr.addSpacer();
      const dot = vr.addText("●"); dot.font = Font.systemFont(7); dot.textColor = new Color(sportHex(k));
      vr.addSpacer(3);
      const dec = (v) => fmt(v, v < 10 ? 1 : 0);
      text(vr, `${l} ${dec(bs[k].km)}`, 10, { bold: true });
      text(vr, bs[k].plannedKm ? ` / ${dec(bs[k].plannedKm)} km` : " km", 10, { color: COL.muted });
    });
  }
  w.addSpacer();
  footer(w, res, d, W);
  return w;
}

function footer(w, res, d, W) {
  const f = w.addStack(); f.centerAlignContent(); f.size = new Size(W, 0);
  if (d.hip.recent > 0) text(f, `\u26a0\ufe0e H\u00fcfte/Leiste/Knie erw\u00e4hnt (${d.hip.recent}\u00d7 in 14 Tagen)`, 10, { bold: true, color: COL.bad });
  else if (d.sourcesFailed.length) text(f, `Quelle fehlt: ${d.sourcesFailed.join(", ")}`, 9, { color: COL.bad });
  f.addSpacer();
  const time = new Date(d.generatedAt).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
  text(f, res.stale ? `Stand ${time} \u00b7 veraltet, keine Verbindung` : `Stand ${time}`, 9, { color: res.stale ? COL.warn : COL.muted, align: "right" });
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
function smallBarsImage(w, h, values, todayIso, dates, goals) {
  h -= DAY_LABEL_H; // Platz fuer die Wochentags-Kuerzel darunter
  const dc = newCtx(w, h + DAY_LABEL_H), slot = w / values.length, bw = slot * 0.6;
  dc.setFont(Font.systemFont(11));
  dc.setTextAlignedCenter();
  dates.forEach((dt, i) => {
    dc.setTextColor(dt === todayIso ? new Color("#e6eaf0") : new Color("#9aa5b4"));
    dc.setFont(dt === todayIso ? Font.boldSystemFont(11) : Font.systemFont(11));
    dc.drawTextInRect(dayShort(dt), new Rect(i * slot, h + 2, slot, DAY_LABEL_H - 2));
  });
  const max = Math.max(1, ...values.filter((v) => v != null), ...(goals || []).filter((v) => v != null));
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
  const bh = (compact ? 20 : 30) + DAY_LABEL_H;
  const im = w.addImage(smallBarsImage(IW, bh, s.days.map((x) => x.hours), d.today, s.days.map((x) => x.date)));
  im.imageSize = new Size(IW, bh);
  if (!compact) { w.addSpacer(); text(w, "Ruhepuls = Tageswert, \u00d8 = letzte 14 Tage", 7, { color: COL.muted }); }
}

function progressBar(w, IW, ratio, hex) {
  const dc = newCtx(IW, 6), track = new Path();
  track.addRoundedRect(new Rect(0, 0, IW, 6), 3, 3);
  dc.addPath(track); dc.setFillColor(new Color("#8a94a3", 0.25)); dc.fillPath();
  if (ratio > 0) {
    const bar = new Path();
    bar.addRoundedRect(new Rect(0, 0, Math.max(6, Math.min(1, ratio) * IW), 6), 3, 3);
    dc.addPath(bar); dc.setFillColor(new Color(hex)); dc.fillPath();
  }
  const im = w.addImage(dc.getImage()); im.imageSize = new Size(IW, 6);
}

// Inhalt "Ernaehrung", nach Rennphase: normal/taper Protein gross und darunter Kalorien, recovery zusaetzlich
// Kohlenhydrate, carbload Kohlenhydrate gross und Protein klein daneben. Ohne heutige Yazio-Werte steht das
// Tagesziel der Phase mit leerem Balken da.
function fillFood(w, d, IW, roomy, compact) {
  const f = d.food, ph = racePhase(d.goal ? d.goal.daysToGo : null), t = ph.targets;
  const td = f.days[f.days.length - 1] || {};
  const has = [td.calories, td.protein, td.carbs, td.fat].some((x) => x != null);
  const carb = ph.name === "carbload";
  const craving = d.cravings ? (d.cravings.count ? `Hei\u00dfhunger 7 Tage: ${d.cravings.count}\u00d7${d.cravings.strongest ? `, st\u00e4rkster ${d.cravings.strongest.strength}` : ""}` : "Hei\u00dfhunger 7 Tage: keiner") : null;
  const head = w.addStack(); head.centerAlignContent();
  text(head, "ERN\u00c4HRUNG", 9, { bold: true, color: COL.muted });
  if (ph.label) { head.addSpacer(); text(head, ph.label, 8, { bold: true, color: COL.race }); }
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
    const have = has && m.v != null, row = w.addStack(); row.centerAlignContent();
    text(row, m.label, 9, { color: COL.muted });
    row.addSpacer();
    const shown = have ? `${fmt(m.v)}${m.goal ? ` / ${fmt(m.goal)}` : ""} ${m.unit}` : m.goal ? `Ziel ${fmt(m.goal)} ${m.unit}` : "\u2013";
    text(row, shown, 11, { bold: true, color: have ? COL.text : COL.muted });
    if (m.goal) { w.addSpacer(2); progressBar(w, IW, have ? m.v / m.goal : 0, m.hex); }
  });
  if (compact) w.addSpacer(2); else w.addSpacer();
  if (!has) text(w, "Yazio noch nicht synchron", 8, { bold: true, color: COL.warn });
  else if (roomy && craving) text(w, craving, 9, { bold: true, color: COL.text });
}

function buildSleep(res) { const w = smallWidget(); fillSleep(w, res.data, 134); return w; }
function buildFood(res) { const w = smallWidget(); fillFood(w, res.data, 134); return w; }

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
    menu.addAction("Zugangsdaten setzen / zur\u00fccksetzen");
    menu.addCancelAction("Schlie\u00dfen");
    const choice = await menu.presentAlert();
    if (choice === -1) return;
    if (choice === 1) VIEW = "detail";
    if (choice === 2) VIEW = "training";
    if (choice === 3) VIEW = "sleepfood";
    if (choice === 4) VIEW = "sleep";
    if (choice === 5) VIEW = "food";
    if (choice === 6 || !Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) { if (!(await askConfig())) return; if (choice === 6) return; }
  } else if (!Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) {
    Script.setWidget(messageWidget("Bitte das Skript einmal in Scriptable \u00f6ffnen und die Zugangsdaten eingeben."));
    return;
  }
  let widget;
  try { const res = await loadData(); widget = VIEW === "detail" ? buildMedium(res) : VIEW === "training" ? buildTraining(res) : VIEW === "sleepfood" ? buildSleepFood(res) : VIEW === "sleep" ? buildSleep(res) : VIEW === "food" ? buildFood(res) : buildWidget(res); }
  catch (e) { widget = messageWidget(`Keine Daten: ${String(e.message || e)}`); }
  if (inWidget) Script.setWidget(widget); else if (VIEW === "detail" || VIEW === "training" || VIEW === "sleepfood") await widget.presentMedium(); else if (VIEW === "sleep" || VIEW === "food") await widget.presentSmall(); else await widget.presentLarge();
}
await main();
Script.complete();

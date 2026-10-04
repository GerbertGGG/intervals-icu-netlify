// Trainings-Widget fuer Scriptable (iOS).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfuegen.
//  2. Skript einmal in Scriptable oeffnen und ausfuehren: Es fragt nach der Adresse deines Workers
//     (https://....workers.dev, ohne Pfad) und dem Dashboard-Token. Beides landet im Schluesselbund
//     dieses iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Groesse waehlen, Skript auswaehlen.
//     Gross:   Rennen-Countdown, Bereitschaft, heutige Einheit mit Warum, Wochenlast, die naechsten drei Tage.
//     Mittel:  Ernaehrung heute (Standard). Parameter  form  = Form, Wochen-TSS und Halbmarathon-Zeiten, Schwellen;
//              Parameter  training  = je Disziplin Wochenvolumen gegen Plan und Zeitverteilung.
//              Parameter  intensitaet  = Zonenzeit locker / mittel / hart: diese Woche und Schnitt der letzten 4 Wochen.
//              Parameter  vdot  = VDOT (Runalyze) mit Verlauf und die Trainingsbereiche (Paces).
//              Parameter  kraft  = Krafttraining aus EGYM: Trainingszeit gegen Wochenziel (60 min), bewegtes Gewicht, Bestwerte, Muskelalter.
//     Klein:   Fitness (CTL, Standard). Parameter  ernaehrung,  bereit  (Ringe) oder  schlaf.
// Schluesseleinheiten kennzeichnest du im Intervals-Kalender mit dem Stichwort (Tag) #key am Workout.
// CTL, TSB und TSS sind sportartuebergreifend. Fehlende Werte stehen als "–", nie als 0.
// Zugangsdaten spaeter aendern: Skript in Scriptable ausfuehren und "Zugangsdaten setzen" waehlen.
// Einschaetzungen (Rennphase, Koerper-Ampeln, Urteil) kommen fertig vom Worker; das Skript stellt nur dar.

const KEY_URL = "training-dashboard-url";
const KEY_TOKEN = "training-dashboard-token";

/* ---------- Ansichten ---------- */
// Jede Ansicht: Endpunkt (view=...), Zeichenfunktion, Groesse fuer die Vorschau, Name im Menue.
// Widget-Groesse und Parameter bestimmen die Ansicht (Parameter = Anfang des Wortes reicht).
const VIEWS = {
  main: { endpoint: "", build: (r) => buildWidget(r), size: "large", label: "Vorschau groß" },
  detail: { endpoint: "detail", build: (r) => buildMedium(r), size: "medium", label: "Vorschau mittel: Form und Halbmarathon-Zeiten" },
  training: { endpoint: "training", build: (r) => buildTraining(r), size: "medium", label: "Vorschau mittel: Training (Disziplinen)" },
  intensity: { endpoint: "training", build: (r) => buildIntensity(r), size: "medium", label: "Vorschau mittel: Intensität" },
  kraft: { endpoint: "kraft", build: (r) => buildKraft(r), size: "medium", label: "Vorschau mittel: Kraft (EGYM)" },
  vdot: { endpoint: "vdot", build: (r) => buildVdot(r), size: "medium", label: "Vorschau mittel: VDOT und Trainingsbereiche" },
  foodmedium: { endpoint: "small", build: (r) => buildFoodMedium(r), size: "medium", label: "Vorschau mittel: Ernährung heute" },
  sleep: { endpoint: "small", build: (r) => buildSleep(r), size: "small", label: "Vorschau klein: Schlaf" },
  food: { endpoint: "small", build: (r) => buildFoodCard(r), size: "small", label: "Vorschau klein: Ernährung" },
  fitness: { endpoint: "small", build: (r) => buildFitness(r), size: "small", label: "Vorschau klein: Fitness (CTL)" },
  ready: { endpoint: "main", build: (r) => buildReady(r), size: "small", label: "Vorschau klein: Bereitschaft" },
};
const PARAM_VIEWS = {
  medium: { fallback: "foodmedium", rules: [[/^(form|zeit|hm|detail)/, "detail"], [/^(intens|polar)/, "intensity"], [/^(train|tri)/, "training"], [/^(vdot|pace|bereich|zone)/, "vdot"], [/^(kraft|egym|strength|gym)/, "kraft"]] },
  small: { fallback: "fitness", rules: [[/^(ern|food|essen|kcal)/, "food"], [/^(schlaf|sleep|erhol)/, "sleep"], [/^(bereit|ready)/, "ready"]] },
};
const WIDGET_PARAM = String((typeof args !== "undefined" && args.widgetParameter) || "").trim().toLowerCase();
function viewFromWidget() {
  const fam = config.widgetFamily, set = PARAM_VIEWS[fam];
  if (!set) return "main";
  const hit = set.rules.find(([re]) => re.test(WIDGET_PARAM));
  return hit ? hit[1] : set.fallback;
}
let VIEW = typeof config !== "undefined" && config.runsInWidget ? viewFromWidget() : "main";
// "main" und "ready" lesen dieselben Daten; der Cache haengt am Endpunkt, nicht an der Ansicht
const endpointOf = () => (VIEWS[VIEW].endpoint === "main" ? "" : VIEWS[VIEW].endpoint);
const cacheFile = () => `training-widget${endpointOf() ? "-" + endpointOf() : ""}-cache.json`;

/* ---------- Design ---------- */
const DARK = typeof Device !== "undefined" && Device.isUsingDarkAppearance();
const dyn = (l, d) => Color.dynamic(new Color(l), new Color(d));
const COL = {
  bg: dyn("#f2f4f7", "#0f1318"), card: dyn("#ffffff", "#1a2029"), text: dyn("#1c2430", "#e6eaf0"), muted: dyn("#5d6877", "#9aa5b4"),
  ok: dyn("#1f7a4d", "#6fd4a0"), warn: dyn("#9a6400", "#f0c060"), bad: dyn("#b3261e", "#f2a29c"), none: dyn("#5d6877", "#9aa5b4"),
  accent: dyn("#3b6ea8", "#7ba3dc"), race: dyn("#c2410c", "#fb923c"),
  tile: dyn("#eef1f5", "#232b36"), line: dyn("#d6dbe3", "#2b323d"),
};
// Gezeichnete Bilder (DrawContext) kennen keine dynamischen Farben: sie bekommen die zur Darstellung passende Farbe
const INK_HEX = DARK ? "#e6eaf0" : "#1c2430";
const MUTED_HEX = DARK ? "#9aa5b4" : "#5d6877";
const NEUTRAL_HEX = "#8a94a3";
const ACCENT_HEX = DARK ? "#7ba3dc" : "#3b6ea8";
const ACCENT_HI_HEX = DARK ? "#a9c8f0" : "#285489";
const ZONE_RGB = { ok: "#2f9d64", warn: "#d99a1a", bad: "#d0453b", none: NEUTRAL_HEX };
const RACE_HEX = "#fb923c";
const SPORTS = { run: ["Laufen", "#5b8fd6"], bike: ["Rad", "#a283d8"], swim: ["Schwimmen", "#3fb6cc"], strength: ["Kraft", "#d0a03c"], other: ["Sonst.", "#9aa5b4"] };
const sportHex = (k) => (SPORTS[k] || SPORTS.other)[1];
// Schriftgroessen: nichts unter 11 pt (auf dem Homescreen sonst kaum lesbar)
const FS = { xs: 11, sm: 12, md: 14, lg: 17, xl: 24, xxl: 34 };
const MISSING = "–";
const colorFor = (cls) => COL[cls] || COL.none;

// Groesse des Widgets in Punkten nach Bildschirmbreite (Apple-Sollwerte): [Bildschirmbreite, Breite Mittel/Gross, Seite Klein]
const WIDGET_SIZES = [[430, 364, 170], [428, 364, 170], [414, 360, 169], [393, 338, 158], [390, 338, 158], [375, 321, 148]];
function widgetSize() {
  const sw = Math.min(Device.screenSize().width, Device.screenSize().height);
  const hit = WIDGET_SIZES.find(([w]) => sw >= w) || WIDGET_SIZES[WIDGET_SIZES.length - 1];
  return { w: hit[1], small: hit[2] };
}
const PAD = 14; // einheitlicher Rand aller Widgets
const widgetInnerWidth = () => widgetSize().w - 2 * PAD;
const smallInnerWidth = () => widgetSize().small - 2 * PAD;
const baseUrl = () => (Keychain.get(KEY_URL).match(/^https:\/\/[^\/\s?#]+/) || [Keychain.get(KEY_URL)])[0];

// Gemeinsamer Rahmen: Hintergrund, Rand, Link auf das Dashboard, Aktualisierung. flat = Karte ist die Widget-Flaeche
function baseWidget({ flat = true, padV = 12 } = {}) {
  const w = new ListWidget();
  w.backgroundColor = flat ? COL.card : COL.bg;
  w.setPadding(padV, PAD, padV - 2, PAD);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 15 * 60 * 1000);
  return w;
}

/* ---------- Konfiguration ---------- */
async function askConfig() {
  const a = new Alert();
  a.title = "Trainings-Widget einrichten";
  a.message = "Adresse deines Workers (nur https://….workers.dev) und Dashboard-Token. Beides bleibt im Schlüsselbund dieses iPhones.";
  a.addTextField("https://….workers.dev", Keychain.contains(KEY_URL) ? Keychain.get(KEY_URL) : "");
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

/* ---------- Daten laden (ein Request je Ansicht, mit Cache fuer Offline) ---------- */
function cachePath() { const fm = FileManager.local(); return { fm, path: fm.joinPath(fm.documentsDirectory(), cacheFile()) }; }

async function fetchJson(base, path) {
  const req = new Request(`${base}${path}`);
  req.headers = { Authorization: `Bearer ${Keychain.get(KEY_TOKEN)}` };
  req.timeoutInterval = 25;
  const body = await req.loadString();
  const status = req.response ? req.response.statusCode : 0;
  if (status === 401) throw new Error("Token wurde abgelehnt");
  if (status === 503) throw new Error("Worker: DASHBOARD_TOKEN nicht gesetzt");
  if (status === 404) throw new Error(`404 bei ${base}${path} – falsche Adresse oder Deploy noch nicht durch`);
  if (status !== 200) throw new Error(`Worker antwortet mit ${status}`);
  return JSON.parse(body);
}
const widgetPath = (view) => `/api/widget${view ? "?view=" + view : ""}`;

// Bei Fehlern die zuletzt geladenen Daten zeigen (Kennzeichnung "veraltet" samt Zeitpunkt)
async function loadData() {
  const { fm, path } = cachePath();
  try {
    const data = await fetchJson(baseUrl(), widgetPath(endpointOf()));
    fm.writeString(path, JSON.stringify(data));
    return { data, stale: false, error: null };
  } catch (e) {
    if (fm.fileExists(path)) return { data: JSON.parse(fm.readString(path)), stale: true, error: String(e.message || e) };
    throw e;
  }
}

/* ---------- Format ---------- */
const fmt = (n, d = 0) => (n == null || !Number.isFinite(Number(n)) ? MISSING : Number(n).toLocaleString("de-DE", { minimumFractionDigits: d, maximumFractionDigits: d }));
const fmtTime = (s) => { const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = Math.round(s % 60); return h ? `${h}:${String(m).padStart(2, "0")}:${String(x).padStart(2, "0")}` : `${m}:${String(x).padStart(2, "0")}`; };
const fmtPace = (s) => `${Math.floor(s / 60)}:${String(Math.round(s % 60)).padStart(2, "0")}`;
const dateShort = (iso) => { const [, m, d] = iso.split("-"); return `${d}.${m}.`; };
const DAY_FULL = ["Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"];
const DAY_SHORT = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"];
const dayShort = (iso) => DAY_SHORT[new Date(iso + "T12:00:00").getDay()];
const weekdayOf = (iso) => dayShort(iso).toUpperCase();
const signed = (n, d = 0) => (n == null ? MISSING : `${n > 0 ? "+" : n < 0 ? "−" : ""}${fmt(Math.abs(n), d)}`);
const clock = (iso) => new Date(iso).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
const addIso = (iso, n) => new Date(Date.parse(iso + "T12:00:00Z") + n * 86400000).toISOString().slice(0, 10);

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

// Kleine Kapsel mit getoenter Flaeche (Status, Rennen, Hinweise)
function chip(parent, str, hex, size = FS.xs, textColor = null) {
  const s = parent.addStack();
  s.backgroundColor = new Color(hex, 0.2);
  s.cornerRadius = 9;
  s.setPadding(2, 9, 2, 9);
  text(s, str, size, { color: textColor || new Color(hex) });
  return s;
}

// Hinweiszeile nur bei Bedarf: veraltete Daten (mit Zeitpunkt des Abrufs) oder ausgefallene Quellen
function notice(w, res, d) {
  const failed = (d.sourcesFailed || []).length ? `Quelle fehlt: ${d.sourcesFailed.join(", ")}` : null;
  if (!res.stale && !failed) return;
  w.addSpacer(3);
  text(w, res.stale ? `veraltet, Stand ${clock(d.generatedAt)}` : failed, FS.xs, { color: res.stale ? COL.warn : COL.bad, minScale: 1 });
}

// Kopfzeile: Titel links, Zusatz (meist Uhrzeit des Abrufs) rechts
function header(w, W, title, extra, extraColor = COL.muted) {
  const h = w.addStack(); h.centerAlignContent(); if (W) h.size = new Size(W, 0);
  text(h, title, FS.xs, { bold: true, color: COL.muted, minScale: 0.8 });
  h.addSpacer();
  if (extra) text(h, extra, FS.xs, { color: extraColor, minScale: 1 });
  return h;
}

// Fortschrittsbalken, optional mit Marke (z. B. Ziel)
function progressBar(w, IW, ratio, hex, alpha = 1) {
  const dc = newCtx(IW, 6), track = new Path();
  track.addRoundedRect(new Rect(0, 0, IW, 6), 3, 3);
  dc.addPath(track); dc.setFillColor(new Color(NEUTRAL_HEX, 0.25)); dc.fillPath();
  if (ratio > 0) {
    const bar = new Path();
    bar.addRoundedRect(new Rect(0, 0, Math.max(6, Math.min(1, ratio) * IW), 6), 3, 3);
    dc.addPath(bar); dc.setFillColor(new Color(hex, alpha)); dc.fillPath();
  }
  const im = w.addImage(dc.getImage()); im.imageSize = new Size(IW, 6);
}

// Mini-Kurve ueber die Breite: Luecken (null) bleiben offen, Punkt am Ende
function sparkImage(w, h, values, hex, { dot = false, second = null } = {}) {
  const dc = newCtx(w, h), pts = values.map((v, i) => [i, v]).filter(([, v]) => v != null);
  // Zweite Kurve (z. B. TSB) mit eigener Skala, blasser und duenner hinter der Hauptkurve
  if (second && second.hex) {
    const sp = second.values.map((v, i) => [i, v]).filter(([, v]) => v != null);
    if (sp.length >= 2) {
      const sv = sp.map(([, v]) => v), slo = Math.min(...sv), shi = Math.max(...sv), sspan = shi - slo || 1, spad = 4;
      const SX = (i) => spad + (i / (second.values.length - 1)) * (w - 2 * spad), SY = (v) => h - spad - ((v - slo) / sspan) * (h - 2 * spad);
      const sPath = new Path();
      let sPen = false;
      for (const [i, v] of sp) {
        const pt = new Point(SX(i), SY(v));
        if (sPen && second.values[i - 1] != null) sPath.addLine(pt); else sPath.move(pt);
        sPen = true;
      }
      dc.addPath(sPath);
      dc.setStrokeColor(new Color(second.hex, 0.85));
      dc.setLineWidth(1.6);
      dc.strokePath();
    }
  }
  if (pts.length < 2) {
    // Zu wenig Werte: nur eine blasse Grundlinie (ein leerer DrawContext liefert in Scriptable kein Bild, sondern null)
    dc.setFillColor(new Color(NEUTRAL_HEX, 0.3));
    dc.fillRect(new Rect(0, h - 3, w, 1.5));
    return dc.getImage();
  }
  const all = pts.map(([, v]) => v);
  const lo = Math.min(...all), hi = Math.max(...all), span = hi - lo || 1, pad = 4;
  const X = (i) => pad + (i / (values.length - 1)) * (w - 2 * pad), Y = (v) => h - pad - ((v - lo) / span) * (h - 2 * pad);
  dc.setStrokeColor(new Color(hex));
  dc.setLineWidth(2.2);
  const p = new Path();
  let pen = false;
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

// Saeulen der letzten 7 Tage; fehlende Tage sind nur ein kurzer Strich, nie ein Wert. Optional Ziel-Marken und Referenzlinie.
const DAY_LABEL_H = 15;
function barsImage(w, h, values, todayIso, dates, goals, refLine) {
  h -= DAY_LABEL_H; // Platz fuer die Wochentags-Kuerzel darunter
  const dc = newCtx(w, h + DAY_LABEL_H), slot = w / values.length, bw = slot * 0.6;
  dc.setTextAlignedCenter();
  dates.forEach((dt, i) => {
    dc.setTextColor(new Color(dt === todayIso ? INK_HEX : MUTED_HEX));
    dc.setFont(dt === todayIso ? Font.boldSystemFont(FS.xs) : Font.systemFont(FS.xs));
    dc.drawTextInRect(dayShort(dt), new Rect(i * slot, h + 2, slot, DAY_LABEL_H - 2));
  });
  const max = Math.max(1, ...values.filter((v) => v != null), ...(goals || []).filter((v) => v != null), refLine ?? 0);
  values.forEach((v, i) => {
    const cx = i * slot + slot / 2;
    if (v == null) { dc.setFillColor(new Color(NEUTRAL_HEX, 0.3)); dc.fillRect(new Rect(cx - bw / 2, h - 2, bw, 1.5)); return; }
    const bh = Math.max(2, (v / max) * (h - 3)), p = new Path();
    p.addRoundedRect(new Rect(cx - bw / 2, h - bh, bw, bh), 2.5, 2.5);
    dc.addPath(p);
    dc.setFillColor(new Color(dates[i] === todayIso ? ACCENT_HI_HEX : ACCENT_HEX));
    dc.fillPath();
    if (goals && goals[i] != null) { dc.setFillColor(new Color(INK_HEX, 0.85)); dc.fillRect(new Rect(cx - bw / 2 - 1, h - (goals[i] / max) * (h - 3) - 1, bw + 2, 1.5)); }
  });
  if (refLine) {
    // Referenzlinie (z. B. 8 h Schlaf) gestrichelt ueber den Saeulen
    const y = h - (refLine / max) * (h - 3);
    dc.setFillColor(new Color(INK_HEX, 0.5));
    for (let x = 0; x < w; x += 6) dc.fillRect(new Rect(x, y, 3, 1));
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

// Fortschrittsring (Start oben, im Uhrzeigersinn); ratio wird auf 0..1 begrenzt
function ringProgressImage(size, ratio, hex, alpha = 1, center = null) {
  const dc = newCtx(size, size), lw = 10, r = (size - lw) / 2, c = size / 2;
  dc.setStrokeColor(new Color(NEUTRAL_HEX, 0.25));
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
  // Mitteltext (gezeichnet, damit er im Ring sitzt): grosse Zahl, darunter eine kleine Zeile
  if (center) {
    dc.setTextAlignedCenter();
    dc.setTextColor(new Color(INK_HEX));
    dc.setFont(Font.boldSystemFont(size * 0.3));
    dc.drawTextInRect(String(center.big), new Rect(0, size * 0.27, size, size * 0.34));
    dc.setTextColor(new Color(MUTED_HEX));
    dc.setFont(Font.systemFont(FS.xs));
    dc.drawTextInRect(String(center.small), new Rect(0, size * 0.6, size, 15));
  }
  return dc.getImage();
}

/* ---------- Grosses Widget ---------- */
// Koerper: Ampeln kommen fertig vom Worker (readiness.body); hier nur Pfeil und Text
const HRV_ARROW = { down: " ↓", up: " ↑", flat: " →" };
// Gefuehl: nur so einstufen wie das Dashboard (gegen den eigenen ueblichen Bereich). Ohne Vergleich kein Urteil.
function feelWord(it) {
  if (!it || it.v == null) return { txt: MISSING, cls: "none" };
  if (it.cls === "ok") return { txt: "gut", cls: "ok" };
  if (it.cls === "warn") return { txt: "mittel", cls: "warn" };
  if (it.cls === "bad") return { txt: "schlecht", cls: "bad" };
  return { txt: String(it.v), cls: "none" };
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
// Warum: Plan-Zweck, sonst regelbasiert aus Rennphase (vom Worker), Bereitschaft und Einheit
function whyText(d, p, v) {
  const pu = p && usablePurpose(p.purpose);
  if (pu) return pu;
  const g = d.goal, phase = g && g.phase;
  if (g && g.daysToGo === 0) return "Renntag: gleichmäßig starten und das Tempo nicht überziehen.";
  if (phase === "recovery") return "Nach dem Rennen zählt Erholung. Nur locker bewegen, nichts erzwingen.";
  if (phase === "taper") return "Der Umfang sinkt vor dem Rennen. Die Beine sollen frisch werden, nicht müde. Locker bleiben.";
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

function dayTile(row, TW, iso, g, upcoming) {
  const race = !!g && g.date === iso;
  const items = upcoming ? upcoming.filter((e) => e.date === iso) : null;
  const t = row.addStack();
  t.layoutVertically();
  t.size = new Size(TW, 0);
  t.cornerRadius = 12;
  t.setPadding(6, 9, 6, 9);
  t.backgroundColor = race ? new Color(RACE_HEX, 0.16) : COL.tile;
  let main, sub;
  if (race) { main = "Rennen"; sub = g.name; }
  else if (!items) { main = MISSING; sub = "kein Kalender"; }
  else if (!items.length) { main = "Pause"; sub = "frei"; }
  else {
    const mins = items.reduce((a, e) => a + (e.durationMin || 0), 0);
    main = mins ? `${mins} min` : items[0].name || "Einheit";
    sub = shortLabel(items[0]);
  }
  text(t, dayShort(iso), FS.xs, { color: race ? COL.race : COL.muted, minScale: 1 });
  text(t, main, 16, { bold: true, color: race ? COL.race : COL.text });
  text(t, sub, FS.xs, { color: race ? COL.race : COL.muted, minScale: 0.9 });
}

// Wochenlast: TSS bisher gegen Wochenziel (Plan oder Konfiguration), sonst gegen die Vorwoche
function weekRow(w, W, wk) {
  if (!wk || wk.total == null) return;
  const row = w.addStack(); row.centerAlignContent(); row.size = new Size(W, 0);
  text(row, "Woche", FS.xs, { color: COL.muted, minScale: 1 });
  row.addSpacer();
  if (wk.goal) text(row, `${fmt(wk.total)} / ${fmt(wk.goal)} TSS`, FS.md, { bold: true });
  else {
    text(row, `${fmt(wk.total)} TSS`, FS.md, { bold: true });
    if (wk.lastTotal != null) text(row, `  Vorwoche ${fmt(wk.lastTotal)}`, FS.xs, { color: COL.muted, minScale: 0.8 });
  }
  if (wk.goal) { w.addSpacer(3); progressBar(w, W, wk.total / wk.goal, ACCENT_HEX); }
}

function buildWidget(res) {
  const d = res.data;
  const W = widgetInnerWidth();
  const g = d.goal;
  const r = d.readiness, v = r.verdict, p = d.plan.today[0];
  const none = { cls: "none" }, body = r.body || { sleep: none, hrv: none, resting: none }; // alte Cache-Daten ohne Ampeln
  const w = baseWidget({ flat: true, padV: 12 });
  const rule = () => { const s = w.addStack(); s.size = new Size(W, 0.5); s.backgroundColor = COL.line; };

  // 1. Kopf: Wochentag, Datum und Abrufzeit links, Rennen-Countdown als Kapsel rechts (nur bis zum Renntag)
  const head = w.addStack(); head.centerAlignContent(); head.size = new Size(W, 0);
  const dt = new Date(d.today + "T12:00:00");
  text(head, `${DAY_FULL[dt.getDay()]} ${dateShort(d.today)}`, FS.md, { color: COL.muted });
  text(head, `  ${clock(d.generatedAt)}`, FS.xs, { color: COL.muted, minScale: 1 });
  head.addSpacer();
  if (g && g.daysToGo != null && g.daysToGo >= 0) chip(head, g.daysToGo === 0 ? `${g.name} heute` : `${g.name} in ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "en"}`, RACE_HEX, FS.md, COL.race);
  w.addSpacer(4);

  // 2. Bereitschaft: Urteil gross, ein Satz, darunter Koerper (objektiv) und Gefuehl (subjektiv)
  text(w, v.text, FS.xxl, { bold: true, color: v.cls === "none" ? COL.muted : colorFor(v.cls), minScale: 0.6 });
  text(w, verdictLine(v, p), FS.sm, { color: COL.muted, lines: 1, minScale: 0.8 });
  w.addSpacer(7);
  const CW = Math.floor((W - 16) / 2);
  const cols = w.addStack(); cols.size = new Size(W, 0); cols.spacing = 16;
  const col = (title) => { const c = cols.addStack(); c.layoutVertically(); c.size = new Size(CW, 0); text(c, title, FS.xs, { color: COL.muted, minScale: 1 }); c.addSpacer(2); return c; };
  const line = (c, label, val, cls) => {
    const l = c.addStack(); l.centerAlignContent(); l.size = new Size(CW, 0);
    text(l, "●", FS.xs, { color: colorFor(cls || "none"), minScale: 1 });
    l.addSpacer(6);
    text(l, label, FS.md, { minScale: 0.8 });
    l.addSpacer();
    text(l, val, FS.md, { bold: true, color: val === MISSING ? COL.muted : COL.text, align: "right", minScale: 0.8 });
    c.addSpacer(3);
  };
  const bodyCol = col("Körper"), feel = col("Gefühl");
  line(bodyCol, "Schlaf", r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h` : MISSING, body.sleep.cls);
  line(bodyCol, "HRV", r.hrv != null ? `${fmt(r.hrv)}${HRV_ARROW[body.hrv.trend] || ""}` : MISSING, body.hrv.cls);
  line(bodyCol, "Ruhepuls", fmt(r.restingHR), body.resting.cls);
  for (const [lab, short] of FEEL) {
    const f = feelWord((r.items || []).find((i) => i.label === lab));
    line(feel, short, f.txt, f.cls);
  }
  w.addSpacer(3);
  rule();
  w.addSpacer(6);

  // 3. Heute: Einheitenname gross, darunter Dauer, Distanz und RPE aus dem Plan
  if (p) {
    text(w, p.name || "Einheit", FS.xl, { bold: true, minScale: 0.7 });
    const meta = [p.durationMin && `${p.durationMin} min`, p.distanceKm && `${fmt(p.distanceKm, 1)} km`].filter(Boolean).join(" · ");
    if (meta || p.rpe) {
      const m = w.addStack(); m.bottomAlignContent();
      if (meta) text(m, meta, 16, { bold: true });
      if (p.rpe) text(m, `${meta ? " · " : ""}RPE ${p.rpe}`, 16, { color: COL.muted });
    }
  } else {
    text(w, "Keine Einheit geplant", FS.xl, { bold: true, minScale: 0.7 });
    if (d.plan.next) text(w, `Nächste: ${weekdayOf(d.plan.next.date)} ${dateShort(d.plan.next.date)} ${d.plan.next.name || "Einheit"}`, FS.sm, { color: COL.muted });
  }
  w.addSpacer(5);

  // 4. Warum: Akzentstrich links, 1 bis 3 Zeilen
  const why = whyText(d, p, v);
  const perLine = Math.max(20, Math.floor((W - 14) / 6.6));
  const nLines = Math.min(3, Math.max(1, Math.ceil(why.length / perLine)));
  const wr = w.addStack(); wr.size = new Size(W, nLines * 17 + 2);
  const bar = wr.addStack(); bar.size = new Size(3, nLines * 17 + 2); bar.backgroundColor = COL.accent;
  wr.addSpacer(10);
  text(wr, why, FS.md, { lines: 3, minScale: 0.85 });
  w.addSpacer(7);

  // 5. Wochenlast
  weekRow(w, W, d.week);
  w.addSpacer(7);

  // 6. Naechste Tage: drei Kacheln
  const TW = Math.floor((W - 12) / 3);
  const tiles = w.addStack(); tiles.size = new Size(W, 0); tiles.spacing = 6;
  for (let i = 1; i <= 3; i++) dayTile(tiles, TW, addIso(d.today, i), g, d.plan.upcoming);
  notice(w, res, d);
  w.addSpacer();
  return w;
}

/* ---------- Mittel: Form und Halbmarathon-Zeiten (Parameter "form") ---------- */
function formImage(w, h, form, zones) {
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
  line("atl", NEUTRAL_HEX);
  line("ctl", ACCENT_HEX);
  const tsbs = form.map((f) => (f.ctl != null && f.atl != null ? f.ctl - f.atl : null));
  const tv = tsbs.filter((v) => v != null);
  const lo = Math.min(-15, ...tv), hi = Math.max(15, ...tv), bandTop = splitY + 6, bandH = h - bandTop - 1;
  const yz = (v) => bandTop + (1 - (v - lo) / (hi - lo)) * bandH;
  dc.setFillColor(new Color(NEUTRAL_HEX, 0.35));
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

// Wochen-TSS als Saeulen (aelteste links); laufende Woche in Akzentfarbe, Luecken nur als kurzer Strich, optional Ziellinie
function tssImage(w, h, weeks) {
  const dc = newCtx(w, h), n = weeks.length;
  const vals = weeks.flatMap((k) => [k.tss, k.goal]).filter((v) => v != null);
  const max = Math.max(50, ...vals), slot = w / n, bw = slot * 0.62;
  weeks.forEach((k, i) => {
    const cur = i === n - 1, x = i * slot + (slot - bw) / 2;
    if (k.tss == null) { dc.setFillColor(new Color(NEUTRAL_HEX, 0.3)); dc.fillRect(new Rect(x, h - 2, bw, 1.5)); return; }
    const bh = Math.max(2, (k.tss / max) * (h - 2));
    dc.setFillColor(cur ? new Color(ACCENT_HEX) : new Color(NEUTRAL_HEX, 0.55));
    dc.fillRect(new Rect(x, h - bh, bw, bh));
  });
  const goal = weeks[n - 1]?.goal;
  if (goal != null) { dc.setFillColor(new Color(ZONE_RGB.ok, 0.9)); dc.fillRect(new Rect((n - 1) * slot, h - (goal / max) * (h - 2) - 0.5, slot, 1)); }
  return dc.getImage();
}

// Links die Form der letzten 28 Tage, rechts die Zeiten gegen das Ziel, unten Paces und Schwellen
function buildMedium(res) {
  const d = res.data;
  const W = widgetInnerWidth();
  const w = baseWidget({ flat: false, padV: 10 });
  const row = w.addStack(); row.spacing = 7;
  const LW = Math.floor((W - 7) * 0.5), RW = W - 7 - LW;

  // Links: Form
  const L = d.load, fc = card(row, LW, 7);
  text(fc, "FORM · 28 TAGE", FS.xs, { bold: true, color: COL.muted });
  fc.addSpacer(2);
  if (d.form.some((f) => f.ctl != null)) { const im = fc.addImage(formImage(LW - 22, 54, d.form, d.tsbZones)); im.imageSize = new Size(LW - 22, 54); }
  else text(fc, "Keine CTL/ATL-Werte", FS.xs, { color: COL.muted });
  fc.addSpacer(2);
  const nums = fc.addStack(); nums.centerAlignContent();
  text(nums, `CTL ${fmt(L.ctl, 1)}`, FS.xs, { bold: true, color: COL.accent });
  nums.addSpacer();
  text(nums, `TSB ${signed(L.tsb, 1)}`, FS.xs, { bold: true, color: colorFor(L.tsbCls) });
  // Wochen-TSS der letzten 6 Wochen
  const tw = d.tssWeeks || [];
  if (tw.some((k) => k.tss != null)) {
    fc.addSpacer(3);
    const cur = tw[tw.length - 1], th = fc.addStack(); th.centerAlignContent();
    text(th, "TSS · 6 WOCHEN", FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
    th.addSpacer();
    if (cur.tss != null) text(th, `${fmt(cur.tss)}${cur.goal ? ` / ${fmt(cur.goal)}` : ""}`, FS.xs, { bold: true, color: COL.text, minScale: 0.7 });
    fc.addSpacer(1);
    const ti = fc.addImage(tssImage(LW - 22, 32, tw)); ti.imageSize = new Size(LW - 22, 32);
  }

  // Rechts: Zeiten gegen das Ziel
  const rc = card(row, RW, 7), goal = d.goal.targetTimeSecs;
  const rh = rc.addStack(); rh.centerAlignContent();
  text(rh, String(d.goal.name || "Rennen").toUpperCase(), FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
  rh.addSpacer();
  if (d.goal.daysToGo > 0) text(rh, `${d.goal.daysToGo} Tg`, FS.xs, { bold: true, color: COL.muted, minScale: 1 });
  rc.addSpacer(2);
  const line = (label, secs, bold, tone) => {
    const r = rc.addStack(); r.centerAlignContent();
    text(r, label, FS.xs, { bold, color: bold ? COL.text : COL.muted });
    r.addSpacer();
    text(r, fmtTime(secs), FS.sm, { bold: true, color: tone || COL.text });
    if (goal && secs !== goal) { const diff = secs - goal; r.addSpacer(4); text(r, `${diff > 0 ? "+" : "−"}${fmtTime(Math.abs(diff))}`, FS.xs, { color: diff <= 0 ? COL.ok : COL.muted, minScale: 0.7 }); }
  };
  if (goal) line("Ziel", goal, true, COL.accent);
  if (d.hm && d.hm.estimates.length) {
    // Zwei Vergleichswerte: Runalyze-Prognose und die schnellste Bestzeit-Rechnung (VDOT hat ein eigenes Widget)
    const est = d.hm.estimates, best = est.filter((e) => e.key.startsWith("best-")).sort((a, b) => a.seconds - b.seconds)[0];
    const prog = est.find((e) => e.kind === "prognosis");
    for (const e of [prog, best].filter(Boolean)) line(e.kind === "prognosis" ? "Runalyze" : e.label.replace("aus ", "").replace("-Bestzeit", ""), e.seconds, false);
    if (best) text(rc, "nach Daniels gerechnet", FS.xs, { color: COL.muted, minScale: 0.7 });
  } else text(rc, "Kein Runalyze-Snapshot", FS.xs, { color: COL.muted, lines: 2 });
  if (goal && d.goal.runKm) {
    rc.addSpacer(3);
    const gp = rc.addStack(); gp.centerAlignContent();
    text(gp, "Zielpace", FS.xs, { color: COL.muted });
    gp.addSpacer();
    text(gp, `${fmtPace(goal / d.goal.runKm)}/km`, FS.sm, { bold: true, color: COL.accent });
  }
  w.addSpacer(5);

  // Unten: Schwellen
  const bc = card(w, W, 6), T = d.thresholds;
  text(bc, `Lauf ${T.run.thresholdPaceSecPerKm ? fmtPace(T.run.thresholdPaceSecPerKm) + "/km" : MISSING} · FTP ${T.bike.ftp ? T.bike.ftp + " W" : MISSING} · Schwimmen ${T.swim.thresholdPaceSecPer100m ? fmtPace(T.swim.thresholdPaceSecPer100m) + "/100 m" : MISSING}`, FS.xs);
  notice(w, res, d);
  return w;
}

/* ---------- Schlaf und Erholung, Ernaehrung, Fitness (klein) ---------- */
function smallWidget() { return baseWidget({ flat: true, padV: 12 }); }

function buildSleep(res) {
  const d = res.data, s = d.sleep, IW = smallInnerWidth();
  const w = smallWidget();
  const last = s.latest, isToday = last && last.date === d.today;
  header(w, IW, "SCHLAF", last ? (isToday ? "heute" : dateShort(last.date)) : null);
  text(w, last ? `${fmt(last.hours, 1)} h` : MISSING, 28, { bold: true, color: last ? COL.text : COL.muted });
  const t = s.days[s.days.length - 1];
  const cmp = (v, med) => (v == null || med == null ? "" : ` (Ø ${fmt(med)})`);
  text(w, `HRV ${fmt(t.hrv)}${cmp(t.hrv, s.medianHrv)}`, FS.xs, { bold: true });
  text(w, `Ruhepuls ${fmt(t.restingHR)}${cmp(t.restingHR, s.medianRestingHR)}`, FS.xs, { bold: true });
  w.addSpacer(4);
  const missing = s.days.filter((x) => x.hours == null);
  if (missing.length >= 3 && s.days.length - missing.length <= 1) {
    // Mehrere Tage ohne Daten: eine Zeile statt lauter leerer Striche
    text(w, `keine Daten ${dayShort(missing[0].date)}–${dayShort(missing[missing.length - 1].date)}`, FS.xs, { bold: true, color: COL.warn, minScale: 0.8 });
  } else {
    const bh = 22 + DAY_LABEL_H;
    const im = w.addImage(barsImage(IW, bh, s.days.map((x) => x.hours), d.today, s.days.map((x) => x.date), null, 8));
    im.imageSize = new Size(IW, bh);
  }
  w.addSpacer();
  notice(w, res, d);
  return w;
}

// Bereitschaft: Urteil als Kapsel, Schlafdauer, die fuenf Skalen als Ringe (3 + 2). Daten wie im grossen Widget.
function buildReady(res) {
  const d = res.data, r = d.readiness, v = r.verdict, g = d.goal, IW = smallInnerWidth();
  const w = smallWidget();
  const head = w.addStack(); head.centerAlignContent();
  chip(head, v.text, ZONE_RGB[v.cls] || ZONE_RGB.none, FS.lg - 1, colorFor(v.cls));
  head.addSpacer();
  if (g && g.daysToGo >= 0) text(head, g.daysToGo === 0 ? "heute" : `${g.daysToGo} T`, FS.sm, { bold: true, color: COL.race, minScale: 1 });
  w.addSpacer(2);
  text(w, r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h Schlaf` : "Schlaf –", FS.xs, { color: COL.muted });
  w.addSpacer(5);
  const short = { Schlaf: "Schlaf", Ermüdung: "Ermüd.", Muskelkater: "Muskeln", Stimmung: "Stimm.", Motivation: "Motiv." };
  const ring = (parent, it, cw) => {
    const col = parent.addStack(); col.layoutVertically(); col.size = new Size(cw, 0);
    const c = col.addStack(); c.addSpacer();
    const rg = c.addStack(); rg.size = new Size(25, 25); rg.backgroundImage = ringImage(25, it.cls); rg.centerAlignContent();
    rg.addSpacer(); text(rg, it.v == null ? MISSING : it.v, FS.sm, { bold: true, align: "center" }); rg.addSpacer();
    c.addSpacer();
    const l = col.addStack(); l.addSpacer(); text(l, short[it.label] || it.label, FS.xs, { color: COL.muted, align: "center", minScale: 0.7 }); l.addSpacer();
  };
  const rows = [r.items.slice(0, 3), r.items.slice(3)];
  rows.forEach((items, ri) => {
    if (ri) w.addSpacer(3);
    const row = w.addStack(); row.size = new Size(IW, 0);
    if (ri) row.addSpacer(IW / 6);
    items.forEach((it) => ring(row, it, IW / 3));
  });
  w.addSpacer();
  notice(w, res, d);
  return w;
}

// Fitness (CTL): Wert gross, Veraenderung als Kapsel, Wochenkurve mit Punkt am Ende
function buildFitness(res) {
  const d = res.data, f = d.fitness || { ctl: null, delta: null, deltaWeeks: null, weekly: [] }, IW = smallInnerWidth();
  const w = smallWidget();
  header(w, IW, "FITNESS (CTL)", clock(d.generatedAt));
  w.addSpacer(2);
  const row = w.addStack(); row.centerAlignContent();
  text(row, f.ctl != null ? fmt(f.ctl) : MISSING, 40, { bold: true, color: f.ctl != null ? COL.accent : COL.muted });
  row.addSpacer();
  if (f.delta != null) chip(row, signed(f.delta), f.delta > 0 ? ZONE_RGB.ok : f.delta < 0 ? ZONE_RGB.warn : ZONE_RGB.none, FS.md, f.delta > 0 ? COL.ok : f.delta < 0 ? COL.warn : COL.muted);
  w.addSpacer(4);
  const im = w.addImage(sparkImage(IW, 44, f.weekly, ACCENT_HEX, { dot: true, second: { values: f.tsbWeekly || [], hex: NEUTRAL_HEX } })); im.imageSize = new Size(IW, 44);
  w.addSpacer();
  const foot = w.addStack(); foot.centerAlignContent();
  text(foot, `${f.deltaWeeks || 6} Wochen`, FS.sm, { color: COL.muted });
  foot.addSpacer();
  if (f.tsb != null) text(foot, `TSB ${signed(f.tsb)}`, FS.sm, { bold: true, color: COL.muted, minScale: 0.7 });
  notice(w, res, d);
  return w;
}

// Ernaehrung (klein): Protein gross, darunter Energie mit Balken. Ziele immer aus Yazio.
function buildFoodCard(res) {
  const d = res.data, f = d.food, IW = smallInnerWidth();
  const hasVals = (x) => x && [x.calories, x.protein, x.carbs, x.fat].some((v) => v != null);
  let td = f.days[f.days.length - 1] || {};
  const has = hasVals(td), prev = !has ? f.days.slice(0, -1).reverse().find(hasVals) : null;
  if (prev) td = prev;
  const show = has || !!prev, yg = f.goals || {};
  const kcalGoal = td.goal ?? yg.kcal ?? null;
  const w = smallWidget(), dim = !has;
  header(w, IW, "ERNÄHRUNG", prev ? (prev === f.days[f.days.length - 2] ? "gestern" : dateShort(prev.date)) : null);
  w.addSpacer(1);
  text(w, show && td.protein != null ? `${fmt(td.protein)} g` : MISSING, 34, { bold: true, color: dim ? COL.muted : COL.text });
  text(w, `Protein${yg.proteinG ? ` · Ziel ${fmt(yg.proteinG)}` : ""}`, FS.sm, { color: COL.muted });
  w.addSpacer();
  text(w, show && td.calories != null ? `${fmt(td.calories)}${kcalGoal ? ` / ${fmt(kcalGoal)}` : ""} kcal` : kcalGoal ? `Ziel ${fmt(kcalGoal)} kcal` : "kcal –", FS.md, { bold: true, color: dim ? COL.muted : COL.text });
  w.addSpacer(4);
  progressBar(w, IW, show && td.calories != null && kcalGoal ? td.calories / kcalGoal : 0, ACCENT_HEX, dim ? 0.5 : 1);
  if (!has) { w.addSpacer(3); text(w, prev ? "Yazio heute noch nicht synchron" : "Yazio noch nicht synchron", FS.xs, { bold: true, color: COL.warn, minScale: 0.7 }); }
  else notice(w, res, d);
  return w;
}

/* ---------- Mittel: Ernaehrung heute ---------- */
const OVER_HEX = "#f0b95a"; // Wert ueber dem Ziel: gelb
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
  const w = baseWidget({ flat: true, padV: 10 });
  // Abrufzeit: iOS aktualisiert Widgets nach eigenem Ermessen, so sieht man, wie alt die Anzeige ist
  header(w, W, stale ? "ERNÄHRUNG GESTERN" : "ERNÄHRUNG HEUTE", clock(d.generatedAt));
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
    ring.backgroundImage = ringProgressImage(RS, td.calories != null && kcalGoal ? td.calories / kcalGoal : 0, ACCENT_HEX, stale ? 0.5 : 1);
    const cen = (str, size, o) => { const s = ring.addStack(); s.addSpacer(); text(s, str, size, { align: "center", minScale: 0.6, ...o }); s.addSpacer(); };
    cen(td.calories != null ? fmt(td.calories) : MISSING, td.calories != null ? 24 : 16, { color: td.calories != null ? COL.text : COL.muted, opacity: op });
    if (kcalGoal) cen(`von ${fmt(kcalGoal)}`, FS.sm, { color: COL.muted, minScale: 0.85 });
    // Rechts: Protein, Kohlenhydrate, Fett als Balken (g / Ziel g); fehlende Werte als Strich ohne Balken
    const col = row.addStack(); col.layoutVertically(); col.size = new Size(RW, 0);
    const macros = [
      { label: "Protein", v: td.protein, goal: yg.proteinG ?? null },
      { label: "Kohlenhydrate", v: td.carbs, goal: yg.carbsG ?? null },
      { label: "Fett", v: td.fat, goal: yg.fatG ?? null },
    ];
    macros.forEach((m, i) => {
      if (i) col.addSpacer(7);
      const l = col.addStack(); l.centerAlignContent(); l.size = new Size(RW, 0);
      text(l, m.label, FS.md, { minScale: 0.8 });
      l.addSpacer();
      if (m.v == null) text(l, MISSING, FS.md, { color: COL.muted, minScale: 1 });
      else {
        const over = m.goal && m.v > m.goal;
        text(l, fmt(m.v), FS.lg, { bold: true, opacity: op, minScale: 0.8, color: over ? new Color(OVER_HEX) : COL.text });
        text(l, m.goal ? ` / ${fmt(m.goal)} g` : " g", FS.sm, { color: COL.muted, minScale: 0.8 });
      }
      if (m.v != null && m.goal) { col.addSpacer(3); progressBar(col, RW, m.v / m.goal, m.v > m.goal ? OVER_HEX : ACCENT_HEX, stale ? 0.5 : 1); }
    });
  }
  notice(w, res, d);
  w.addSpacer();
  return w;
}

/* ---------- Mittel: VDOT und Trainingsbereiche (Parameter "vdot") ---------- */
// Farbe je Bereich von locker (blau) bis schnell (rot)
const PACE_ZONES_UI = { easy: ["Easy", "E", "#3fb6cc"], marathon: ["Marathon", "M", "#2f9d64"], threshold: ["Schwelle", "T", "#d99a1a"], interval: ["Intervall", "I", "#e0722a"], repetition: ["Repetition", "R", "#d0453b"] };

function buildVdot(res) {
  const d = res.data, W = widgetInnerWidth(), v = d.vdot;
  const w = baseWidget({ flat: false, padV: 10 });
  if (!v) {
    header(w, W, "VDOT · TRAININGSBEREICHE", null);
    w.addSpacer();
    text(w, "Kein Runalyze-Snapshot", 16, { bold: true, color: COL.warn });
    text(w, "VDOT und Bereiche erscheinen, sobald der Coaching-Task einen Snapshot schickt.", FS.xs, { color: COL.muted, lines: 2 });
    w.addSpacer();
    notice(w, res, d);
    return w;
  }
  const row = w.addStack(); row.spacing = 7;
  const LW = Math.floor((W - 7) * 0.4), RW = W - 7 - LW;

  // Links: VDOT gross, Veraenderung und Verlauf
  const lc = card(row, LW, 7);
  text(lc, "VDOT", FS.xs, { bold: true, color: COL.muted });
  text(lc, fmt(v.value, 1), 34, { bold: true, color: COL.accent, minScale: 0.7 });
  const dl = v.delta;
  text(lc, dl == null ? (v.historySince ? `Verlauf ab ${dateShort(v.historySince)}` : "kein Verlauf") : `${signed(dl, 1)} seit ${dateShort(v.deltaSince)}`, FS.xs, { bold: dl != null, color: dl == null ? COL.muted : dl > 0 ? COL.ok : dl < 0 ? COL.warn : COL.muted, minScale: 0.7 });
  lc.addSpacer(3);
  const im = lc.addImage(sparkImage(LW - 22, 26, v.history.map((h) => h.vdot), ACCENT_HEX, { dot: true })); im.imageSize = new Size(LW - 22, 26);
  lc.addSpacer(2);
  text(lc, `Stand ${dateShort(v.fetchedAt.slice(0, 10))}`, FS.xs, { color: COL.muted, minScale: 0.8 });

  // Rechts: die fuenf Bereiche als Pace von-bis (schnell bis langsam), wie in den Runalyze-Lauftabellen
  const rc = card(row, RW, 7);
  text(rc, "TRAININGSBEREICHE · MIN/KM", FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
  rc.addSpacer(2);
  const bare = (x) => (x ? x.replace("/km", "") : MISSING);
  for (const p of v.paces) {
    const ui = PACE_ZONES_UI[p.key] || [p.label, "", NEUTRAL_HEX];
    const r = rc.addStack(); r.centerAlignContent();
    text(r, "●", FS.xs, { color: new Color(ui[2]), minScale: 1 });
    r.addSpacer(5);
    text(r, ui[0], FS.sm, { minScale: 0.7 });
    r.addSpacer();
    text(r, `${bare(p.fastPace)}–${bare(p.slowPace)}`, FS.sm, { bold: true, minScale: 0.7 });
    rc.addSpacer(1);
  }
  w.addSpacer(4);
  text(w, "Bereiche wie in den Runalyze-Lauftabellen (% vVO2max)", FS.xs, { color: COL.muted, minScale: 0.7 });
  notice(w, res, d);
  return w;
}

/* ---------- Mittel: Disziplinen (Parameter "training") ---------- */
// Anteile der Sportarten an der Trainingszeit als ein Balken mit Prozentzahl im Segment; Soll als helle Striche
function splitImage(w, h, share, target, keys = ["swim", "bike", "run"], hexOf = sportHex) {
  const dc = newCtx(w, h), tot = keys.reduce((a, k) => a + share[k], 0) || 1;
  let acc = 0;
  dc.setFont(Font.boldSystemFont(FS.xs));
  dc.setTextAlignedCenter();
  for (const k of keys) {
    const a = (acc / tot) * w, b = ((acc + share[k]) / tot) * w;
    if (b - a > 1) {
      const p = new Path(); p.addRoundedRect(new Rect(a + 0.5, 0, b - a - 1, h), 3, 3);
      dc.addPath(p); dc.setFillColor(new Color(hexOf(k))); dc.fillPath();
      if (b - a > 34) { dc.setTextColor(new Color("#ffffff")); dc.drawTextInRect(`${share[k]} %`, new Rect(a, (h - FS.xs) / 2 - 2, b - a, h)); }
    }
    acc += share[k];
  }
  if (target) {
    dc.setFillColor(new Color(INK_HEX, 0.95));
    for (const at of [target.swim, target.swim + target.bike]) dc.fillRect(new Rect((at / 100) * w - 0.5, -1, 1.5, h + 2));
  }
  return dc.getImage();
}

function buildTraining(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = baseWidget({ flat: false, padV: 10 });
  header(w, W, "TRAINING · DISZIPLINEN", "diese Woche");
  w.addSpacer(3);
  const row = w.addStack(); row.spacing = 7;
  const CW = Math.floor((W - 14) / 3), IW = CW - 20;
  for (const k of ["swim", "bike", "run"]) {
    const s = d.sports[k], hex = sportHex(k);
    const c = card(row, CW, 5);
    const nl = c.addStack(); nl.centerAlignContent(); nl.spacing = 3;
    const dot = nl.addText("●"); dot.font = Font.systemFont(FS.xs); dot.textColor = new Color(hex);
    text(nl, SPORTS[k][0], FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
    // Wochenlast: TSS gegen Plan (Plan nur, wenn im Kalender Last steht)
    const vl = c.addStack(); vl.bottomAlignContent(); vl.spacing = 3;
    text(vl, `${Math.round(s.weekLoad || 0)}`, FS.xl - 4, { bold: true });
    text(vl, s.plannedLoad ? `/ ${Math.round(s.plannedLoad)} TSS` : "TSS", FS.xs, { color: COL.muted, minScale: 0.7 });
    c.addSpacer(4);
    progressBar(c, IW, s.plannedLoad ? (s.weekLoad || 0) / s.plannedLoad : 0, hex);
    c.addSpacer(4);
    // Trainingszeit und Tage seit der letzten Einheit; ab 7 Tagen Pause hervorgehoben
    const mins = Math.round(s.weekMinutes || 0);
    const since = s.daysSince == null ? MISSING : s.daysSince === 0 ? "heute" : `vor ${s.daysSince} T`;
    const long = s.daysSince != null && s.daysSince >= 7;
    const bl = c.addStack(); bl.centerAlignContent();
    text(bl, mins ? `${Math.floor(mins / 60)}:${String(mins % 60).padStart(2, "0")} h` : MISSING, FS.xs, { color: COL.muted, minScale: 0.7 });
    bl.addSpacer();
    text(bl, since, FS.xs, { bold: long, color: long ? COL.warn : COL.muted, minScale: 0.7 });
  }
  w.addSpacer(5);
  const sc = card(w, W, 5), sp = d.split;
  header(sc, null, `ZEITVERTEILUNG · ${sp.weeks} WOCHEN`, sp.target ? `Soll S ${sp.target.swim} · R ${sp.target.bike} · L ${sp.target.run} %` : null);
  if (sp.share) {
    sc.addSpacer(3);
    const im = sc.addImage(splitImage(W - 22, 16, sp.share, sp.target)); im.imageSize = new Size(W - 22, 16);
  } else text(sc, "Noch keine abgeschlossene Woche mit Training.", FS.xs, { color: COL.muted });
  notice(w, res, d);
  return w;
}

/* ---------- Mittel: Intensitaet (Parameter "intensitaet") ---------- */
// Zeitanteile locker (Z1-Z2), mittel (Z3-Z4), hart (Z5+) wie die Polarisation in Intervals: diese Woche und Schnitt der letzten 4 Wochen
const INT_HEX = { easy: "#5BA88A", mid: "#E0B454", hard: "#D9695F" };
const INT_LABEL = { easy: "locker", mid: "mittel", hard: "hart" };

function buildIntensity(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = baseWidget({ flat: false, padV: 10 });
  header(w, W, "INTENSITÄT · ZONENZEIT", "Z1–2 · Z3–4 · Z5+");
  w.addSpacer(6);
  const it = d.intensity || {};
  const row = (title, sh) => {
    const c = card(w, W, 6);
    header(c, null, title, sh ? `locker ${sh.easy} · mittel ${sh.mid} · hart ${sh.hard} %` : null);
    c.addSpacer(4);
    if (sh) { const im = c.addImage(splitImage(W - 22, 22, sh, null, ["easy", "mid", "hard"], (k) => INT_HEX[k])); im.imageSize = new Size(W - 22, 22); }
    else text(c, "Noch keine Zonenzeiten.", FS.xs, { color: COL.muted });
  };
  row("DIESE WOCHE", it.week);
  w.addSpacer(6);
  row("Ø 4 WOCHEN", it.avg);
  notice(w, res, d);
  return w;
}

/* ---------- Mittel: Kraft aus EGYM (Parameter "kraft") ---------- */
// Links der Ring der Woche (Saetze gegen das Wochenziel) mit Trainingstagen. Rechts das Muskelalter mit Balken je Bereich.
const KRAFT_GOAL_SETS = 27; // Wochenziel in Saetzen: 9 Geraete x 3 Saetze

function buildKraft(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = baseWidget({ flat: false, padV: 10 });
  if (d.configured === false) {
    header(w, W, "KRAFT · EGYM", null);
    w.addSpacer(6);
    text(w, "EGYM ist im Worker nicht eingerichtet (EGYM_USERNAME und EGYM_PASSWORD).", FS.sm, { color: COL.muted, lines: 4 });
    return w;
  }
  const wk = d.week, sets = wk.sets || 0, hex = sportHex("strength");
  header(w, W, "KRAFT · EGYM", `Woche ab ${dateShort(wk.start)}`, COL.muted);
  w.addSpacer(6);
  const row = w.addStack(); row.centerAlignContent(); row.spacing = 12;
  const LW = 110, RW = W - LW - 13, RING = 84;

  // Links: Saetze der Woche gegen das Ziel, darunter die Trainingstage und die Einheiten
  const lc = row.addStack(); lc.layoutVertically(); lc.size = new Size(LW, 0);
  // Zahl als echter Text im Ring (nicht ins Bild gezeichnet): so passt sie sich Hell/Dunkel an und bleibt lesbar
  const rs = lc.addStack(); rs.addSpacer();
  const ring = rs.addStack(); ring.size = new Size(RING, RING); ring.layoutVertically(); ring.centerAlignContent();
  ring.backgroundImage = ringProgressImage(RING, sets / KRAFT_GOAL_SETS, hex, 1);
  const cen = (str, size, o) => { const x = ring.addStack(); x.addSpacer(); text(x, str, size, { align: "center", minScale: 0.6, ...o }); x.addSpacer(); };
  cen(String(sets), 28, { bold: true });
  cen(`von ${KRAFT_GOAL_SETS} Sätzen`, FS.xs, { color: COL.muted, minScale: 0.5 });
  rs.addSpacer();
  lc.addSpacer(5);
  const dots = lc.addStack(); dots.centerAlignContent();
  for (let i = 0; i < 7; i++) {
    if (i) dots.addSpacer();
    text(dots, "●", FS.xs, { color: wk.days[i] ? new Color(hex) : new Color(NEUTRAL_HEX, 0.3), minScale: 1 });
  }
  lc.addSpacer(3);
  const n = wk.sessions || 0;
  text(lc, n ? `${n} ${n === 1 ? "Einheit" : "Einheiten"} diese Woche` : "Keine Einheit diese Woche", FS.xs, { color: COL.muted, lines: 2, minScale: 0.7 });

  // Rechts: Muskelalter gross, darunter je Bereich ein Balken (Wert / 60) mit Zahl
  const rc = row.addStack(); rc.layoutVertically(); rc.size = new Size(RW, 0);
  const b = d.bioAge, mt = rc.addStack(); mt.bottomAlignContent(); mt.spacing = 5;
  text(mt, "MUSKELALTER", FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
  mt.addSpacer();
  if (b && b.muscle != null) {
    text(mt, String(b.muscle), 30, { bold: true, color: new Color(hex), minScale: 0.7 });
    text(mt, "Jahre", FS.sm, { color: COL.muted, minScale: 0.8 });
  } else text(mt, MISSING, FS.lg, { color: COL.muted });
  rc.addSpacer(4);
  const parts = b && b.upper != null ? [["Oberkörper", b.upper], ["Rumpf", b.core], ["Beine", b.lower]] : [];
  const worst = Math.max(...parts.map((x) => x[1] ?? 0));
  const BW = Math.max(40, Math.round(RW * 0.4));
  parts.forEach(([label, v], i) => {
    if (i) rc.addSpacer(4);
    const r = rc.addStack(); r.centerAlignContent(); r.size = new Size(RW, 0);
    text(r, label, FS.md, { minScale: 0.7 });
    r.addSpacer();
    progressBar(r, BW, v != null ? v / 60 : 0, v === worst ? "#e39460" : hex);
    r.addSpacer(8);
    text(r, v != null ? String(v) : MISSING, FS.lg, { bold: true, color: v === worst ? new Color("#e39460") : COL.text, minScale: 0.8 });
  });
  notice(w, res, d);
  return w;
}

function messageWidget(msg) {
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  text(w, "Trainings-Widget", FS.md, { bold: true });
  text(w, msg, FS.sm, { color: COL.muted, lines: 6 });
  return w;
}

/* ---------- Start ---------- */
async function main() {
  // Widget und Siri/Kurzbefehle koennen keine Dialoge zeigen: dort direkt die Ansicht bauen, ohne Menue
  const inWidget = config.runsInWidget || config.runsWithSiri;
  if (!inWidget) {
    const keys = Object.keys(VIEWS);
    const menu = new Alert();
    menu.title = "Trainings-Widget";
    keys.forEach((k) => menu.addAction(VIEWS[k].label));
    menu.addAction("Zugangsdaten setzen / zurücksetzen");
    menu.addCancelAction("Schließen");
    const choice = await menu.presentAlert();
    if (choice === -1) return;
    const reset = choice === keys.length;
    if (!reset) VIEW = keys[choice];
    if (reset || !Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) { if (!(await askConfig())) return; if (reset) return; }
  } else if (!Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) {
    Script.setWidget(messageWidget("Bitte das Skript einmal in Scriptable öffnen und die Zugangsdaten eingeben."));
    return;
  }
  let widget;
  try { widget = VIEWS[VIEW].build(await loadData()); }
  catch (e) { widget = messageWidget(`Keine Daten (${VIEW}${e.line ? `, Zeile ${e.line}` : ""}): ${String(e.message || e)}`); }
  if (inWidget) Script.setWidget(widget);
  else { const size = VIEWS[VIEW].size; await (size === "small" ? widget.presentSmall() : size === "medium" ? widget.presentMedium() : widget.presentLarge()); }
}
await main();
Script.complete();

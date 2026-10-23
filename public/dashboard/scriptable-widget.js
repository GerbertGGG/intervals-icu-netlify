// Trainings-Widget fuer Scriptable (iOS).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfuegen.
//  2. Skript einmal in Scriptable oeffnen und ausfuehren: Es fragt nach der Adresse deines Workers
//     (https://....workers.dev, ohne Pfad) und dem Dashboard-Token. Beides landet im Schluesselbund
//     dieses iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Groesse waehlen, Skript auswaehlen.
//     Empfohlene Aufteilung: Bildschirm 1 = Gross (Heute) und zwei kleine (Ernaehrung, Schlaf und Form);
//     Bildschirm 2 = Mittel  woche,  6wochen,  block, darunter ein Smart Stack mit Ernaehrung, Kraft und VDOT.
//     Gross:   Heute. Rennen-Countdown, Kopfzeile des Coaches, Koerper- und Gefuehlswerte, Hinweis (Coach, sonst Zweck aus
//              dem Plan), heutige Einheit, Woche als ein Balken (Zeit gegen Wochenziel und Rahmen des Coaches), offene Minuten
//              je Sportart und Kraftsaetze, die naechsten drei Tage.
//     Mittel:  Ernaehrung heute (Standard).
//              Parameter  woche  (auch  training, intensitaet) = je Sportart Zeit gegen Ziel, TSS der Woche, Zonenzeit.
//              Parameter  6wochen  (auch  zeit, form, tss) = Trainingszeit und TSS der letzten 6 Wochen, Rahmen gestrichelt.
//              Parameter  block  (auch  rahmen, experiment) = Block Woche x von N, naechster Test, was erlaubt ist und was
//              nicht, Messwerte des Experiments, Abschlussziele.
//              Parameter  kraft  = Krafttraining aus EGYM.  Parameter  vdot  = VDOT und Trainingsbereiche.
//     Klein:   Schlaf und Form (Standard, auch  schlaf, bereit). Parameter  ernaehrung  oder  fitness.
//     Zeiten stehen in allen Widgets als h:mm.
// Schluesseleinheiten kennzeichnest du im Intervals-Kalender mit dem Stichwort (Tag) #key am Workout.
// CTL, TSB und TSS sind sportartuebergreifend. Fehlende Werte stehen als "–", nie als 0.
// Zugangsdaten spaeter aendern: Skript in Scriptable ausfuehren und "Zugangsdaten setzen" waehlen.
// Ein Urteil ueber den Tag gibt es nur vom Coach (kommt fertig vom Worker); das Skript stellt nur dar und bewertet nichts selbst.

const KEY_URL = "training-dashboard-url";
const KEY_TOKEN = "training-dashboard-token";

/* ---------- Ansichten ---------- */
// Jede Ansicht: Endpunkt (view=...), Zeichenfunktion, Groesse fuer die Vorschau, Name im Menue.
// Widget-Groesse und Parameter bestimmen die Ansicht (Parameter = Anfang des Wortes reicht).
const VIEWS = {
  main: { endpoint: "", build: (r) => buildWidget(r), size: "large", label: "Vorschau groß: Heute" },
  training: { endpoint: "training", build: (r) => buildWoche(r), size: "medium", label: "Vorschau mittel: Woche je Sportart" },
  time: { endpoint: "detail", build: (r) => buildSixWeeks(r), size: "medium", label: "Vorschau mittel: 6 Wochen" },
  block: { endpoint: "kompass", build: (r) => buildKompass(r), size: "medium", label: "Vorschau mittel: Block" },
  foodmedium: { endpoint: "small", build: (r) => buildFoodMedium(r), size: "medium", label: "Vorschau mittel: Ernährung heute" },
  kraft: { endpoint: "kraft", build: (r) => buildKraft(r), size: "medium", label: "Vorschau mittel: Kraft (EGYM)" },
  vdot: { endpoint: "vdot", build: (r) => buildVdot(r), size: "medium", label: "Vorschau mittel: VDOT und Trainingsbereiche" },
  schlafform: { endpoint: "small", build: (r) => buildSchlafForm(r), size: "small", label: "Vorschau klein: Schlaf und Form" },
  food: { endpoint: "small", build: (r) => buildFoodCard(r), size: "small", label: "Vorschau klein: Ernährung" },
  fitness: { endpoint: "small", build: (r) => buildFitness(r), size: "small", label: "Vorschau klein: Fitness (CTL)" },
};
// Alte Parameter fuehren auf die zusammengelegten Ansichten: form/tss -> 6 Wochen, intensitaet -> Woche,
// rahmen/experiment -> Block, schlaf/bereit -> Schlaf und Form
const PARAM_VIEWS = {
  medium: { fallback: "foodmedium", rules: [[/^(zeit|time|dauer|min|form|tss|belast|hm|detail|6)/, "time"], [/^(woche|week|intens|polar|train|tri)/, "training"], [/^(vdot|pace|bereich|zone)/, "vdot"], [/^(kraft|egym|strength|gym)/, "kraft"], [/^(block|ziel|saison|season|exp|versuch|rahmen|frame|leit|regel|kompass)/, "block"], [/^(ern|food|essen|kcal)/, "foodmedium"]] },
  small: { fallback: "schlafform", rules: [[/^(ern|food|essen|kcal)/, "food"], [/^(fit|ctl)/, "fitness"]] },
};
const WIDGET_PARAM = String((typeof args !== "undefined" && args.widgetParameter) || "").trim().toLowerCase();
function viewFromWidget() {
  const fam = config.widgetFamily, set = PARAM_VIEWS[fam];
  if (!set) return "main";
  const hit = set.rules.find(([re]) => re.test(WIDGET_PARAM));
  return hit ? hit[1] : set.fallback;
}
let VIEW = typeof config !== "undefined" && config.runsInWidget ? viewFromWidget() : "main";
// Der Cache haengt am Endpunkt, nicht an der Ansicht
const endpointOf = () => VIEWS[VIEW].endpoint;
const cacheFile = () => `training-widget${endpointOf() ? "-" + endpointOf() : ""}-cache.json`;

/* ---------- Design ---------- */
const dyn = (l, d) => Color.dynamic(new Color(l), new Color(d));
// iOS-Systemfarben: flache Widget-Flaeche, graue Kacheln nur zum Gruppieren, Ampel nur mit Bedeutung,
// Blau fuer Werte, Rot-Pink fuer Datum und Rennen (wie im Kalender-Widget), Indigo fuer Schlaf
const COL = {
  bg: dyn("#ffffff", "#1c1c1e"), card: dyn("#ffffff", "#1c1c1e"), tile: dyn("#f2f2f7", "#2c2c2e"),
  text: dyn("#1c1c1e", "#ffffff"), muted: dyn("#6c6c70", "#8e8e93"), line: dyn("#e5e5ea", "#38383a"),
  ok: dyn("#248a3d", "#30d158"), warn: dyn("#c93400", "#ff9f0a"), bad: dyn("#d70015", "#ff453a"), none: dyn("#6c6c70", "#8e8e93"),
  accent: dyn("#0a63c9", "#64a8ff"), race: dyn("#d30f45", "#ff375f"), sleep: dyn("#5856d6", "#7d7aff"),
};
// Gezeichnete Bilder (DrawContext) kennen keine dynamischen Farben, und im Widget meldet iOS den Dunkelmodus nicht
// (Device.isUsingDarkAppearance liefert dort immer false). Bilder bekommen deshalb Farben, die auf hellem und dunklem
// Grund funktionieren: kraeftige Systemfarben, mittleres Grau fuer Marken und Beschriftung, Blau fuer "heute".
const INK_HEX = "#a1a1a6";
const MUTED_HEX = "#8e8e93";
const NEUTRAL_HEX = "#8e8e93";
const ACCENT_HEX = "#0a84ff";
const ZONE_RGB = { ok: "#30d158", warn: "#ff9f0a", bad: "#ff453a", none: NEUTRAL_HEX };
const RACE_HEX = "#ff375f";
const SLEEP_HEX = "#7d7aff";
// Sportfarben wie auf der Uhr und im Dashboard: Laufen gelb, Rad grün, Schwimmen blau, Kraft orange; dazu je Sport ein SF Symbol
const SPORTS = { run: ["Laufen", "#ffd60a"], bike: ["Rad", "#30d158"], swim: ["Schwimmen", "#409cff"], strength: ["Kraft", "#ff9f45"], other: ["Sonst.", NEUTRAL_HEX] };
const SPORT_SYMBOL = { run: "figure.run", bike: "bicycle", swim: "figure.pool.swim", strength: "dumbbell.fill", other: "figure.mixed.cardio" };
const sportHex = (k) => (SPORTS[k] || SPORTS.other)[1];
// Schriftgroessen: nichts unter 11 pt (auf dem Homescreen sonst kaum lesbar)
const FS = { xs: 11, sm: 12, md: 14, lg: 17, xl: 22, xxl: 30 };
const MISSING = "–";
const colorFor = (cls) => (cls === "coach" ? COL.accent : COL[cls] || COL.none);
// Ampel nie nur ueber die Farbe: jede Stufe hat eine eigene Form (SF Symbols), dazu steht immer ein Text.
// Urteil: Kreis mit Haken / Dreieck mit Ausrufezeichen / Quadrat mit Kreuz, ohne Urteil gestrichelt.
const STATUS_SYMBOL = { ok: "checkmark.circle.fill", warn: "exclamationmark.triangle.fill", bad: "xmark.square.fill", none: "circle.dashed", coach: "text.bubble.fill" };
function symbol(parent, name, size, color) {
  const sym = typeof SFSymbol !== "undefined" ? SFSymbol.named(name) : null;
  if (!sym) return text(parent, "●", size, { color, minScale: 1 }); // aeltere Systeme ohne das Symbol
  sym.applyFont(Font.boldSystemFont(size));
  const im = parent.addImage(sym.image);
  im.imageSize = new Size(size, size);
  im.applyFittingContentMode();
  im.tintColor = color;
  return im;
}
const statusIcon = (parent, cls, size) => symbol(parent, STATUS_SYMBOL[cls] || STATUS_SYMBOL.none, size, colorFor(cls));
// Zahlen in der runden Systemschrift (wie die Apple-Widgets), Einheit klein und gedaempft dahinter
function num(parent, str, size, opts = {}) {
  const t = text(parent, str, size, { minScale: 0.6, ...opts });
  t.font = Font.boldRoundedSystemFont(size);
  return t;
}
function valueUnit(parent, value, unit, size, { color = COL.text } = {}) {
  const s = parent.addStack(); s.bottomAlignContent(); s.spacing = 3;
  num(s, value, size, { color });
  if (unit && value !== MISSING) { const u = s.addStack(); u.layoutVertically(); text(u, unit, Math.max(FS.sm, Math.round(size * 0.4)), { bold: true, color: COL.muted, minScale: 1 }); u.addSpacer(Math.round(size * 0.12)); }
  return s;
}
// Sportsymbol im getoenten Kreis (wie in Apple Fitness)
function sportBadge(parent, k, d = 22) {
  const c = parent.addStack();
  c.size = new Size(d, d); c.cornerRadius = d / 2; c.centerAlignContent();
  c.backgroundColor = new Color(sportHex(k), 0.22);
  c.addSpacer(); symbol(c, SPORT_SYMBOL[k] || SPORT_SYMBOL.other, Math.round(d * 0.56), new Color(sportHex(k))); c.addSpacer();
  return c;
}
// Kleine Kapsel mit getoenter Flaeche (Rennen)
function pill(parent, str, color, hex) {
  const s = parent.addStack();
  s.backgroundColor = new Color(hex, 0.16); s.cornerRadius = 9; s.setPadding(2, 8, 2, 8);
  text(s, str, FS.xs, { bold: true, color, minScale: 0.7 });
  return s;
}

// Groesse des Widgets in Punkten nach Bildschirmbreite (Apple-Sollwerte): [Bildschirmbreite, Breite Mittel/Gross, Seite Klein, Hoehe Gross]
const WIDGET_SIZES = [[430, 364, 170, 382], [428, 364, 170, 382], [414, 360, 169, 379], [393, 338, 158, 354], [390, 338, 158, 354], [375, 321, 148, 338]];
function widgetSize() {
  const sw = Math.min(Device.screenSize().width, Device.screenSize().height);
  const hit = WIDGET_SIZES.find(([w]) => sw >= w) || WIDGET_SIZES[WIDGET_SIZES.length - 1];
  return { w: hit[1], small: hit[2], large: hit[3] };
}
const PAD = 14; // einheitlicher Rand aller Widgets
const widgetInnerWidth = () => widgetSize().w - 2 * PAD;
const smallInnerWidth = () => widgetSize().small - 2 * PAD;
const baseUrl = () => (Keychain.get(KEY_URL).match(/^https:\/\/[^\/\s?#]+/) || [Keychain.get(KEY_URL)])[0];

// Gemeinsamer Rahmen: Hintergrund, Rand, Link auf das Dashboard, Aktualisierung. flat = Karte ist die Widget-Flaeche
function baseWidget({ flat = true, padV = 12 } = {}) {
  const w = new ListWidget();
  w.backgroundColor = COL.card; // flach: Inhalte direkt auf der Flaeche, graue Kacheln nur zum Gruppieren
  w.setPadding(padV, PAD, padV - 2, PAD);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 15 * 60 * 1000);
  return w;
}

// Morgens kommen die Uhrwerte in Etappen (erst ein vorlaeufiger Ruhepuls, Schlaf und HRV einige Minuten spaeter).
// Fehlt bis 11 Uhr noch Schlaf oder HRV, frueher nachladen, damit die Werte nicht auf dem Zwischenstand stehen bleiben.
function refreshSoonIfPending(w, r) {
  const pending = r && (r.hrv == null || (r.sleepHours == null && !(r.body && r.body.sleep && r.body.sleep.score != null)));
  if (pending && new Date().getHours() < 11) w.refreshAfterDate = new Date(Date.now() + 5 * 60 * 1000);
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
// Dauer aus Minuten, in allen Widgets gleich als h:mm (1:20, 2:00, 0:45)
const fmtHm = (min) => { if (min == null || !Number.isFinite(Number(min))) return MISSING; const t = Math.round(Number(min)); return `${Math.floor(t / 60)}:${String(t % 60).padStart(2, "0")}`; };
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

// Fortschrittsbalken. opts.mark = Marke als Anteil des Ziels (z. B. "Soll bis jetzt"), opts.overHex = Farbe für den Überhang:
// Liegt der Wert über dem Ziel (ratio > 1), reicht der Balken bis zum Wert, die Zielmarke steht dann innen und der Rest ist der Überhang.
function progressBar(w, IW, ratio, hex, alpha = 1, opts = {}) {
  const dc = newCtx(IW, 6), track = new Path();
  track.addRoundedRect(new Rect(0, 0, IW, 6), 3, 3);
  dc.addPath(track); dc.setFillColor(new Color(NEUTRAL_HEX, 0.25)); dc.fillPath();
  const over = opts.overHex && ratio > 1, full = over ? ratio : 1;
  if (ratio > 0) {
    const bar = new Path(), base = (Math.min(1, ratio) / full) * IW;
    bar.addRoundedRect(new Rect(0, 0, Math.max(6, base), 6), 3, 3);
    dc.addPath(bar); dc.setFillColor(new Color(hex, alpha)); dc.fillPath();
    if (over) {
      const x0 = IW / full, ex = new Path();
      ex.addRoundedRect(new Rect(x0, 0, IW - x0, 6), 3, 3);
      dc.addPath(ex); dc.setFillColor(new Color(opts.overHex, alpha)); dc.fillPath();
    }
  }
  dc.setFillColor(new Color(INK_HEX, 0.9));
  if (over) dc.fillRect(new Rect(IW / full - 1, -1, 2, 8)); // Ziel
  if (opts.mark != null) { const x = (opts.mark / full) * IW; dc.fillRect(new Rect(x - 0.5, -2, 1.2, 10)); } // Soll bis jetzt
  const im = w.addImage(dc.getImage()); im.imageSize = new Size(IW, 6);
}
// Soll bis jetzt: Anteil des Tagesziels, der nach Tageszeit erreicht sein sollte (linear von 6 bis 22 Uhr, grobe Orientierung)
function dayFraction() { const n = new Date(), h = n.getHours() + n.getMinutes() / 60; return Math.max(0, Math.min(1, (h - 6) / 16)); }
// Feste Farbe je Ernährungsgröße, passend zur Seite (Kalorien Rad-Grün, Eiweiß Schwimm-Blau, Kohlenhydrate Lauf-Gelb, Fett Kraft-Orange)
const OVER_HEX = "#f0b95a"; // Wert ueber dem Ziel: gelb (Warnung); Rot bleibt Alarmen vorbehalten
// Ernaehrung in eigenen Farben, nicht in Sportfarben (Gelb heisst sonst Laufen)
const NUT_HEX = { kcal: "#40c8e0", protein: "#bf5af2", carbs: "#ac8e68", fat: "#ff6482" };

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
// Koerper und Gefuehl als Werte ohne Ampel (die Einordnung macht der Coach); Gefuehl als Zahl, 1 = bestmoeglich
const feelWord = (it) => (it && it.v != null ? String(it.v) : MISSING);
const FEEL = [["Muskelkater", "Muskeln"], ["Stimmung", "Stimmung"], ["Motivation", "Motiv."]];
// Urteil des Coaches (go) als Text und Farbe
const GO_TEXT = { hart: ["Hart erlaubt", "ok"], locker: ["Nur locker", "warn"], frei: ["Frei", "bad"] };

// Zweck aus dem Plan nur, wenn er ein Satz ist und keine Intervall-/Zonenzeile
function usablePurpose(s) {
  const t = String(s || "").replace(/\bRPE\s*:?\s*\d[\d\s–-]*/gi, "").trim();
  if (t.length < 25 || t.split(/\s+/).length < 4) return null;
  if (/^[\d\-•*]/.test(t) || /\d\s*(%|x\b)|\bz[1-7]\b|\bbpm\b|\d+\s*min\b/i.test(t)) return null;
  return t;
}
// Ohne Coach-Notiz: der Zweck aus dem Plan (so wie er im Kalender steht), sonst nur der Hinweis, dass der Coach noch nicht
// geschrieben hat. Keine eigenen Ratschlaege: die Einordnung macht der Coach.
function whyText(p) {
  const pu = p && usablePurpose(p.purpose);
  return pu || "Der Coach hat heute noch nicht geschrieben.";
}

// Kurzlabel unter der Dauer in den Tagekacheln
function shortLabel(e) {
  const n = e.name || "";
  if (e.key) return "Schlüssel";
  if (/locker|easy|ruhig|regen|recovery|grundlagen/i.test(n)) return "locker";
  if (/intervall|tempo|schwelle|threshold|interval/i.test(n)) return "intensiv";
  return n || "Einheit";
}

const SPORT_LABEL = { run: "Laufen", bike: "Rad", swim: "Schwimmen", strength: "Kraft" };

// Sportart zuerst, dann die Art der Einheit (bei Rad/Schwimmen/Kraft ohne Zusatz, wenn es nur der Sport selbst wäre)
function tileSub(e) {
  const sport = SPORT_LABEL[e.sport];
  const label = shortLabel(e);
  if (!sport) return label;
  if (e.sport !== "run" && (label === "Einheit" || label === e.name || label === sport)) return sport;
  return `${sport} · ${label}`;
}

// Tageskachel (naechste drei Tage) in einer Zeile: Tag, Dauer (oder Pause/Rennen) und rechts das Sportsymbol
function dayTile(row, TW, iso, g, upcoming) {
  const race = !!g && g.date === iso;
  const items = upcoming ? upcoming.filter((e) => e.date === iso) : null;
  const t = row.addStack();
  t.size = new Size(TW, 0); t.cornerRadius = 10; t.setPadding(5, 8, 5, 8); t.centerAlignContent();
  t.backgroundColor = race ? new Color(RACE_HEX, 0.16) : COL.tile;
  let main;
  if (race) main = "Rennen";
  else if (!items) main = MISSING;
  else if (!items.length) main = "Pause";
  else { const mins = items.reduce((a, e) => a + (e.durationMin || 0), 0); main = mins ? fmtHm(mins) : shortLabel(items[0]); }
  text(t, dayShort(iso), FS.xs, { bold: true, color: race ? COL.race : COL.muted, minScale: 1 });
  t.addSpacer(5);
  text(t, main, FS.sm, { bold: true, color: race ? COL.race : items && !items.length ? COL.muted : COL.text, minScale: 0.7 });
  t.addSpacer();
  // Ein Symbol je Sportart des Tages (hoechstens zwei), sonst sieht Kraft + Rad wie reine Kraft aus
  if (items && items.length && !race) [...new Set(items.map((e) => e.sport))].slice(0, 2).forEach((sp, k) => { if (k) t.addSpacer(3); symbol(t, SPORT_SYMBOL[sp] || SPORT_SYMBOL.other, 11, new Color(sportHex(sp))); });
}

// Senkrechter Streifen (xa bis xb) aus einer Kapsel als Pfad: so wird eine Kapsel in Spalten geteilt (z. B. Rad und Kraft)
function capsuleColumn(x, y, w, h, xa, xb) {
  const r = Math.min(w / 2, h / 2), cx = x + w / 2;
  const half = (yy) => {
    if (yy < y + r) { const d = y + r - yy; return Math.sqrt(Math.max(0, r * r - d * d)); }
    if (yy > y + h - r) { const d = yy - (y + h - r); return Math.sqrt(Math.max(0, r * r - d * d)); }
    return w / 2;
  };
  const n = Math.max(8, Math.ceil(h * 2)), left = [], right = [];
  for (let k = 0; k <= n; k++) {
    const yy = y + (h * k) / n, a = Math.max(xa, cx - half(yy)), b = Math.min(xb, cx + half(yy));
    if (a < b) { left.push(new Point(a, yy)); right.push(new Point(b, yy)); }
  }
  const p = new Path(); p.addLines([...left, ...right.reverse()]); p.closeSubpath();
  return p;
}

// Woche Mo bis So als Kreise: erledigt = gefuellt in Sportfarbe (mehrere Sportarten geteilt), geplant und noch offen =
// Umriss in der Sportfarbe, heute mit weissem Ring. Tage ohne alles bleiben ein blasser Kreis.
const CAPS_LABEL_H = 14, CAPS_PAD = 4; // Rand fuer den Heute-Ring (2 px ausserhalb des Kreises plus halbe Strichbreite)
function capsImage(W, H, days, todayIso) {
  // Echte Kreise: Durchmesser = Hoehe, der Abstand verteilt den Rest der Breite
  const P = CAPS_PAD, dc = newCtx(W, P + H + 2 + CAPS_LABEL_H), cw = Math.min(H, (W - 2 * P - 4 * 6) / 7), gap = (W - 2 * P - 7 * cw) / 6, r = cw / 2;
  const cap = (rect, hex, a = 1, stroke = false) => {
    const p = new Path(); p.addEllipse(rect); dc.addPath(p);
    if (stroke) { dc.setStrokeColor(new Color(hex, a)); dc.setLineWidth(1.5); dc.strokePath(); } else { dc.setFillColor(new Color(hex, a)); dc.fillPath(); }
  };
  dc.setTextAlignedCenter();
  days.forEach((x, i) => {
    const x0 = P + i * (cw + gap), today = x.date === todayIso;
    const y0 = P + H - cw;
    cap(new Rect(x0, y0, cw, cw), NEUTRAL_HEX, 0.2);
    // Erledigt: voller Kreis in Sportfarbe; mehrere Sportarten teilen ihn senkrecht in gleich breite Teile
    // (groesster Load links), damit auch eine kurze Kraft-Einheit neben dem Rad klar zu sehen ist
    const parts = Object.entries(x.sports || {}).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]).slice(0, 3);
    const tot = parts.reduce((a, [, v]) => a + v, 0);
    if (tot > 0) {
      if (parts.length === 1) cap(new Rect(x0, y0, cw, cw), sportHex(parts[0][0]));
      else {
        const sw = cw / parts.length;
        parts.forEach(([k], j) => {
          const xa = x0 + j * sw + (j ? 0.7 : 0), xb = x0 + (j + 1) * sw - (j < parts.length - 1 ? 0.7 : 0);
          dc.addPath(capsuleColumn(x0, y0, cw, cw, xa, xb)); dc.setFillColor(new Color(sportHex(k))); dc.fillPath();
        });
      }
    }
    // Geplant und noch nicht erledigt (heute oder spaeter): Umriss in der Sportfarbe
    const open = x.date >= todayIso ? (x.plan || []).filter((p) => !(x.sports || {})[p.sport]) : [];
    if (open.length) {
      cap(new Rect(x0 + 1, y0 + 1, cw - 2, cw - 2), sportHex(open[0].sport), 0.95, true);
    }
    if (today) { const p = new Path(); p.addEllipse(new Rect(x0 - 2, y0 - 2, cw + 4, cw + 4)); dc.addPath(p); dc.setStrokeColor(new Color(ACCENT_HEX)); dc.setLineWidth(2); dc.strokePath(); }
    dc.setTextColor(new Color(today ? ACCENT_HEX : MUTED_HEX));
    dc.setFont(today ? Font.boldSystemFont(FS.xs) : Font.mediumSystemFont(FS.xs));
    dc.drawTextInRect(dayShort(x.date), new Rect(x0 - 6, P + H + 2, cw + 12, CAPS_LABEL_H));
  });
  return dc.getImage();
}

// Workout-Profil: Bloecke nebeneinander, Breite = Dauer, Hoehe = Intensitaet (% der Schwelle); ab 80 % in Sportfarbe, darunter grau
function profileImage(W, H, steps, sport) {
  const dc = newCtx(W, H), total = steps.reduce((a, s) => a + (s.secs || 0), 0) || 1;
  let x = 0;
  for (const s of steps) {
    const w = (W * (s.secs || 0)) / total, pct = s.pct ?? 70;
    const h = Math.max(0.25, Math.min(1, (pct - 40) / 70)) * H;
    if (w - 2 > 0.5) {
      const p = new Path(); p.addRoundedRect(new Rect(x, H - h, w - 2, h), Math.min(3, h / 2), Math.min(3, h / 2)); dc.addPath(p);
      dc.setFillColor(pct >= 80 ? new Color(sportHex(sport)) : new Color(NEUTRAL_HEX, 0.45)); dc.fillPath();
    }
    x += w;
  }
  return dc.getImage();
}

// Rennen als Kapsel im Kopf: Name kurz, bis 8 Wochen in Tagen, sonst in Wochen
function raceShort(g) {
  const name = String(g.name || "Rennen").replace(/^triathlon\s+/i, "");
  if (g.daysToGo === 0) return `${name} heute`;
  return g.daysToGo <= 56 ? `${name} · ${g.daysToGo} T` : `${name} · ${Math.round(g.daysToGo / 7)} W`;
}
const MONTH_SHORT = ["Jan", "Feb", "März", "Apr", "Mai", "Juni", "Juli", "Aug", "Sep", "Okt", "Nov", "Dez"];

function buildWidget(res) {
  const d = res.data;
  const W = widgetInnerWidth();
  const g = d.goal;
  const r = d.readiness, v = r.verdict, p = d.plan.today[0];
  const w = baseWidget({ padV: 12 });
  refreshSoonIfPending(w, r);

  // 1. Kopf: Wochentag und Datum wie im Kalender-Widget, Abrufzeit, Rennen als Kapsel (nur bis zum Renntag)
  const head = w.addStack(); head.centerAlignContent(); head.size = new Size(W, 0);
  const dt = new Date(d.today + "T12:00:00");
  text(head, `${DAY_FULL[dt.getDay()]} ${dt.getDate()}. ${MONTH_SHORT[dt.getMonth()]}`.toUpperCase(), FS.xs, { bold: true, color: COL.race, minScale: 0.8 });
  text(head, ` · ${clock(d.generatedAt)}`, FS.xs, { color: COL.muted, minScale: 1 });
  head.addSpacer();
  if (g && g.daysToGo != null && g.daysToGo >= 0) pill(head, raceShort(g), COL.race, RACE_HEX);
  w.addSpacer(5);

  // 2. Urteil des Coaches: Kapsel "Hart erlaubt" / "Nur locker" / "Frei" (Farbe aus seinem go), daneben seine Blocker.
  // Ohne go keine Zeile (seine Notiz steht unten in der Kachel, die Kopfzeile wuerde dasselbe noch einmal sagen), ohne
  // Aussage von heute grau "Noch keine Coach-Auswertung". Kein eigenes Urteil des Widgets.
  const c0 = d.coach, go = c0 && GO_TEXT[c0.go];
  if (go || !c0) {
  const vr = w.addStack(); vr.centerAlignContent(); vr.spacing = 6; vr.size = new Size(W, 0);
  if (go) {
    const gp = vr.addStack(); gp.cornerRadius = 11; gp.setPadding(3, 10, 3, 10); gp.backgroundColor = new Color(ZONE_RGB[go[1]] || NEUTRAL_HEX, 0.22);
    text(gp, go[0], FS.md, { bold: true, color: colorFor(go[1]), minScale: 0.8 });
    const bl = (c0.blockers || []).join(" · ");
    if (bl) { symbol(vr, "exclamationmark.triangle.fill", 11, COL.warn); text(vr, bl, FS.xs, { bold: true, color: COL.warn, lines: 2, minScale: 0.7 }); }
  } else {
    statusIcon(vr, v.cls, 15);
    text(vr, v.text, FS.md, { bold: true, color: v.cls === "none" ? COL.muted : COL.text, lines: 2, minScale: 0.7 });
  }
  vr.addSpacer();
  w.addSpacer(6);
  } else w.addSpacer(2);

  // Koerper und Gefuehl als eine kompakte Zeile: Wert gross, Kurzname klein darunter (Gefuehl 1 = bestmoeglich). Gefuehl in
  // Farbe nur gegen den eigenen ueblichen Bereich (cls vom Worker): gruen = wie ueblich, gelb = schlechter, rot = deutlich schlechter
  const vals = [
    [r.sleepHours != null ? `${fmt(r.sleepHours, 1)}h` : MISSING, "Schlaf"], [r.hrv != null ? fmt(r.hrv) : MISSING, "HRV"], [fmt(r.restingHR), "Puls"],
    ...FEEL.map(([lab, short]) => { const it = (r.items || []).find((i) => i.label === lab); return [feelWord(it), short, it && it.v != null && ["ok", "warn", "bad"].includes(it.cls) ? colorFor(it.cls) : null]; }),
  ];
  const vrow = w.addStack(); vrow.size = new Size(W, 0);
  const VW = W / vals.length;
  vals.forEach(([val, lab, col]) => {
    const cell = vrow.addStack(); cell.layoutVertically(); cell.size = new Size(VW, 0);
    const a = cell.addStack(); a.addSpacer(); num(a, val, FS.md, { color: val === MISSING ? COL.muted : col || COL.text, minScale: 0.6 }); a.addSpacer();
    const b = cell.addStack(); b.addSpacer(); text(b, lab, FS.xs, { color: COL.muted, minScale: 0.6 }); b.addSpacer();
  });
  w.addSpacer(7);

  // 3. Hinweis in einer grauen Kachel: Notiz des Coaches (Strich blau), sonst der Zweck aus dem Plan (Strich grau)
  const coachLine = d.coach && d.coach.note ? d.coach.note : null;
  const rawWhy = coachLine;
  // Nie abschneiden: ganze Saetze, solange sie in 3 Zeilen passen. Der erste Satz steht immer komplett da.
  const sentences = (s) => {
    const parts = s.trim().split(/(?<=[.!?])\s+(?=[A-ZÄÖÜ])/);
    let out = parts[0];
    for (const x of parts.slice(1)) { if ((out + " " + x).length > 140) break; out += " " + x; }
    return out;
  };
  const why = rawWhy ? sentences(rawWhy) : whyText(p);
  // Hoehe nach Textlaenge schaetzen (gut 6 pt je Zeichen bei 12 pt) und fest setzen: In einem Stack mit fester Hoehe und
  // fester Textbreite bricht Scriptable zuverlaessig um; ohne das blieb der Text einzeilig mit "...".
  // 5,7 pt je Zeichen (gemessen: 48 Zeichen fuellen eine Zeile); 6,1 hat eine Zeile zu viel geschaetzt
  const TXW = W - 9 - 3 - 9 - 10, perLine = Math.max(20, Math.floor(TXW / 5.7));
  const nLines = Math.max(1, Math.ceil(why.length / perLine));
  const ct = w.addStack(); ct.size = new Size(W, nLines * 16 + 12); ct.backgroundColor = COL.tile; ct.cornerRadius = 12; ct.setPadding(6, 9, 6, 10); ct.centerAlignContent();
  const bar = ct.addStack(); bar.size = new Size(3, nLines * 16); bar.cornerRadius = 1.5;
  bar.backgroundColor = coachLine ? COL.accent : COL.muted;
  ct.addSpacer(9);
  const tx = ct.addStack(); tx.layoutVertically(); tx.size = new Size(TXW, 0);
  text(tx, why, FS.sm, { lines: nLines + 1, minScale: 0.85 });
  w.addSpacer(10);

  // 4. Heute: Sportsymbol, Einheit, darunter Dauer, Distanz und RPE; ohne Einheit Ruhetag und die naechste Einheit.
  // Mit Wochenzielen und ohne geplante Einheit entfaellt die Zeile: Was zu tun ist, steht unten unter "offen".
  const wk = d.week || {};
  // Ohne Einheit heute ist unten Platz: darunter die naechsten drei Tage (nicht bei langem Hinweis)
  // Platz unter der Woche (Punkte), geschaetzt aus dem, was darueber steht: gemessen frei sind 60 pt im grossen Widget
  // (Hoehe 354) bei drei Zeilen Hinweis, ohne Urteils-Zeile, ohne Einheit heute und ohne Quellen-Hinweis
  const room = (extra) => 60 + (widgetSize().large - 354) + 16 * (3 - nLines) - (go || !c0 ? 27 : 0) - (res.stale || (d.sourcesFailed || []).length ? 18 : 0) - extra;
  // Mit Zeitzielen steht die Woche als ein Balken da (Ziel und Rahmen als Marken), ohne eigene Zeile fuer die Wochenziele
  if (wk.rings && !p) return buildWeekTargets(w, W, d, wk, res, room(0));
  const tr = w.addStack(); tr.centerAlignContent(); tr.spacing = 9; tr.size = new Size(W, 0);
  if (p) sportBadge(tr, p.sport, 32);
  else { const c = tr.addStack(); c.size = new Size(32, 32); c.cornerRadius = 16; c.backgroundColor = COL.tile; c.centerAlignContent(); c.addSpacer(); symbol(c, "bed.double.fill", 16, COL.muted); c.addSpacer(); }
  const tc = tr.addStack(); tc.layoutVertically();
  if (p) {
    text(tc, p.name || "Einheit", FS.lg, { bold: true, minScale: 0.7 });
    const meta = [p.durationMin ? fmtHm(p.durationMin) : p.distanceKm && `${fmt(p.distanceKm, 1)} km`, p.rpe && `RPE ${p.rpe}`].filter(Boolean).join(" · ");
    if (meta) text(tc, meta, FS.xs, { color: COL.muted });
  } else {
    text(tc, "Keine Einheit geplant", FS.lg, { bold: true, minScale: 0.7 });
    const nx = d.plan.next;
    if (nx) text(tc, `Nächste: ${dayShort(nx.date)} ${dateShort(nx.date)} · ${nx.items && nx.items.length ? nx.items.map(tileSub).join(" + ") : nx.name || "Einheit"}`, FS.xs, { color: COL.muted, minScale: 0.6 });
  }
  tr.addSpacer();
  // Aufbau der geplanten Einheit: Breite = Dauer, Hoehe = Intensitaet, Arbeitsbloecke in Sportfarbe. Mit Wochenzielen ist
  // der Platz knapp: dann nur bei kurzem Hinweis und nur mit Arbeitsbloecken (eine gleichmaessige Einheit waere nur ein grauer Balken).
  const structured = !!(p && p.steps && p.steps.some((x) => (x.pct ?? 0) >= 80));
  const showProfile = !!(p && p.steps && p.steps.length) && (!wk.rings || (structured && nLines <= 2));
  if (showProfile) { w.addSpacer(8); const im = w.addImage(profileImage(W, 22, p.steps, p.sport)); im.imageSize = new Size(W, 22); }
  w.addSpacer(10);

  if (wk.rings) return buildWeekTargets(w, W, d, wk, res, room(42 + (showProfile ? 30 : 0)));
  goalsLine(w, W, wk.goals);

  // 5. Woche: Kapseln Mo bis So, rechts TSS bisher (gegen Wochenziel oder Vorwoche) und die Frische als kleine Zeile
  const wr = w.addStack(); wr.bottomAlignContent(); wr.size = new Size(W, 0); wr.spacing = 8;
  const KW = Math.round(W * 0.6), KH = Math.floor((KW - 2 * CAPS_PAD - 24) / 7); // Kreisdurchmesser passend zur Breite
  if (wk.days) { const im = wr.addImage(capsImage(KW, KH, wk.days, d.today)); im.imageSize = new Size(KW, CAPS_PAD + KH + 2 + CAPS_LABEL_H); }
  wr.addSpacer();
  const wc = wr.addStack(); wc.layoutVertically();
  const right = (fn) => { const s = wc.addStack(); s.addSpacer(); fn(s); };
  right((s) => { s.bottomAlignContent(); num(s, fmt(wk.total), FS.xl); text(s, wk.goal ? ` / ${fmt(wk.goal)} TSS` : " TSS", FS.xs, { bold: true, color: COL.muted, minScale: 0.8 }); });
  // Vorwoche und Frische in einer kleinen Zeile: die Frische ist nur eine berechnete Zahl und soll nicht dominieren
  if (wk.lastTotal != null) right((s) => text(s, `Vorwoche ${fmt(wk.lastTotal)}`, FS.xs, { color: COL.muted, minScale: 0.7 }));
  if (wk.tsb != null) right((s) => text(s, `TSB ${signed(wk.tsb)}`, FS.xs, { color: COL.muted, minScale: 0.7 }));
  // Ohne Kapseln (alte Cache-Daten) der Balken gegen das Wochenziel
  if (wk.goal && !wk.days) { w.addSpacer(4); progressBar(w, W, wk.total / wk.goal, ACCENT_HEX); }
  w.addSpacer(6);

  // 6. Naechste Tage: drei Kacheln, nur wenn Platz ist (die Kreise zeigen den Plan der Woche ohnehin als Umriss)
  // Platz: ein langer Hinweis hat Vorrang; mit Profil der Einheit reicht er schon ab drei Zeilen nicht mehr fuer die Kacheln
  const withProfile = !!(p && p.steps && p.steps.length);
  if (nLines > 3 || (withProfile && nLines >= 3)) { notice(w, res, d); w.addSpacer(); return w; }
  const TW = Math.floor((W - 12) / 3);
  const tiles = w.addStack(); tiles.size = new Size(W, 0); tiles.spacing = 6;
  for (let i = 1; i <= 3; i++) dayTile(tiles, TW, addIso(d.today, i), g, d.plan.upcoming);
  notice(w, res, d);
  w.addSpacer();
  return w;
}

// Zeile mit den Wochenzielen des Coaches (Worker: week.goals): Ausdauer h:mm gegen das Wochenziel, Kraftsaetze in 2 Wochen.
// Werte in Textfarbe, Ziele gedaempft; kein Rot unter dem Ziel. Ohne Daten (alter Worker) keine Zeile.
function goalsLine(w, W, g) {
  if (!g) return;
  const hm = (m) => `${Math.floor(Math.max(0, m) / 60)}:${String(Math.round(Math.max(0, m)) % 60).padStart(2, "0")}`;
  const row = w.addStack(); row.centerAlignContent(); row.size = new Size(W, 0);
  text(row, "Ausdauer ", FS.xs, { color: COL.muted, minScale: 0.7 }); text(row, hm(g.min), FS.xs, { bold: true, minScale: 0.7 }); text(row, `/${hm(g.goalMin)}`, FS.xs, { color: COL.muted, minScale: 0.7 });
  text(row, " · Kraft ", FS.xs, { color: COL.muted, minScale: 0.7 }); text(row, fmt(g.sets2w), FS.xs, { bold: true, minScale: 0.7 }); text(row, `/${fmt(g.goalSets2w)}`, FS.xs, { color: COL.muted, minScale: 0.7 });
  row.addSpacer();
  w.addSpacer(6);
}

// Woche im grossen Widget mit Wochenzielen des Coaches: eine Zeile mit der Zeit dieser Woche, dem Wochenziel (Summe der
// Zeitziele je Sportart) und dem Rahmen (Ausdauerstunden, week.goals), darunter ein Balken mit beiden als Marken: Ziel
// durchgezogen, Rahmen gestrichelt. TSS und Frische stehen im Widget "Woche" (Parameter training), nicht hier.
// Darunter ueber die volle Breite je Sportart eine Kapsel mit dem, was noch offen ist (Minuten, Kraft in Saetzen;
// voll mit Haken = erreicht). room = freier Platz darunter (pt): er fuellt sich mit den naechsten drei Tagen (wenn dort
// etwas geplant ist) bzw. der Woche Mo bis So, der Rueckfrage des Coaches und zuletzt mit hoeheren Kapseln.
// TSS-Korridor als Text: "220–275", ohne Untergrenze "≤ 275"
function tssRange(lo, hi) { return lo > 0 ? `${fmt(lo)}–${fmt(hi)}` : `≤ ${fmt(hi)}`; }
// Balken: Fuellung = erledigt, Strich = Ziel, gestrichelter Strich = Rahmen; Massstab = das Groesste der drei
function weekBarImage(W, H, v, goal, frame) {
  const dc = newCtx(W, H), BH = 8, y = (H - BH) / 2, top = Math.max(v, goal || 0, frame || 0) || 1, x = (m) => Math.min(W - 1, Math.max(1, (m / top) * W));
  const track = new Path(); track.addRoundedRect(new Rect(0, y, W, BH), BH / 2, BH / 2); dc.addPath(track); dc.setFillColor(new Color(NEUTRAL_HEX, 0.25)); dc.fillPath();
  if (v > 0) { const f = new Path(); f.addRoundedRect(new Rect(0, y, Math.max(BH, (v / top) * W), BH), BH / 2, BH / 2); dc.addPath(f); dc.setFillColor(new Color(ACCENT_HEX)); dc.fillPath(); }
  const tick = (m, dashed) => {
    const px = x(m) - 1;
    for (let t = 0; t < H; t += dashed ? 4 : H) { dc.setFillColor(new Color(INK_HEX)); dc.fillRect(new Rect(px, t, 2, dashed ? 2 : H)); }
  };
  if (goal > 0) tick(goal, false);
  if (frame > 0 && frame !== goal) tick(frame, true);
  return dc.getImage();
}
function buildWeekTargets(w, W, d, wk, res, room = 0) {
  const zeit = wk.rings.zeit;
  // Unten, was in den Platz passt: die naechsten drei Tage, sofern dort etwas geplant ist (sonst stuende dreimal "Pause"),
  // sonst die Woche Mo bis So (Kreise so gross, wie Breite und Platz erlauben), danach die Rueckfrage des Coaches.
  // Der Balken ist niedriger als die frueheren Ringe; gemessen (iPhone, 23.10., Blocker zweizeilig, drei Zeilen Hinweis,
  // Einheit heute) blieben mit +40 unten gut 40 pt frei, deshalb +60: dann passen die naechsten drei Tage.
  const q = d.coach && d.coach.question, upcoming = (d.plan.upcoming || []).length > 0;
  let left = room - 2 + 60;
  const TILE_H = 36, Q_H = 54, KH = Math.min(Math.floor((W - 2 * CAPS_PAD - 6 * 14) / 7), left - 10 - CAPS_PAD - 2 - CAPS_LABEL_H);
  const showTiles = upcoming && left >= TILE_H; if (showTiles) left -= TILE_H;
  const showDays = !showTiles && wk.days && KH >= 22; if (showDays) left -= 10 + CAPS_PAD + KH + 2 + CAPS_LABEL_H;
  const showQ = !!q && left >= Q_H; if (showQ) left -= Q_H;
  const grow = Math.max(0, Math.min(12, left)); // Rest: Kapseln hoeher, Abstand groesser
  const sum = (xs, i) => xs.reduce((a, x) => a + x[i], 0);
  const doneMin = sum(zeit, 1), goal = sum(zeit, 2), frame = wk.goals && wk.goals.goalMin > 0 ? wk.goals.goalMin : null;
  const row = w.addStack(); row.bottomAlignContent(); row.size = new Size(W, 0);
  text(row, `WOCHE · ${wk.daysLeft <= 1 ? "NUR NOCH HEUTE" : `NOCH ${wk.daysLeft} TAGE`}`, FS.xs, { bold: true, color: COL.muted, minScale: 0.8 });
  row.addSpacer();
  num(row, fmtHm(doneMin), FS.lg);
  text(row, `  Ziel ${fmtHm(goal)}${frame ? ` · Rahmen ${fmtHm(frame)}` : ""}`, FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
  w.addSpacer(4);
  const im = w.addImage(weekBarImage(W, 14, doneMin, goal, frame)); im.imageSize = new Size(W, 14);
  // Offen: eine Kapsel je Sportart in der Sportfarbe ueber die volle Breite, Zahl = was noch fehlt
  const open = wk.open || [];
  if (open.length) {
    w.addSpacer(8 + Math.floor(grow / 3));
    const gap = 6, CW = Math.floor((W - gap * (open.length - 1)) / open.length), PH = 24 + grow - Math.floor(grow / 3);
    const chips = w.addStack(); chips.spacing = gap; chips.size = new Size(W, 0);
    for (const o of open) {
      const hex = sportHex(o.s), done = !(o.v > 0);
      const c = chips.addStack(); c.layoutVertically(); c.size = new Size(CW, 0);
      const pill = c.addStack(); pill.size = new Size(CW, PH); pill.cornerRadius = PH / 2; pill.centerAlignContent();
      pill.backgroundColor = done ? new Color(hex) : new Color(hex, 0.22);
      pill.addSpacer(); text(pill, done ? "✓" : o.unit === "min" ? fmtHm(o.v) : fmt(o.v), FS.md, { bold: true, color: done ? new Color("#000000") : new Color(hex), minScale: 0.6 }); pill.addSpacer();
      const lb = c.addStack(); lb.size = new Size(CW, 0); lb.addSpacer(); text(lb, o.unit === "min" ? SPORTS[o.s][0] : `${SPORTS[o.s][0]} · ${o.unit}`, FS.xs, { color: COL.muted, minScale: 0.6 }); lb.addSpacer();
    }
  }
  if (showTiles) {
    w.addSpacer(10);
    const TW = Math.floor((W - 12) / 3);
    const tiles = w.addStack(); tiles.size = new Size(W, 0); tiles.spacing = 6;
    for (let i = 1; i <= 3; i++) dayTile(tiles, TW, addIso(d.today, i), d.goal, d.plan.upcoming);
  } else if (showDays) {
    // Sonst die Woche Mo bis So: was an welchem Tag gemacht wurde (Kreise in Sportfarbe), heute umrandet
    w.addSpacer(10);
    const im = w.addImage(capsImage(W, KH, wk.days, d.today)); im.imageSize = new Size(W, CAPS_PAD + KH + 2 + CAPS_LABEL_H);
  }
  if (showQ) {
    w.addSpacer(10);
    const qs = w.addStack(); qs.layoutVertically(); qs.size = new Size(W, 0);
    text(qs, d.coach.answer ? "FRAGE DES COACHES · BEANTWORTET" : "FRAGE DES COACHES", FS.xs, { bold: true, color: COL.muted, minScale: 0.8 });
    text(qs, q, FS.sm, { lines: 2, minScale: 0.8 });
  }
  notice(w, res, d);
  w.addSpacer();
  return w;
}

/* ---------- Mittel: 6 Wochen (Parameter "zeit", auch "form" / "tss") ---------- */
// Je Woche eine Saeule = Trainingszeit, gestapelt in Sportfarben (groesster Anteil unten); blasse Flaeche = Zeitziel des
// Coaches (sonst Plan), die die Saeule fuellt, Strich nur ueber dem Ziel. Der Rahmen des Coaches (Ausdauerstunden) steht als
// gestrichelte Linie quer ueber der Saeule. Ohne Ziel keine Flaeche, ohne Training keine Fuellung, nie ein 0-Balken.
// Darunter je Woche die Zeit, die TSS und der Wochenanfang als echte Widget-Texte (weekLabels), weil Text im Bild feste Farben hat.
const WEEK_PLOT_H = 54;
const WEEK_KINDS = {
  time: {
    val: (k) => k.min, goal: (k) => k.minGoal, floor: (k) => k.minFrame ?? null, parts: (k) => k.minSports,
    sub: (k) => (k.tss != null ? `TSS ${fmt(k.tss)}` : MISSING),
  },
};
function weekImage(w, weeks, K) {
  // Schmale Saeule, das Ziel als blasse Flaeche, die die Saeule fuellt. Ueber dem Ziel waechst die Saeule heraus und
  // ein Strich markiert die Zielhoehe. 3 pt Rand oben fuer den Strich.
  const n = weeks.length, H = WEEK_PLOT_H, M = 3, h = H - M - 1, base = H - 1, dc = newCtx(w, H);
  const vals = weeks.flatMap((k) => [K.val(k), K.goal(k), K.floor ? K.floor(k) : null]).filter((v) => v != null);
  const max = Math.max(50, ...vals), slot = w / n, bw = Math.min(16, slot * 0.34), tick = bw + 12;
  dc.setFillColor(new Color(NEUTRAL_HEX, 0.35)); dc.fillRect(new Rect(0, base, w, 1)); // Grundlinie
  weeks.forEach((k, i) => {
    const x = i * slot + (slot - bw) / 2, v = K.val(k), g = K.goal(k);
    const gy = g != null ? base - Math.max(2, (g / max) * h) : null;
    if (gy != null) { dc.setFillColor(new Color(NEUTRAL_HEX, 0.16)); dc.fillRect(new Rect(x, gy, bw, base - gy)); }
    // Saeule: je Sportart ein Abschnitt (groesster unten), ohne Luecken
    if (v != null && v > 0) {
      const ht = Math.max(2, (v / max) * h), top = base - ht;
      const parts = Object.entries(K.parts(k) || {}).filter(([, pv]) => pv > 0).sort((a, b) => b[1] - a[1]);
      if (!parts.length) parts.push(["other", v]);
      const tot = parts.reduce((a, [, pv]) => a + pv, 0);
      let yb = base;
      parts.forEach(([sp, pv], j) => {
        const yt = j === parts.length - 1 ? top : Math.max(top, yb - (ht * pv) / tot);
        dc.setFillColor(new Color(sportHex(sp))); dc.fillRect(new Rect(x, yt, bw, yb - yt));
        yb = yt;
      });
    }
    // Ueber dem Ziel verdeckt die Saeule die Flaeche: dann markiert ein Strich die Zielhoehe
    if (gy != null && v > g) { dc.setFillColor(new Color(INK_HEX)); dc.fillRect(new Rect(x + bw / 2 - tick / 2, gy - 1.25, tick, 2.5)); }
    const fl = K.floor ? K.floor(k) : null;
    if (fl != null) {
      const fy = base - Math.max(2, (fl / max) * h);
      dc.setFillColor(new Color(INK_HEX));
      for (let dx = 0; dx < tick; dx += 4) dc.fillRect(new Rect(x + bw / 2 - tick / 2 + dx, fy - 0.75, Math.min(2.5, tick - dx), 1.5));
    }
  });
  return dc.getImage();
}

// Je Woche eine Spalte unter der Saeule: Zeit (fett), TSS, Wochenanfang
function weekLabels(parent, W, weeks, K) {
  const slot = Math.floor(W / weeks.length), row = parent.addStack();
  row.size = new Size(slot * weeks.length, 0);
  weeks.forEach((k, i) => {
    const cur = i === weeks.length - 1, v = K.val(k);
    const col = row.addStack(); col.layoutVertically(); col.size = new Size(slot, 0);
    const line = (fn) => { const r = col.addStack(); r.addSpacer(); fn(r); r.addSpacer(); };
    line((r) => num(r, v != null ? fmtHm(v) : MISSING, FS.md, { align: "center", minScale: 0.7 }));
    line((r) => text(r, K.sub(k), FS.xs, { color: COL.muted, align: "center", minScale: 0.7 }));
    line((r) => text(r, dateShort(k.weekStart), FS.xs, { bold: cur, color: cur ? COL.text : COL.muted, align: "center", minScale: 0.7 }));
  });
}

function buildSixWeeks(res) {
  const d = res.data, K = WEEK_KINDS.time;
  const W = widgetInnerWidth();
  const w = baseWidget({ padV: 11 });
  const tw = d.tssWeeks || [], cur = tw[tw.length - 1];
  header(w, W, "6 WOCHEN", cur ? `diese Woche ${fmtHm(K.val(cur) || 0)}${cur.tss != null ? ` · TSS ${fmt(cur.tss)}` : ""}` : null);
  w.addSpacer(5);
  if (tw.some((k) => K.val(k) != null)) {
    const im = w.addImage(weekImage(W, tw, K)); im.imageSize = new Size(W, WEEK_PLOT_H);
    w.addSpacer(3);
    weekLabels(w, W, tw, K);
  } else text(w, "Keine Trainingszeiten", FS.xs, { color: COL.muted });
  w.addSpacer();
  const th = w.addStack(); th.centerAlignContent(); th.spacing = 4;
  // Unten: Zeit dieser Woche je Sportart (nur Sportarten mit Training), rechts der Rahmen des Coaches (gestrichelte Linie)
  const ms = Object.entries((cur && cur.minSports) || {}).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]);
  if (ms.length) ms.forEach(([k, v]) => { symbol(th, SPORT_SYMBOL[k], 11, new Color(sportHex(k))); text(th, fmtHm(v), FS.xs, { bold: true, color: COL.muted, minScale: 0.7 }); th.addSpacer(8); });
  else text(th, "Diese Woche noch kein Training", FS.xs, { color: COL.muted });
  if (cur && cur.minFrame) { th.addSpacer(); text(th, `┄ Rahmen ${fmtHm(cur.minFrame)}`, FS.xs, { bold: true, color: COL.muted, minScale: 0.7 }); }
  notice(w, res, d);
  return w;
}

/* ---------- Schlaf und Erholung, Ernaehrung, Fitness (klein) ---------- */
function smallWidget() { return baseWidget({ padV: 12 }); }

// Fitness (CTL): Wert gross, Veraenderung als Zahl, Wochenkurve (CTL blau, TSB grau dahinter)
function buildFitness(res) {
  const d = res.data, f = d.fitness || { ctl: null, delta: null, deltaWeeks: null, weekly: [] }, IW = smallInnerWidth();
  const w = smallWidget();
  header(w, IW, "FITNESS", clock(d.generatedAt));
  w.addSpacer(2);
  const row = w.addStack(); row.bottomAlignContent(); row.size = new Size(IW, 0);
  valueUnit(row, f.ctl != null ? fmt(f.ctl) : MISSING, "CTL", 38, { color: f.ctl != null ? COL.accent : COL.muted });
  row.addSpacer();
  if (f.delta != null) { const c = row.addStack(); c.layoutVertically(); text(c, signed(f.delta), FS.md, { bold: true, color: COL.muted, minScale: 1 }); c.addSpacer(6); }
  w.addSpacer(4);
  const im = w.addImage(sparkImage(IW, 40, f.weekly, ACCENT_HEX, { dot: true, second: { values: f.tsbWeekly || [], hex: NEUTRAL_HEX } })); im.imageSize = new Size(IW, 40);
  w.addSpacer();
  const foot = w.addStack(); foot.centerAlignContent();
  text(foot, `${f.deltaWeeks || 6} Wochen`, FS.xs, { color: COL.muted });
  foot.addSpacer();
  if (f.tsb != null) text(foot, `TSB ${signed(f.tsb)}`, FS.xs, { bold: true, minScale: 0.7 });
  notice(w, res, d);
  return w;
}

// Ernaehrung (klein) wie das mittlere Widget, nur kompakt: oben gross die uebrigen Kalorien (Ziel - gegessen, darueber gelb
// "drüber"), darunter Protein, Kohlenhydrate und Fett als drei gleiche Zeilen mit Wert / Ziel und duennem Balken
// (Strich = Soll bis jetzt). Ziele immer aus Yazio; ueber dem Ziel die Zahl gelb.
function buildFoodCard(res) {
  const d = res.data, f = d.food, IW = smallInnerWidth();
  const hasVals = (x) => x && [x.calories, x.protein, x.carbs, x.fat].some((v) => v != null);
  let td = f.days[f.days.length - 1] || {};
  const has = hasVals(td), prev = !has ? f.days.slice(0, -1).reverse().find(hasVals) : null;
  if (prev) td = prev;
  const show = has || !!prev, yg = f.goals || {};
  const kcalGoal = td.goal ?? yg.kcal ?? null;
  const w = smallWidget(), dim = !has;
  header(w, IW, "ERNÄHRUNG", prev ? (prev === f.days[f.days.length - 2] ? "gestern" : dateShort(prev.date)) : clock(d.generatedAt));
  w.addSpacer(3);
  const left = show && td.calories != null && kcalGoal ? kcalGoal - td.calories : null;
  const top = w.addStack(); top.bottomAlignContent(); top.spacing = 4;
  num(top, left != null ? fmt(Math.abs(left)) : show && td.calories != null ? fmt(td.calories) : MISSING, 30, { color: dim ? COL.muted : left != null && left < 0 ? new Color(OVER_HEX) : COL.text });
  const u = top.addStack(); u.layoutVertically();
  text(u, left == null ? "kcal" : left < 0 ? "kcal drüber" : "kcal übrig", FS.sm, { bold: true, color: COL.muted, minScale: 0.8 });
  u.addSpacer(4);
  const soll = has ? dayFraction() : null;
  for (const [label, v, goal, hex] of [["Protein", td.protein, yg.proteinG, NUT_HEX.protein], ["Kohlenh.", td.carbs, yg.carbsG, NUT_HEX.carbs], ["Fett", td.fat, yg.fatG, NUT_HEX.fat]]) {
    w.addSpacer();
    const r = w.addStack(); r.bottomAlignContent(); r.size = new Size(IW, 0);
    text(r, label, FS.xs, { color: COL.muted, minScale: 0.9 });
    r.addSpacer();
    const over = show && v != null && goal && v > goal;
    num(r, show && v != null ? fmt(v) : MISSING, FS.sm, { color: dim ? COL.muted : over ? new Color(OVER_HEX) : COL.text, minScale: 0.8 });
    text(r, goal ? ` / ${fmt(goal)} g` : " g", FS.xs, { color: COL.muted, minScale: 0.8 });
    w.addSpacer(2);
    progressBar(w, IW, show && v != null && goal ? v / goal : 0, hex, dim ? 0.5 : 1, { overHex: OVER_HEX, mark: soll });
  }
  if (!has) { w.addSpacer(3); text(w, prev ? "Yazio heute noch nicht synchron" : "Yazio noch nicht synchron", FS.xs, { bold: true, color: COL.warn, minScale: 0.7 }); }
  else notice(w, res, d);
  return w;
}

/* ---------- Mittel: Ernaehrung heute ---------- */
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
    ring.backgroundImage = ringProgressImage(RS, td.calories != null && kcalGoal ? td.calories / kcalGoal : 0, NUT_HEX.kcal, stale ? 0.5 : 1);
    const cen = (str, size, o) => { const s = ring.addStack(); s.addSpacer(); (size >= 20 ? num : text)(s, str, size, { align: "center", minScale: 0.6, ...o }); s.addSpacer(); };
    // Mitte: uebrige Kalorien (Ziel - gegessen); ueber dem Ziel gelb mit "drüber"
    const left = td.calories != null && kcalGoal ? kcalGoal - td.calories : null;
    if (left != null) {
      cen(fmt(Math.abs(left)), 24, { opacity: op, color: left < 0 ? new Color(OVER_HEX) : COL.text });
      cen(left < 0 ? "kcal drüber" : "kcal übrig", FS.sm, { color: COL.muted, minScale: 0.85 });
    } else {
      cen(td.calories != null ? fmt(td.calories) : MISSING, td.calories != null ? 24 : 16, { color: td.calories != null ? COL.text : COL.muted, opacity: op });
      if (kcalGoal) cen(`von ${fmt(kcalGoal)}`, FS.sm, { color: COL.muted, minScale: 0.85 });
    }
    // Rechts: Protein, Kohlenhydrate, Fett als Balken (g / Ziel g); fehlende Werte als Strich ohne Balken
    const col = row.addStack(); col.layoutVertically(); col.size = new Size(RW, 0);
    const macros = [
      { label: "Protein", v: td.protein, goal: yg.proteinG ?? null, hex: NUT_HEX.protein },
      { label: "Kohlenhydrate", v: td.carbs, goal: yg.carbsG ?? null, hex: NUT_HEX.carbs },
      { label: "Fett", v: td.fat, goal: yg.fatG ?? null, hex: NUT_HEX.fat },
    ];
    const soll = has ? dayFraction() : null; // Marke "Soll bis jetzt" nur für heute
    macros.forEach((m, i) => {
      if (i) col.addSpacer(7);
      const l = col.addStack(); l.centerAlignContent(); l.size = new Size(RW, 0);
      text(l, m.label, FS.md, { minScale: 0.8 });
      l.addSpacer();
      if (m.v == null) text(l, MISSING, FS.md, { color: COL.muted, minScale: 1 });
      else {
        const over = m.goal && m.v > m.goal;
        num(l, fmt(m.v), FS.lg, { opacity: op, minScale: 0.8, color: over ? new Color(OVER_HEX) : COL.text });
        text(l, m.goal ? ` / ${fmt(m.goal)} g` : " g", FS.sm, { color: COL.muted, minScale: 0.8 });
      }
      if (m.v != null && m.goal) { col.addSpacer(3); progressBar(col, RW, m.v / m.goal, m.hex, stale ? 0.5 : 1, { overHex: OVER_HEX, mark: soll }); }
    });
    if (has) { w.addSpacer(4); text(w, "Strich = Soll bis jetzt · gelbe Zahl = über Ziel", FS.xs, { color: COL.muted, minScale: 0.7 }); }
  }
  notice(w, res, d);
  w.addSpacer();
  return w;
}

/* ---------- Mittel: VDOT und Trainingsbereiche (Parameter "vdot") ---------- */
// Farbe je Bereich von locker (blau) bis schnell (rot); nur Kennzeichnung, keine Ampel
const PACE_ZONES_UI = { easy: ["Easy", "E", "#5ac8fa"], marathon: ["Marathon", "M", "#30d158"], threshold: ["Schwelle", "T", "#ffd60a"], interval: ["Intervall", "I", "#ff9f0a"], repetition: ["Repetition", "R", "#ff453a"] };

function buildVdot(res) {
  const d = res.data, W = widgetInnerWidth(), v = d.vdot;
  const w = baseWidget({ padV: 12 });
  if (!v) {
    header(w, W, "VDOT · TRAININGSBEREICHE", null);
    w.addSpacer();
    text(w, "Kein Runalyze-Snapshot", 16, { bold: true, color: COL.warn });
    text(w, "VDOT und Bereiche erscheinen, sobald der Coaching-Task einen Snapshot schickt.", FS.xs, { color: COL.muted, lines: 2 });
    w.addSpacer();
    notice(w, res, d);
    return w;
  }
  const row = w.addStack(); row.spacing = 14;
  const LW = Math.floor((W - 14) * 0.36), RW = W - 14 - LW;

  // Links: VDOT gross, Veraenderung und Verlauf
  const lc = row.addStack(); lc.layoutVertically(); lc.size = new Size(LW, 0);
  text(lc, "VDOT", FS.xs, { bold: true, color: COL.muted });
  lc.addSpacer(2);
  num(lc, fmt(v.value, 1), 36, { color: COL.accent });
  const dl = v.delta;
  text(lc, dl == null ? (v.historySince ? `Verlauf ab ${dateShort(v.historySince)}` : "kein Verlauf") : `${dl > 0 ? "▲" : dl < 0 ? "▼" : "="} ${fmt(Math.abs(dl), 1)} seit ${dateShort(v.deltaSince)}`, FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
  lc.addSpacer(5);
  const im = lc.addImage(sparkImage(LW, 26, v.history.map((h) => h.vdot), ACCENT_HEX, { dot: true })); im.imageSize = new Size(LW, 26);
  lc.addSpacer(3);
  text(lc, `Runalyze · ${dateShort(v.fetchedAt.slice(0, 10))}`, FS.xs, { color: COL.muted, minScale: 0.7 });

  // Rechts: die fuenf Bereiche als Pace von-bis (schnell bis langsam), wie in den Runalyze-Lauftabellen
  const rc = row.addStack(); rc.layoutVertically(); rc.size = new Size(RW, 0);
  header(rc, RW, "BEREICHE", "min/km");
  rc.addSpacer(3);
  const bare = (x) => (x ? x.replace("/km", "") : MISSING);
  for (const p of v.paces) {
    const ui = PACE_ZONES_UI[p.key] || [p.label, "", NEUTRAL_HEX];
    const r = rc.addStack(); r.centerAlignContent(); r.size = new Size(RW, 0);
    const b = r.addStack(); b.size = new Size(4, 14); b.cornerRadius = 2; b.backgroundColor = new Color(ui[2]);
    r.addSpacer(7);
    text(r, ui[0], FS.sm, { minScale: 0.7 });
    r.addSpacer();
    text(r, `${bare(p.fastPace)}–${bare(p.slowPace)}`, FS.sm, { bold: true, minScale: 0.7 });
    rc.addSpacer(3);
  }
  notice(w, res, d);
  return w;
}

// Anteile an der Trainingszeit als ein Balken (Prozent nur bei Platz), Soll als Striche
function splitImage(w, h, share, target, keys = ["swim", "bike", "run"], hexOf = sportHex) {
  const dc = newCtx(w, h + 4), tot = keys.reduce((a, k) => a + share[k], 0) || 1;
  let acc = 0;
  dc.setFont(Font.boldRoundedSystemFont(FS.xs));
  dc.setTextAlignedCenter();
  for (const k of keys) {
    const a = (acc / tot) * w, b = ((acc + share[k]) / tot) * w;
    if (b - a > 1) {
      const p = new Path(); p.addRoundedRect(new Rect(a + 1, 2, b - a - 2, h), Math.min(6, h / 2), Math.min(6, h / 2));
      dc.addPath(p); dc.setFillColor(new Color(hexOf(k))); dc.fillPath();
      if (h >= 16 && b - a > 34) { dc.setTextColor(new Color("#1c1c1e")); dc.drawTextInRect(`${share[k]} %`, new Rect(a, 2 + (h - FS.xs) / 2 - 2, b - a, h)); }
    }
    acc += share[k];
  }
  if (target) {
    dc.setFillColor(new Color(INK_HEX, 0.95));
    for (const at of [target.swim, target.swim + target.bike]) dc.fillRect(new Rect((at / 100) * w - 1, 0, 2, h + 4));
  }
  return dc.getImage();
}

/* ---------- Mittel: Kraft aus EGYM (Parameter "kraft") ---------- */
// Links der Ring der Woche (Saetze gegen das Wochenziel) mit Trainingstagen. Rechts das Muskelalter mit Balken je Bereich;
// der aelteste Bereich kraeftig, die anderen blasser (keine Ampel: das Muskelalter hat kein Soll).
function buildKraft(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = baseWidget({ padV: 12 });
  if (d.configured === false) {
    header(w, W, "KRAFT · EGYM", null);
    w.addSpacer(6);
    text(w, "EGYM ist im Worker nicht eingerichtet (EGYM_USERNAME und EGYM_PASSWORD).", FS.sm, { color: COL.muted, lines: 4 });
    return w;
  }
  // Ziel über mehrere Wochen ("≥90 Sätze in 2 Wochen"): der Ring zeigt den laufenden Abschnitt ab Blockstart
  const wk = d.week, P = wk.periodWeeks > 1 ? wk.periodWeeks : 1, sets = (P > 1 ? wk.windowSets ?? wk.sets : wk.sets) || 0, goalSets = wk.goalSets || 60, hex = sportHex("strength");
  header(w, W, "KRAFT · EGYM", `Woche ab ${dateShort(wk.start)}`);
  w.addSpacer(6);
  const row = w.addStack(); row.centerAlignContent(); row.spacing = 14;
  const LW = 104, RW = W - LW - 14, RING = 80;

  const lc = row.addStack(); lc.layoutVertically(); lc.size = new Size(LW, 0);
  // Zahl als echter Text im Ring (nicht ins Bild gezeichnet): so passt sie sich Hell/Dunkel an und bleibt lesbar
  const rs = lc.addStack(); rs.addSpacer();
  const ring = rs.addStack(); ring.size = new Size(RING, RING); ring.layoutVertically(); ring.centerAlignContent();
  ring.backgroundImage = ringProgressImage(RING, sets / goalSets, hex, 1);
  const cen = (fn) => { const x = ring.addStack(); x.addSpacer(); fn(x); x.addSpacer(); };
  cen((x) => num(x, String(sets), 24, { align: "center" }));
  cen((x) => text(x, sets >= goalSets ? "Ziel erreicht" : `von ${goalSets}${P > 1 ? ` · ${P * 7} Tage` : ""}`, FS.xs, { color: COL.muted, align: "center", minScale: 0.6 }));
  rs.addSpacer();
  lc.addSpacer(5);
  const dots = lc.addStack(); dots.centerAlignContent(); dots.size = new Size(LW, 0);
  dots.addSpacer();
  for (let i = 0; i < 7; i++) {
    if (i) dots.addSpacer(4);
    const dot = dots.addStack(); dot.size = new Size(7, 7); dot.cornerRadius = 3.5; dot.backgroundColor = wk.days[i] ? new Color(hex) : new Color(NEUTRAL_HEX, 0.3);
  }
  dots.addSpacer();
  lc.addSpacer(3);
  const n = wk.sessions || 0;
  const cap = lc.addStack(); cap.addSpacer(); text(cap, `Sätze · ${n} ${n === 1 ? "Einheit" : "Einheiten"}`, FS.xs, { color: COL.muted, minScale: 0.7 }); cap.addSpacer();

  const rc = row.addStack(); rc.layoutVertically(); rc.size = new Size(RW, 0);
  const b = d.bioAge, mt = rc.addStack(); mt.bottomAlignContent(); mt.size = new Size(RW, 0);
  text(mt, "MUSKELALTER", FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
  mt.addSpacer();
  if (b && b.muscle != null) { num(mt, String(b.muscle), 28, { color: new Color(hex) }); text(mt, " Jahre", FS.sm, { bold: true, color: COL.muted, minScale: 0.8 }); }
  else text(mt, MISSING, FS.lg, { color: COL.muted });
  const parts = b && b.upper != null ? [["Oberkörper", b.upper], ["Rumpf", b.core], ["Beine", b.lower]] : [];
  const worst = Math.max(...parts.map((x) => x[1] ?? 0));
  const BW = Math.max(40, Math.round(RW * 0.42));
  parts.forEach(([label, v]) => {
    rc.addSpacer(6);
    const r = rc.addStack(); r.centerAlignContent(); r.size = new Size(RW, 0);
    text(r, label, FS.md, { minScale: 0.7 });
    r.addSpacer();
    progressBar(r, BW, v != null ? v / 60 : 0, hex, v === worst ? 1 : 0.5);
    r.addSpacer(8);
    num(r, v != null ? String(v) : MISSING, FS.lg, { minScale: 0.8 });
  });
  notice(w, res, d);
  return w;
}

// Wochenleiste des Blocks: je Woche ein Abschnitt, laufende Woche kraeftig, vergangene blass
function weekStrip(w, IW, total, nowIdx, pastAll) {
  const H = 5, gap = 3, seg = (IW - gap * (total - 1)) / total, dc = newCtx(IW, H);
  for (let i = 0; i < total; i++) {
    const p = new Path();
    p.addRoundedRect(new Rect(i * (seg + gap), 0, seg, H), 2.5, 2.5);
    dc.addPath(p);
    dc.setFillColor(i + 1 === nowIdx ? new Color(ACCENT_HEX, 1) : pastAll || i + 1 < nowIdx ? new Color(ACCENT_HEX, 0.45) : new Color(NEUTRAL_HEX, 0.25));
    dc.fillPath();
  }
  const im = w.addImage(dc.getImage()); im.imageSize = new Size(IW, H);
}

/* ---------- Mittel: Woche je Sportart (Parameter "training", auch "intensitaet") ---------- */
// Je Sportart eine Zeile: Zeit gegen das Zeitziel des Coaches (ohne Zeitziel TSS gegen den Plan), oben rechts die TSS der Woche
// gegen den Korridor. Darunter die Zonenzeit dieser Woche als ein Balken und der Schnitt der letzten 4 Wochen als Zahl.
// Zonen in Blautoenen statt Gruen/Gelb/Rot: das sind Anteile, keine Ampel.
const INT_HEX = { easy: "#9cc3f0", mid: "#5a8fd6", hard: "#2f5fa8" };
function buildWoche(res) {
  const d = res.data, W = widgetInnerWidth(), S = d.sports || {};
  const w = baseWidget({ padV: 12 });
  const keys = ["swim", "bike", "run"].filter((k) => S[k]);
  const all = Object.values(S), tss = all.reduce((a, s) => a + (s.weekLoad || 0), 0);
  const hi = all.reduce((a, s) => a + (s.targetLoad || 0), 0), lo = all.reduce((a, s) => a + (s.targetLoadMin || 0), 0);
  header(w, W, "WOCHE · JE SPORTART", `TSS ${Math.round(tss)}${hi ? ` / ${tssRange(lo, hi)}` : ""}`);
  w.addSpacer(7);
  const NW = 92, VW = 86, BW = W - NW - VW - 8;
  keys.forEach((k, i) => {
    if (i) w.addSpacer(6);
    const s = S[k], hex = sportHex(k), byTime = !!s.targetMin;
    const r = w.addStack(); r.centerAlignContent(); r.size = new Size(W, 0);
    const n = r.addStack(); n.centerAlignContent(); n.spacing = 5; n.size = new Size(NW, 0);
    symbol(n, SPORT_SYMBOL[k], 12, new Color(hex)); text(n, SPORTS[k][0], FS.sm, { bold: true, minScale: 0.8 }); n.addSpacer();
    const b = r.addStack(); b.size = new Size(BW, 0);
    progressBar(b, BW, byTime ? (s.weekMinutes || 0) / s.targetMin : s.plannedLoad ? (s.weekLoad || 0) / s.plannedLoad : 0, hex);
    r.addSpacer(8);
    const v = r.addStack(); v.bottomAlignContent(); v.size = new Size(VW, 0); v.addSpacer();
    num(v, byTime ? fmtHm(s.weekMinutes || 0) : String(Math.round(s.weekLoad || 0)), FS.md);
    text(v, byTime ? ` / ${fmtHm(s.targetMin)}` : s.plannedLoad ? ` / ${Math.round(s.plannedLoad)} TSS` : " TSS", FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
  });
  w.addSpacer();
  const it = d.intensity || {}, sh = it.week, avg = it.avg;
  const zh = w.addStack(); zh.centerAlignContent(); zh.size = new Size(W, 0);
  text(zh, "ZONENZEIT", FS.xs, { bold: true, color: COL.muted, minScale: 0.8 });
  zh.addSpacer();
  text(zh, sh ? `locker ${sh.easy} · mittel ${sh.mid} · hart ${sh.hard} %` : "noch keine Zonenzeiten", FS.xs, { color: COL.muted, minScale: 0.7 });
  w.addSpacer(3);
  if (sh) { const im = w.addImage(splitImage(W, 10, sh, null, ["easy", "mid", "hard"], (k) => INT_HEX[k])); im.imageSize = new Size(W, 14); }
  if (avg) { w.addSpacer(2); text(w, `Ø 4 Wochen: locker ${avg.easy} · mittel ${avg.mid} · hart ${avg.hard} %`, FS.xs, { color: COL.muted, minScale: 0.7 }); }
  notice(w, res, d);
  return w;
}

/* ---------- Mittel: Block (Parameter "block", auch "rahmen", "experiment") ---------- */
// Alles, was im laufenden Block gilt, in einem Widget (Worker: view=kompass): Block mit Woche x von N und naechstem Test,
// darunter je eine Zeile Erlaubt (Intensitaet des Coaches mit Stand dieser Woche), Nicht erlaubt, Messung (Experiment:
// Ausgangswert -> aktueller Wert) und Ziele (Abschlussziele des Blocks als Werte). Leitplanken und lange Texte stehen im
// Dashboard. Nicht erlaubt darf zweizeilig sein, alles andere eine Zeile (Erlaubt: bis zu zwei Sportarten); mehr passt im
// mittleren Widget nicht. Lange Intensitaets-Texte kuerzt der Coach mit short.
// Nichts wird bewertet; Werte und Ziele kommen fertig vom Worker.
const GOAL_SHORT = { decoupling: "Drift", strength: "Kraft", runDecoupling: "Lauf", runStable: "Lauf stabil", bikeDecoupling: "Rad", swimTest: "Schwimmen" };
function goalValue(g) {
  if (g.value == null) return g.key === "swimTest" ? "offen" : MISSING;
  if (g.key === "strength") return `${g.value}/${g.of}`;
  if (g.key === "swimTest") return `${g.value > 0 ? "+" : ""}${fmt(g.value, 1)} s`;
  if (g.key === "runStable") return g.value === 0 ? "< 1:00" : fmtHm(g.value);
  return `${fmt(g.value, 1)} %`;
}
// Kurzfassung einer Intensitaets-Vorgabe: short vom Coach, sonst der Text bis zum ersten Komma, Semikolon oder "z. B."
const intensityShort = (x) => x.short || String(x.text).split(/[,;]|\s+z\.\s*B\./)[0].trim();
function buildKompass(res) {
  const d = res.data, W = widgetInnerWidth(), b = d.block, f = d.frame, T = d.nextTest, x = d.experiment;
  const w = baseWidget({ padV: 12 });
  const name = b ? String(b.name).replace(/^block\s+/i, "").toUpperCase() : null;
  const when = !b ? null : b.status === "active" ? ` · WOCHE ${b.weekNo}${b.weeks ? `/${b.weeks}` : ""}` : b.status === "upcoming" ? ` · START IN ${b.daysToStart} T` : " · BEENDET";
  const test = T ? (T.daysToGo === 0 ? "Test heute" : `Test in ${T.daysToGo} ${T.daysToGo === 1 ? "Tag" : "Tagen"}`) : null;
  header(w, W, b ? `BLOCK ${name}${when}` : "BLOCK", test, COL.race);
  w.addSpacer(5);
  if (b && b.weeks) { weekStrip(w, W, b.weeks, b.weekNo || 0, b.status === "done"); w.addSpacer(4); }
  const LW = 64;
  const line = (label, build) => {
    w.addSpacer(5);
    const r = w.addStack(); r.topAlignContent(); r.size = new Size(W, 0);
    const l = r.addStack(); l.size = new Size(LW, 0); l.setPadding(1, 0, 0, 0);
    text(l, label, FS.xs, { bold: true, color: COL.muted, minScale: 0.8 }); l.addSpacer();
    const c = r.addStack(); c.layoutVertically(); c.size = new Size(W - LW, 0);
    build(c);
  };
  const intens = (f && f.intensity) || [];
  if (intens.length) line("ERLAUBT", (c) => intens.slice(0, 2).forEach((it) => {
    const r = c.addStack(); r.topAlignContent(); r.spacing = 5; r.size = new Size(W - LW, 0);
    const k = FRAME_SPORT[it.sport] || "other";
    symbol(r, SPORT_SYMBOL[k], 11, new Color(sportHex(k)));
    text(r, intensityShort(it), FS.sm, { minScale: 0.9 });
    r.addSpacer();
    const wk = it.week;
    if (wk && wk.skip) text(r, wk.skip, FS.xs, { color: COL.muted, minScale: 0.8 });
    else if (wk && wk.count > 0) symbol(r, "checkmark.circle.fill", 11, COL.ok);
    else if (wk) text(r, "offen", FS.xs, { color: COL.muted, minScale: 0.8 });
  }));
  if (f && f.notAllowed && f.notAllowed.length) line("NICHT", (c) => text(c, f.notAllowed.join(" · "), FS.sm, { lines: 2, minScale: 0.9 }));
  const ms = ((x && x.metrics) || []).filter((m) => m.baseline != null);
  if (ms.length) line("MESSUNG", (c) => {
    const parts = ms.slice(0, 3).map((m) => `${(EXP_SHORT[m.key] || [m.label])[0]} ${expVal(m, m.baseline)}→${m.current == null ? "?" : expVal(m, m.current)}${m.provisional ? "*" : ""}`);
    text(c, parts.join(" · "), FS.sm, { minScale: 0.8 });
  });
  const goals = b ? (b.goals || []).filter((g) => !g.group || g.group === "ziel") : [];
  if (goals.length) line("ZIELE", (c) => text(c, goals.map((g) => `${GOAL_SHORT[g.key] || g.label} ${goalValue(g)}`).join(" · "), FS.sm, { minScale: 0.8 }));
  if (!b && !f && !x) { w.addSpacer(6); text(w, "Kein Block, kein Rahmen und kein Experiment vom Coach.", FS.sm, { color: COL.muted, lines: 2 }); }
  w.addSpacer();
  notice(w, res, d);
  return w;
}
const FRAME_SPORT = { lauf: "run", rad: "bike", schwimmen: "swim", kraft: "strength" };
const EXP_SHORT = { lauf_hf130: ["Lauf HF 130", "run"], rad_hf165w: ["Rad 165 W", "bike"], schwimm_amstueck: ["Am Stück", "swim"], schwimm_100m: ["100 m", "swim"] };
const expVal = (m, v) => (v == null ? MISSING : m.key === "lauf_hf130" || m.key === "schwimm_100m" ? fmtPace(Math.round(v)) : fmt(v, Number.isInteger(v) ? 0 : 1));

/* ---------- Klein: Schlaf und Form (Standard, auch "schlaf", "bereit") ---------- */
// Schlaf der letzten Nacht gross, HRV und Ruhepuls mit dem eigenen Median, unten Fitness (CTL) und Frische (TSB)
function buildSchlafForm(res) {
  const d = res.data, s = d.sleep || { days: [] }, f = d.fitness || {}, IW = smallInnerWidth();
  const w = smallWidget();
  const last = s.latest, isToday = last && last.date === d.today;
  header(w, IW, "SCHLAF · FORM", last ? (isToday ? "heute" : dateShort(last.date)) : null);
  w.addSpacer(2);
  valueUnit(w, last ? fmt(last.hours, 1) : MISSING, "h", 32, { color: last ? COL.sleep : COL.muted });
  w.addSpacer();
  const t = s.days[s.days.length - 1] || {};
  const row = (label, val, extra) => {
    const r = w.addStack(); r.centerAlignContent(); r.size = new Size(IW, 0);
    text(r, label, FS.xs, { color: COL.muted, minScale: 0.8 });
    r.addSpacer();
    num(r, val, FS.sm, { minScale: 0.8 });
    if (extra) text(r, ` ${extra}`, FS.xs, { color: COL.muted, minScale: 0.8 });
  };
  row("HRV", fmt(t.hrv), s.medianHrv != null ? `Ø ${fmt(s.medianHrv)}` : null);
  row("Ruhepuls", fmt(t.restingHR), s.medianRestingHR != null ? `Ø ${fmt(s.medianRestingHR)}` : null);
  w.addSpacer(3);
  row("Fitness", f.ctl != null ? fmt(f.ctl) : MISSING, f.tsb != null ? `TSB ${signed(f.tsb)}` : null);
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

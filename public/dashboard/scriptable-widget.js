// Trainings-Widget fuer Scriptable (iOS).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfuegen.
//  2. Skript einmal in Scriptable oeffnen und ausfuehren: Es fragt nach der Adresse deines Workers
//     (https://....workers.dev, ohne Pfad) und dem Dashboard-Token. Beides landet im Schluesselbund
//     dieses iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Groesse waehlen, Skript auswaehlen.
//     Gross:   Rennen-Countdown, Bereitschaft, Hinweis (Coach, sonst Befund aus "Was auffaellt" mit Konsequenz, sonst Warum), heutige Einheit,
//              Woche Mo bis So als Kapseln mit Wochenlast, die naechsten drei Tage.
//     Mittel:  Ernaehrung heute (Standard). Parameter  form  = Wochen-TSS der letzten 6 Wochen gegen das Wochenziel, Schwellen;
//              Parameter  training  = je Disziplin Wochenvolumen gegen Plan, Umfang der letzten 4 Wochen mit Entwicklung und Zeitverteilung.
//              Parameter  intensitaet  = Zonenzeit locker / mittel / hart: diese Woche und Schnitt der letzten 4 Wochen.
//              Parameter  vdot  = VDOT (Runalyze) mit Verlauf und die Trainingsbereiche (Paces).
//              Parameter  kraft  = Krafttraining aus EGYM: Saetze gegen Wochenziel (60), bewegtes Gewicht, Bestwerte, Muskelalter.
//              Parameter  block  (auch  ziele)  = Trainingsblock aus dem Intervals-Kalender: Woche x von N, Abschlussziele je Sportart, Leitplanken und Entscheidung zu Woche 12.
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
  block: { endpoint: "block", build: (r) => buildBlock(r), size: "medium", label: "Vorschau mittel: Trainingsblock und Ziele" },
  vdot: { endpoint: "vdot", build: (r) => buildVdot(r), size: "medium", label: "Vorschau mittel: VDOT und Trainingsbereiche" },
  foodmedium: { endpoint: "small", build: (r) => buildFoodMedium(r), size: "medium", label: "Vorschau mittel: Ernährung heute" },
  sleep: { endpoint: "small", build: (r) => buildSleep(r), size: "small", label: "Vorschau klein: Schlaf" },
  food: { endpoint: "small", build: (r) => buildFoodCard(r), size: "small", label: "Vorschau klein: Ernährung" },
  fitness: { endpoint: "small", build: (r) => buildFitness(r), size: "small", label: "Vorschau klein: Fitness (CTL)" },
  ready: { endpoint: "main", build: (r) => buildReady(r), size: "small", label: "Vorschau klein: Bereitschaft" },
};
const PARAM_VIEWS = {
  medium: { fallback: "foodmedium", rules: [[/^(form|zeit|hm|detail)/, "detail"], [/^(intens|polar)/, "intensity"], [/^(train|tri)/, "training"], [/^(vdot|pace|bereich|zone)/, "vdot"], [/^(kraft|egym|strength|gym)/, "kraft"], [/^(block|ziel|saison|season)/, "block"]] },
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
const pick = (l, d) => (DARK ? d : l);
// iOS-Systemfarben: flache Widget-Flaeche, graue Kacheln nur zum Gruppieren, Ampel nur mit Bedeutung,
// Blau fuer Werte, Rot-Pink fuer Datum und Rennen (wie im Kalender-Widget), Indigo fuer Schlaf
const COL = {
  bg: dyn("#ffffff", "#1c1c1e"), card: dyn("#ffffff", "#1c1c1e"), tile: dyn("#f2f2f7", "#2c2c2e"),
  text: dyn("#1c1c1e", "#ffffff"), muted: dyn("#6c6c70", "#8e8e93"), line: dyn("#e5e5ea", "#38383a"),
  ok: dyn("#248a3d", "#30d158"), warn: dyn("#c93400", "#ff9f0a"), bad: dyn("#d70015", "#ff453a"), none: dyn("#6c6c70", "#8e8e93"),
  accent: dyn("#0a63c9", "#64a8ff"), race: dyn("#d30f45", "#ff375f"), sleep: dyn("#5856d6", "#7d7aff"),
};
// Gezeichnete Bilder (DrawContext) kennen keine dynamischen Farben: sie bekommen die zur Darstellung passende Farbe
const INK_HEX = pick("#1c1c1e", "#ffffff");
const MUTED_HEX = pick("#6c6c70", "#8e8e93");
const NEUTRAL_HEX = "#8e8e93";
const ACCENT_HEX = pick("#0a63c9", "#64a8ff");
const ZONE_RGB = { ok: pick("#248a3d", "#30d158"), warn: pick("#c93400", "#ff9f0a"), bad: pick("#d70015", "#ff453a"), none: NEUTRAL_HEX };
const RACE_HEX = pick("#d30f45", "#ff375f");
const SLEEP_HEX = pick("#5856d6", "#7d7aff");
// Sportfarben wie auf der Uhr und im Dashboard: Laufen gelb, Rad grün, Schwimmen blau, Kraft orange; dazu je Sport ein SF Symbol
const SPORTS = { run: ["Laufen", pick("#b38f00", "#ffd60a")], bike: ["Rad", pick("#248a3d", "#30d158")], swim: ["Schwimmen", pick("#0a6fd9", "#409cff")], strength: ["Kraft", pick("#c95e00", "#ff9f45")], other: ["Sonst.", NEUTRAL_HEX] };
const SPORT_SYMBOL = { run: "figure.run", bike: "bicycle", swim: "figure.pool.swim", strength: "dumbbell.fill", other: "figure.mixed.cardio" };
const sportHex = (k) => (SPORTS[k] || SPORTS.other)[1];
// Schriftgroessen: nichts unter 11 pt (auf dem Homescreen sonst kaum lesbar)
const FS = { xs: 11, sm: 12, md: 14, lg: 17, xl: 22, xxl: 30 };
const MISSING = "–";
const colorFor = (cls) => COL[cls] || COL.none;
// Ampel nie nur ueber die Farbe: jede Stufe hat eine eigene Form (SF Symbols), dazu steht immer ein Text.
// Urteil: Kreis mit Haken / Dreieck mit Ausrufezeichen / Quadrat mit Kreuz. Einzelwerte: Kreis / Dreieck / Quadrat, ohne Wert gestrichelt.
const STATUS_SYMBOL = { ok: "checkmark.circle.fill", warn: "exclamationmark.triangle.fill", bad: "xmark.square.fill", none: "circle.dashed" };
const MARK_SYMBOL = { ok: "circle.fill", warn: "triangle.fill", bad: "square.fill", none: "circle.dashed" };
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
const statusMark = (parent, cls, size = 9) => symbol(parent, MARK_SYMBOL[cls] || MARK_SYMBOL.none, size, colorFor(cls));
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
  w.backgroundColor = COL.card; // flach: Inhalte direkt auf der Flaeche, graue Kacheln nur zum Gruppieren
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

// Graue Kachel mit fester Breite zum Gruppieren (z. B. die Ziele im Block)
function card(parent, width, pad = 8, radius = 12) {
  const s = parent.addStack();
  s.layoutVertically();
  s.backgroundColor = COL.tile;
  s.cornerRadius = radius;
  s.setPadding(pad, pad + 2, pad, pad + 2);
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
const NUT_HEX = { kcal: SPORTS.bike[1], protein: SPORTS.swim[1], carbs: SPORTS.run[1], fat: SPORTS.strength[1] };

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
function barsImage(w, h, values, todayIso, dates, goals, refLine, hex = ACCENT_HEX) {
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
    dc.setFillColor(new Color(hex, dates[i] === todayIso ? 1 : 0.55));
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
  else { const mins = items.reduce((a, e) => a + (e.durationMin || 0), 0); main = mins ? `${mins} min` : shortLabel(items[0]); }
  text(t, dayShort(iso), FS.xs, { bold: true, color: race ? COL.race : COL.muted, minScale: 1 });
  t.addSpacer(5);
  text(t, main, FS.sm, { bold: true, color: race ? COL.race : items && !items.length ? COL.muted : COL.text, minScale: 0.7 });
  t.addSpacer();
  if (items && items.length && !race) symbol(t, SPORT_SYMBOL[items[0].sport] || SPORT_SYMBOL.other, 11, new Color(sportHex(items[0].sport)));
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

// Woche Mo bis So als Kapseln: Hoehe = Load des Tages, gestapelt in Sportfarben; geplant und noch offen = Umriss in der
// Sportfarbe; heute mit Rahmen. Fehlende Tage bleiben leere Kapseln, nie 0-Balken.
const CAPS_LABEL_H = 14;
function capsImage(W, H, days, todayIso) {
  const P = 2, dc = newCtx(W, H + CAPS_LABEL_H + P), gap = 5, cw = (W - 2 * P - gap * 6) / 7, r = cw / 2;
  const sum = (o) => Object.values(o || {}).reduce((a, b) => a + (b || 0), 0);
  const max = Math.max(60, ...days.map((x) => sum(x.sports)), ...days.map((x) => (x.plan || []).reduce((a, p) => a + (p.load || 0), 0)));
  const cap = (rect, hex, a = 1, stroke = false) => {
    const p = new Path(); p.addRoundedRect(rect, Math.min(r, rect.height / 2), Math.min(r, rect.height / 2)); dc.addPath(p);
    if (stroke) { dc.setStrokeColor(new Color(hex, a)); dc.setLineWidth(1.5); dc.strokePath(); } else { dc.setFillColor(new Color(hex, a)); dc.fillPath(); }
  };
  dc.setTextAlignedCenter();
  days.forEach((x, i) => {
    const x0 = P + i * (cw + gap), today = x.date === todayIso;
    cap(new Rect(x0, P, cw, H), NEUTRAL_HEX, 0.2);
    // Erledigt: eine Kapsel mit der Hoehe des Tages-Loads; mehrere Sportarten teilen sie senkrecht in gleich breite
    // Spalten (groesster Load links), damit auch kleine Anteile wie eine kurze Kraft-Einheit klar zu sehen sind
    const parts = Object.entries(x.sports || {}).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]).slice(0, 3);
    const tot = parts.reduce((a, [, v]) => a + v, 0);
    if (tot > 0) {
      const ht = Math.min(H, Math.max(cw, (H * tot) / max)), top = P + H - ht, sw = cw / parts.length;
      parts.forEach(([k], j) => {
        const xa = x0 + j * sw + (j ? 0.7 : 0), xb = x0 + (j + 1) * sw - (j < parts.length - 1 ? 0.7 : 0);
        dc.addPath(parts.length === 1 ? capsuleColumn(x0, top, cw, ht, x0, x0 + cw) : capsuleColumn(x0, top, cw, ht, xa, xb));
        dc.setFillColor(new Color(sportHex(k))); dc.fillPath();
      });
    }
    // Geplant und noch nicht erledigt (heute oder spaeter): Umriss in der Sportfarbe
    const open = x.date >= todayIso ? (x.plan || []).filter((p) => !(x.sports || {})[p.sport]) : [];
    if (open.length) {
      const h = Math.min(H, Math.max(cw, (H * open.reduce((a, p) => a + (p.load || 0), 0)) / max));
      cap(new Rect(x0 + 1, P + H - h + 1, cw - 2, h - 2), sportHex(open[0].sport), 0.95, true);
    }
    if (today) { const p = new Path(); p.addRoundedRect(new Rect(x0 - 1.5, P - 1.5, cw + 3, H + 3), r + 1.5, r + 1.5); dc.addPath(p); dc.setStrokeColor(new Color(INK_HEX)); dc.setLineWidth(1.4); dc.strokePath(); }
    dc.setTextColor(new Color(today ? INK_HEX : MUTED_HEX));
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
  const none = { cls: "none" }, body = r.body || { sleep: none, hrv: none, resting: none }; // alte Cache-Daten ohne Ampeln
  const w = baseWidget({ padV: 12 });

  // 1. Kopf: Wochentag und Datum wie im Kalender-Widget, Abrufzeit, Rennen als Kapsel (nur bis zum Renntag)
  const head = w.addStack(); head.centerAlignContent(); head.size = new Size(W, 0);
  const dt = new Date(d.today + "T12:00:00");
  text(head, `${DAY_FULL[dt.getDay()]} ${dt.getDate()}. ${MONTH_SHORT[dt.getMonth()]}`.toUpperCase(), FS.xs, { bold: true, color: COL.race, minScale: 0.8 });
  text(head, ` · ${clock(d.generatedAt)}`, FS.xs, { color: COL.muted, minScale: 1 });
  head.addSpacer();
  if (g && g.daysToGo != null && g.daysToGo >= 0) pill(head, raceShort(g), COL.race, RACE_HEX);
  w.addSpacer(5);

  // 2. Urteil mit Symbol, ein Satz, darunter Koerper (objektiv) und Gefuehl (subjektiv)
  const vr = w.addStack(); vr.centerAlignContent(); vr.spacing = 7;
  statusIcon(vr, v.cls, 20);
  num(vr, v.text, 21, { color: v.cls === "none" ? COL.muted : colorFor(v.cls), minScale: 0.7 });
  w.addSpacer(6);
  const CW = Math.floor((W - 20) / 2);
  const cols = w.addStack(); cols.size = new Size(W, 0); cols.spacing = 20;
  const col = (title) => { const c = cols.addStack(); c.layoutVertically(); c.size = new Size(CW, 0); text(c, title, FS.xs, { bold: true, color: COL.muted, minScale: 1 }); c.addSpacer(1); return c; };
  const line = (c, label, val, cls) => {
    const l = c.addStack(); l.centerAlignContent(); l.size = new Size(CW, 0);
    statusMark(l, cls || "none");
    l.addSpacer(6);
    text(l, label, FS.md, { minScale: 0.8 });
    l.addSpacer();
    text(l, val, FS.md, { bold: true, color: val === MISSING ? COL.muted : COL.text, align: "right", minScale: 0.8 });
  };
  const bodyCol = col("KÖRPER"), feel = col("GEFÜHL");
  line(bodyCol, "Schlaf", r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h` : MISSING, body.sleep.cls);
  line(bodyCol, "HRV", r.hrv != null ? `${fmt(r.hrv)}${HRV_ARROW[body.hrv.trend] || ""}` : MISSING, body.hrv.cls);
  line(bodyCol, "Ruhepuls", fmt(r.restingHR), body.resting.cls);
  for (const [lab, short] of FEEL) {
    const f = feelWord((r.items || []).find((i) => i.label === lab));
    line(feel, short, f.txt, f.cls);
  }
  w.addSpacer(7);

  // 3. Hinweis in einer grauen Kachel: Coach vor Auffaelligkeit vor dem allgemeinen Warum; Strich links in Blau (Coach),
  // Orange (Auffaelligkeit) oder Grau (Warum). Auffaelligkeit nicht am Renntag und nicht in Taper und Erholung.
  const raceFocus = g && (g.daysToGo === 0 || g.phase === "taper" || g.phase === "recovery");
  const focusLine = d.focus && !raceFocus ? `${d.focus.short}. ${d.focus.act}` : null;
  const coachLine = d.coach && d.coach.note ? d.coach.note : null;
  const rawWhy = coachLine || focusLine;
  // Nie abschneiden: ganze Saetze, solange sie in 3 Zeilen passen. Der erste Satz steht immer komplett da.
  const sentences = (s) => {
    const parts = s.trim().split(/(?<=[.!?])\s+(?=[A-ZÄÖÜ])/);
    let out = parts[0];
    for (const x of parts.slice(1)) { if ((out + " " + x).length > 140) break; out += " " + x; }
    return out;
  };
  const why = rawWhy ? sentences(rawWhy) : whyText(d, p, v);
  const perLine = Math.max(20, Math.floor((W - 34) / 6.3));
  const nLines = Math.max(1, Math.ceil(why.length / perLine));
  const ct = w.addStack(); ct.size = new Size(W, 0); ct.backgroundColor = COL.tile; ct.cornerRadius = 12; ct.setPadding(6, 9, 6, 10); ct.centerAlignContent();
  const bar = ct.addStack(); bar.size = new Size(3, Math.min(5, nLines) * 15 + 1); bar.cornerRadius = 1.5;
  bar.backgroundColor = coachLine ? COL.accent : focusLine ? COL.warn : COL.muted;
  ct.addSpacer(9);
  text(ct, why, FS.sm, { lines: 0, minScale: 1 }); // nie abschneiden: der Hinweis ist das Wichtigste nach dem Urteil
  ct.addSpacer();
  w.addSpacer(6);

  // 4. Heute: Sportsymbol, Einheit, darunter Dauer, Distanz und RPE; ohne Einheit Ruhetag und die naechste Einheit
  const tr = w.addStack(); tr.centerAlignContent(); tr.spacing = 9; tr.size = new Size(W, 0);
  if (p) sportBadge(tr, p.sport, 26);
  else { const c = tr.addStack(); c.size = new Size(26, 26); c.cornerRadius = 13; c.backgroundColor = COL.tile; c.centerAlignContent(); c.addSpacer(); symbol(c, "bed.double.fill", 13, COL.muted); c.addSpacer(); }
  const tc = tr.addStack(); tc.layoutVertically();
  if (p) {
    text(tc, p.name || "Einheit", 15, { bold: true, minScale: 0.7 });
    const meta = [p.durationMin && `${p.durationMin} min`, p.distanceKm && `${fmt(p.distanceKm, 1)} km`, p.rpe && `RPE ${p.rpe}`].filter(Boolean).join(" · ");
    if (meta) text(tc, meta, FS.xs, { color: COL.muted });
  } else {
    text(tc, "Keine Einheit geplant", 15, { bold: true, minScale: 0.7 });
    if (d.plan.next) text(tc, `Nächste: ${dayShort(d.plan.next.date)} ${dateShort(d.plan.next.date)} · ${d.plan.next.name || "Einheit"}`, FS.xs, { color: COL.muted, minScale: 0.8 });
  }
  tr.addSpacer();
  // Aufbau der geplanten Einheit: Breite = Dauer, Hoehe = Intensitaet, Arbeitsbloecke in Sportfarbe
  if (p && p.steps && p.steps.length) { w.addSpacer(5); const im = w.addImage(profileImage(W, 16, p.steps, p.sport)); im.imageSize = new Size(W, 16); }
  w.addSpacer(6);

  // 5. Woche: Kapseln Mo bis So, rechts TSS bisher (gegen Wochenziel oder Vorwoche) und die Frische als kleine Zeile
  const wk = d.week || {};
  const wr = w.addStack(); wr.bottomAlignContent(); wr.size = new Size(W, 0);
  const KW = Math.round(W * 0.6), KH = 24;
  if (wk.days) { const im = wr.addImage(capsImage(KW, KH, wk.days, d.today)); im.imageSize = new Size(KW, KH + CAPS_LABEL_H + 2); }
  wr.addSpacer();
  const wc = wr.addStack(); wc.layoutVertically();
  const right = (fn) => { const s = wc.addStack(); s.addSpacer(); fn(s); };
  right((s) => { s.bottomAlignContent(); num(s, fmt(wk.total), FS.xl); text(s, wk.goal ? ` / ${fmt(wk.goal)} TSS` : " TSS", FS.xs, { bold: true, color: COL.muted, minScale: 0.8 }); });
  // Vorwoche und Frische in einer kleinen Zeile: die Frische ist nur eine berechnete Zahl und soll nicht dominieren
  const sub = [wk.lastTotal != null ? `Vorwoche ${fmt(wk.lastTotal)}` : null, wk.tsb != null ? `TSB ${signed(wk.tsb)}` : null].filter(Boolean).join(" · ");
  if (sub) right((s) => text(s, sub, FS.xs, { color: COL.muted, minScale: 0.7 }));
  // Ohne Kapseln (alte Cache-Daten) der Balken gegen das Wochenziel
  if (wk.goal && !wk.days) { w.addSpacer(4); progressBar(w, W, wk.total / wk.goal, ACCENT_HEX); }
  w.addSpacer(6);

  // 6. Naechste Tage: drei Kacheln, nur wenn der Hinweis kurz ist (die Kapseln zeigen den Plan der Woche ohnehin als Umriss)
  if (nLines > 2) { notice(w, res, d); w.addSpacer(); return w; }
  const TW = Math.floor((W - 12) / 3);
  const tiles = w.addStack(); tiles.size = new Size(W, 0); tiles.spacing = 6;
  for (let i = 1; i <= 3; i++) dayTile(tiles, TW, addIso(d.today, i), g, d.plan.upcoming);
  notice(w, res, d);
  w.addSpacer();
  return w;
}

/* ---------- Mittel: Belastung der letzten 6 Wochen (Parameter "form") ---------- */
// Wochen-TSS als Kapseln (aelteste links): abgeschlossene Wochen grau, laufende Woche blau, Ziel je Woche als Umriss
// (nur wo ein Plan oder Wochenziel existiert); ohne Werte bleibt die Kapsel leer, nie 0. Die Zahlen stehen als echte Widget-Texte
// darunter (tssLabels), weil Text im Bild feste Farben hat und im hellen/dunklen Modus nicht mitwechselt.
const TSS_PLOT_H = 50;
function tssImage(w, weeks) {
  const n = weeks.length, h = TSS_PLOT_H, dc = newCtx(w, h);
  const vals = weeks.flatMap((k) => [k.tss, k.goal]).filter((v) => v != null);
  const max = Math.max(50, ...vals), slot = w / n, bw = Math.min(26, slot * 0.5), r = bw / 2;
  const cap = (x, hh, color, stroke) => {
    const i = stroke ? 0.8 : 0, ww = bw - 2 * i, rr = Math.min(ww / 2, (hh - i) / 2);
    const p = new Path(); p.addRoundedRect(new Rect(x + i, h - hh, ww, hh - i), rr, rr); dc.addPath(p);
    if (stroke) { dc.setStrokeColor(color); dc.setLineWidth(1.6); dc.strokePath(); } else { dc.setFillColor(color); dc.fillPath(); }
  };
  weeks.forEach((k, i) => {
    const cur = i === n - 1, x = i * slot + (slot - bw) / 2;
    cap(x, h, new Color(NEUTRAL_HEX, 0.15));
    if (k.tss != null && k.tss > 0) cap(x, Math.max(bw, (k.tss / max) * h), cur ? new Color(ACCENT_HEX) : new Color(NEUTRAL_HEX, 0.6));
    // Ziel als Umriss in Hoehe des Ziels: liegt die Fuellung darunter, sieht man direkt, wie viel fehlt
    if (k.goal != null) cap(x, Math.max(bw, (k.goal / max) * h), new Color(INK_HEX, 0.9), true);
  });
  return dc.getImage();
}

// Je Woche eine Spalte unter der Saeule: TSS (fett), Ziel, Wochenanfang
function tssLabels(parent, W, weeks) {
  const slot = Math.floor(W / weeks.length), row = parent.addStack();
  row.size = new Size(slot * weeks.length, 0);
  weeks.forEach((k, i) => {
    const cur = i === weeks.length - 1;
    const col = row.addStack(); col.layoutVertically(); col.size = new Size(slot, 0);
    const line = (fn) => { const r = col.addStack(); r.addSpacer(); fn(r); r.addSpacer(); };
    line((r) => num(r, k.tss != null ? fmt(k.tss) : MISSING, FS.md, { align: "center", minScale: 0.7 }));
    line((r) => text(r, k.goal != null ? `/${fmt(k.goal)}` : "ohne Ziel", FS.xs, { color: COL.muted, align: "center", minScale: 0.7 }));
    line((r) => text(r, dateShort(k.weekStart), FS.xs, { bold: cur, color: cur ? COL.text : COL.muted, align: "center", minScale: 0.7 }));
  });
}

function buildMedium(res) {
  const d = res.data;
  const W = widgetInnerWidth();
  const w = baseWidget({ padV: 11 });
  const tw = d.tssWeeks || [], cur = tw[tw.length - 1];
  header(w, W, "BELASTUNG · 6 WOCHEN", cur && cur.tss != null ? `${fmt(cur.tss)}${cur.goal ? ` / ${fmt(cur.goal)}` : ""} TSS diese Woche` : null);
  w.addSpacer(5);
  if (tw.some((k) => k.tss != null)) {
    const im = w.addImage(tssImage(W, tw)); im.imageSize = new Size(W, TSS_PLOT_H);
    w.addSpacer(3);
    tssLabels(w, W, tw);
  } else text(w, "Keine TSS-Werte", FS.xs, { color: COL.muted });
  w.addSpacer();
  // Unten: Schwellen mit Sportsymbol
  const T = d.thresholds, th = w.addStack(); th.centerAlignContent(); th.spacing = 4;
  const item = (k, str) => { symbol(th, SPORT_SYMBOL[k], 11, new Color(sportHex(k))); text(th, str, FS.xs, { bold: true, color: COL.muted, minScale: 0.7 }); th.addSpacer(8); };
  item("run", T.run.thresholdPaceSecPerKm ? `${fmtPace(T.run.thresholdPaceSecPerKm)}/km` : MISSING);
  item("bike", T.bike.ftp ? `FTP ${T.bike.ftp} W` : MISSING);
  item("swim", T.swim.thresholdPaceSecPer100m ? `${fmtPace(T.swim.thresholdPaceSecPer100m)}/100 m` : MISSING);
  notice(w, res, d);
  return w;
}

/* ---------- Schlaf und Erholung, Ernaehrung, Fitness (klein) ---------- */
function smallWidget() { return baseWidget({ padV: 12 }); }

function buildSleep(res) {
  const d = res.data, s = d.sleep, IW = smallInnerWidth();
  const w = smallWidget();
  const last = s.latest, isToday = last && last.date === d.today, cls = isToday && s.cls ? s.cls : null;
  header(w, IW, "SCHLAF", last ? (isToday ? "heute" : dateShort(last.date)) : null);
  w.addSpacer(2);
  const top = w.addStack(); top.centerAlignContent(); top.size = new Size(IW, 0);
  valueUnit(top, last ? fmt(last.hours, 1) : MISSING, "h", 32, { color: last ? COL.sleep : COL.muted });
  top.addSpacer();
  if (cls && cls.sleep !== "none") statusIcon(top, cls.sleep, 15);
  const t = s.days[s.days.length - 1];
  const vital = (label, val, med, c) => {
    const r = w.addStack(); r.centerAlignContent(); r.size = new Size(IW, 0);
    if (c) { statusMark(r, c, 8); r.addSpacer(5); }
    text(r, label, FS.xs, { minScale: 0.8 });
    r.addSpacer();
    text(r, fmt(val), FS.xs, { bold: true, minScale: 0.8 });
    if (med != null) text(r, ` Ø ${fmt(med)}`, FS.xs, { color: COL.muted, minScale: 0.8 });
  };
  vital("HRV", t.hrv, s.medianHrv, cls && cls.hrv);
  vital("Ruhepuls", t.restingHR, s.medianRestingHR, cls && cls.resting);
  w.addSpacer(4);
  const missing = s.days.filter((x) => x.hours == null);
  if (missing.length >= 3 && s.days.length - missing.length <= 1) {
    // Mehrere Tage ohne Daten: eine Zeile statt lauter leerer Striche
    text(w, `keine Daten ${dayShort(missing[0].date)}–${dayShort(missing[missing.length - 1].date)}`, FS.xs, { bold: true, color: COL.warn, minScale: 0.8 });
  } else {
    const bh = 20 + DAY_LABEL_H;
    const im = w.addImage(barsImage(IW, bh, s.days.map((x) => x.hours), d.today, s.days.map((x) => x.date), null, 8, SLEEP_HEX)); im.imageSize = new Size(IW, bh);
  }
  w.addSpacer();
  notice(w, res, d);
  return w;
}

// Bereitschaft: Urteil mit Symbol gross, darunter die fuenf Werte als Form (Kreis/Dreieck/Quadrat) mit Kurzname. Daten wie im grossen Widget.
function buildReady(res) {
  const d = res.data, r = d.readiness, v = r.verdict, IW = smallInnerWidth();
  const w = smallWidget();
  header(w, IW, "BEREITSCHAFT", null);
  w.addSpacer(7);
  statusIcon(w, v.cls, 26);
  w.addSpacer(5);
  num(w, v.text, FS.lg, { color: v.cls === "none" ? COL.muted : colorFor(v.cls), lines: 2, minScale: 0.7 });
  w.addSpacer();
  const short = { Schlaf: "Schlaf", Ermüdung: "Ermüd.", Muskelkater: "Musk.", Stimmung: "Stimm.", Motivation: "Motiv." };
  const row = w.addStack(); row.size = new Size(IW, 0);
  const cw = IW / Math.max(1, r.items.length);
  for (const it of r.items) {
    const c = row.addStack(); c.layoutVertically(); c.size = new Size(cw, 0);
    const m = c.addStack(); m.addSpacer(); statusMark(m, it.v == null ? "none" : it.cls, 10); m.addSpacer();
    c.addSpacer(2);
    const l = c.addStack(); l.addSpacer(); text(l, short[it.label] || it.label, FS.xs, { color: COL.muted, align: "center", minScale: 0.6 }); l.addSpacer();
  }
  notice(w, res, d);
  return w;
}

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
  header(w, IW, "ERNÄHRUNG", prev ? (prev === f.days[f.days.length - 2] ? "gestern" : dateShort(prev.date)) : clock(d.generatedAt));
  w.addSpacer(3);
  valueUnit(w, show && td.protein != null ? fmt(td.protein) : MISSING, "g Protein", 34, { color: dim ? COL.muted : COL.text });
  if (yg.proteinG) text(w, `Ziel ${fmt(yg.proteinG)} g`, FS.xs, { color: COL.muted });
  w.addSpacer();
  const k = w.addStack(); k.bottomAlignContent();
  if (show && td.calories != null) { num(k, fmt(td.calories), FS.md, { color: dim ? COL.muted : COL.text }); text(k, kcalGoal ? ` / ${fmt(kcalGoal)} kcal` : " kcal", FS.xs, { color: COL.muted, minScale: 0.8 }); }
  else text(k, kcalGoal ? `Ziel ${fmt(kcalGoal)} kcal` : "kcal –", FS.xs, { bold: true, color: COL.muted });
  w.addSpacer(5);
  progressBar(w, IW, show && td.calories != null && kcalGoal ? td.calories / kcalGoal : 0, NUT_HEX.kcal, dim ? 0.5 : 1, { overHex: OVER_HEX, mark: has && kcalGoal ? dayFraction() : null });
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

/* ---------- Mittel: Disziplinen (Parameter "training") ---------- */
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

function buildTraining(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = baseWidget({ padV: 11 });
  header(w, W, "TRAINING · DIESE WOCHE", null);
  w.addSpacer(6);
  const row = w.addStack(); row.spacing = 14;
  const CW = Math.floor((W - 28) / 3);
  for (const k of ["swim", "bike", "run"]) {
    const s = d.sports[k], hex = sportHex(k);
    const c = row.addStack(); c.layoutVertically(); c.size = new Size(CW, 0);
    const nl = c.addStack(); nl.centerAlignContent(); nl.spacing = 5;
    sportBadge(nl, k, 20);
    text(nl, SPORTS[k][0], FS.sm, { bold: true, minScale: 0.7 });
    c.addSpacer(4);
    // Wochenlast: TSS gegen Plan (Plan nur, wenn im Kalender Last steht)
    const vl = c.addStack(); vl.bottomAlignContent(); vl.spacing = 3;
    num(vl, `${Math.round(s.weekLoad || 0)}`, FS.xl);
    text(vl, s.plannedLoad ? `/ ${Math.round(s.plannedLoad)} TSS` : "TSS", FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
    c.addSpacer(4);
    progressBar(c, CW, s.plannedLoad ? (s.weekLoad || 0) / s.plannedLoad : 0, hex);
    c.addSpacer(4);
    // Umfang der letzten 4 Wochen und Entwicklung gegen die 4 davor (Rad und Laufen km ohne Nachkommastelle, Schwimmen mit)
    if (s.km4 != null) {
      const tr = s.trendPct == null ? "" : ` ${s.trendPct > 0 ? "▲" : s.trendPct < 0 ? "▼" : "="}${Math.abs(s.trendPct)} %`;
      text(c, `4 W ${fmt(s.km4, k === "swim" ? 1 : 0)} km${tr}`, FS.xs, { color: COL.muted, minScale: 0.6 });
    }
  }
  w.addSpacer();
  const sp = d.split;
  header(w, W, `ZEIT · ${sp.weeks} WOCHEN`, sp.target ? `Soll S ${sp.target.swim} · R ${sp.target.bike} · L ${sp.target.run} %` : null);
  w.addSpacer(3);
  if (sp.share) { const im = w.addImage(splitImage(W, 10, sp.share, sp.target)); im.imageSize = new Size(W, 14); }
  else text(w, "Noch keine abgeschlossene Woche mit Training.", FS.xs, { color: COL.muted });
  notice(w, res, d);
  return w;
}

/* ---------- Mittel: Intensitaet (Parameter "intensitaet") ---------- */
// Zeitanteile locker (Z1-Z2), mittel (Z3-Z4), hart (Z5+) wie die Polarisation in Intervals: diese Woche und Schnitt der letzten 4 Wochen.
// Mehr als 20 % hart gilt wie im Dashboard ("Was auffällt") als viel Intensitaet und bekommt das Warn-Dreieck.
const INT_HEX = { easy: "#5BA88A", mid: "#E0B454", hard: "#D9695F" };
const HARD_SHARE = 20;

function buildIntensity(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = baseWidget({ padV: 12 });
  header(w, W, "INTENSITÄT · ZONENZEIT", null);
  const it = d.intensity || {};
  const block = (title, sh) => {
    w.addSpacer(7);
    const h = w.addStack(); h.centerAlignContent(); h.size = new Size(W, 0);
    text(h, title, FS.sm, { bold: true });
    h.addSpacer();
    if (sh && sh.hard > HARD_SHARE) { text(h, `hart ${sh.hard} % `, FS.xs, { bold: true, color: COL.warn, minScale: 0.8 }); statusIcon(h, "warn", 11); }
    else if (sh) text(h, `locker ${sh.easy} · mittel ${sh.mid} · hart ${sh.hard} %`, FS.xs, { color: COL.muted, minScale: 0.7 });
    w.addSpacer(4);
    if (sh) { const im = w.addImage(splitImage(W, 20, sh, null, ["easy", "mid", "hard"], (k) => INT_HEX[k])); im.imageSize = new Size(W, 24); }
    else text(w, "Noch keine Zonenzeiten.", FS.xs, { color: COL.muted });
  };
  block("Diese Woche", it.week);
  block("Ø 4 Wochen", it.avg);
  w.addSpacer();
  const lg = w.addStack(); lg.centerAlignContent(); lg.spacing = 4;
  for (const [k, l] of [["easy", "locker Z1–2"], ["mid", "mittel Z3–4"], ["hard", "hart Z5+"]]) {
    const sq = lg.addStack(); sq.size = new Size(8, 8); sq.cornerRadius = 2; sq.backgroundColor = new Color(INT_HEX[k]);
    text(lg, l, FS.xs, { color: COL.muted, minScale: 0.8 }); lg.addSpacer(8);
  }
  notice(w, res, d);
  return w;
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
  const wk = d.week, sets = wk.sets || 0, goalSets = wk.goalSets || 60, hex = sportHex("strength");
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
  cen((x) => text(x, sets >= goalSets ? "Ziel erreicht" : `von ${goalSets}`, FS.xs, { color: COL.muted, align: "center", minScale: 0.6 }));
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

/* ---------- Mittel: Trainingsblock und Ziele (Parameter "block") ---------- */
// Oben Blockname und Woche x von N mit Wochenleiste, darunter je Ziel eine graue Kachel: Sport, Wert mit Status-Form, Ziel.
const STATUS_HEX = (st) => ZONE_RGB[st] || NEUTRAL_HEX;
const STATUS_CLS = (st) => (st === "ok" || st === "warn" || st === "bad" ? st : "none");
const GOAL_SPORT = { runDecoupling: "run", bikeDecoupling: "bike", swimTest: "swim", strength: "strength", decoupling: "run" };

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

function buildBlock(res) {
  const d = res.data, W = widgetInnerWidth();
  const w = baseWidget({ padV: 12 });
  const b = d.block;
  if (!b) {
    header(w, W, "TRAININGSBLOCK", null);
    w.addSpacer(6);
    text(w, "Kein Block im Intervals-Kalender gefunden (Eintrag der Kategorie Saison, Name mit „Block“).", FS.sm, { color: COL.muted, lines: 4 });
    notice(w, res, d);
    return w;
  }
  const when = b.status === "active" ? `Woche ${b.weekNo}${b.weeks ? ` von ${b.weeks}` : ""}` : b.status === "upcoming" ? `Start in ${b.daysToStart} ${b.daysToStart === 1 ? "Tag" : "Tagen"}` : "beendet";
  header(w, W, `BLOCK · ${String(b.name).replace(/^block\s+/i, "").toUpperCase()}`, when);
  w.addSpacer(5);
  if (b.weeks) weekStrip(w, W, b.weeks, b.weekNo || 0, b.status === "done");
  const all = b.goals || [];
  if (!all.some((g) => g.group)) {
    w.addSpacer(3);
    text(w, `${dateShort(b.start)} – ${b.end ? dateShort(b.end) : MISSING}${b.daysLeft != null ? ` · noch ${b.daysLeft} Tage` : ""}`, FS.xs, { color: COL.muted });
  }
  w.addSpacer(7);
  if (!all.length) { text(w, "Im Eintrag stehen keine erkennbaren Ziele.", FS.sm, { color: COL.muted, lines: 2 }); notice(w, res, d); return w; }
  // Block mit Abschlusszielen: je Sportart eine Kachel, darunter die Leitplanken als eine Zeile und die Entscheidung zu Woche 12
  const grouped = all.some((g) => g.group);
  const goals = grouped ? all.filter((g) => g.group === "ziel") : all;
  const rails = grouped ? all.filter((g) => g.group === "leitplanke") : [];
  const gap = 6, CW = Math.floor((W - gap * (goals.length - 1)) / goals.length), IW = CW - 18;
  const row = w.addStack(); row.spacing = gap;
  const SHORT = { decoupling: "DECOUPLING", strength: "KRAFT", acwr: "ACWR", runDecoupling: "LAUF", bikeDecoupling: "RAD", swimTest: "SCHWIMMEN" };
  const valOf = (g) => {
    if (g.value == null) return MISSING;
    if (g.key === "strength") return `${g.value} von ${g.of}`;
    if (g.key === "swimTest") return `${g.value > 0 ? "+" : ""}${fmt(g.value, 1)} s`;
    return `${fmt(g.value, g.key === "acwr" ? 0 : 1)} %`;
  };
  const capOf = (g) => {
    if (g.status === "base") return g.key === "strength" && g.goalSets ? `Ausgangswert · Wochen mit ≥ ${g.goalSets} Sätzen` : "Ausgangswert";
    if (g.key === "strength" && g.goalSets) return `Wochen mit ≥ ${g.goalSets} Sätzen`;
    if (g.key === "runDecoupling" || g.key === "bikeDecoupling") return `Ziel ≤ ${g.max} %${g.must > g.max ? ` · Muss ≤ ${g.must}` : ""}${g.count != null && g.lastN && g.count < g.lastN ? ` · ${g.count}/${g.lastN}` : ""}`;
    if (g.key === "swimTest") return g.value == null ? "2× 500 m noch offen" : "2. 500 m je 100 m";
    return g.target;
  };
  goals.forEach((g) => {
    const c = card(row, CW, 8);
    const t = c.addStack(); t.centerAlignContent(); t.spacing = 4;
    if (GOAL_SPORT[g.key]) symbol(t, SPORT_SYMBOL[GOAL_SPORT[g.key]], 10, new Color(sportHex(GOAL_SPORT[g.key])));
    text(t, SHORT[g.key] || g.label, FS.xs, { bold: true, color: COL.muted, minScale: 0.7 });
    c.addSpacer(3);
    const vr = c.addStack(); vr.centerAlignContent(); vr.spacing = 5;
    statusMark(vr, STATUS_CLS(g.status), 8);
    num(vr, valOf(g), FS.lg, { color: g.value == null ? COL.muted : COL.text, minScale: 0.6 });
    c.addSpacer(3);
    if (g.key === "acwr" && g.value != null) { progressBar(c, IW, g.value / 100, STATUS_HEX(g.status)); c.addSpacer(3); }
    else if (g.key === "strength") {
      // erreicht = Kreis, verfehlt = Quadrat, offen = leerer Kreis; Entlastungswochen und Ausgangswerte blass
      const dots = c.addStack(); dots.centerAlignContent();
      (g.weeks || []).forEach((x, i) => {
        if (i) dots.addSpacer(2);
        const cls = x.done && x.load !== false ? (x.hit ? "ok" : g.status === "base" ? "none" : "bad") : "none";
        const name = !x.done ? "circle" : x.hit ? "circle.fill" : "square.fill";
        symbol(dots, name, 8, cls === "none" ? new Color(NEUTRAL_HEX, x.load === false || x.done ? 0.45 : 0.8) : colorFor(cls));
      });
      c.addSpacer(3);
    } else if ((g.key === "runDecoupling" || g.key === "bikeDecoupling") && g.value != null && g.must) { progressBar(c, IW, Math.min(1, g.value / (g.must * 1.25)), STATUS_HEX(g.status)); c.addSpacer(3); }
    text(c, capOf(g), FS.xs, { color: COL.muted, lines: 2, minScale: 0.7 });
  });
  if (rails.length) {
    w.addSpacer(6);
    const lr = w.addStack(); lr.centerAlignContent();
    const RAIL = { strength: (g) => `Kraft ${g.value ?? MISSING}/${g.of ?? MISSING}`, ramp: (g) => `Ramp ${g.value != null ? fmt(g.value, 1) : MISSING}`, pause: (g) => `Pause ${g.value ?? MISSING} T` };
    rails.forEach((g, i) => {
      if (i) lr.addSpacer(12);
      statusMark(lr, STATUS_CLS(g.status));
      lr.addSpacer(4);
      text(lr, (RAIL[g.key] || ((x) => x.label))(g), FS.xs, { bold: true, minScale: 0.7 });
    });
  }
  const dec = b.decision;
  if (dec) {
    w.addSpacer(4);
    const dr = w.addStack(); dr.centerAlignContent(); dr.spacing = 4;
    const dcls = dec.state === "next" ? "ok" : dec.state === "extend" ? "warn" : "bad";
    statusIcon(dr, dcls, FS.xs);
    text(dr, `Woche ${dec.week}${dec.due ? "" : " (Stand heute)"}: ${dec.text}`, FS.xs, { color: colorFor(dcls), lines: 1, minScale: 0.6 });
  }
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

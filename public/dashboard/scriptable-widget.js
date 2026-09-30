// Trainings-Widget fuer Scriptable (iOS) - gedacht fuer das grosse Widget (Large).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfuegen.
//  2. Skript einmal in Scriptable oeffnen und ausfuehren: Es fragt nach der Adresse deines Workers
//     (https://....workers.dev, ohne Pfad) und dem Dashboard-Token. Beides landet im Schluesselbund
//     dieses iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Groesse "Gross", Skript auswaehlen.
//     Zweites Widget mit Details (Form, Zielkorridor, Paces, Schwellen, Wellness): dasselbe Skript
//     nochmal als grosses Widget anlegen und im Feld "Parameter" das Wort  detail  eintragen.
// Zugangsdaten spaeter aendern: Skript in Scriptable ausfuehren und "Zugangsdaten setzen" waehlen.
//
// Es zeigt: Bereitschaft heute, Countdown, Frische (TSB), ACWR, heutige Einheit, Wochenbelastung
// je Tag und Sportart, Kraft und Hueft-Warnung. Fehlende Werte stehen als "fehlt", nie als 0.

const KEY_URL = "training-dashboard-url";
const KEY_TOKEN = "training-dashboard-token";
// Widget-Parameter "detail" (im Widget unter "Parameter" eintragen) zeigt die zweite Ansicht
let VIEW = String((typeof args !== "undefined" && args.widgetParameter) || "").trim().toLowerCase() === "detail" ? "detail" : "main";
const cacheFile = () => (VIEW === "detail" ? "training-widget-detail-cache.json" : "training-widget-cache.json");
const SPORTS = [["run", "Laufen", "#3b6ea8"], ["bike", "Rad", "#8a6bb8"], ["swim", "Schwimmen", "#3a9db0"], ["strength", "Kraft", "#b08a3a"], ["other", "Sonst.", "#9aa5b4"]];

const dyn = (l, d) => Color.dynamic(new Color(l), new Color(d));
const COL = {
  bg: dyn("#f2f4f7", "#0f1318"), card: dyn("#ffffff", "#1a2029"), text: dyn("#1c2430", "#e6eaf0"), muted: dyn("#5d6877", "#9aa5b4"),
  ok: dyn("#1f7a4d", "#6fd4a0"), warn: dyn("#9a6400", "#f0c060"), bad: dyn("#b3261e", "#f2a29c"), none: dyn("#5d6877", "#9aa5b4"),
  accent: dyn("#3b6ea8", "#6da2dc"),
};
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

async function loadData() {
  const base = baseUrl();
  const { fm, path } = cachePath();
  try {
    const req = new Request(`${base}/api/widget${VIEW === "detail" ? "?view=detail" : ""}`);
    req.headers = { Authorization: `Bearer ${Keychain.get(KEY_TOKEN)}` };
    req.timeoutInterval = 25;
    const body = await req.loadString();
    const status = req.response ? req.response.statusCode : 0;
    if (status === 401) throw new Error("Token wurde abgelehnt");
    if (status === 503) throw new Error("Worker: DASHBOARD_TOKEN nicht gesetzt");
    if (status === 404) throw new Error(`404 bei ${base}/api/widget \u2013 falsche Adresse oder Deploy noch nicht durch`);
    if (status !== 200) throw new Error(`Worker antwortet mit ${status}`);
    const data = JSON.parse(body);
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
function gaugeImage(w, min, max, zones, value) {
  const h = 16, dc = newCtx(w, h);
  const x = (v) => ((Math.min(max, Math.max(min, v)) - min) / (max - min)) * w;
  for (const z of zones) {
    const a = x(z.from) + 1, b = x(z.to) - 1;
    const p = new Path();
    p.addRoundedRect(new Rect(a, 4, Math.max(2, b - a), 8), 4, 4);
    dc.addPath(p);
    dc.setFillColor(new Color(ZONE_RGB[z.cls], 0.55));
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

// Saeulen je Wochentag (Mo-So, nur die Saeulen; Beschriftung als Text darunter). Tage nach heute bleiben leer.
function dayBarsImage(w, h, days, todayIso) {
  const dc = newCtx(w, h);
  const max = Math.max(1, ...days.map((x) => x.load ?? 0)), slot = w / 7, bw = slot * 0.56, top = 11, base = h - 1;
  dc.setFont(Font.systemFont(9));
  dc.setTextAlignedCenter();
  days.forEach((x, i) => {
    const cx = i * slot + slot / 2;
    if (x.load == null) { dc.setFillColor(new Color("#8a94a3", 0.25)); dc.fillRect(new Rect(cx - bw / 2, base - 1.5, bw, 1.5)); return; }
    const bh = x.load ? Math.max(3, (x.load / max) * (base - top)) : 1.5;
    const p = new Path();
    p.addRoundedRect(new Rect(cx - bw / 2, base - bh, bw, bh), 2.5, 2.5);
    dc.addPath(p);
    dc.setFillColor(x.load ? new Color(x.date === todayIso ? "#7db0f5" : "#4f7fbf") : new Color("#8a94a3", 0.35));
    dc.fillPath();
    if (x.load) { dc.setTextColor(new Color("#8a94a3")); dc.drawTextInRect(String(Math.round(x.load)), new Rect(i * slot, base - bh - 11, slot, 10)); }
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
  const W = Math.floor(Math.min(Device.screenSize().width - 28, 364) - 26); // Innenbreite des gro\u00dfen Widgets
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(10, 13, 8, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);

  // Kopf: Rennen und Countdown
  const g = d.goal;
  const head = w.addStack();
  head.centerAlignContent();
  head.size = new Size(W, 0);
  const left = head.addStack(); left.layoutVertically();
  text(left, `${g.name.toUpperCase()} \u00b7 ${dateShort(g.date)}`, 10, { bold: true, color: COL.muted });
  text(left, `Ziel ${fmtTime(g.targetTimeSecs)} \u00b7 ${fmtPace(g.targetTimeSecs / 21.0975)}/km`, 10, { color: COL.muted });
  if (d.hm && d.hm.estimates.length) {
    const rz = d.hm.estimates.find((e) => e.kind === "prognosis"), calc = d.hm.estimates.find((e) => e.key === "vdot");
    const bits = [rz && `Runalyze ${fmtTime(rz.seconds)}`, calc && `Rechnung ${fmtTime(calc.seconds)}`].filter(Boolean);
    if (bits.length) text(left, bits.join(" \u00b7 "), 10, { color: COL.muted });
  }
  head.addSpacer();
  text(head, g.daysToGo > 0 ? `noch ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "e"}` : g.daysToGo === 0 ? "Heute!" : "vorbei", 23, { bold: true });
  w.addSpacer(5);

  // Bereitschaft
  const r = d.readiness, v = r.verdict;
  const rc = card(w, W);
  const top = rc.addStack(); top.centerAlignContent();
  const pill = top.addStack();
  pill.backgroundColor = new Color(ZONE_RGB[v.cls] || ZONE_RGB.none, 0.2);
  pill.cornerRadius = 9;
  pill.setPadding(2, 9, 2, 9);
  text(pill, v.text, 15, { bold: true, color: colorFor(v.cls) });
  top.addSpacer();
  text(top, r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h Schlaf` : "Schlafdauer fehlt", 11, { color: COL.muted });
  rc.addSpacer(4);
  const rings = rc.addStack();
  for (const [i, it] of r.items.entries()) {
    const col = rings.addStack(); col.layoutVertically(); col.centerAlignContent();
    const ring = col.addStack();
    ring.size = new Size(30, 30);
    ring.backgroundImage = ringImage(30, it.cls);
    ring.centerAlignContent();
    ring.addSpacer();
    text(ring, it.v == null ? "\u2013" : it.v, 13, { bold: true, align: "center" });
    ring.addSpacer();
    text(col, it.label.replace("Muskelkater", "Muskeln").replace("Motivation", "Motiv."), 9, { color: COL.muted, align: "center" });
    if (i < r.items.length - 1) rings.addSpacer();
  }
  if (v.cls === "none") text(rc, v.sub, 10, { color: COL.muted });
  w.addSpacer(5);

  // Frische und ACWR
  const L = d.load, T = d.thresholds;
  const row = w.addStack(); row.spacing = 7;
  const CW = Math.floor((W - 7) / 2), IW = CW - 22;
  const gcol = (title, valueTxt, cls, statusTxt, img) => {
    const c = card(row, CW, 7);
    text(c, title, 10, { bold: true, color: COL.muted });
    const line = c.addStack(); line.centerAlignContent();
    text(line, valueTxt, 17, { bold: true, color: colorFor(cls) });
    line.addSpacer();
    text(line, statusTxt, 10, { color: colorFor(cls) });
    c.addSpacer(2);
    const im = c.addImage(img); im.imageSize = new Size(IW, 16);
  };
  gcol("FRISCHE (TSB)", L.tsb == null ? "fehlt" : fmt(L.tsb, 1), L.tsbCls, TSB_TEXT[L.tsbCls], gaugeImage(IW, -40, 30, [{ from: -40, to: T.tsb.warn, cls: "bad" }, { from: T.tsb.warn, to: T.tsb.ok, cls: "warn" }, { from: T.tsb.ok, to: 30, cls: "ok" }], L.tsb));
  gcol("ACWR (ATL/CTL)", L.acwr == null ? "fehlt" : fmt(L.acwr, 2), L.acwrCls, ACWR_TEXT[L.acwrCls], gaugeImage(IW, 0.4, 1.8, [{ from: 0.4, to: T.acwr.lo, cls: "warn" }, { from: T.acwr.lo, to: T.acwr.hi, cls: "ok" }, { from: T.acwr.hi, to: 1.8, cls: "bad" }], L.acwr));
  w.addSpacer(5);

  if (!large) { footer(w, res, d, W); return w; }

  // Heutige Einheit
  const pc = card(w, W, 7);
  const p = d.plan.today[0];
  if (p) {
    const meta = [p.durationMin && `${p.durationMin} min`, p.distanceKm && `${fmt(p.distanceKm, 1)} km`].filter(Boolean).join(" \u00b7 ");
    const line = pc.addStack(); line.centerAlignContent();
    text(line, p.name || "Einheit", 13, { bold: true });
    line.addSpacer();
    if (meta) text(line, meta, 11, { color: COL.muted });
    text(pc, p.purpose || "Kein Zweck im Plan hinterlegt.", 10, { color: COL.muted, lines: 1 });
  } else {
    text(pc, "Heute keine Einheit geplant", 13, { bold: true });
    if (d.plan.next) text(pc, `N\u00e4chste: ${dateShort(d.plan.next.date)} ${d.plan.next.name || "Einheit"}`, 10, { color: COL.muted });
  }
  w.addSpacer(5);

  // Woche: TSS je Tag, Fortschritt zum Wochenziel, Kraft
  const wc = card(w, W, 7), IW2 = W - 22;
  const wl = wc.addStack(); wl.centerAlignContent();
  const goal = d.week.goal, total = d.week.total;
  text(wl, "WOCHE \u00b7 TSS", 10, { bold: true, color: COL.muted });
  wl.addSpacer(6);
  text(wl, goal ? `${fmt(total)} / ${fmt(goal)}` : fmt(total), 12, { bold: true, color: goal && total >= goal ? COL.ok : COL.text });
  if (goal) { wl.addSpacer(4); text(wl, `${Math.round((100 * total) / goal)} %`, 10, { color: COL.muted }); }
  wl.addSpacer();
  const sc = d.week.strengthCount;
  text(wl, `Kraft ${sc}\u00d7 (Ziel 2\u20133)`, 10, { bold: sc >= 2, color: sc >= 2 ? COL.ok : COL.muted });
  const sub = [goal ? `Ziel ${fmt(goal)} ${d.week.goalSource === "plan" ? "laut Plan" : "eingestellt"}` : "Kein Wochenziel im Plan hinterlegt", d.week.lastTotal != null ? `Vorwoche ${fmt(d.week.lastTotal)}` : null].filter(Boolean).join(" \u00b7 ");
  text(wc, sub, 9, { color: COL.muted });
  wc.addSpacer(2);
  const bars = wc.addImage(dayBarsImage(IW2, 30, d.week.days, d.today)); bars.imageSize = new Size(IW2, 30);
  const lab = wc.addStack(); lab.size = new Size(IW2, 0);
  ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"].forEach((n, i) => {
    const c = lab.addStack(); c.size = new Size(IW2 / 7, 0);
    const isToday = d.week.days[i].date === d.today;
    c.addSpacer(); text(c, n, 9, { bold: isToday, color: isToday ? COL.text : COL.muted }); c.addSpacer();
  });
  wc.addSpacer(3);
  const gb = wc.addImage(goalBarImage(IW2, 5, d.week, goal)); gb.imageSize = new Size(IW2, 5);

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

// Zeitachse mit Ziel und den Halbmarathon-Zeiten aus Runalyze und den Daniels-Rechnungen
function corridorImage(w, h, goalSec, estimates) {
  const dc = newCtx(w, h);
  const all = [goalSec, ...estimates.map((e) => e.seconds)];
  const lo = Math.floor((Math.min(...all) - 180) / 300) * 300, hi = Math.ceil((Math.max(...all) + 180) / 300) * 300;
  const L = 24, R = 24, x = (v) => L + ((v - lo) / (hi - lo)) * (w - L - R), ay = 22;
  const fast = new Path();
  fast.addRect(new Rect(L, ay - 9, x(goalSec) - L, 18));
  dc.addPath(fast);
  dc.setFillColor(new Color(ZONE_RGB.ok, 0.16));
  dc.fillPath();
  dc.setFillColor(new Color("#8a94a3", 0.5));
  dc.fillRect(new Rect(L, ay - 0.75, w - L - R, 1.5));
  dc.setFont(Font.systemFont(8));
  dc.setTextColor(new Color("#8a94a3"));
  dc.setTextAlignedCenter();
  for (let t = lo; t <= hi; t += 300) dc.drawTextInRect(`${Math.floor(t / 3600)}:${String(Math.floor((t % 3600) / 60)).padStart(2, "0")}`, new Rect(x(t) - 14, ay + 9, 28, 10));
  dc.setFillColor(new Color("#5b8fd0"));
  dc.fillRect(new Rect(x(goalSec) - 1, ay - 12, 2, 24));
  [...estimates].sort((a, b) => a.seconds - b.seconds).forEach((e, i) => {
    const cx = x(e.seconds);
    dc.setFillColor(new Color("#8a94a3", e.kind === "prognosis" ? 0.25 : 1));
    dc.fillEllipse(new Rect(cx - 4, ay - 4, 8, 8));
    if (e.kind === "prognosis") { dc.setStrokeColor(new Color("#8a94a3")); dc.setLineWidth(1.5); dc.strokeEllipse(new Rect(cx - 4, ay - 4, 8, 8)); }
    const short = e.kind === "prognosis" ? "Runalyze" : e.key === "vdot" ? "VDOT" : e.label.replace("aus ", "").replace("-Bestzeit", "");
    dc.setTextColor(new Color("#a5aebb"));
    dc.drawTextInRect(`${short} ${fmtTime(e.seconds)}`, new Rect(cx - 36, i % 2 === 0 ? 0 : 33, 72, 10));
  });
  return dc.getImage();
}

// Kleine Heatmap der letzten 14 Tage: fehlende Tage sind nur eine Kontur, nie "gut"
function heatImage(w, h, rows) {
  const dc = newCtx(w, h), L = 58, n = rows[0].values.length, rh = h / rows.length, cw = (w - L) / n;
  dc.setFont(Font.systemFont(8));
  dc.setTextColor(new Color("#8a94a3"));
  rows.forEach((r, i) => {
    dc.setTextAlignedRight();
    dc.drawTextInRect(r.label, new Rect(0, i * rh + 1, L - 5, rh));
    r.values.forEach((v, j) => {
      const p = new Path();
      p.addRoundedRect(new Rect(L + j * cw + 0.75, i * rh + 1, cw - 1.5, rh - 2.5), 2, 2);
      dc.addPath(p);
      if (v == null) { dc.setStrokeColor(new Color("#8a94a3", 0.4)); dc.setLineWidth(0.8); dc.strokePath(); return; }
      dc.setFillColor(new Color("#5b8fd0", 0.16 + 0.84 * ((v - 1) / Math.max(1, r.max - 1))));
      dc.fillPath();
    });
  });
  return dc.getImage();
}

function buildDetail(res) {
  const d = res.data;
  const W = Math.floor(Math.min(Device.screenSize().width - 28, 364) - 26), IW = W - 22;
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(10, 13, 8, 13);
  w.url = `${baseUrl()}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);

  // Form
  const L = d.load, fc = card(w, W, 7);
  const fh = fc.addStack(); fh.centerAlignContent();
  text(fh, "FORM \u00b7 28 TAGE", 10, { bold: true, color: COL.muted });
  fh.addSpacer();
  text(fh, `CTL ${fmt(L.ctl, 1)}`, 10, { bold: true, color: COL.accent });
  fh.addSpacer(6);
  text(fh, `ATL ${fmt(L.atl, 1)}`, 10, { bold: true, color: COL.muted });
  fh.addSpacer(6);
  text(fh, `TSB ${fmt(L.tsb, 1)}`, 10, { bold: true, color: colorFor(L.tsbCls) });
  fc.addSpacer(3);
  if (d.form.some((f) => f.ctl != null)) { const im = fc.addImage(formImage(IW, 66, d.form, d.tsbZones)); im.imageSize = new Size(IW, 66); }
  else text(fc, "Keine CTL/ATL-Werte im Zeitraum", 10, { color: COL.muted });
  w.addSpacer(5);

  // Zielkorridor
  const cc = card(w, W, 7);
  const ch = cc.addStack(); ch.centerAlignContent();
  text(ch, "HALBMARATHON \u00b7 ZEITEN VS. ZIEL", 10, { bold: true, color: COL.muted });
  ch.addSpacer();
  text(ch, `Ziel ${fmtTime(d.goal.targetTimeSecs)}`, 10, { bold: true, color: COL.accent });
  cc.addSpacer(3);
  if (d.hm && d.hm.estimates.length) {
    const im = cc.addImage(corridorImage(IW, 44, d.hm.goalSec, d.hm.estimates)); im.imageSize = new Size(IW, 44);
    text(cc, "Rechnungen nach Daniels, keine Vorhersage; Runalyze-Prognose = leerer Punkt", 8, { color: COL.muted });
  } else text(cc, "Noch kein Runalyze-Snapshot eingespielt", 10, { color: COL.muted });
  w.addSpacer(5);

  // VDOT/Paces und Schwellen
  const row = w.addStack(); row.spacing = 7;
  const CW = Math.floor((W - 7) / 2);
  const vc = card(row, CW, 7);
  text(vc, d.vdot && d.vdot.value != null ? `VDOT ${fmt(d.vdot.value, 1)}` : "VDOT fehlt", 12, { bold: true });
  if (d.vdot && d.vdot.paces) {
    const pz = Object.fromEntries(d.vdot.paces.map((p) => [p.key, p.pace.replace("/km", "")]));
    text(vc, `Easy ${pz.easy} \u00b7 Marathon ${pz.marathon}`, 9, { color: COL.muted });
    text(vc, `Schwelle ${pz.threshold} \u00b7 Ziel ${fmtPace(d.goal.targetTimeSecs / 21.0975)}`, 9, { color: COL.muted });
  } else text(vc, "Paces fehlen (kein Snapshot)", 9, { color: COL.muted });
  const T = d.thresholds, tc = card(row, CW, 7);
  text(tc, "SCHWELLEN", 10, { bold: true, color: COL.muted });
  text(tc, `Lauf ${T.run.thresholdPaceSecPerKm ? fmtPace(T.run.thresholdPaceSecPerKm) + "/km" : "fehlt"} \u00b7 FTP ${T.bike.ftp ? T.bike.ftp + " W" : "fehlt"}`, 9, { color: COL.text });
  text(tc, `Schwimmen ${T.swim.thresholdPaceSecPer100m ? fmtPace(T.swim.thresholdPaceSecPer100m) + "/100 m" : "fehlt"}`, 9, { color: COL.text });
  w.addSpacer(5);

  // Wellness der letzten 14 Tage
  const wc = card(w, W, 7);
  const wh = wc.addStack(); wh.centerAlignContent();
  text(wh, "WELLNESS \u00b7 14 TAGE", 10, { bold: true, color: COL.muted });
  wh.addSpacer();
  text(wh, "dunkler = schlechter, Kontur = fehlt", 8, { color: COL.muted });
  wc.addSpacer(3);
  const hm = wc.addImage(heatImage(IW, 48, d.wellness.rows)); hm.imageSize = new Size(IW, 48);

  w.addSpacer();

  // Fuss: Ernaehrung und Heisshunger
  const f = w.addStack(); f.centerAlignContent(); f.size = new Size(W, 0);
  const last = [...d.nutrition.days].reverse().find((x) => x.calories != null);
  const nut = d.nutrition.hasData && last ? `Kalorien ${fmt(last.calories)}${last.goal ? " / " + fmt(last.goal) : ""} kcal (${dateShort(last.date)})` : "Ern\u00e4hrung: noch keine Daten";
  const cr = d.cravings.count ? `Hei\u00dfhunger 7 Tage: ${d.cravings.count}\u00d7${d.cravings.strongest ? `, st\u00e4rkster ${d.cravings.strongest.strength}` : ""}` : "Hei\u00dfhunger 7 Tage: keiner";
  text(f, `${nut} \u00b7 ${cr}`, 9, { color: COL.muted });
  f.addSpacer();
  const time = new Date(d.generatedAt).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
  text(f, res.stale ? `${time} \u00b7 veraltet` : time, 9, { color: res.stale ? COL.warn : COL.muted, align: "right" });
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
    menu.addAction("Vorschau Details (zweites Widget)");
    menu.addAction("Zugangsdaten setzen / zur\u00fccksetzen");
    menu.addCancelAction("Schlie\u00dfen");
    const choice = await menu.presentAlert();
    if (choice === -1) return;
    if (choice === 1) VIEW = "detail";
    if (choice === 2 || !Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) { if (!(await askConfig())) return; if (choice === 2) return; }
  } else if (!Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) {
    Script.setWidget(messageWidget("Bitte das Skript einmal in Scriptable \u00f6ffnen und die Zugangsdaten eingeben."));
    return;
  }
  let widget;
  try { const res = await loadData(); widget = VIEW === "detail" ? buildDetail(res) : buildWidget(res); }
  catch (e) { widget = messageWidget(`Keine Daten: ${String(e.message || e)}`); }
  if (inWidget) Script.setWidget(widget); else await widget.presentLarge();
}
await main();
Script.complete();

// Trainings-Widget fuer Scriptable (iOS) - gedacht fuer das grosse Widget (Large).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfuegen.
//  2. Skript einmal in Scriptable oeffnen und ausfuehren: Es fragt nach der Adresse deines Workers
//     (https://....workers.dev, ohne Pfad) und dem Dashboard-Token. Beides landet im Schluesselbund
//     dieses iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Groesse "Gross", Skript auswaehlen.
// Zugangsdaten spaeter aendern: Skript in Scriptable ausfuehren und "Zugangsdaten setzen" waehlen.
//
// Es zeigt: Bereitschaft heute, Countdown, Frische (TSB), ACWR, heutige Einheit, Wochenbelastung
// je Tag und Sportart, Kraft und Hueft-Warnung. Fehlende Werte stehen als "fehlt", nie als 0.

const KEY_URL = "training-dashboard-url";
const KEY_TOKEN = "training-dashboard-token";
const CACHE_FILE = "training-widget-cache.json";
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
function cachePath() { const fm = FileManager.local(); return { fm, path: fm.joinPath(fm.documentsDirectory(), CACHE_FILE) }; }

async function loadData() {
  const base = baseUrl();
  const { fm, path } = cachePath();
  try {
    const req = new Request(`${base}/api/widget`);
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
    menu.addAction("Zugangsdaten setzen / zur\u00fccksetzen");
    menu.addCancelAction("Schlie\u00dfen");
    const choice = await menu.presentAlert();
    if (choice === -1) return;
    if (choice === 1 || !Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) { if (!(await askConfig())) return; if (choice === 1) return; }
  } else if (!Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) {
    Script.setWidget(messageWidget("Bitte das Skript einmal in Scriptable \u00f6ffnen und die Zugangsdaten eingeben."));
    return;
  }
  let widget;
  try { widget = buildWidget(await loadData()); }
  catch (e) { widget = messageWidget(`Keine Daten: ${String(e.message || e)}`); }
  if (inWidget) Script.setWidget(widget); else await widget.presentLarge();
}
await main();
Script.complete();

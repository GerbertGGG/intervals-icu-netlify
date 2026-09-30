// Trainings-Widget für Scriptable (iOS) – gedacht für das große Widget (Large).
//
// Einrichten:
//  1. In Scriptable ein neues Skript anlegen und diesen Text komplett einfügen.
//  2. Skript einmal in Scriptable öffnen und ausführen: Es fragt nach der Adresse deines Workers
//     (https://….workers.dev) und dem Dashboard-Token. Beides landet im Schlüsselbund dieses
//     iPhones, nicht im Skript.
//  3. Widget auf den Home-Bildschirm legen: Scriptable, Größe „Groß", Skript auswählen.
// Zugangsdaten später ändern: Skript in Scriptable mit „Zurücksetzen" ausführen (Alert erscheint).
//
// Es zeigt: Bereitschaft heute, Countdown, Frische (TSB), ACWR, heutige Einheit, Wochen-TSS je
// Sportart, Kraft und Hüft-Warnung. Fehlende Werte stehen als „fehlt", nie als 0.

const KEY_URL = "training-dashboard-url";
const KEY_TOKEN = "training-dashboard-token";
const CACHE_FILE = "training-widget-cache.json";
const SPORTS = [["run", "Laufen", "#3b6ea8"], ["bike", "Rad", "#8a6bb8"], ["swim", "Schwimmen", "#3a9db0"], ["strength", "Kraft", "#b08a3a"], ["other", "Sonst.", "#9aa5b4"]];

const dyn = (l, d) => Color.dynamic(new Color(l), new Color(d));
const COL = {
  bg: dyn("#f6f7f9", "#12161c"), card: dyn("#ffffff", "#1a2029"), text: dyn("#1c2430", "#e6eaf0"), muted: dyn("#5d6877", "#9aa5b4"),
  ok: dyn("#1f7a4d", "#6fd4a0"), warn: dyn("#9a6400", "#f0c060"), bad: dyn("#b3261e", "#f2a29c"), none: dyn("#5d6877", "#9aa5b4"),
};
const ZONE_RGB = { ok: "#2f9d64", warn: "#d99a1a", bad: "#d0453b" };

/* ---------- Konfiguration ---------- */
async function askConfig() {
  const a = new Alert();
  a.title = "Trainings-Widget einrichten";
  a.message = "Adresse deines Workers und Dashboard-Token. Beides bleibt im Schlüsselbund dieses iPhones.";
  a.addTextField("https://….workers.dev", Keychain.contains(KEY_URL) ? Keychain.get(KEY_URL) : "");
  a.addSecureTextField("Token", "");
  a.addAction("Speichern");
  a.addCancelAction("Abbrechen");
  if ((await a.presentAlert()) === -1) return false;
  const url = a.textFieldValue(0).trim().replace(/\/+$/, "");
  const token = a.textFieldValue(1).trim();
  if (!/^https:\/\//.test(url) || !token) return false;
  Keychain.set(KEY_URL, url);
  Keychain.set(KEY_TOKEN, token);
  return true;
}

/* ---------- Daten laden (mit Cache für Offline) ---------- */
function cachePath() { const fm = FileManager.local(); return { fm, path: fm.joinPath(fm.documentsDirectory(), CACHE_FILE) }; }

async function loadData() {
  const base = Keychain.get(KEY_URL);
  const { fm, path } = cachePath();
  try {
    const req = new Request(`${base}/api/widget`);
    req.headers = { Authorization: `Bearer ${Keychain.get(KEY_TOKEN)}` };
    req.timeoutInterval = 25;
    const body = await req.loadString();
    const status = req.response ? req.response.statusCode : 0;
    if (status === 401) throw new Error("Token wurde abgelehnt");
    if (status === 503) throw new Error("Worker: DASHBOARD_TOKEN nicht gesetzt");
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
const fmt = (n, d = 0) => (n == null || !Number.isFinite(Number(n)) ? "–" : Number(n).toLocaleString("de-DE", { minimumFractionDigits: d, maximumFractionDigits: d }));
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
  t.minimumScaleFactor = 0.8;
  t.textOpacity = opacity;
  if (align === "right") t.rightAlignText(); else if (align === "center") t.centerAlignText();
  return t;
}

function card(parent, pad = 8) {
  const s = parent.addStack();
  s.layoutVertically();
  s.backgroundColor = COL.card;
  s.cornerRadius = 10;
  s.setPadding(pad, pad + 2, pad, pad + 2);
  return s;
}

// Skala mit Ampelzonen und Marker (Bild, halbtransparente Zonen funktionieren in Hell und Dunkel)
function gaugeImage(w, h, min, max, zones, value) {
  const dc = new DrawContext();
  dc.size = new Size(w, h);
  dc.opaque = false;
  dc.respectScreenScale = true;
  const x = (v) => ((Math.min(max, Math.max(min, v)) - min) / (max - min)) * w;
  for (const z of zones) { dc.setFillColor(new Color(ZONE_RGB[z.cls], 0.4)); dc.fillRect(new Rect(x(z.from), 8, Math.max(0, x(z.to) - x(z.from)), 10)); }
  if (value != null) {
    dc.setFillColor(Device.isUsingDarkAppearance() ? new Color("#e6eaf0") : new Color("#1c2430"));
    dc.fillEllipse(new Rect(x(value) - 5, 6, 10, 14));
  }
  return dc.getImage();
}

// Gestapelter Balken: Anteil der Sportarten an der Wochen-TSS
function shareImage(w, h, week) {
  const dc = new DrawContext();
  dc.size = new Size(w, h);
  dc.opaque = false;
  dc.respectScreenScale = true;
  const total = SPORTS.reduce((a, [k]) => a + (week.bySport[k]?.load ?? 0), 0);
  if (!total) { dc.setFillColor(new Color("#9aa5b4", 0.3)); dc.fillRect(new Rect(0, 0, w, h)); return dc.getImage(); }
  let acc = 0;
  for (const [k, , hex] of SPORTS) {
    const v = week.bySport[k]?.load ?? 0;
    if (!v) continue;
    dc.setFillColor(new Color(hex));
    dc.fillRect(new Rect((acc / total) * w, 0, (v / total) * w, h));
    acc += v;
  }
  return dc.getImage();
}

/* ---------- Widget ---------- */
function buildWidget(res) {
  const d = res.data;
  const large = config.widgetFamily === "large" || !config.runsInWidget;
  const w = new ListWidget();
  w.backgroundColor = COL.bg;
  w.setPadding(12, 14, 10, 14);
  w.url = `${Keychain.get(KEY_URL)}/dashboard/`;
  w.refreshAfterDate = new Date(Date.now() + 30 * 60 * 1000);

  // Kopf: Rennen und Countdown
  const g = d.goal;
  const head = w.addStack();
  head.centerAlignContent();
  const left = head.addStack(); left.layoutVertically();
  text(left, `${g.name.toUpperCase()} · ${dateShort(g.date)}`, 10, { bold: true, color: COL.muted });
  text(left, `Ziel ${fmtTime(g.targetTimeSecs)} · ${fmtPace(g.targetTimeSecs / 21.0975)}/km`, 10, { color: COL.muted });
  head.addSpacer();
  text(head, g.daysToGo > 0 ? `noch ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "e"}` : g.daysToGo === 0 ? "Heute!" : "vorbei", 22, { bold: true });
  w.addSpacer(6);

  // Bereitschaft
  const r = d.readiness, v = r.verdict;
  const rc = card(w);
  const top = rc.addStack(); top.centerAlignContent();
  text(top, v.text, 17, { bold: true, color: colorFor(v.cls) });
  top.addSpacer();
  text(top, r.sleepHours != null ? `${fmt(r.sleepHours, 1)} h Schlaf` : "Schlafdauer fehlt", 11, { color: COL.muted });
  rc.addSpacer(4);
  const dots = rc.addStack();
  for (const [i, it] of r.items.entries()) {
    const col = dots.addStack(); col.layoutVertically(); col.centerAlignContent();
    text(col, it.cls === "none" ? "○" : "●", 15, { color: colorFor(it.cls), align: "center" });
    text(col, it.label.replace("Muskelkater", "Muskeln").replace("Motivation", "Motiv."), 9, { color: COL.muted, align: "center" });
    if (i < r.items.length - 1) dots.addSpacer();
  }
  if (v.cls === "none") text(rc, v.sub, 10, { color: COL.muted });
  w.addSpacer(6);

  // Frische und ACWR
  const L = d.load, T = d.thresholds;
  const row = w.addStack(); row.spacing = 8;
  const gcol = (parent, title, value, cls, img) => {
    const c = card(parent, 7);
    text(c, title, 10, { bold: true, color: COL.muted });
    text(c, value, 16, { bold: true, color: colorFor(cls) });
    const im = c.addImage(img); im.imageSize = new Size(126, 24);
  };
  gcol(row, "FRISCHE (TSB)", L.tsb == null ? "fehlt" : fmt(L.tsb, 1), L.tsbCls, gaugeImage(126, 24, -40, 30, [{ from: -40, to: T.tsb.warn, cls: "bad" }, { from: T.tsb.warn, to: T.tsb.ok, cls: "warn" }, { from: T.tsb.ok, to: 30, cls: "ok" }], L.tsb));
  gcol(row, "ACWR", L.acwr == null ? "fehlt" : fmt(L.acwr, 2), L.acwrCls, gaugeImage(126, 24, 0.4, 1.8, [{ from: 0.4, to: T.acwr.lo, cls: "warn" }, { from: T.acwr.lo, to: T.acwr.hi, cls: "ok" }, { from: T.acwr.hi, to: 1.8, cls: "bad" }], L.acwr));
  w.addSpacer(6);

  if (!large) { footer(w, res); return w; }

  // Heutige Einheit
  const pc = card(w);
  const p = d.plan.today[0];
  if (p) {
    const meta = [p.durationMin && `${p.durationMin} min`, p.distanceKm && `${fmt(p.distanceKm, 1)} km`].filter(Boolean).join(" · ");
    const line = pc.addStack(); line.centerAlignContent();
    text(line, p.name || "Einheit", 13, { bold: true });
    line.addSpacer();
    if (meta) text(line, meta, 11, { color: COL.muted });
    text(pc, p.purpose || "Kein Zweck im Plan hinterlegt.", 10, { color: COL.muted, lines: 2 });
  } else {
    text(pc, "Heute keine Einheit geplant", 13, { bold: true });
    if (d.plan.next) text(pc, `Nächste: ${dateShort(d.plan.next.date)} ${d.plan.next.name || "Einheit"}`, 10, { color: COL.muted });
  }
  w.addSpacer(6);

  // Woche: TSS je Sportart, Kraft
  const wc = card(w);
  const wl = wc.addStack(); wl.centerAlignContent();
  text(wl, "WOCHE · TSS", 10, { bold: true, color: COL.muted });
  wl.addSpacer();
  text(wl, `${fmt(d.week.total)}${d.week.lastTotal != null ? ` (Vorwoche ${fmt(d.week.lastTotal)})` : ""}`, 11, { bold: true });
  wc.addSpacer(3);
  const bar = wc.addImage(shareImage(300, 8, d.week)); bar.imageSize = new Size(300, 8);
  wc.addSpacer(3);
  const parts = SPORTS.filter(([k]) => (d.week.bySport[k]?.load ?? 0) > 0).map(([k, name]) => `${name} ${fmt(d.week.bySport[k].load)}`);
  text(wc, parts.length ? parts.join(" · ") : "Noch keine Belastung diese Woche", 10, { color: COL.muted });
  const sc = d.week.strengthCount;
  text(wc, `Kraft: ${sc}× (Ziel 2–3)`, 10, { color: sc >= 2 ? COL.ok : COL.muted });

  // Hüft-Warnung
  if (d.hip.recent > 0) { w.addSpacer(4); text(w, `⚠︎ Hüfte/Leiste/Knie erwähnt (${d.hip.recent}× in 14 Tagen)`, 11, { bold: true, color: COL.bad }); }

  w.addSpacer();
  footer(w, res);
  return w;
}

function footer(w, res) {
  const d = res.data;
  const f = w.addStack(); f.centerAlignContent();
  const time = new Date(d.generatedAt).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
  const bits = [`Stand ${time}`];
  if (res.stale) bits.push("veraltet, keine Verbindung");
  text(f, bits.join(" · "), 9, { color: res.stale ? COL.warn : COL.muted });
  f.addSpacer();
  if (d.sourcesFailed.length) text(f, `Quelle fehlt: ${d.sourcesFailed.join(", ")}`, 9, { color: COL.bad, align: "right" });
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
    menu.addAction("Vorschau (groß)");
    menu.addAction("Zugangsdaten setzen / zurücksetzen");
    menu.addCancelAction("Schließen");
    const choice = await menu.presentAlert();
    if (choice === -1) return;
    if (choice === 1 || !Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) { if (!(await askConfig())) return; if (choice === 1) return; }
  } else if (!Keychain.contains(KEY_URL) || !Keychain.contains(KEY_TOKEN)) {
    Script.setWidget(messageWidget("Bitte das Skript einmal in Scriptable öffnen und die Zugangsdaten eingeben."));
    return;
  }
  let widget;
  try { widget = buildWidget(await loadData()); }
  catch (e) { widget = messageWidget(`Keine Daten: ${String(e.message || e)}`); }
  if (inWidget) Script.setWidget(widget); else await widget.presentLarge();
}
await main();
Script.complete();

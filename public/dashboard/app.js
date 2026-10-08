"use strict";
/* Trainings-Dashboard: Laden der Daten vom Worker und Aufbau der Bereiche. Diagramme: charts.js. */
const $ = (id) => document.getElementById(id);
const { esc, fmt, fmtDate, weekday, fmtTime, pace: paceLabel, addDays, dayRange } = U;

const SPORT_LABEL = { run: "Laufen", bike: "Rad", swim: "Schwimmen", strength: "Kraft", other: "Sonstiges" };
const SPORT_ORDER = ["run", "bike", "swim", "strength", "other"];
const HISTORY_DAYS = 56;
const FORM_DAYS = 180; // Formkurve: längerer Verlauf (siehe FORM_DAYS im Worker)
const SLEEP_TARGET_H = 7.5; // Richtwert je Nacht für Athleten (7–9 h), kein persönlich kalibrierter Wert
const SLEEP_ACUTE_WEIGHT = 0.6; // Gewicht der letzten Nacht im akuten 2-Nächte-Wert
const isTaper = (g) => g.daysToGo >= 0 && g.daysToGo <= 14;
const hm = (secs) => (secs < 3600 ? fmtTime(secs) : fmtTime(Math.round(secs / 60) * 60).replace(/:00$/, "")); // ab 1 h als h:mm, darunter m:ss
// Schlafkonto: akut (2 Nächte, letzte Nacht 60 %) und chronisch (7 Nächte). Schlaf eines Tages = Nacht auf diesen Morgen. Fehlende Nacht zählt nie als gut.
function sleepAccount(d) {
  const by = Object.fromEntries(d.wellness.map((w) => [w.date, w.sleepHours]));
  const night = (i) => { const date = addDays(d.today, -i); return { date, h: by[date] ?? null }; };
  const nights = [night(0), night(1)];
  const known = nights.filter((n) => n.h != null);
  const w = [SLEEP_ACUTE_WEIGHT, 1 - SLEEP_ACUTE_WEIGHT];
  const wSum = known.reduce((a, n) => a + w[nights.indexOf(n)], 0);
  const avg = wSum ? known.reduce((a, n) => a + n.h * w[nights.indexOf(n)], 0) / wSum : null; // gewichtetes Mittel je Nacht
  const complete = known.length === 2;
  const cls = avg == null ? "none" : avg >= SLEEP_TARGET_H - 0.25 ? "ok" : avg >= SLEEP_TARGET_H - 1 ? "warn" : "bad";
  const text = avg == null ? "nicht erfasst" : complete ? (cls === "ok" ? "gut gefüllt" : cls === "warn" ? "leicht im Minus" : "im Minus") : "unvollständig";
  const week = Array.from({ length: 7 }, (_, i) => night(i)).filter((n) => n.h != null);
  const wSumH = week.reduce((a, n) => a + n.h, 0), wAvg = week.length >= 4 ? wSumH / week.length : null;
  const wCls = wAvg == null ? "none" : wAvg >= SLEEP_TARGET_H - 0.25 ? "ok" : wAvg >= SLEEP_TARGET_H - 0.75 ? "warn" : "bad";
  const wText = wAvg == null ? "zu wenig Daten" : wCls === "ok" ? "Woche ausgeglichen" : wCls === "warn" ? "Woche leicht im Minus" : "Woche im Minus";
  return {
    nights, avg, target: SLEEP_TARGET_H, cls: complete || cls === "none" ? cls : cls === "ok" ? "warn" : cls, text,
    week: { avg: wAvg, n: week.length, debt: wAvg == null ? null : Math.max(0, SLEEP_TARGET_H * week.length - wSumH), cls: wCls, text: wText },
  };
}

/* ---------- Laden ---------- */
async function load() {
  const url = new URL(location.href);
  const fromUrl = url.searchParams.get("token");
  if (fromUrl) {
    try { localStorage.setItem("dashboard-token", fromUrl); } catch {}
    url.searchParams.delete("token");
    history.replaceState(null, "", url.pathname + url.search);
  }
  let token = null;
  try { token = localStorage.getItem("dashboard-token"); } catch {}
  if (!token) return showLogin();
  let r;
  try { r = await fetch("/api/dashboard", { headers: { Authorization: "Bearer " + token }, cache: "no-store" }); }
  catch (e) { return showLogin("Worker nicht erreichbar: " + e.message); }
  if (r.status === 401 || r.status === 503) {
    try { localStorage.removeItem("dashboard-token"); } catch {}
    return showLogin(r.status === 503 ? "Auf dem Worker ist noch kein DASHBOARD_TOKEN gesetzt." : "Token wurde abgelehnt.");
  }
  if (!r.ok) return showLogin("Fehler vom Worker (" + r.status + ").");
  const dash = await r.json();
  render(dash);
  loadEgym(token, dash); // EGYM ist langsam und optional: die Karte kommt nach, die Seite wartet nicht darauf
}
function showLogin(msg) {
  $("app").hidden = true; $("login").hidden = false;
  if (msg) $("login-msg").textContent = msg;
  const go = () => { try { localStorage.setItem("dashboard-token", $("token").value.trim()); } catch {} location.reload(); };
  $("go").onclick = go; $("token").onkeydown = (e) => { if (e.key === "Enter") go(); };
}

function render(d) {
  $("login").hidden = true; $("app").hidden = false;
  $("stamp").textContent = "Stand " + new Date(d.generatedAt).toLocaleString("de-DE", { dateStyle: "short", timeStyle: "short" });
  const bad = Object.entries(d.sources).filter(([, s]) => !s.ok);
  $("sources").innerHTML = bad.map(([k, s]) => `<div class="notice bad"><b>Nicht erreichbar: ${esc(k)}</b><br>${esc(s.error)}</div>`).join("");
  renderSources(d);
  initKraftTabs();
  initNav();
  const steps = [renderCockpit, renderRennplan, renderReady, renderHistory, renderLongruns, renderIntervals, renderSport, renderFitness, renderDisciplines, renderThresholds, renderWellness, renderNutrition, renderStrength, renderStudie];
  for (const step of steps) {
    try { step(d); } catch (e) { console.error(step.name, e); const n = document.createElement("div"); n.className = "notice bad"; n.textContent = `Bereich „${step.name.replace("render", "")}" konnte nicht gezeichnet werden: ${e.message}`; $("sources").append(n); }
  }
}

/* Sprungleiste: der Abschnitt, in dem man gerade ist, ist hervorgehoben (leichtgewichtig über den Scroll-Stand) */
function initNav() {
  const links = [...document.querySelectorAll(".jump a")], targets = links.map((l) => document.querySelector(l.getAttribute("href")));
  const mark = () => {
    let cur = 0;
    targets.forEach((t, i) => { if (t && !t.closest("[hidden]") && t.getBoundingClientRect().top <= 90) cur = i; });
    links.forEach((l, i) => l.classList.toggle("active", i === cur));
  };
  addEventListener("scroll", mark, { passive: true });
  mark();
}

/* Kraft-Karte: ein Umschalter statt drei Boxen (Minuten, Sätze, Wochen mit 2×) */
function initKraftTabs() {
  const card = $("strength-card");
  if (card.dataset.ready) return;
  card.dataset.ready = "1";
  card.querySelector(".seg").addEventListener("click", (ev) => {
    const b = ev.target.closest("button[data-k]");
    if (!b) return;
    for (const x of card.querySelectorAll(".seg button")) { x.classList.toggle("on", x === b); x.setAttribute("aria-pressed", x === b); }
    for (const k of ["min", "sets", "weeks"]) $(`kp-${k}`).hidden = k !== b.dataset.k;
  });
}

/* ---------- Quellen-Status: letzter Stand je Quelle, leere Box und hängende Quelle unterscheiden ---------- */
const SRC_STALE_DAYS = { runalyze: 7 };
const SOURCES = {};
function setSource(key, label, cls, text, tip = "") { SOURCES[key] = { label, cls, text, tip }; drawSources(); }
function drawSources() {
  const order = ["intervals", "runalyze", "yazio", "egym"];
  $("srcstatus").innerHTML = order.filter((k) => SOURCES[k]).map((k) => { const x = SOURCES[k]; return `<span class="src" title="${esc(x.tip)}"><b>${esc(x.label)}</b> <span class="badge ${x.cls}">${esc(x.text)}</span></span>`; }).join("");
}
function renderSources(d) {
  const ago = (iso) => { const m = Math.round((Date.now() - Date.parse(iso)) / 60000); return m < 2 ? "gerade eben" : m < 90 ? `vor ${m} min` : m < 2880 ? `vor ${Math.round(m / 60)} h` : `vor ${Math.round(m / 1440)} Tagen`; };
  const bad = Object.values(d.sources).some((x) => !x.ok);
  setSource("intervals", "Intervals", bad ? "bad" : "ok", bad ? "Fehler" : "live · " + new Date(d.generatedAt).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }), "Intervals.icu wird bei jedem Öffnen live abgefragt.");
  const rz = d.runalyze?.fetchedAt;
  if (!rz) setSource("runalyze", "Runalyze", "none", "kein Snapshot", "Es wurde noch kein Runalyze-Snapshot eingespielt.");
  else {
    const age = (Date.now() - Date.parse(rz)) / 86400000;
    setSource("runalyze", "Runalyze", age > SRC_STALE_DAYS.runalyze ? "warn" : "ok", ago(rz), age > SRC_STALE_DAYS.runalyze ? "Snapshot älter als 7 Tage: Quelle hängt, VDOT und Prognose können veraltet sein." : "Letzter Runalyze-Snapshot");
  }
  // Yazio schreibt Tagesziel (immer) und Einträge (nur nach Tagebuch) in den Intervals-Wellness-Eintrag.
  // Aktuelles Ziel = Sync lebt; kein Eintrag heute = leere Box, nicht Störung.
  const last = (key) => [...d.wellness].reverse().find((w) => w[key] != null)?.date ?? null;
  const goalDay = last("calorieGoal"), foodDay = last("calories");
  const yesterday = addDays(d.today, -1);
  if (!goalDay && !foodDay) setSource("yazio", "Yazio", "none", "keine Daten", "Im Zeitraum kam nichts von Yazio an.");
  else if (!goalDay || goalDay < yesterday) setSource("yazio", "Yazio", "bad", `hängt · Sync ${fmtDate(goalDay ?? foodDay)}`, "Seit mehr als einem Tag kommt nichts mehr von Yazio an: Der Sync hängt.");
  else if (foodDay === d.today) setSource("yazio", "Yazio", "ok", "Einträge von heute", "Sync läuft, heute ist etwas eingetragen.");
  else setSource("yazio", "Yazio", "ok", "Sync ok · heute leer", `Sync läuft, aber heute ist noch nichts eingetragen${foodDay ? ` (zuletzt ${fmtDate(foodDay)})` : ""}. Das ist keine Störung.`);
}

/* ---------- 5 · Erholung: Bereitschaft ---------- */
// Die Einschätzungen (Bereitschaft, TSB, ACWR) rechnet der Worker (dashboard-summary.js), damit Seite
// und Widget dasselbe zeigen. Hier wird nur dargestellt.
function renderReady(d) {
  const host = $("ready");
  if (!d.sources.intervalsWellness.ok) { host.innerHTML = '<div class="card"><h3>Bereit für Training?</h3><div class="muted">Wellness nicht abrufbar.</div></div>'; return; }
  const R = d.summary.readiness, items = R.items, verdict = R.verdict;
  const sleep = R.sleepHours != null ? `${fmt(R.sleepHours, 1)} h Schlaf` : "Schlafdauer nicht erfasst";
  const extra = [R.hrv != null && `HRV ${fmt(R.hrv)}`, R.restingHR != null && `Ruhepuls ${fmt(R.restingHR)} bpm (Tageswert)`].filter(Boolean);
  host.innerHTML = `<div class="card"><h3>Bereit für Training?</h3>
    <div><span class="badge ${verdict.cls} big-badge">${verdict.text}</span> <span class="muted">${verdict.sub}</span></div>
    ${verdict.reasons?.length ? `<div class="muted" style="margin-top:6px">Auslöser: ${esc(verdict.reasons.join(" · "))}</div>` : ""}
    <div style="margin-top:8px">${esc([sleep, ...extra].join(" · "))}</div>
    <div class="ready-grid">${items.map((i) => `<div class="rrow"><span class="lbl">${i.label}</span><span class="strip" id="strip-${i.key}"></span><span class="badge ${i.cls}">${i.text}</span></div>`).join("")}</div>
    <div class="muted" style="margin-top:6px">Punkt = heute, graues Band = dein üblicher Bereich der letzten 8 Wochen (mind. 5 Einträge). Skala 1 = bestmöglich, links. Lücken zählen nie als „gut". Grenzen sind Standardwerte.</div></div>`;
  for (const i of items) C.strip($(`strip-${i.key}`), { label: i.label, max: i.max, band: i.band, value: i.v, cls: i.cls });
}

/* ---------- Cockpit: alles Wichtige auf einen Blick ---------- */
function renderCockpit(d) {
  const S = d.summary, w = S.load, { ctl, atl, tsb, acwr, tsbCls, acwrCls } = w, g = d.goal;
  const TSB_BANDS = S.thresholds.tsb, ACWR = S.thresholds.acwr;
  const tsbText = { ok: "frisch", warn: "belastet", bad: "stark ermüdet", none: "keine Daten" }[tsbCls];
  const taper = isTaper(g);
  // Im Taper ist ein niedriger ACWR gewollt: kein Alarm, nur "über Zielkorridor" bleibt eine Warnung.
  const acwrShow = taper && acwrCls === "warn" ? "ok" : acwrCls;
  const acwrText = taper && acwrCls === "warn" ? "Taper: bewusst niedrig" : { ok: "im Zielkorridor", warn: "unter Zielkorridor", bad: "über Zielkorridor", none: "keine Daten" }[acwrCls];
  const wellOk = d.sources.intervalsWellness.ok;
  const cur = d.weeks[d.weeks.length - 1];
  const days7 = dayRange(addDays(d.today, -13), d.today), map = (k) => Object.fromEntries(d.wellness.map((x) => [x.date, x[k]]));
  const last = (k) => { const m = map(k); for (let i = days7.length - 1; i >= 0; i--) if (m[days7[i]] != null) return { v: m[days7[i]], date: days7[i] }; return null; };
  // Das Cockpit ist nur die Kurzfassung: eine Kennzahl je Thema und ein Link zum Detail weiter unten.
  const tile = (title, body, cls = "", more = null) => `<div class="tile ${cls}"><h3>${title}</h3>${body}${more ? `<a class="more" href="${more[0]}">${more[1]} →</a>` : ""}</div>`;
  const goalPace = g.targetTimeSecs ? g.targetTimeSecs / (g.runKm ?? 21.0975) : null;

  // Training: Wochenvorschau Mo-So mit Plan und Ist, darunter die Schritte der heutigen Einheit
  const plan = d.planned.filter((p) => p.date === d.today);
  const shown = plan.find((p) => p.steps?.length) ?? plan[0];
  const weekTile = renderWeekPreview(d, tile);
  const todayTile = shown?.steps?.length
    ? tile(`Heute: ${esc(shown.name || "Einheit")}`, `<div class="today"><div><div class="sub">${[shown.durationMin && shown.durationMin + " min", shown.distanceKm && fmt(shown.distanceKm, 1) + " km", shown.load && "Load " + fmt(shown.load)].filter(Boolean).join(" · ") || "&nbsp;"}</div>${shown.description ? `<div class="sub clamp">${esc(shown.description)}</div>` : ""}</div><div id="ck-workout" class="wprofile"></div></div>`, "span4")
    : "";
  const raceTile = tile(esc(g.name), `<div class="val">${g.daysToGo > 0 ? `${g.daysToGo} <small>Tag${g.daysToGo === 1 ? "" : "e"}</small>` : g.daysToGo === 0 ? "Heute!" : "vorbei"}</div><div id="ck-countdown"></div><div class="sub">${weekday(g.date)} ${fmtDate(g.date)}${g.triathlon ? (g.totalTargetSecs ? ` · Ziel ${fmtTime(g.totalTargetSecs)}` : "") : ` · Ziel ${fmtTime(g.targetTimeSecs)} (${paceLabel(goalPace)} min/km)`}</div>`, "span15", g.daysToGo >= 0 ? ["#rennplan", "Zielzeiten und Plan"] : null);
  const supportTile = (d.supportRaces ?? []).length ? tile("Vorbereitungsrennen (B)", d.supportRaces.slice(0, 3).map((r) => `<div class="mrow"><div class="mhead"><b>${esc(r.name)}</b><span class="mval">${r.daysToGo > 0 ? `${r.daysToGo} <small>Tage</small>` : r.daysToGo === 0 ? "Heute!" : "vorbei"}</span></div><div class="sub">${weekday(r.date)} ${fmtDate(r.date)}${r.runKm ? ` · Lauf ${fmt(r.runKm, 1)} km` : ""}${r.targetTimeSecs ? ` · Ziel ${fmtTime(r.targetTimeSecs)}` : ""}</div></div>`).join("") + `<div class="sub">Zwischenziel auf dem Weg zu: ${esc(g.name)}</div>`, "span15") : "";
  const blockTile = renderBlock(d.seasonBlock);
  const prevWk = d.weeks[d.weeks.length - 2];
  const taperTile = taper ? tile("Taper-Status", `<div class="val" style="font-size:1.1rem">Taper-Phase · noch ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "e"}</div><div class="sub">Weniger Umfang ist jetzt gewollt: Diese Woche ${cur ? fmt(cur.km, 1) : "–"} km${prevWk ? `, Vorwoche ${fmt(prevWk.km, 1)} km` : ""}. Niedriger ACWR und steigende Frische (TSB${tsb != null ? ` ${fmt(tsb)}` : ""}) gelten als Soll, nicht als Mangel.</div>`, "span4") : "";
  const tsbTile = tile("Frische (TSB)", `<div id="ck-tsb"></div><div><span class="badge ${tsbCls}">${tsbText}</span></div>`, "span15", ["#form", "Formverlauf"]);
  const acwrTile = tile("Belastung (ACWR)", `<div id="ck-acwr"></div><div><span class="badge ${acwrShow}">${acwrText}</span> <span class="sub">${taper ? "Taper-Woche" : `Ziel ${fmt(ACWR.lo, 1)}–${fmt(ACWR.hi, 1)}`}</span></div>`, "span15", ["#form", "Wochenbelastung"]);

  // Erholung: Urteil, Schlafkonto und die letzten Werte als Zahl; Verläufe und Bänder stehen in Abschnitt 5
  const R = S.readiness;
  const readyTile = wellOk ? tile("Bereit für Training?", `<div><span class="badge ${R.verdict.cls} big-badge">${R.verdict.text}</span> <span class="sub">${esc(R.verdict.sub)}</span></div>${R.verdict.reasons?.length ? `<div class="sub">${esc(R.verdict.reasons.join(" · "))}</div>` : ""}<div class="dots">${R.items.map((i) => `<span title="${esc(i.text)}"><i class="${i.cls}"></i>${esc(i.label)}</span>`).join("")}</div>`, "span3", ["#erholung", "Bänder und Verläufe"]) : tile("Bereit für Training?", '<div class="sub">Wellness nicht abrufbar.</div>', "span3");
  const SA = sleepAccount(d);
  const lastVal = (key, unit) => { const l = last(key); return l ? `${key === "hrv" ? "HRV" : "Ruhepuls"} ${fmt(l.v)} ${unit}${l.date !== d.today ? ` (${fmtDate(l.date)})` : ""}` : null; };
  const sleepTile = tile("Schlafkonto", `<div class="val">${SA.avg != null ? fmt(SA.avg, 1) : "–"} <small>h Ø akut · Ziel ${fmt(SA.target, 1)} h</small></div><div><span class="badge ${SA.cls}">${SA.text}</span> <span class="badge ${SA.week.cls}">${SA.week.text}</span></div><div class="sub">${[lastVal("hrv", "ms"), lastVal("restingHR", "bpm")].filter(Boolean).join(" · ") || "&nbsp;"}</div>`, "span3", ["#erholung", "Schlaf, HRV, Ruhepuls"]);

  // Ernährung: Kalorien heute und die Makros als eine Zeile; Balken je Größe stehen in Abschnitt 6
  const kcal = map("calories"), goal = map("calorieGoal"), carbs = map("carbs"), prot = map("protein"), fat = map("fat");
  // Heute immer anzeigen: vor dem ersten Yazio-Eintrag leer (0), nicht der Stand von gestern.
  const lastData = [...days7].reverse().find((x) => kcal[x] != null || carbs[x] != null || prot[x] != null);
  const nDay = d.today;
  const hasToday = kcal[nDay] != null || carbs[nDay] != null || prot[nDay] != null;
  const kcalToday = kcal[nDay] ?? 0, kcalGoal = goal[nDay] ?? d.nutritionGoals?.kcal ?? null;
  const emptyNote = hasToday ? "" : `Heute noch nichts eingetragen${lastData ? ` (zuletzt ${fmtDate(lastData)}: ${fmt(kcal[lastData])} kcal)` : ""}`;
  const ng = d.nutritionGoals ?? {};
  const macroLine = [["Eiweiß", prot[nDay], ng.proteinG], ["KH", carbs[nDay], ng.carbsG], ["Fett", fat[nDay], ng.fatG]].map(([l, v, gl]) => `${l} ${fmt(v ?? 0)}${gl ? `/${fmt(gl)}` : ""} g`).join(" · ");
  const nutTile = lastData || d.nutritionGoals
    ? tile(`Ernährung · ${fmtDate(nDay)}`, `<div class="val">${fmt(kcalToday)} <small>kcal${kcalGoal ? ` von ${fmt(kcalGoal)}` : ""}</small></div><div id="ck-kcal"></div><div class="sub">${emptyNote || macroLine}</div>`, "span4", ["#ernaehrung", "Makros und Wochenübersicht"])
    : tile("Ernährung", '<div class="sub">Noch keine Yazio-Daten – erscheinen nach dem Sync.</div>', "span4");

  $("cockpit").innerHTML = `
    <div class="cockpit-group">Training</div>
    ${weekTile}${todayTile ? `<div class="cockpit" style="margin-top:10px">${todayTile}</div>` : ""}
    <div class="cockpit-group">Rennen und Form</div>
    <div class="cockpit flow">${raceTile}${supportTile}${tsbTile}${acwrTile}</div>${blockTile}
    ${taperTile ? `<div class="cockpit" style="margin-top:10px">${taperTile}</div>` : ""}
    <div class="cockpit-group">Erholung</div>
    <div class="cockpit">${readyTile}${sleepTile}</div>
    <div class="cockpit-group">Ernährung</div>
    <div class="cockpit">${nutTile}</div>`;

  if (g.daysToGo >= 0) C.countdown($("ck-countdown"), { total: HISTORY_DAYS + g.daysToGo, elapsed: HISTORY_DAYS, raceLabel: `${fmtDate(g.date)} Rennen` });
  C.gauge($("ck-tsb"), { label: "TSB", min: -40, max: 30, value: tsb, ticks: [-40, -25, -10, 0, 15, 30], zones: [{ from: -40, to: TSB_BANDS.warn, cls: "bad" }, { from: TSB_BANDS.warn, to: TSB_BANDS.ok, cls: "warn" }, { from: TSB_BANDS.ok, to: 30, cls: "ok" }] });
  C.gauge($("ck-acwr"), { label: "ACWR", min: 0.4, max: 1.8, value: acwr, dec: 2, ticks: [0.4, 0.8, 1.0, 1.3, 1.8], tickLabel: (t) => fmt(t, 1), zones: [{ from: 0.4, to: ACWR.lo, cls: taper ? "ok" : "warn" }, { from: ACWR.lo, to: ACWR.hi, cls: "ok" }, { from: ACWR.hi, to: 1.8, cls: "bad" }] });
  if (shown?.steps?.length && $("ck-workout")) C.workout($("ck-workout"), shown.steps);
  if ($("ck-kcal")) C.meter($("ck-kcal"), { label: "Kalorien", value: kcalToday, goal: kcalGoal, max: Math.max(kcalToday, kcalGoal ?? 0, 1) * 1.15, unit: "kcal" });
}

/* Wochenvorschau Mo-So: je Tag die geplanten Einheiten mit Plan-Load neben dem Ist. Vergangene Tage zeigen, ob der Plan
   erfüllt wurde (ab 75 % des Plan-Loads), heutige und kommende Tage nur den Plan. Einheiten ohne Load zählen im Plan nicht mit. */
const WEEK_HIT = 0.75;
function renderWeekPreview(d, tile) {
  const dow = (new Date(d.today + "T00:00:00Z").getUTCDay() + 6) % 7, monday = addDays(d.today, -dow);
  const days = dayRange(monday, addDays(monday, 6));
  const planned = d.plannedWeek ?? d.planned.filter((p) => p.date >= monday && p.date <= days[6]);
  const actual = Object.fromEntries(d.daily.map((x) => [x.date, x]));
  const cur = d.weeks[d.weeks.length - 1];
  const cell = (x) => {
    const items = planned.filter((p) => p.date === x), isToday = x === d.today, past = x < d.today, a = actual[x];
    const planLoad = items.reduce((n, p) => n + (p.load ?? 0), 0), ist = a?.load ?? 0;
    const state = !past ? "" : planLoad > 0 ? (ist >= planLoad * WEEK_HIT ? "hit" : "miss") : ist > 0 ? "extra" : "";
    const lines = items.map((p) => `<div class="wk-item"><i style="background:var(--s-${p.sport})"></i><span>${esc(p.name || SPORT_LABEL[p.sport] || "Einheit")}<small>${[p.durationMin && p.durationMin + " min", p.distanceKm && fmt(p.distanceKm, 1) + " km"].filter(Boolean).join(" · ")}</small></span></div>`).join("");
    const done = Object.keys(a?.sports ?? {}).map((k) => `<i style="background:var(--s-${k})" title="${SPORT_LABEL[k]} ${fmt(a.sports[k])} Load"></i>`).join("");
    const istTxt = isToday || past ? (ist > 0 ? `<b>${fmt(ist)}</b>` : "–") : "";
    const verdict = { hit: "erfüllt", miss: planLoad > 0 && ist === 0 ? "ausgefallen" : "unter Plan", extra: "ungeplant" }[state];
    return `<div class="wk-day${isToday ? " now" : ""}${past ? " past" : ""} ${state}"><div class="wk-h"><b>${weekday(x)}</b> <span>${fmtDate(x)}</span></div><div class="wk-plan">${lines || '<span class="wk-rest">frei</span>'}</div><div class="wk-nums"><span>Plan <b>${planLoad ? fmt(planLoad) : "–"}</b></span><span>Ist ${istTxt || "–"} ${done}</span>${verdict ? `<span class="wk-v">${verdict}</span>` : ""}</div></div>`;
  };
  const planSum = cur?.plannedLoad ?? planned.reduce((n, p) => n + (p.load ?? 0), 0);
  const parts = cur ? SPORT_ORDER.filter((k) => cur.bySport[k].load > 0) : [];
  const pct = cur && planSum ? Math.min(100, (cur.load / planSum) * 100) : 0;
  const head = cur ? `<div class="wk-head"><div class="val">${fmt(cur.load)} <small>Load${planSum ? ` von ${fmt(planSum)} geplant` : ""}</small></div>${planSum ? `<div class="cbar wk-bar"><div class="cfill" style="width:${pct}%;background:var(--accent)"></div></div>` : ""}<div class="dots">${parts.map((k) => `<span><i style="background:var(--s-${k})"></i>${SPORT_LABEL[k]} ${fmt(cur.bySport[k].load)}</span>`).join("") || '<span class="sub">noch nichts trainiert</span>'}<span class="sub">${fmt(SPORT_ORDER.reduce((n, k) => n + cur.bySport[k].minutes, 0) / 60, 1)} h${cur.km ? ` · Laufen ${fmt(cur.km, 1)} km` : ""}</span></div></div>` : "";
  return tile(`Woche ab ${fmtDate(monday)}`, `${head}<div class="wk-grid">${days.map(cell).join("")}</div>`, "span4", ["#form", "Wochenverlauf und Vergleich"]);
}

/* ---------- 2 · Rennplan: Zielstaffel, Splits, Verpflegung, Checkliste ---------- */
// Realistische Spanne = die beiden verbleibenden Rechnungen (aus VDOT, aus 10-km-Bestzeit). Beides sind Modellwerte
// nach Daniels; ohne Lauf ab 16 km in den letzten 8 Wochen gibt es keinen Beleg für die Distanz, deshalb +3 % auf die Obergrenze.
const RANGE_KEYS = ["vdot", "best-10"];
const NO_LONGRUN_PENALTY = 0.03;
function raceScenarios(d) {
  const g = d.goal;
  if (g.triathlon || g.daysToGo < 0 || !g.targetTimeSecs) return null;
  const km = g.runKm ?? 21.0975;
  const est = (d.runalyze?.hmEstimates ?? []).filter((e) => RANGE_KEYS.includes(e.key));
  const noLong = (d.fitness.longRunTracker?.count16 ?? 0) === 0;
  const lo = est.length ? Math.min(...est.map((e) => e.seconds)) : null;
  const hi = est.length ? Math.max(...est.map((e) => e.seconds)) * (noLong ? 1 + NO_LONGRUN_PENALTY : 1) : null;
  const times = [...new Set([g.targetTimeSecs, lo, hi].filter((x) => x != null).map((x) => Math.round(x)))].sort((a, b) => a - b);
  const scenarios = times.map((secs, i) => ({ id: "ABC"[i], secs, pace: secs / km, isGoal: secs === Math.round(g.targetTimeSecs) }));
  return { km, lo, hi, noLong, scenarios, goal: g.targetTimeSecs };
}

// Checkliste: Häkchen bleiben im Browser (localStorage), je Renntermin getrennt.
// Nur in der letzten Woche vor dem Rennen (daysToGo 0–7) sichtbar.
function renderChecklist(d, items) {
  const card = $("checklist").closest(".card"), show = d.goal.daysToGo >= 0 && d.goal.daysToGo <= 7;
  if (card) card.hidden = !show;
  if (!show) { $("checklist").innerHTML = ""; return; }
  const key = "rennplan-check-" + d.goal.date;
  let done = {}; try { done = JSON.parse(localStorage.getItem(key) || "{}"); } catch {}
  $("checklist").innerHTML = items.map((t, i) => `<label style="display:flex;gap:8px;align-items:flex-start;padding:3px 0"><input type="checkbox" data-i="${i}"${done[i] ? " checked" : ""}> <span>${esc(t)}</span></label>`).join("");
  $("checklist").onchange = (e) => { done[e.target.dataset.i] = e.target.checked; try { localStorage.setItem(key, JSON.stringify(done)); } catch {} };
}

/* Triathlon: Ziele je Disziplin (Eintrag hat Vorrang vor Vorschlag aus FTP/CSS), Marken je Disziplin, Verpflegung nach Faustwert.
   Wechselzeiten sind nicht eingerechnet. */
function renderTriRennplan(d, box) {
  const g = d.goal, T = g.triathlon, tg = T.targets ?? {};
  box.hidden = false;
  const tag = (x) => (x?.source === "vorschlag" ? ' <small class="muted">(Vorschlag)</small>' : "");
  const miss = '<span class="muted">fehlt</span>';
  const sw = tg.swim, b = tg.bike, r = tg.run;
  const rows = [
    [`Schwimmen ${fmt(T.swimKm, 2)} km`, sw ? `<b>${fmtTime(sw.timeSecs)}</b>` : miss, sw ? `${paceLabel(sw.pacePer100m)} /100 m${tag(sw)}` : "CSS fehlt"],
    [`Rad ${fmt(T.bikeKm)} km`, b?.timeSecs ? `<b>${fmtTime(b.timeSecs)}</b>` : miss, b ? [b.watts ? (b.wattsLow ? `${fmt(b.wattsLow)}–${fmt(b.wattsHigh)} W` : `${fmt(b.watts)} W`) : "", b.speedKmh ? `${fmt(b.speedKmh, 1)} km/h` : ""].filter(Boolean).join(" · ") + tag(b) : "FTP fehlt"],
    [`Laufen ${fmt(T.runKm, 1)} km`, r ? `<b>${fmtTime(r.timeSecs)}</b>` : miss, r ? `${paceLabel(r.pacePerKm)} min/km${tag(r)}` : "VDOT fehlt"],
  ];
  const known = [sw?.timeSecs, b?.timeSecs, r?.timeSecs], all = known.every((x) => x != null), sum = known.reduce((a, x) => a + (x ?? 0), 0);
  const total = g.totalTargetSecs;
  const head = total ? `<div class="big" style="font-size:1.4rem">${hm(total)}</div><div class="muted">Gesamtziel</div>`
    : all ? `<div class="big" style="font-size:1.4rem">${hm(sum)}</div><div class="muted">Vorschlag Summe (ohne Wechselzeiten, kein Gesamtziel hinterlegt)</div>`
    : '<div class="muted">Kein Gesamtziel hinterlegt.</div>';
  $("staffel").innerHTML = `${head}
    <div class="scroll"><table class="mini" style="margin-top:8px"><thead><tr><th></th><th>Zeit</th><th>Pace / Leistung</th></tr></thead><tbody>${rows.map((x) => `<tr><td><b>${x[0]}</b></td><td>${x[1]}</td><td>${x[2]}</td></tr>`).join("")}</tbody></table></div>
    ${all && total ? `<div class="${sum + 120 > total ? "notice" : "muted"}" style="margin-top:8px">Summe der Disziplinen ${fmtTime(sum)} ${sum > total ? `liegt ${hm(sum - total)} über dem Gesamtziel` : `plus Wechselzeiten unter dem Gesamtziel`}.</div>` : !all ? '<div class="muted" style="margin-top:8px">Für Vorschläge fehlen Schwimmschwelle, FTP oder VDOT. Alternativ in die Beschreibung des Rennens schreiben, z. B. „Schwimmen 40:00“, „Rad 3:00:00“, „Lauf 1:55:00“.</div>' : ""}
    <div class="muted">Vorschläge sind Modellwerte (Schwimmschwelle, FTP mit Leistungsmodell, Lauf aus VDOT plus 6 % fürs Laufen nach dem Rad), kein Trainingsplan; Einträge in der Beschreibung haben Vorrang. Wechselzeiten sind nicht eingerechnet.</div>`;

  $("splits").closest(".card").hidden = true;

  renderChecklist(d, ["Startunterlagen, Startzeit und Anreise geprüft", "Wetter und Wassertemperatur geprüft, Neoprenpflicht klären", "Rad gewartet, Reifendruck, Ersatzschlauch und Werkzeug", "Wechselzone: Material je Disziplin sortiert, Wechselplätze angeschaut", "Gels, Riegel und Getränke nach Plan eingepackt", "Leistungs- und Pace-Plan (Rad Watt, Lauf Splits) auf die Uhr", "Frühstück 2–3 h vor dem Start geplant", "Schlaf: zwei Nächte vor dem Rennen früh ins Bett"]);
}

function renderRennplan(d) {
  const box = $("rennplan");
  if (d.goal.triathlon && d.goal.daysToGo >= 0) return renderTriRennplan(d, box);
  const sc = raceScenarios(d);
  if (!sc) { box.hidden = true; return; }
  box.hidden = false;
  const note = { A: "nur bei perfektem Tag", B: "realistisch", C: "sicher" };
  const hasRange = sc.lo != null;
  const label = (x) => (sc.scenarios.length === 1 ? "Ziel" : x.id + "-Ziel");
  const rows = sc.scenarios.map((x) => `<tr><td><b>${label(x)}</b>${x.isGoal ? ' <small class="muted">(dein Ziel)</small>' : ""}</td><td><b>${hm(x.secs)}</b> <small class="muted">${fmtTime(x.secs)}</small></td><td>${paceLabel(x.pace)} min/km</td><td class="muted">${hasRange && sc.scenarios.length === 3 ? note[x.id] : ""}</td></tr>`).join("");
  $("staffel").innerHTML = `${hasRange ? `<div class="big" style="font-size:1.4rem">${hm(sc.lo)}–${hm(sc.hi)}</div><div class="muted">Realistische Spanne (aus VDOT und 10-km-Bestzeit${sc.noLong ? ", Obergrenze +3 %, da kein Lauf ab 16 km in 8 Wochen" : ""})</div>` : '<div class="muted">Keine Runalyze-Werte für eine Spanne – nur das Ziel ist hinterlegt.</div>'}
    <div class="scroll"><table class="mini" style="margin-top:8px"><thead><tr><th></th><th>Zeit</th><th>Pace</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>
    ${hasRange && sc.goal < sc.lo ? `<div class="notice">Das Ziel ${hm(sc.goal)} liegt unter der realistischen Spanne und gelingt nur bei perfektem Tag. Die ersten 5 km nicht schneller als die Pace des B-Ziels laufen, sonst droht der Einbruch ab km 15.</div>` : ""}
    <div class="muted">Die Spanne ist eine Rechnung, keine Vorhersage.</div>`;

  $("splits").closest(".card").hidden = false;
  // Splits (gleichmäßige Pace) und Verpflegung nach Faustwert: Kohlenhydrate alle ca. 40 min
  const marks = [5, 10, 15, 20, sc.km].filter((m, i, a) => a.indexOf(m) === i);
  const splitRows = marks.map((m) => `<tr><td>${m === sc.km ? fmt(m, 1) : m} km</td>${sc.scenarios.map((x) => `<td>${fmtTime(Math.round(x.pace * m))}</td>`).join("")}</tr>`).join("");
  const ref = sc.scenarios.find((x) => x.id === "B") ?? sc.scenarios[0];
  const gelKm = []; for (let t = 40 * 60; t < ref.secs - 10 * 60; t += 40 * 60) gelKm.push(fmt(t / ref.pace, 1));
  $("splits").innerHTML = `<div class="scroll"><table class="mini"><thead><tr><th>Marke</th>${sc.scenarios.map((x) => `<th>${label(x)}</th>`).join("")}</tr></thead><tbody>${splitRows}</tbody></table></div>
    <div style="margin-top:8px"><b>Verpflegung</b> <span class="muted">(Faustwert nach ${label(ref)}-Pace)</span><div>${gelKm.length ? `Kohlenhydrate (Gel/Getränk) etwa alle 40 min: bei km ${gelKm.join(", ")}.` : "Rennen unter 50 min: keine Verpflegung nötig."} Dazu Wasser an den Verpflegungsstellen.</div><div class="muted">Richtwert 30–60 g Kohlenhydrate pro Stunde; nur Bewährtes aus dem Training verwenden.</div></div>`;

  renderChecklist(d, ["Startnummer, Startzeit und Anreise geprüft", "Wetter geprüft, Kleidung und Schuhe festgelegt", "Gels und Getränk nach Plan eingepackt", "Frühstück 2–3 h vor dem Start geplant", "Pace-Plan (Splits) aufs Handgelenk oder in die Uhr", "Schlaf: zwei Nächte vor dem Rennen früh ins Bett"]);
}

/* ---------- 3 · Form: Longrun-Tracker ---------- */
function renderLongruns(d) {
  const host = $("longruns"), T = d.fitness.longRunTracker;
  if (!d.sources.intervalsActivities.ok) { host.innerHTML = '<div class="muted">Aktivitäten nicht abrufbar.</div>'; return; }
  if (!T) { host.innerHTML = ""; return; }
  const need = (d.goal.runKm ?? 21.0975) >= 20;
  const warn = need && T.count16 === 0 ? `<div class="notice bad"><b>Kein Lauf ab 16 km in den letzten 8 Wochen.</b> Es gibt keinen Beleg, dass ${fmt(d.goal.runKm ?? 21.0975, 1)} km in Zielpace getragen werden. Der Aufbau davor sagt über die Langstreckentauglichkeit weniger als der längste Einzellauf.</div>` : "";
  // Decoupling (Puls driftet gegen Pace) sagt mehr als die Pace: Long Runs laufen bewusst langsam. Wert nur bei Grundlagen-/Long-Läufen (Pulsregel im Worker).
  const decCls = (v) => (v == null ? "none" : v < 5 ? "ok" : v < 8 ? "warn" : "bad");
  const list = T.recent.length ? `<table class="mini"><thead><tr><th>Datum</th><th>Distanz</th><th>Pace</th><th>Decoupling</th></tr></thead><tbody>${T.recent.map((r) => `<tr><td>${fmtDate(r.date)}</td><td>${fmt(r.distanceKm, 1)} km</td><td>${r.pace ?? "–"}</td><td>${r.decoupling != null ? `<span class="badge ${decCls(r.decoupling)}">${fmt(r.decoupling, 1)} %</span>` : '<span class="muted">–</span>'}</td></tr>`).join("")}</tbody></table>` : `<div class="muted">${T.source === "runalyze" ? "Kein Lauf der Art „Langer Lauf“" : `Kein Lauf ab ${T.minKm} km`} in den letzten 8 Wochen.</div>`;
  const decN = T.recent.filter((r) => r.decoupling != null).length;
  const decNote = T.recent.length && !decN ? '<div class="notice" style="margin-top:8px">Kein Long Run mit Decoupling-Wert: Ohne ihn fehlt der Nachweis, dass die Ausdauer stabil bleibt.</div>' : "";
  host.innerHTML = `${T.longest ? `<div class="big">${fmt(T.longest.distanceKm, 1)} <small class="muted" style="font-size:.9rem">km längster Lauf · ${fmtDate(T.longest.date)}${T.longest.pace ? " · " + T.longest.pace + " min/km" : ""}</small></div>` : ""}
    <div class="muted" style="margin:4px 0">${T.count16}× ab 16 km · ${T.source === "runalyze" ? "Läufe der Art „Langer Lauf“ in Runalyze" : `Läufe ab ${T.minKm} km`}:</div>${list}${decNote}${warn}
    <div class="muted" style="margin-top:6px">Decoupling: unter 5 % stabil, ab 8 % deutlicher Puls-Drift; aus Runalyze (Pace-Decoupling), wo vorhanden. Racepace-Blöcke innerhalb von Läufen werden nicht erkannt.</div>`;
}

/* ---------- 3 · Form: Intervall-Splits ---------- */
// Pace je Wiederholung aus Intervals.icu (nur Arbeitsintervalle, bewusst ohne Puls). Fade = zweite Hälfte minus erste Hälfte.
function renderIntervals(d) {
  const esc = (t) => String(t).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
  const list = d.intervalSessions ?? [];
  $("intervals-card").hidden = !list.length;
  if (!list.length) return;
  const fadeCls = (f) => (f <= 3 ? "ok" : f <= 8 ? "warn" : "bad");
  $("interval-splits").innerHTML = list.map((s) => `<div style="margin-bottom:12px"><b>${fmtDate(s.date)}</b> · ${esc(s.name ?? "Intervalle")}
    <div class="muted">${s.count} Wdh. · ${fmt(s.workKm, 1)} km · Ø ${s.avgPace} min/km · Streuung ${s.spreadSecPerKm} s/km · <span class="badge ${fadeCls(s.fadeSecPerKm)}">${s.fadeSecPerKm > 0 ? "+" : ""}${s.fadeSecPerKm} s/km</span> Fade</div>
    <div class="muted" style="margin-top:2px">${s.reps.map((r) => `${r.distanceM != null ? r.distanceM + " m " : ""}${r.pace}`).join(" · ")}</div></div>`).join("") +
    '<div class="muted">Fade: Pace der zweiten Hälfte minus erste Hälfte der Wiederholungen. Bis +3 s/km stabil, ab +8 s/km deutlicher Einbruch.</div>';
}

/* ---------- 3 · Form: Verlauf ---------- */
function renderHistory(d) {
  const items = (key, plan) => d.weeks.map((w) => ({ label: fmtDate(w.weekStart), tip: `Woche ab ${fmtDate(w.weekStart)}`, value: w[key], plan: w[plan], partial: !w.complete }));
  C.bars($("chart-km"), items("km", "plannedKm"), { unit: "km", label: "Laufumfang pro Woche", xLabel: "Woche ab (Montag) · helle Säule = laufende Woche", valueLabels: true, dec: 0 });
  C.bars($("chart-load"), items("load", "plannedLoad"), { unit: "Load", label: "Belastung pro Woche", xLabel: "Woche ab (Montag) · helle Säule = laufende Woche", valueLabels: true, dec: 0 });
  const noPlan = d.weeks.every((w) => w.plannedKm == null && w.plannedLoad == null);
  $("plan-note").textContent = noPlan ? "Für diesen Zeitraum liegen keine geplanten Workouts mit Distanz/Load vor – ein Plan-Marker wird deshalb nicht gezeigt." : "Zahl über der Säule = Wert, darüber die Abweichung zum Plan (grün bis 15 %, sonst gelb); Plan-Marker nur, wenn geplante Workouts Distanz bzw. Load enthalten. Laufumfang zählt nur Läufe, Belastung alle Sportarten.";

  // Formkurve bis zum Renntag (nur wenn er in den nächsten 4 Wochen liegt)
  const TSB_BANDS = d.summary.thresholds.tsb;
  const ctl = {}, atl = {};
  const series = d.formSeries?.length ? d.formSeries : d.wellness;
  for (const w of series) { ctl[w.date] = w.ctl; atl[w.date] = w.atl; }
  const first = d.formSeries?.length ? addDays(d.today, -(FORM_DAYS - 1)) : addDays(d.today, -(HISTORY_DAYS - 1));
  const raceSoon = d.goal.daysToGo >= 0 && d.goal.daysToGo <= 28;
  const end = raceSoon ? d.goal.date : d.today;
  C.form($("form-chart"), {
    days: dayRange(first, end), ctl, atl, events: formEvents(d, series, first),
    zones: [{ from: TSB_BANDS.ok, to: 200, cls: "ok" }, { from: TSB_BANDS.warn, to: TSB_BANDS.ok, cls: "warn" }, { from: -200, to: TSB_BANDS.warn, cls: "bad" }],
    marks: [{ day: d.today, label: "heute", color: "var(--muted)", anchor: raceSoon ? "end" : "middle" }, ...(raceSoon ? [{ day: d.goal.date, label: "Renntag", color: "var(--accent)", anchor: "start" }] : [])],
  });
  if (!series.some((w) => w.ctl != null)) $("form-chart").insertAdjacentHTML("beforeend", '<div class="notice">Keine CTL/ATL-Werte im Zeitraum.</div>');
  C.calendar($("calendar"), d.daily, d.today);
}

/* Ereignisse für die Formkurve aus dem, was die Seite ohnehin kennt: Rennen (Ziel, letztes, Vorbereitung), Blockstart und Pausen.
   Eine Pause ist eine Strecke von mindestens 7 Tagen ohne Belastung; die Tageslast steckt in der CTL
   (CTL = CTL_gestern + (Last − CTL_gestern) / 42), die Pause braucht also keinen Kalendereintrag. */
const PAUSE = { minDays: 7, maxLoad: 6, ctlDays: 42 };
function formEvents(d, series, first) {
  const out = [], seen = new Set();
  const add = (e) => { const k = e.kind + e.date; if (e.date >= first && !seen.has(k)) { seen.add(k); out.push(e); } };
  if (d.goal?.date) add({ kind: "race", date: d.goal.date, label: d.goal.name });
  if (d.recentRace?.date) add({ kind: "race", date: d.recentRace.date, label: d.recentRace.name || "Rennen" });
  for (const r of d.supportRaces ?? []) add({ kind: "race", date: r.date, label: r.name });
  if (d.seasonBlock?.start) add({ kind: "block", date: d.seasonBlock.start, label: d.seasonBlock.name ? `Start ${d.seasonBlock.name}` : "Blockstart" });
  const ctl = new Map(series.filter((x) => x.ctl != null).map((x) => [x.date, x.ctl]));
  let run = null, lastDay = null;
  const flush = (end) => { if (run && U.dayDiff(run, end) + 1 >= PAUSE.minDays) add({ kind: "pause", date: run, end, label: "Pause" }); run = null; };
  for (const [day, v] of [...ctl.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const prev = ctl.get(addDays(day, -1));
    if (prev == null) { flush(lastDay ?? day); lastDay = day; continue; }
    if (prev + PAUSE.ctlDays * (v - prev) < PAUSE.maxLoad) run ??= day; else flush(addDays(day, -1));
    lastDay = day;
  }
  flush(lastDay ?? first);
  return out.sort((a, b) => a.date.localeCompare(b.date));
}

/* ---------- 3 · Form: Wochenbericht nach Sportart ---------- */
function renderSport(d) {
  if (!d.sources.intervalsActivities.ok) { $("chart-sport").innerHTML = '<div class="muted">Aktivitäten nicht abrufbar.</div>'; $("week-report").innerHTML = ""; return; }
  const used = SPORT_ORDER.filter((k) => d.weeks.some((w) => w.bySport[k].load > 0));
  const weeks = d.weeks.map((w) => ({ weekStart: w.weekStart, label: fmtDate(w.weekStart), partial: !w.complete, by: Object.fromEntries(SPORT_ORDER.map((k) => [k, w.bySport[k].load])) }));
  C.stacked($("chart-sport"), weeks, used, "TSS pro Woche und Sportart");
  C.shares($("chart-share"), weeks, used, SPORT_LABEL);
  // Intensität: Zeitanteile locker (Z1-2), mittel (Z3-4), hart (Z5+) wie die Polarisation in Intervals
  const INT = { easy: "locker (Z1–2)", mid: "mittel (Z3–4)", hard: "hart (Z5+)" };
  const iw = d.weeks.map((w) => ({ weekStart: w.weekStart, label: fmtDate(w.weekStart), by: w.intensity || { easy: 0, mid: 0, hard: 0 } }));
  C.shares($("chart-intensity"), iw, Object.keys(INT), INT, { prefix: "i", unit: "min", label: "Intensität je Woche nach Zonenzeit", end: "100 % der Zonenzeit" });
  $("intensity-legend").innerHTML = Object.entries(INT).map(([k, t]) => `<span><i style="background:var(--i-${k})"></i>${t}</span>`).join("");
  $("sport-legend").innerHTML = used.map((k) => `<span><i style="background:var(--s-${k})"></i>${SPORT_LABEL[k]}</span>`).join("");
  const cur = d.weeks[d.weeks.length - 1], done = [...d.weeks].reverse().find((w) => w.complete), prev = done ? d.weeks[d.weeks.indexOf(done) - 1] : null;
  const rows = (w, ref) => SPORT_ORDER.filter((k) => w.bySport[k].count > 0 || (ref && ref.bySport[k].count > 0) || w.bySport[k].plannedLoad != null).map((k) => {
    const b = w.bySport[k], r = ref ? ref.bySport[k] : null;
    return `<tr><td>${SPORT_LABEL[k]}</td><td>${b.count}</td><td>${fmt(b.minutes / 60, 1)} h</td><td>${b.km ? fmt(b.km, 1) + " km" : "–"}</td><td><b>${fmt(b.load)}</b></td><td class="muted">${r ? fmt(r.load) : "–"}</td><td class="muted">${b.plannedLoad != null ? fmt(b.plannedLoad) : "–"}</td></tr>`;
  }).join("");
  const table = (w, ref, title) => {
    if (!w) return "";
    const sum = (x) => SPORT_ORDER.reduce((a, k) => a + x.bySport[k].load, 0), body = rows(w, ref);
    return `<h3>${title} <span class="muted">ab ${fmtDate(w.weekStart)}</span></h3>` + (body ? `<table><thead><tr><th>Sport</th><th>Einh.</th><th>Zeit</th><th>Distanz</th><th>TSS</th><th>Vorwoche</th><th>Plan</th></tr></thead><tbody>${body}<tr><td><b>Gesamt</b></td><td></td><td></td><td></td><td><b>${fmt(sum(w))}</b></td><td class="muted">${ref ? fmt(sum(ref)) : "–"}</td><td class="muted">${w.plannedLoad != null ? fmt(w.plannedLoad) : "–"}</td></tr></tbody></table>` : '<div class="muted">Keine Einheiten in dieser Woche.</div>');
  };
  // Laufende Woche ohne Einheit: eine Zeile statt einer Tabelle voller Nullen
  const curEmpty = cur && cur !== done && SPORT_ORDER.every((k) => cur.bySport[k].count === 0);
  const curHtml = curEmpty
    ? `<h3>Laufende Woche <span class="muted">ab ${fmtDate(cur.weekStart)}</span></h3><div class="muted">Woche läuft – noch keine Einheit${cur.plannedLoad != null ? `, ${fmt(cur.plannedLoad)} TSS geplant` : ""}.</div>`
    : table(cur, done, "Laufende Woche");
  $("week-report").innerHTML = table(done, prev, "Letzte volle Woche") + '<div style="height:12px"></div>' + curHtml + sportFindings(d);
}

/* "Was auffällt": gebündelte Muster statt Einzelzeilen. Jeder Befund hat ein Gewicht (sev 2 = Warnung, 1 = Hinweis, 0 = Info),
   n = Anzahl betroffener Tage oder Wochen für die Reihenfolge, und optional eine Konsequenz. Zuerst das Wichtigste; unter der Liste
   steht die Konsequenz der obersten zwei Befunde. Ohne Befund erscheint nur der Okay-Text. */
function findingsBox(list, okText, top = 12) {
  if (!list.length) return okText ? `<div class="notice ok" style="margin-top:${top}px">${okText}</div>` : "";
  const sorted = [...list].sort((a, b) => b.sev - a.sev || (b.n ?? 0) - (a.n ?? 0));
  const acts = [...new Set(sorted.slice(0, 2).map((f) => f.act).filter(Boolean))];
  return `<div class="findings" style="margin-top:${top}px"><div class="notice${sorted[0].sev === 2 ? " bad" : ""}"><b>Was auffällt</b><ul class="fl">${sorted.map((f) => `<li class="s${f.sev}"><i></i><span>${esc(f.text)}</span></li>`).join("")}</ul>${acts.length ? `<div class="act"><b>Konsequenz:</b> ${acts.map(esc).join(" ")}</div>` : ""}</div></div>`;
}

/* Auffälligkeiten im Training. Nur abgeschlossene Wochen zählen (die laufende ist unvollständig),
   Vergleich gegen den Schnitt der bis zu 4 Wochen davor. Schwellen sind grobe Faustwerte, kein Trainingsplan. */
const TRN = { jump: 1.3, drop: 0.6, planUnder: 0.75, planOver: 1.15, hardShare: 0.2, midShare: 0.25, streak: 6, restDays: 4 };
function sportFindings(d) {
  const full = d.weeks.filter((w) => w.complete), last = full[full.length - 1];
  if (!last) return "";
  const findings = [], from = fmtDate(last.weekStart), base = full.slice(-5, -1).filter((w) => w.load > 0);
  const avg = (key) => base.length ? base.reduce((n, w) => n + w[key], 0) / base.length : null;
  const aLoad = avg("load"), aKm = avg("km");
  const add = (sev, text, act = null, n = 1) => findings.push({ sev, text: `Woche ab ${from}: ${text}`, act, n });
  if (aLoad && last.load > aLoad * TRN.jump) add(2, `Belastung ${fmt(last.load)} TSS, ${Math.round((last.load / aLoad - 1) * 100)} % mehr als dein Schnitt (${fmt(aLoad)}) – steiler Anstieg.`, "Die nächste Woche nicht noch steigern, 1–2 lockere Tage fest einplanen (Verletzungsrisiko).");
  else if (aLoad && last.load < aLoad * TRN.drop) add(1, `Belastung ${fmt(last.load)} TSS, nur ${Math.round((last.load / aLoad) * 100)} % deines Schnitts (${fmt(aLoad)}) – deutlich weniger Training.`, "Wenn das kein gewollter Entlastungsblock war: die Ursache klären und die Last schrittweise zurückholen, nicht in einer Woche.");
  if (aKm && last.km > aKm * TRN.jump) add(2, `Laufumfang ${fmt(last.km, 1)} km, ${Math.round((last.km / aKm - 1) * 100)} % mehr als dein Schnitt (${fmt(aKm, 1)} km).`, "Laufumfang in der Folgewoche auf den Schnitt oder knapp darüber begrenzen.");
  if (last.plannedLoad) {
    if (last.load < last.plannedLoad * TRN.planUnder) add(1, `nur ${fmt(last.load)} von ${fmt(last.plannedLoad)} TSS des Plans geschafft (${Math.round((last.load / last.plannedLoad) * 100)} %).`, "Verpasste Einheiten nicht nachholen, sondern den Plan der laufenden Woche halten.");
    else if (last.load > last.plannedLoad * TRN.planOver) add(1, `${fmt(last.load - last.plannedLoad)} TSS über dem Plan (${fmt(last.load)} von ${fmt(last.plannedLoad)}).`, "Mehr als geplant bringt vor dem Rennen selten etwas: lockere Einheiten wirklich locker laufen.");
  }
  const skipped = SPORT_ORDER.filter((k) => last.bySport[k].plannedLoad > 0 && last.bySport[k].count === 0);
  if (skipped.length) add(1, `${skipped.map((k) => `${SPORT_LABEL[k]} (${fmt(last.bySport[k].plannedLoad)} TSS)`).join(", ")} war geplant, aber nichts absolviert.`, "Die ausgefallene Sportart gezielt wieder einplanen, damit sie im Plan nicht ganz verschwindet.", skipped.length);
  const z = last.intensity, zt = z.easy + z.mid + z.hard;
  if (zt >= 60) {
    if (z.hard / zt > TRN.hardShare) add(1, `${Math.round((z.hard / zt) * 100)} % der Zonenzeit hart (Z5+) – viel Intensität.`, "Nächste Woche mehr lockere Zeit, harte Einheiten auf höchstens zwei begrenzen.");
    if (z.mid / zt > TRN.midShare) add(1, `${Math.round((z.mid / zt) * 100)} % der Zonenzeit im mittleren Bereich (Z3–4) – weder locker noch richtig hart.`, "Lockere Einheiten langsamer, harte gezielt härter: weniger Grauzone.");
  }
  // Tage in Folge: Training ohne Pause bzw. lange Pause bis heute (heute zählt nur, wenn schon trainiert)
  const load = Object.fromEntries(d.daily.map((x) => [x.date, x.load]));
  let streak = 0, pause = 0;
  for (let x = d.today; load[x] != null; x = addDays(x, -1)) { if (load[x] > 0) streak++; else if (x !== d.today) break; }
  for (let x = addDays(d.today, -1); load[x] != null && !(load[x] > 0); x = addDays(x, -1)) pause++;
  if (streak >= TRN.streak) findings.push({ sev: 2, n: streak, text: `${streak} Trainingstage in Folge ohne Ruhetag.`, act: "Heute oder morgen einen echten Ruhetag setzen." });
  if (load[d.today] === 0 && pause >= TRN.restDays) findings.push({ sev: 1, n: pause, text: `Seit ${pause} Tagen kein Training.`, act: "Mit einer lockeren Einheit wieder einsteigen, nicht mit dem Plantempo." });
  const body = findingsBox(findings, "Nichts Auffälliges in der letzten vollen Woche.");
  return `${body}<div class="muted" style="margin-top:6px">Faustwerte: Anstieg = mehr als ${Math.round((TRN.jump - 1) * 100)} % über dem Schnitt der bis zu 4 Wochen davor, deutlich weniger = unter ${Math.round(TRN.drop * 100)} %, Plan verfehlt = unter ${Math.round(TRN.planUnder * 100)} % bzw. über ${Math.round(TRN.planOver * 100)} % des Plans, viel Intensität = über ${TRN.hardShare * 100} % hart oder über ${TRN.midShare * 100} % mittel, ${TRN.streak}+ Tage ohne Ruhetag, ${TRN.restDays}+ Tage Pause.</div>`;
}

/* ---------- 4 · Leistung ---------- */
function renderFitness(d) {
  const r = d.runalyze;
  if (!r) {
    $("vdot").innerHTML = '<div class="muted">Noch kein Runalyze-Snapshot eingespielt – VDOT und Prognose fehlen.</div>';
    return;
  }
  const stale = (Date.now() - Date.parse(r.fetchedAt)) / 86400000 > 7;
  const staleHtml = stale ? '<div class="notice">Der Runalyze-Snapshot ist älter als 7 Tage – VDOT und Prognose können veraltet sein.</div>' : "";
  const goal = d.goal.targetTimeSecs, goalPace = goal ? goal / (d.goal.runKm ?? 21.0975) : null;

  // VDOT und Zonen-Paces
  if (r.vdot == null) $("vdot").innerHTML = '<div class="muted">Kein VDOT im Runalyze-Snapshot – Paces fehlen.</div>';
  else {
    const toSec = (p) => { const [m, s] = p.replace("/km", "").split(":").map(Number); return m * 60 + s; };
    const zones = r.paces.map((p) => ({ label: p.label.replace(/ \(.\)$/, ""), sec: toSec(p.pace), fast: p.fastSecPerKm, slow: p.slowSecPerKm })).sort((a, b) => a.sec - b.sec);
    $("vdot").innerHTML = `<div class="big">${fmt(r.vdot, 1)}</div><div class="muted">VDOT (effektive VO2max laut Runalyze), Stand ${new Date(r.fetchedAt).toLocaleDateString("de-DE")}</div><div id="vdot-trend" style="margin-top:6px"></div><div id="vdot-ruler" style="margin-top:6px"></div>${staleHtml}<div class="muted">Runalyze liefert nur das VDOT, die Bereiche sind daraus wie in den Runalyze-Lauftabellen berechnet (Prozent der Geschwindigkeit bei vVO2max).</div>`;
    C.paceRuler($("vdot-ruler"), zones, goalPace);
    // Verlauf: ein Wert je Runalyze-Snapshot (aus dem gespeicherten Halbmarathon-Verlauf)
    const vh = (r.hmHistory ?? []).filter((x) => x.vdot != null).map((x) => ({ date: x.date, value: x.vdot }));
    if (vh.length >= 2) {
      const dv = vh[vh.length - 1].value - vh[0].value;
      $("vdot-trend").innerHTML = `<div id="vdot-spark"></div><div class="muted">${dv === 0 ? "unverändert" : `<b style="color:var(--${dv > 0 ? "ok" : "muted"})">${dv > 0 ? "▲ +" : "▼ −"}${fmt(Math.abs(dv), 1)}</b>`} seit ${fmtDate(vh[0].date)} (${vh.length} Snapshots)</div>`;
      C.trend($("vdot-spark"), vh, { label: "VDOT-Verlauf", dec: 1 });
    } else $("vdot-trend").innerHTML = '<div class="muted">Verlauf entsteht mit jedem weiteren Runalyze-Snapshot.</div>';
  }

  renderPredictionTables(r);

  // Verlauf nur zeigen, wenn es einen gibt
  if ((r.hmHistory ?? []).length > 1) { $("hm-trend-card").hidden = false; renderHmTrend(r); }
}

/* Prognose je Distanz (von = schnellste, bis = langsamste Schätzung) und Daniels-Trainingsbereiche mit Prozent und Pace. */
function renderPredictionTables(r) {
  const pk = (sec) => `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, "0")}/km`;
  const est = (r.raceEstimates ?? []).map((dst) => {
    const secs = dst.estimates.map((e) => e.seconds);
    const lo = Math.min(...secs), hi = Math.max(...secs);
    return `<tr><td>${esc(dst.label)}</td><td style="text-align:right;white-space:nowrap"><span class="muted">Von</span> ${lo === hi ? "--" : `<b>${fmtTime(lo)}</b>`} <span class="muted">bis</span> <b>${fmtTime(hi)}</b> <span class="muted">(${pk(Math.round(hi / dst.km))})</span></td></tr>`;
  }).join("");
  if (est) {
    $("race-pred-card").hidden = false;
    $("race-pred").innerHTML = `<table class="mini"><tbody>${est}</tbody></table><div class="muted" style="margin-top:6px">Von/bis = schnellste und langsamste Schätzung aus Runalyze-Prognose, aktuellem VDOT und 10-km-Bestzeit. Das ist eine Rechnung nach Daniels, kein Ergebnis.</div>`;
  }
  if (r.paces?.length) {
    $("pace-zones-card").hidden = false;
    const rows = r.paces.map((p) => `<tr><td>${esc(p.label.replace(/ \(.\)$/, ""))} <span class="muted">(${p.pct[0]}% – ${p.pct[1]}%)</span></td><td style="text-align:right;white-space:nowrap">${pk(p.slowSecPerKm)} – ${pk(p.fastSecPerKm)}</td></tr>`).join("");
    $("pace-zones").innerHTML = `<div class="muted" style="margin-bottom:4px">Daniels, aus VDOT ${fmt(r.vdot, 1)}</div><table class="mini"><tbody>${rows}</tbody></table>`;
  }
}

/* Prognose-Entwicklung: wird die Prognose von Snapshot zu Snapshot schneller oder nicht? Ein Eintrag je Tag aus dem
   Runalyze-Snapshot, verglichen mit dem ersten Eintrag und dem Stand vor einer Woche. Kein Ergebnis, nur die Prognose. */
function renderHmTrend(r) {
  const rows = r.hmHistory ?? [], last = rows[rows.length - 1];
  const SERIES = [
    { key: "hmProgSecs", label: "HM", color: "var(--accent)", bold: true },
    { key: "p10Secs", label: "10 km", color: "var(--muted)" },
    { key: "p5Secs", label: "5 km", color: "var(--muted)", dash: "4 3" },
  ];
  if (!last) { $("hm-trend").innerHTML = '<div class="muted">Noch kein Verlauf – er entsteht mit jedem Runalyze-Snapshot.</div>'; return; }
  const fmtD = (v) => v == null ? "–" : v === 0 ? "unverändert" : `<span style="color:var(${v < 0 ? "--ok" : "--muted"});font-weight:600">${v < 0 ? "▼ −" : "▲ +"}${fmtTime(Math.abs(v))}</span>`;
  const weekAgo = [...rows].reverse().find((x) => (Date.parse(last.date) - Date.parse(x.date)) / 86400000 >= 7);
  const KEYS = ["hmProgSecs", "p10Secs", "p5Secs"];
  // Spalten nur, wenn sie Inhalt haben: Vergleich seit Start erst ab zwei Einträgen, seit 7 Tagen erst, wenn ein Eintrag so alt ist
  const showStart = rows.length > 1, showWeek = weekAgo != null && KEYS.some((k) => weekAgo[k] != null);
  const tr = (label, key) => {
    const have = rows.filter((x) => x[key] != null);
    if (!have.length) return "";
    const now = have[have.length - 1], first = have[0], wk = weekAgo && weekAgo[key] != null ? weekAgo : null;
    return `<tr><td>${label}</td><td><b>${fmtTime(now[key])}</b></td>${showStart ? `<td>${have.length > 1 ? fmtD(now[key] - first[key]) : "–"}</td>` : ""}${showWeek ? `<td>${wk ? fmtD(now[key] - wk[key]) : "–"}</td>` : ""}</tr>`;
  };
  const table = `<table class="mini"><thead><tr><th></th><th>Prognose jetzt</th>${showStart ? `<th>seit ${fmtDate(rows[0].date)}</th>` : ""}${showWeek ? "<th>seit 7 Tagen</th>" : ""}</tr></thead><tbody>${tr("Halbmarathon", "hmProgSecs")}${tr("10 km", "p10Secs")}${tr("5 km", "p5Secs")}</tbody></table>`;
  // Die beiden Halbmarathon-Werte nebeneinander mit einer Zeile Erklärung, damit der Abstand nicht wie ein Fehler wirkt
  const hmP = last.hmProgSecs, hmV = last.hmVdotSecs;
  const hmVHave = rows.filter((x) => x.hmVdotSecs != null), hmVd = hmVHave.length > 1 ? hmVHave[hmVHave.length - 1].hmVdotSecs - hmVHave[0].hmVdotSecs : null;
  const pair = hmP != null && hmV != null ? `<div class="hm2"><div><div class="muted">Halbmarathon · Runalyze-Prognose</div><div class="big2">${fmtTime(hmP)}</div></div><div><div class="muted">Halbmarathon · aus VDOT gerechnet</div><div class="big2">${fmtTime(hmV)}</div>${hmVd != null ? `<div class="muted">${fmtD(hmVd)} seit ${fmtDate(hmVHave[0].date)}</div>` : ""}</div></div><div class="muted" style="margin-bottom:8px">${fmtTime(Math.abs(hmP - hmV))} Unterschied: Runalyze rechnet aus deinen tatsächlichen Läufen und ist vorsichtiger; die VDOT-Rechnung unterstellt Ausdauer wie über deine Testdistanz und ist bei fehlenden langen Läufen eher zu optimistisch.</div>` : "";
  const first = rows.length === 1 ? '<div class="muted" style="margin:6px 0">Der Vergleich beginnt heute. Mit jedem neuen Runalyze-Snapshot siehst du hier, ob die Prognose schneller wird – früher gespeicherte Werte gibt es nicht.</div>' : "";
  $("hm-trend").innerHTML = `${pair}${table}${first}<div id="hm-trend-chart" style="margin-top:8px"></div><div class="muted">Linien fallen = Prognose wird schneller. Die Prognose ist eine Rechnung von Runalyze aus deinen Läufen, kein Ergebnis.</div>`;
  if (rows.length > 1) C.deltaTrend($("hm-trend-chart"), rows, { label: "Veränderung der Prognose", series: SERIES });
}

/* ---------- 4 · Leistung: Disziplinen (Rad, Schwimmen, Laufen) ----------
   Je Disziplin Umfang, letzte Einheit und Entwicklung der letzten Wochen. Nur sichtbar, wenn ein Triathlon-Ziel
   besteht oder in der Disziplin trainiert wurde. Datenbasis sind die Wochen- und Tageswerte der Seite. */
const DISC = { bike: { label: "Rad", unit: "km", dec: 0 }, swim: { label: "Schwimmen", unit: "km", dec: 1 }, run: { label: "Laufen", unit: "km", dec: 0 } };
function renderDisciplines(d) {
  const host = $("disciplines"), tri = d.goal.triathlon, used = Object.keys(DISC).filter((k) => tri || d.weeks.some((w) => w.bySport[k].count > 0));
  $("disciplines-head").hidden = host.hidden = !used.length;
  if (!used.length || !d.sources.intervalsActivities.ok) { host.innerHTML = ""; return; }
  const t = d.thresholds, tg = tri?.targets ?? {};
  const sum = (ws, k, f) => ws.reduce((n, w) => n + w.bySport[k][f], 0);
  host.innerHTML = used.map((k) => {
    const cfg = DISC[k], recent = d.weeks.slice(-4), earlier = d.weeks.slice(-8, -4);
    const km4 = sum(recent, k, "km"), minutes4 = sum(recent, k, "minutes"), n4 = sum(recent, k, "count");
    const kmEarlier = sum(earlier, k, "km");
    const lastDay = [...d.daily].reverse().find((x) => x.sports[k] != null);
    const ago = lastDay ? U.dayDiff(lastDay.date, d.today) : null;
    const trend = earlier.length === 4 && kmEarlier > 0 ? Math.round((km4 / kmEarlier - 1) * 100) : null;
    const thr = k === "bike" ? (t.bike.ftp != null ? `FTP ${fmt(t.bike.ftp)} W` : "FTP fehlt")
      : k === "run" ? (t.run.thresholdPaceSecPerKm != null ? `Schwellenpace ${paceLabel(t.run.thresholdPaceSecPerKm)} min/km${d.runalyze?.vdot != null ? ` · VDOT ${fmt(d.runalyze.vdot, 1)}` : ""}` : "Schwellenpace fehlt")
      : t.swim.thresholdPaceSecPer100m != null ? `Schwellenpace ${paceLabel(t.swim.thresholdPaceSecPer100m)} min/100 m` : "Schwellenpace fehlt";
    const raceKm = { bike: tri?.bikeKm, swim: tri?.swimKm, run: tri?.runKm }[k];
    const tgt = tg[k]?.timeSecs ? `Rennziel ${fmtTime(tg[k].timeSecs)} über ${fmt(raceKm, k === "swim" ? 2 : k === "run" ? 1 : 0)} km` : null;
    const lastTxt = lastDay ? `${weekday(lastDay.date)} ${fmtDate(lastDay.date)} · ${ago === 0 ? "heute" : `vor ${ago} Tag${ago === 1 ? "" : "en"}`} · ${fmt(lastDay.sports[k])} TSS` : "keine Einheit in den letzten 8 Wochen";
    const stale = tri && (ago == null || ago > 14);
    return `<div class="card"><h3>${cfg.label}</h3>
      <div class="disc-stats"><div><div class="muted">4 Wochen</div><b class="v">${fmt(km4, cfg.dec)} km</b><div class="muted">${fmt(minutes4 / 60, 1)} h · ${n4} Einh.</div></div>
      <div><div class="muted">Entwicklung</div><b class="v" style="color:var(--${trend == null ? "muted" : trend >= 0 ? "ok" : "warn"})">${trend == null ? "–" : `${trend > 0 ? "▲ +" : trend < 0 ? "▼ −" : ""}${Math.abs(trend)} %`}</b><div class="muted">gegen die 4 Wochen davor</div></div>
      <div><div class="muted">Zuletzt</div><b class="v">${lastDay ? (ago === 0 ? "heute" : `vor ${ago} T`) : "–"}</b><div class="muted">${stale ? '<span style="color:var(--warn)">lange her</span>' : "&nbsp;"}</div></div></div>
      <div class="discbars" id="disc-${k}"></div>
      <div class="muted">Letzte Einheit: ${lastTxt}</div><div class="muted">${thr}${tgt ? ` · ${tgt}` : ""}</div></div>`;
  }).join("");
  for (const k of used) C.miniBars($(`disc-${k}`), d.weeks.map((w) => ({ label: fmtDate(w.weekStart), value: w.bySport[k].km, partial: !w.complete })), { label: `${DISC[k].label}: Kilometer je Woche`, color: `var(--s-${k})`, unit: "km", dec: DISC[k].dec });
}

/* ---------- 4 · Leistung: Schwellen ---------- */
function renderThresholds(d) {
  const host = $("thresholds");
  if (!d.sources.intervalsSportSettings.ok) { host.innerHTML = '<div class="card muted">Sport-Einstellungen nicht abrufbar.</div>'; return; }
  const t = d.thresholds, val = (v, fn) => v != null ? `<b>${fn(v)}</b>` : '<span class="muted">nicht hinterlegt</span>';
  const card = (title, rows) => `<div class="card"><h3>${title}</h3>${rows.map(([l, v]) => `<div class="row"><span>${l}</span><span>${v}</span></div>`).join("")}</div>`;
  host.innerHTML =
    card("Laufen", [["Schwellenpace", val(t.run.thresholdPaceSecPerKm, (v) => paceLabel(v) + " min/km")], ["Schwellenpuls (LTHR)", val(t.run.lthr, (v) => fmt(v) + " bpm")], ["Maximalpuls", val(t.run.maxHr, (v) => fmt(v) + " bpm")]]) +
    card("Rad", [["FTP", val(t.bike.ftp, (v) => fmt(v) + " W")], ["FTP indoor", val(t.bike.indoorFtp, (v) => fmt(v) + " W")], ["Schwellenpuls (LTHR)", val(t.bike.lthr, (v) => fmt(v) + " bpm")], ["Maximalpuls", val(t.bike.maxHr, (v) => fmt(v) + " bpm")]]) +
    card("Schwimmen", [["Schwellenpace", val(t.swim.thresholdPaceSecPer100m, (v) => paceLabel(v) + " min/100 m")]]) +
    (d.goal.daysToGo >= 0 ? "" : triathlonGoalCard(d.goal)); // vor dem Rennen stehen die Zielzeiten im Rennplan
}

/* Ziele je Disziplin: Vorschläge aus FTP und Schwimmschwelle, Angaben in der Beschreibung des Rennens haben Vorrang. */
function triathlonGoalCard(g) {
  const tg = g.triathlon?.targets;
  if (!tg) return "";
  const tag = (x) => (x.source === "eintrag" ? "" : "<small>Vorschlag</small>");
  const row = (label, x, time, detail, cls = "") =>
    `<div class="tri-row ${cls}"><div class="tri-head"><span class="tri-name">${label}${x ? tag(x) : ""}</span><span class="tri-time">${time}</span></div>${detail ? `<div class="tri-detail">${detail}</div>` : ""}</div>`;
  const missing = (t) => `<span class="muted">${t}</span>`;
  const rows = [];
  const sw = tg.swim;
  rows.push(row(`🏊 Schwimmen · ${fmt(g.triathlon.swimKm, 2)} km`, sw, sw ? fmtTime(sw.timeSecs) : "–", sw ? `${paceLabel(sw.pacePer100m)} min/100 m` : missing("Schwellenpace fehlt")));
  const b = tg.bike;
  const bikeDetail = !b ? missing("FTP fehlt")
    : [b.watts ? `${b.wattsLow ? `${fmt(b.wattsLow)}–${fmt(b.wattsHigh)}` : fmt(b.watts)} W` : "", b.pctFtp ? `${fmt(b.pctFtp * 100)} % FTP` : "", b.speedKmh ? `${fmt(b.speedKmh, 1)} km/h` : ""].filter(Boolean).join(" · ");
  rows.push(row(`🚴 Rad · ${fmt(g.triathlon.bikeKm)} km`, b, b?.timeSecs ? fmtTime(b.timeSecs) : "–", bikeDetail));
  const r = tg.run;
  rows.push(row(`🏃 Laufen · ${fmt(g.triathlon.runKm, 1)} km`, null, r ? fmtTime(r.timeSecs) : "–", r ? `${paceLabel(r.pacePerKm)} min/km` : missing("in der Beschreibung ergänzen: „Lauf 1:55:00“")));
  if (tg.totalTargetSecs) rows.push(row("Gesamtziel", null, fmtTime(tg.totalTargetSecs), "", "tri-total"));
  return `<div class="card"><h3>Triathlon-Ziele · ${esc(g.name.replace("Triathlon ", ""))}</h3>${rows.join("")}<div class="muted" style="margin-top:8px">Vorschläge aus FTP und Schwimmschwelle (Faustwerte). Eigene Ziele in die Beschreibung des Rennens schreiben, z. B. „Schwimmen 40:00“, „Rad 215 W“ oder „Rad 3:00:00“, „Lauf 1:55:00“.</div></div>`;
}

/* ---------- 5 · Erholung ---------- */
const WELLNESS = [
  { key: "fatigue", label: "Ermüdung" }, { key: "soreness", label: "Muskelkater" }, { key: "sleepQuality", label: "Schlafqualität" },
];
function renderWellness(d) {
  if (!d.sources.intervalsWellness.ok) { $("wellness-heat").innerHTML = '<div class="muted">Wellness nicht abrufbar.</div>'; return; }
  const days = dayRange(addDays(d.today, -(HISTORY_DAYS - 1)), d.today);
  const rows = WELLNESS.map((m) => {
    const values = Object.fromEntries(d.wellness.map((w) => [w.date, w[m.key]]));
    return { label: m.label, values, max: Math.max(2, ...Object.values(values).filter((v) => v != null)) };
  });
  C.wellnessHeat($("wellness-heat"), days, rows);
  const SA = sleepAccount(d);
  $("sleep-account").innerHTML = `<div class="big">${SA.avg != null ? fmt(SA.avg, 1) : "–"} <small class="muted" style="font-size:.9rem">h Ø akut von ${fmt(SA.target, 1)} h</small></div><div><span class="badge ${SA.cls}">${SA.text}</span></div>
    ${SA.nights.map((n, i) => `<div class="row"><span>${i === 0 ? "Letzte Nacht (auf heute)" : "Nacht davor"} · ${weekday(n.date)} ${fmtDate(n.date)}</span><b>${n.h != null ? fmt(n.h, 1) + " h" : "nicht erfasst"}</b></div>`).join("")}
    <div class="row"><span>7 Nächte (${SA.week.n}/7 erfasst)</span><b>${SA.week.avg != null ? "Ø " + fmt(SA.week.avg, 1) + " h · Defizit " + fmt(SA.week.debt, 1) + " h" : "–"}</b></div>
    <div><span class="badge ${SA.week.cls}">${SA.week.text}</span></div>
    <div class="muted">Akut: letzte Nacht zählt ${Math.round(SLEEP_ACUTE_WEIGHT * 100)} %, die Nacht davor ${Math.round((1 - SLEEP_ACUTE_WEIGHT) * 100)} %. Die Woche zeigt die angesammelte Schlafschuld. Richtwert ${fmt(SLEEP_TARGET_H, 1)} h pro Nacht, kein persönlich kalibrierter Wert.</div>`;
  // Band = mittlere 50 % der eigenen Werte im Zeitraum (wie das graue Band der Bereitschaftskarte); letzter Wert als Zahl
  const quart = (arr, q) => { const a = [...arr].sort((x, y) => x - y), i = (a.length - 1) * q, lo = Math.floor(i); return a[lo] + (a[Math.min(a.length - 1, lo + 1)] - a[lo]) * (i - lo); };
  for (const [id, key, unit, label, dec] of [["rhr", "restingHR", "bpm", "Ruhepuls", 0], ["sleep", "sleepHours", "h", "Schlafdauer", 1], ["hrv", "hrv", "ms", "HRV", 0]]) {
    const vals = Object.fromEntries(d.wellness.map((w) => [w.date, w[key]]));
    const have = days.filter((x) => vals[x] != null);
    if (!have.length) { $(id).innerHTML = `<div class="muted">Keine ${label}-Werte im Zeitraum.</div>`; continue; }
    const lastDay = have[have.length - 1], nums = have.map((x) => vals[x]), q1 = quart(nums, 0.25), q3 = quart(nums, 0.75);
    C.gapLine($(id), days, vals, { unit, label, dec, last: { date: lastDay, value: vals[lastDay] }, band: nums.length >= 5 ? { lo: q1, hi: q3, label: "üblicher Bereich", tip: `Mittlere 50 % deiner Werte: ${fmt(q1, dec)}–${fmt(q3, dec)} ${unit}` } : null });
    $(id).insertAdjacentHTML("afterbegin", `<div><b style="font-size:1.4rem">${fmt(vals[lastDay], dec)}</b> <span class="muted">${unit} · ${lastDay === d.today ? "heute" : fmtDate(lastDay)}${nums.length >= 5 ? ` · üblich ${fmt(q1, dec)}${fmt(q1, dec) === fmt(q3, dec) ? "" : "–" + fmt(q3, dec)}` : ""}</span></div>`);
  }
}

/* Gewichtstrend im Ernährungsteil: Kurve mit 7-Tage-Schnitt, daneben die Kalorien gegen das Ziel über denselben Zeitraum.
   Beides steht nebeneinander, ohne Umrechnung: Das Yazio-Ziel ist kein Verbrauch, die Bilanz gegen das Ziel ist deshalb
   kein Energiedefizit im strengen Sinn. */
const WEIGHT_WINDOW = 28;
function renderWeight(d) {
  const wv = d.wellness.filter((w) => w.weight != null);
  const card = $("weight-card");
  if (wv.length < 2) { card.hidden = true; return; }
  card.hidden = false;
  const days = dayRange(addDays(d.today, -(HISTORY_DAYS - 1)), d.today);
  const raw = Object.fromEntries(wv.map((w) => [w.date, w.weight]));
  const avg7 = {};
  for (const x of days) { const win = dayRange(addDays(x, -6), x).map((y) => raw[y]).filter((v) => v != null); if (raw[x] != null && win.length) avg7[x] = win.reduce((a, b) => a + b, 0) / win.length; }
  const lastDay = [...days].reverse().find((x) => avg7[x] != null);
  C.gapLine($("weight"), days, raw, { unit: "kg", label: "Gewicht", dec: 1, line2: { values: avg7, color: "var(--text)" }, last: { date: lastDay, value: avg7[lastDay] } });
  const from = addDays(d.today, -WEIGHT_WINDOW), before = [...days].find((x) => x >= from && avg7[x] != null);
  const dKg = before && before !== lastDay ? avg7[lastDay] - avg7[before] : null;
  const m = (k) => Object.fromEntries(d.wellness.map((w) => [w.date, w[k]]));
  const kc = m("calories"), kg = m("calorieGoal");
  const both = dayRange(from, addDays(d.today, -1)).filter((x) => kc[x] != null && kg[x] != null);
  const bal = both.length ? both.reduce((n, x) => n + kc[x] - kg[x], 0) : null;
  const sgn = (v, dec = 0) => `${v > 0 ? "+" : v < 0 ? "−" : ""}${fmt(Math.abs(v), dec)}`;
  $("weight").insertAdjacentHTML("afterbegin", `<div class="wt-top"><div><div class="muted">7-Tage-Schnitt</div><b style="font-size:1.4rem">${fmt(avg7[lastDay], 1)} kg</b></div>
    <div><div class="muted">Gewicht, ${dKg != null ? `seit ${fmtDate(before)}` : "Verlauf"}</div><b style="font-size:1.4rem">${dKg != null ? sgn(dKg, 1) + " kg" : "–"}</b></div>
    <div><div class="muted">Kalorien gegen Ziel${both.length ? `, ${both.length} Tage` : ""}</div><b style="font-size:1.4rem">${bal != null ? sgn(bal) + " kcal" : "–"}</b></div></div>`);
  $("weight").insertAdjacentHTML("beforeend", '<div class="muted">Dünne Linie = Tageswert, dicke Linie = 7-Tage-Schnitt. Kalorien gegen das Yazio-Ziel nur an Tagen mit Eintrag und ohne heute; das Ziel ist kein Verbrauch, die Zahl also keine exakte Energiebilanz.</div>');
}

/* ---------- 6 · Ernährung ---------- */
function renderNutrition(d) {
  const days = dayRange(addDays(d.today, -13), d.today);
  const map = (key) => Object.fromEntries(d.wellness.map((w) => [w.date, w[key]]));
  const kcal = map("calories"), any = days.some((x) => kcal[x] != null || map("carbs")[x] != null);
  if (!d.sources.intervalsWellness.ok) $("nutrition").innerHTML = '<div class="muted">Wellness nicht abrufbar.</div>';
  else if (!any) {
    $("nutrition").innerHTML = '<div class="notice">Noch keine Ernährungsdaten aus Yazio – die Werte erscheinen, sobald das Tagebuch geführt und synchronisiert wird. Bis dahin zeige ich bewusst nichts an, nicht 0 kcal.</div>';
  } else {
    const training = Object.fromEntries(d.daily.map((x) => [x.date, x.load > 0]));
    $("nutrition").innerHTML = '<div class="card" id="n-today" style="margin-bottom:12px"></div><div class="card" id="n-week" style="margin-bottom:12px"></div><div class="grid half"><div class="card"><h3>Kalorien gegen Ziel</h3><div id="n-kcal"></div></div><div class="card"><h3>Eiweiß</h3><div id="n-prot"></div></div><div class="card"><h3>Kohlenhydrate</h3><div id="n-carbs"></div></div><div class="card"><h3>Fett</h3><div id="n-fat"></div></div></div>';
    renderNutritionToday(d, map);
    renderNutritionWeek(d, map);
    const ng = d.nutritionGoals ?? {};
    // Eine einheitliche Zielmarke an jedem Tag: Yazio-Tagesziel für Makros (gilt täglich gleich), Kalorien je Tag inkl. Training
    const everyDay = (v) => (v != null ? Object.fromEntries(days.map((x) => [x, v])) : undefined);
    const kcalGoal = Object.fromEntries(days.map((x) => [x, map("calorieGoal")[x] ?? ng.kcal ?? null]).filter(([, v]) => v != null));
    // Tage vor dem ersten Eintrag fressen sonst die halbe Breite: Diagramm beginnt am ersten Tag mit Daten (mindestens 7 Tage)
    const firstIdx = days.findIndex((x) => ["calories", "protein", "carbs", "fat"].some((k) => map(k)[x] != null));
    const shown = days.slice(Math.min(Math.max(firstIdx, 0), days.length - 7));
    const legend = "Marke = Ziel · gelb = drüber · Punkt = Training · schraffiert = leer";
    for (const [id, vals, goal, unit, label, color] of [["n-kcal", kcal, kcalGoal, "kcal", "Kalorien je Tag", "var(--n-kcal)"], ["n-prot", map("protein"), everyDay(ng.proteinG), "g", "Eiweiß je Tag", "var(--n-prot)"], ["n-carbs", map("carbs"), everyDay(ng.carbsG), "g", "Kohlenhydrate je Tag", "var(--n-carb)"], ["n-fat", map("fat"), everyDay(ng.fatG), "g", "Fett je Tag", "var(--n-fat)"]])
      C.nutrition($(id), shown, { values: vals, goal, training, unit, label, legend, color });
  }
  renderWeight(d);

  const all = d.cravings, usable = all.filter((c) => c.hour != null && c.strength != null), unreadable = all.length - usable.length;
  if (!all.length) return; // Panel bleibt ausgeblendet, solange keine Einträge gepflegt werden
  $("cravings-card").hidden = false;
  const maxS = Math.max(5, ...usable.map((c) => c.strength));
  const triggers = {};
  for (const c of all) if (c.trigger) triggers[c.trigger.toLowerCase()] = (triggers[c.trigger.toLowerCase()] ?? 0) + 1;
  const topTrig = Object.entries(triggers).sort((a, b) => b[1] - a[1])[0];
  const recent = [...all].reverse().slice(0, 6);
  $("cravings-list").innerHTML = `<div class="muted">${all.length} Einträge in 8 Wochen${topTrig ? ` · häufigster Auslöser: ${esc(topTrig[0])} (${topTrig[1]}×)` : ""}${unreadable ? ` · ${unreadable} ohne lesbare Uhrzeit oder Stärke (nicht im Diagramm)` : ""}</div>
    <ul class="runs">${recent.map((c) => `<li><b>${fmtDate(c.date)} ${c.time ?? "–"}</b> · Stärke ${c.strength ?? "–"}${c.what ? " · " + esc(c.what) : ""}${c.before ? ` · davor ${esc(c.before)}` : ""}${c.trigger ? ` · Auslöser ${esc(c.trigger)}` : ""}</li>`).join("")}</ul>`;
  if (usable.length) C.cravings($("cravings-chart"), usable, maxS);
}

/* Fortschritt heute: gegessen gegen Tagesziel. Kalorienziel = Yazio-Ziel plus Trainingsverbrauch (siehe sync.js),
   Makro-Ziele direkt aus Yazio; ohne Ziel nur der Wert, kein Balken. */
const NUT_DAY = { from: 6, to: 22 }; // Essenszeit für "Soll bis jetzt": linear von 6 bis 22 Uhr, grobe Orientierung
function renderNutritionToday(d, map) {
  const t = d.today, g = d.nutritionGoals ?? {};
  const rows = [
    ["Kalorien", map("calories")[t], map("calorieGoal")[t] ?? g.kcal ?? null, "kcal", "var(--n-kcal)"],
    ["Eiweiß", map("protein")[t], g.proteinG ?? null, "g", "var(--n-prot)"],
    ["Kohlenhydrate", map("carbs")[t], g.carbsG ?? null, "g", "var(--n-carb)"],
    ["Fett", map("fat")[t], g.fatG ?? null, "g", "var(--n-fat)"],
  ];
  const now = new Date(), hour = now.getHours() + now.getMinutes() / 60;
  const frac = Math.min(1, Math.max(0, (hour - NUT_DAY.from) / (NUT_DAY.to - NUT_DAY.from)));
  // Balken reicht bis zum größeren von Wert und Ziel; Überschreitung als gelbes Segment mit Betrag im Balken
  const bar = ([label, v, goal, unit, color]) => {
    const over = v != null && goal && v > goal, full = Math.max(v ?? 0, goal ?? 0, 1);
    const w = (x) => `${(x / full) * 100}%`;
    const soll = goal ? goal * frac : null;
    const txt = v != null && goal ? (over ? `+${fmt(v - goal)} drüber` : `noch ${fmt(Math.max(0, goal - v))}`) : "";
    const gap = soll != null && v != null && !over ? (v < soll - goal * 0.1 ? "hinter dem Soll" : v > soll + goal * 0.1 ? "vor dem Soll" : "im Soll") : "";
    return `<div class="nbar"><div class="nhead"><b>${label}</b><span>${v != null ? fmt(v) : "–"}${goal ? ` von ${fmt(goal)}` : ""} ${unit}${txt ? ` · ${txt}` : ""}</span></div>
      <div class="ntrack">${goal && v != null ? `<div class="nfill" style="width:${w(Math.min(v, goal))};background:${color}"></div>${over ? `<div class="nover" style="left:${w(goal)};width:${w(v - goal)}" title="+${fmt(v - goal)} ${unit} über dem Ziel">+${fmt(v - goal)} ${unit}</div>` : ""}` : v != null ? `<div class="nfill" style="width:${w(v)};background:${color}"></div>` : ""}
        ${goal ? `<i class="ngoal" style="left:${w(goal)}" title="Tagesziel ${fmt(goal)} ${unit}"></i><i class="nsoll" style="left:${w(soll)}" title="Soll bis jetzt: ${fmt(soll)} ${unit}"></i>` : ""}</div>
      ${soll != null ? `<div class="muted">Soll bis jetzt ${fmt(soll)} ${unit}${gap ? ` · ${gap}` : ""}</div>` : ""}</div>`;
  };
  $("n-today").innerHTML = `<h3>Heute · ${weekday(t)} ${fmtDate(t)}</h3>${rows.map(bar).join("")}<div class="muted">Stand des letzten Syncs (alle 15 Minuten, Yazio). Schwarze Marke = Tagesziel, gestrichelte Marke = Soll bis jetzt (linear von ${NUT_DAY.from} bis ${NUT_DAY.to} Uhr, grobe Orientierung).</div>`;
}

/* Übersicht der letzten 7 Tage: Tageswerte gegen Ziel plus Auffälligkeiten. Heute läuft noch und
   zählt nicht in Auffälligkeiten und Schnitt. Schwellen sind grobe Faustwerte, kein Ernährungsplan. */
const NUT = { over: 1.1, under: 0.75, proteinShare: 0.2 };
function renderNutritionWeek(d, map) {
  const days = dayRange(addDays(d.today, -6), d.today);
  const kcal = map("calories"), goal = map("calorieGoal"), prot = map("protein"), carbs = map("carbs"), fat = map("fat");
  const cravByDay = {}, miss = [], over = [], under = [], lowProt = [], ng = d.nutritionGoals ?? {};
  for (const c of d.cravings) (cravByDay[c.date] ??= []).push(c);
  const hasCrav = d.cravings.length > 0; // HH-Spalte nur, wenn überhaupt Einträge gepflegt werden
  const rows = days.map((x) => {
    const k = kcal[x], g = goal[x], isToday = x === d.today;
    const diff = k != null && g != null ? k - g : null;
    const share = k ? ((prot[x] ?? 0) * 4) / k : null;
    let cls = "", flag = "";
    if (isToday) flag = k != null ? "läuft noch" : "";
    else if (k == null) { flag = "kein Tagebuch"; cls = "warn"; miss.push(x); }
    else {
      if (g != null && k > g * NUT.over) { flag = "über Ziel"; cls = "bad"; over.push({ x, by: k - g }); }
      else if (g != null && k < g * NUT.under) { flag = "deutlich drunter"; cls = "warn"; under.push({ x, by: g - k }); }
      else if (g != null) { flag = "im Rahmen"; cls = "ok"; }
      if (share != null && share < NUT.proteinShare) lowProt.push({ x, share });
    }
    const cr = cravByDay[x]?.length ?? 0;
    return `<tr><td>${weekday(x)} ${fmtDate(x)}</td><td>${k != null ? fmt(k) : "–"}</td><td>${g != null ? fmt(g) : "–"}</td><td>${diff != null ? (diff > 0 ? "+" : "") + fmt(diff) : "–"}</td><td>${prot[x] != null ? fmt(prot[x]) : "–"}</td><td>${carbs[x] != null ? fmt(carbs[x]) : "–"}</td><td>${fat[x] != null ? fmt(fat[x]) : "–"}</td>${hasCrav ? `<td>${cr ? cr + "×" : ""}</td>` : ""}<td>${flag ? `<span class="badge ${cls}">${flag}</span>` : ""}</td></tr>`;
  });
  const done = days.filter((x) => x !== d.today && kcal[x] != null);
  const past = days.length - 1;
  const avg = (m) => { const v = done.map((x) => m[x]).filter((v) => v != null); return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; };
  const gDone = done.filter((x) => goal[x] != null);
  const bal = gDone.length ? gDone.reduce((n, x) => n + kcal[x] - goal[x], 0) : null;
  const lateCrav = days.flatMap((x) => cravByDay[x] ?? []).filter((c) => c.hour != null && c.hour >= 20).length;
  const total = days.reduce((n, x) => n + (cravByDay[x]?.length ?? 0), 0);
  // Muster statt Einzeltage: eine Zeile je Art von Auffälligkeit mit Anzahl, Tagen und Durchschnitt
  const dl = (xs) => xs.map((e) => weekday(e.x ?? e)).join(", ");
  const mean = (xs, key) => xs.reduce((n, e) => n + e[key], 0) / xs.length;
  const findings = [];
  if (miss.length) findings.push({ sev: miss.length * 2 >= past ? 2 : 1, n: miss.length, text: `Kein Tagebuch an ${miss.length} von ${past} Tagen (${dl(miss)}).`, act: "Ohne Einträge sind Schwankungen nicht sichtbar – auch grobe Einträge genügen." });
  if (under.length) {
    const next = under.filter((e) => (cravByDay[addDays(e.x, 1)] ?? []).length).length;
    findings.push({ sev: 2, n: under.length, text: `Deutlich unter dem Kalorienziel an ${under.length} von ${done.length} Tagen (${dl(under)}), im Schnitt ${fmt(mean(under, "by"))} kcal${next ? `; am Folgetag gab es ${next}× Heißhunger` : ""}.`, act: "Ein starkes Defizit begünstigt Heißhunger: Kalorien über den Tag verteilen und abends nicht nachholen müssen." });
  }
  if (over.length) findings.push({ sev: over.length >= 2 ? 2 : 1, n: over.length, text: `Über dem Kalorienziel an ${over.length} von ${done.length} Tagen (${dl(over)}), im Schnitt +${fmt(mean(over, "by"))} kcal.`, act: "Den größten Posten des Tages (meist abends) kleiner planen, statt überall ein bisschen zu sparen." });
  if (lowProt.length) findings.push({ sev: lowProt.length * 2 >= done.length ? 2 : 1, n: lowProt.length, text: `Eiweiß an ${lowProt.length} von ${done.length} Tagen unter ${NUT.proteinShare * 100} % der Kalorien (Schnitt ${fmt(mean(lowProt, "share") * 100)} %; ${dl(lowProt)}).`, act: `Eiweiß zu jeder Mahlzeit einplanen${ng.proteinG ? ` (Tagesziel ${fmt(ng.proteinG)} g)` : ""}, besonders zum Frühstück und an Trainingstagen.` });
  if (total) findings.push({ sev: lateCrav >= 2 ? 2 : 1, n: total, text: `Heißhunger ${total}× in 7 Tagen${lateCrav ? `, davon ${lateCrav}× ab 20 Uhr` : ""}.`, act: lateCrav ? "Das Abendessen sättigender planen (Eiweiß, Ballaststoffe) und tagsüber nicht zu knapp essen." : null });
  $("n-week").innerHTML = `<h3>Letzte 7 Tage</h3>
    <div style="overflow-x:auto"><table><thead><tr><th>Tag</th><th>kcal</th><th>Ziel</th><th>Diff</th><th>Eiweiß g${ng.proteinG ? `<small class="muted"> Ziel ${fmt(ng.proteinG)}</small>` : ""}</th><th>KH g${ng.carbsG ? `<small class="muted"> Ziel ${fmt(ng.carbsG)}</small>` : ""}</th><th>Fett g${ng.fatG ? `<small class="muted"> Ziel ${fmt(ng.fatG)}</small>` : ""}</th>${hasCrav ? "<th>HH</th>" : ""}<th></th></tr></thead><tbody>${rows.join("")}</tbody></table></div>
    <div class="muted" style="margin-top:8px">${done.length ? `Schnitt abgeschlossener Tage (${done.length}): ${fmt(avg(kcal))} kcal, ${avg(prot) != null ? fmt(avg(prot)) : "–"} g Eiweiß${bal != null ? ` · Bilanz gegen Ziel ${bal > 0 ? "+" : ""}${fmt(bal)} kcal` : ""}` : "Noch kein abgeschlossener Tag mit Daten."} · Heute zählt noch nicht mit.</div>
    ${findingsBox(findings, done.length ? "Nichts Auffälliges in den abgeschlossenen Tagen." : "", 8)}
    <div class="muted" style="margin-top:6px">Faustwerte: über Ziel = mehr als ${Math.round((NUT.over - 1) * 100)} % drüber, deutlich drunter = unter ${Math.round(NUT.under * 100)} % des Ziels, wenig Eiweiß = unter ${NUT.proteinShare * 100} % der Kalorien.${hasCrav ? " HH = Heißhunger-Einträge." : ""}</div>`;
}


/* ---------- Trainingsblock: Zeitleiste und Fortschritt der Ziele ---------- */
function renderBlock(SB) {
  if (!SB) return "";
  const stLabel = { ok: "im Soll", warn: "knapp", bad: "unter Soll", base: "Basis", none: "keine Daten" };
  const head = SB.status === "active" ? `Woche ${SB.weekNo}${SB.weeks ? ` von ${SB.weeks}` : ""}` : SB.status === "upcoming" ? `Start in ${SB.daysToStart} Tag${SB.daysToStart === 1 ? "" : "en"}` : "beendet";
  const timeline = SB.weeks ? `<div class="btl">${Array.from({ length: SB.weeks }, (_, i) => `<i class="${SB.status === "active" ? (i + 1 < SB.weekNo ? "past" : i + 1 === SB.weekNo ? "now" : "") : SB.status === "done" ? "past" : ""}" title="Woche ${i + 1}"></i>`).join("")}</div>` : "";
  const goalCard = (g) => {
    const val = g.value == null ? "–" : g.key === "strength" ? `${g.value}<small> von ${g.of} Wochen</small>` : `${fmt(g.value, g.key === "decoupling" ? 1 : 0)}<small> %</small>`;
    let vis = "";
    if (g.key === "decoupling") {
      const max = Math.max(g.max * 2, ...g.series.map((x) => x.value), 1);
      vis = g.series.length ? `<div class="bbars">${g.series.slice(-10).map((x) => `<div title="${fmtDate(x.date)}: ${fmt(x.value, 1)} %" style="height:${Math.max(6, (Math.max(0, x.value) / max) * 100)}%;background:var(--${x.value <= g.max ? "ok" : "warn"})"></div>`).join("")}<u style="bottom:${(g.max / max) * 100}%"></u></div>` : "";
    } else if (g.key === "strength") {
      vis = `<div class="bsq">${g.weeks.map((w) => `<i class="${w.done ? (w.hit ? "ok" : g.status === "base" ? "off" : "bad") : "open"}" title="ab ${fmtDate(w.start)}: ${w.count}× Kraft"></i>`).join("")}</div>`;
    } else if (g.key === "acwr") {
      vis = g.value != null ? `<div class="cbar"><div class="cfill" style="width:${Math.min(100, g.value)}%;background:var(--${g.status === "base" ? "muted" : g.status === "none" ? "muted" : g.status})"></div><i style="left:80%"></i></div>` : "";
    }
    return `<div class="bgoal"><div class="bg-top"><span class="bg-label" style="min-width:0">${esc(g.label)}</span><span class="badge ${g.status === "base" || g.status === "none" ? "none" : g.status}">${stLabel[g.status]}</span></div><div class="val">${val}</div>${vis}<div class="sub">Ziel: ${esc(g.target)}</div>${g.key === "strength" ? `<div class="sub">Jedes Kästchen = eine Woche, grün = Ziel erreicht</div>` : ""}<div class="sub">${[g.note, g.key === "strength" && !g.egym ? "ohne EGYM-Daten" : null, g.basis ? (g.status === "base" ? "Ausgangswert: " : "") + g.basis : null].filter(Boolean).map(esc).join(" · ")}</div></div>`;
  };
  const goals = (SB.goals ?? []).length ? `<div class="bgoals">${SB.goals.map(goalCard).join("")}</div>` : "";
  const text = [SB.goal, ...(SB.notes ?? [])].filter(Boolean);
  return `<div class="cockpit" id="block-card"><div class="tile block"><div class="bhead"><div><h3>Trainingsblock</h3><div class="val" style="font-size:1.25rem">${esc(SB.name)}</div></div><div class="bmeta"><span class="badge ok">${head}</span><div class="sub">${fmtDate(SB.start)}${SB.end ? ` – ${fmtDate(SB.end)}` : ""}${SB.daysLeft != null ? ` · noch ${SB.daysLeft} Tag${SB.daysLeft === 1 ? "" : "e"}` : ""}</div></div></div>${timeline}${goals}${text.length ? `<details class="bdet"><summary>Ziel und Checkpoint</summary>${text.map((t) => `<p class="sub">${esc(t)}</p>`).join("")}</details>` : ""}</div></div>`;
}

/* ---------- 6 · Kraft und Hüfte: nur sichtbar, wenn es etwas zu zeigen gibt ---------- */
function renderStrength(d) {
  const has = d.sources.intervalsActivities.ok && d.weeks.some((w) => w.bySport.strength.minutes > 0);
  if (has || d.hipFlags.length) $("kraft").hidden = false;
  if (!has) $("kp-min").innerHTML = '<div class="muted">Keine Kraft-Einheit aus Intervals in den letzten 8 Wochen.</div>';
  if (has) {
    $("strength-card").hidden = false;
    // Wochenzeilen statt Säulen: Balken gegen die Zielmarke (60 min), laufende Woche oben mit großer Zahl.
    const GOAL = 60, weeks = [...d.weeks].reverse(), cur = weeks[0];
    const max = Math.max(GOAL, ...weeks.map((w) => w.bySport.strength.minutes)) * 1.1;
    const done = d.weeks.filter((w) => w.complete && w.bySport.strength.minutes >= GOAL).length, full = d.weeks.filter((w) => w.complete).length;
    const row = (w) => { const m = w.bySport.strength.minutes, hit = m >= GOAL; return `<div style="display:grid;grid-template-columns:64px 1fr 56px;gap:8px;align-items:center;padding:3px 0;${w.complete ? "" : "opacity:.75"}"><span class="muted">${fmtDate(w.weekStart)}${w.complete ? "" : " ·&nbsp;jetzt"}</span><div class="cbar" style="height:12px"><div class="cfill" style="width:${(m / max) * 100}%;background:${hit ? "var(--ok)" : "var(--s-strength)"}"></div><i style="left:${(GOAL / max) * 100}%"></i></div><b style="text-align:right">${m ? fmt(m) + " min" : "–"}</b></div>`; };
    $("kp-min").innerHTML = `<div class="big">${fmt(cur.bySport.strength.minutes)} <small class="muted" style="font-size:.9rem">min diese Woche von ${GOAL}</small></div>
      <div class="muted" style="margin:2px 0 10px">Ziel erreicht in ${done} von ${full} abgeschlossenen Wochen · senkrechte Marke = ${GOAL} min</div>${weeks.map(row).join("")}`;
  }
  // Wochen mit mindestens zwei Kraft-Einheiten: kommt aus dem Trainingsblock (zählt EGYM mit, wenn vorhanden)
  const bg = d.seasonBlock?.goals?.find((x) => x.key === "strength");
  if (bg?.weeks?.length) {
    $("strength-card").hidden = false; $("kraft").hidden = false;
    $("kt-weeks").hidden = false;
    $("kp-weeks").innerHTML = `<div class="big">${bg.value ?? "–"} <small class="muted" style="font-size:.9rem">von ${bg.of} Wochen mit Ziel erreicht</small></div>
      <div class="muted" style="margin:2px 0 10px">Ziel: ${esc(bg.target)}${bg.egym ? "" : " · ohne EGYM-Daten gezählt"}</div>
      ${[...bg.weeks].reverse().map((w) => `<div style="display:grid;grid-template-columns:64px 1fr 64px;gap:8px;align-items:center;padding:3px 0;${w.done ? "" : "opacity:.75"}"><span class="muted">${fmtDate(w.start)}${w.done ? "" : " ·&nbsp;jetzt"}</span><div class="bsq">${Array.from({ length: Math.max(2, w.count) }, (_, i) => `<i class="${i < w.count ? (w.hit ? "ok" : "off") : "open"}"></i>`).join("")}</div><b style="text-align:right">${w.count}×</b></div>`).join("")}
      <div class="muted" style="margin-top:6px">Jedes Kästchen = eine Kraft-Einheit, grün = Wochenziel erreicht.</div>`;
  }
  if (!d.hipFlags.length) return; // Eine Textsuche ohne Treffer ist keine Aussage
  $("hip-card").hidden = false;
  const recent = d.hipFlags.filter((f) => f.date >= addDays(d.today, -14));
  const list = (arr) => `<ul class="runs">${arr.slice(0, 5).map((f) => `<li><b>${fmtDate(f.date)}</b> · ${esc(f.source)}: „…${esc(f.snippet)}…"</li>`).join("")}</ul>`;
  $("hip").innerHTML = recent.length
    ? `<div class="notice bad"><b>Hinweis auf Hüfte, Leiste oder Knie in den letzten 14 Tagen (${recent.length}×)</b> – Hüft-OP vor 2 Jahren, im Zweifel Belastung anpassen oder abklären.</div>${list(recent)}`
    : `<div class="muted">Keine Treffer in den letzten 14 Tagen. Ältere Treffer in 8 Wochen: ${d.hipFlags.length}.</div>${list(d.hipFlags)}`;
}

/* ---------- Kraft aus EGYM: eigene Abfrage, damit die Seite nicht auf EGYM warten muss ---------- */
const REGION_LABEL = { UPPER: "Oberkörper", CORE: "Rumpf", LOWER: "Beine" };
async function loadEgym(token, dash) {
  try {
    const r = await fetch("/api/widget?view=kraft", { headers: { Authorization: "Bearer " + token }, cache: "no-store" });
    if (!r.ok) return;
    const k = await r.json();
    if (k.configured === false) return; // ohne EGYM-Zugang bleibt die Karte weg
    const nf = (k.sourcesFailed || []).length, at = new Date(k.generatedAt).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
    setSource("egym", "EGYM", nf >= 3 ? "bad" : nf ? "warn" : "ok", nf >= 3 ? "nicht erreichbar" : nf ? "teilweise · " + at : "live · " + at, nf ? `Nicht erreichbar: ${(k.sourcesFailed || []).join(", ")}` : "EGYM wird beim Öffnen abgefragt.");
    renderEgym(k);
    refreshBlock(token, dash, k);
  } catch (e) { console.error("egym", e); setSource("egym", "EGYM", "bad", "Fehler", String(e?.message ?? e)); }
}

// Die Kraft-Ziele des Blocks lesen die EGYM-Tage aus dem Cache. War der beim Laden des Dashboards noch leer oder
// aelter als der gerade geholte Stand, wird die Block-Karte einmal mit frischen Daten neu gezeichnet.
async function refreshBlock(token, dash, k) {
  const g = dash?.seasonBlock?.goals?.find((x) => x.key === "strength");
  if (!g || !$("block-card")) return;
  if (g.egym && !(Date.parse(k.generatedAt) > (g.egymAt ?? 0) + 60000)) return;
  try {
    const r = await fetch("/api/dashboard", { headers: { Authorization: "Bearer " + token }, cache: "no-store" });
    if (r.ok && $("block-card")) $("block-card").outerHTML = renderBlock((await r.json()).seasonBlock);
  } catch (e) { console.error("block refresh", e); }
}

function renderEgym(k) {
  $("kraft").hidden = false;
  $("egym-card").hidden = false;
  const failed = k.sourcesFailed || [];
  if (failed.length >= 3) { $("egym").innerHTML = '<div class="notice bad"><b>EGYM nicht erreichbar.</b> Login oder Abruf ist fehlgeschlagen.</div>'; return; }
  const wk = k.week, goal = wk.goalSets, min = wk.minutes, hit = (wk.sets || 0) >= goal;
  const est = (src) => (src === "estimate" ? "≈" : "");
  const vol = (kg) => (kg >= 1000 ? fmt(kg / 1000, 1) + " t" : fmt(kg) + " kg");
  const max = Math.max(goal, ...k.weeks.map((w) => w.sets || 0)) * 1.1;
  const row = (w, last) => { const m = w.minutes, ok = (w.sets || 0) >= goal; return `<div style="display:grid;grid-template-columns:64px 1fr 128px;gap:8px;align-items:center;padding:3px 0;${last ? "" : "opacity:.85"}"><span class="muted">${fmtDate(w.start)}${last ? " ·&nbsp;jetzt" : ""}</span><div class="cbar" style="height:12px"><div class="cfill" style="width:${((w.sets || 0) / max) * 100}%;background:${ok ? "var(--ok)" : "var(--s-strength)"}${w.minutesSource === "estimate" ? ";opacity:.6" : ""}"></div><i style="left:${(goal / max) * 100}%"></i></div><span style="text-align:right;white-space:nowrap"><b>${w.sets ? w.sets + " Sätze" : "–"}</b><span class="muted" style="font-size:.8rem">${m ? " · " + est(w.minutesSource) + fmt(m) + " min" : ""}</span>${w.volumeKg ? `<span class="muted" style="font-size:.8rem"> · ${vol(w.volumeKg)}</span>` : ""}</span></div>`; };
  const stat = (v, l) => `<div><div style="font-weight:650;font-size:1.15rem">${v}</div><div class="muted" style="font-size:.8rem">${l}</div></div>`;
  const since = k.daysSince == null ? "–" : k.daysSince === 0 ? "heute" : `vor ${k.daysSince} T`;
  const p = k.progress, items = p ? p.items : [];
  const delta = (i) => (i.diffKg == null ? '<span class="muted">–</span>' : `<span class="badge ${i.diffKg > 0 ? "ok" : i.diffKg < 0 ? "bad" : "none"}">${i.diffKg > 0 ? "+" : i.diffKg < 0 ? "−" : ""}${fmt(Math.abs(i.diffKg), 1)} kg</span>`);
  const b = k.bioAge, rg = k.regions;
  const regionRows = ["UPPER", "CORE", "LOWER"].map((key) => ({ label: REGION_LABEL[key], age: b ? { UPPER: b.upper, CORE: b.core, LOWER: b.lower }[key] : null, share: rg ? rg[key] : null }));
  const worstAge = Math.max(...regionRows.map((r) => r.age ?? 0));
  const bio = b && b.muscle != null ? `<h3 style="margin-top:14px">Muskelalter <span style="color:var(--s-strength);font-size:1.6rem">${b.muscle}</span> <span class="muted" style="font-weight:400">Jahre${b.total != null ? ` · gesamt ${b.total}` : ""}</span></h3>
    ${regionRows.map((r) => `<div style="display:grid;grid-template-columns:90px 1fr 40px;gap:8px;align-items:center;padding:4px 0"><span>${r.label}</span><div class="cbar" style="height:10px">${r.age != null ? `<div class="cfill" style="width:${Math.min(100, (r.age / 60) * 100)}%;background:${r.age === worstAge ? "#e39460" : "var(--s-strength)"}"></div>` : ""}</div><b style="text-align:right">${r.age ?? "–"}</b></div>`).join("")}
    ${rg ? `<div class="muted" style="margin-top:2px">Volumenanteil 4 Wochen: ${regionRows.map((r) => `${r.label} ${r.share} %`).join(" · ")}</div>` : ""}` : "";
  const recs = k.records || [];
  const records = recs.length ? `<h3 style="margin-top:14px">Bestwerte diese Woche</h3><ul class="runs">${recs.map((r) => `<li><b>${esc(r.label)}</b> · ${fmt(r.kg, r.kg % 1 ? 1 : 0)} kg × ${r.reps} <span class="badge ok">+${fmt(r.diffKg, 1)} kg · +${fmt(r.pct, 1)} %</span><br><span class="muted">geschätzter 1RM ${fmt(r.e1rm, 1)} kg gegen ${fmt(r.prevE1rm, 1)} kg vorher (bester Satz nach Epley, höchstens 12 Wiederholungen angesetzt)</span></li>`).join("")}</ul>` : `<div class="muted" style="margin-top:12px">Noch kein Bestwert diese Woche: Ein Bestwert ist ein Satz mit höherem geschätzten 1RM als alle früheren Einheiten der letzten 12 Wochen am selben Gerät.</div>`;
  const vt = k.volumeTrend;
  // Karte "Krafttraining": EGYM als Umschalter "Sätze"; Muskelalter, Bestwerte und 1RM bleiben in der EGYM-Karte
  $("strength-card").hidden = false; $("kt-sets").hidden = false;
  $("kp-sets").innerHTML = `<div class="big">${wk.sets || 0} <small class="muted" style="font-size:.9rem">von ${goal} Sätzen diese Woche</small> ${hit ? '<span class="badge ok">Ziel erreicht</span>' : ""}</div><div class="cbar" style="height:12px;margin:6px 0"><div class="cfill" style="width:${Math.min(100, ((wk.sets || 0) / goal) * 100)}%;background:var(--s-strength)"></div></div><div class="muted">${wk.sessions} ${wk.sessions === 1 ? "Einheit" : "Einheiten"} diese Woche · ${min == null ? "–" : est(wk.minutesSource) + fmt(min)} min</div>
    <div class="muted" style="margin:2px 0 10px">Satz-Ziel erreicht in ${k.weeksHit.hit} von ${k.weeksHit.of} abgeschlossenen Wochen${k.streakWeeks ? ` · Serie ${k.streakWeeks} ${k.streakWeeks === 1 ? "Woche" : "Wochen"}` : ""} · senkrechte Marke = ${goal} Sätze${wk.goalSource === "default" ? " (Standardziel)" : ""}</div>
    ${k.weeks.slice().reverse().map((w, i) => row(w, i === 0)).join("")}
    <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:12px">${stat(wk.sessions, "Einheiten")}${stat(wk.sets || "–", "Sätze")}${stat(wk.volumeKg ? (wk.volumeKg >= 1000 ? fmt(wk.volumeKg / 1000, 1) + " t" : fmt(wk.volumeKg) + " kg") : "–", "Volumen")}${stat(since, "zuletzt")}</div>
    ${vt ? `<div class="muted" style="margin-top:8px">Volumen letzte Woche gegen die davor: ${vt.pct > 0 ? "+" : vt.pct < 0 ? "−" : ""}${fmt(Math.abs(vt.pct), 1)} % (${fmt(vt.lastKg)} kg gegen ${fmt(vt.prevKg)} kg)</div>` : ""}
    <div class="muted" style="margin-top:8px">„≈“ und blasse Balken: Dauer nur aus der Spanne der Übungen geschätzt, EGYM lieferte keine.</div>`;
  // Muskelalter mit Skala: Balken von 20 bis 60 Jahren, Gesamtalter als eigene Zeile
  const AGE = { lo: 20, hi: 60 }, ageW = (a) => `${Math.max(0, Math.min(100, ((a - AGE.lo) / (AGE.hi - AGE.lo)) * 100))}%`;
  const bio2 = b && b.muscle != null ? `<h3 style="margin-top:0">Muskelalter</h3><div class="big" style="color:var(--s-strength)">${b.muscle} <small class="muted" style="font-size:.9rem">Jahre${b.total != null ? ` · EGYM-Gesamtalter ${b.total}` : ""}</small></div>
    <div class="muted" style="margin-bottom:6px">Niedriger ist besser. Die Balken zeigen das Alter je Körperbereich auf einer Skala von ${AGE.lo} bis ${AGE.hi} Jahren; der rötliche Balken ist der älteste Bereich.${b.total != null ? " Das Gesamtalter ist ein eigener EGYM-Wert und wird hier nicht nachgerechnet." : ""}</div>
    ${regionRows.map((r) => `<div style="display:grid;grid-template-columns:90px 1fr 40px;gap:8px;align-items:center;padding:4px 0"><span>${r.label}</span><div class="cbar" style="height:10px">${r.age != null ? `<div class="cfill" style="width:${ageW(r.age)};background:${r.age === worstAge ? "#e39460" : "var(--s-strength)"}"></div>` : ""}</div><b style="text-align:right">${r.age ?? "–"}</b></div>`).join("")}
    <div class="agescale"><span>${AGE.lo}</span><span>${(AGE.lo + AGE.hi) / 2}</span><span>${AGE.hi} Jahre</span></div>
    ${rg ? `<div class="muted" style="margin-top:2px">Volumenanteil 4 Wochen: ${regionRows.map((r) => `${r.label} ${r.share} %`).join(" · ")}</div>` : ""}` : "";
  // Tabelle nur mit Spalten, die Inhalt haben (Vorher/Veränderung erst ab dem zweiten Test je Gerät)
  const cmp = items.some((i) => i.prevKg != null);
  $("egym").innerHTML = `${failed.length ? `<div class="notice bad" style="margin-bottom:8px">Teilweise nicht erreichbar: ${esc(failed.join(", "))}</div>` : ""}
    <div class="egym-cols"><div>${bio2}${records}</div><div>
    ${items.length ? `<h3>Fortschritt: 1RM je Gerät</h3><div style="overflow-x:auto"><table><thead><tr><th>Gerät</th><th>Bereich</th><th>1RM</th>${cmp ? "<th>Vorher</th><th>Veränderung</th>" : ""}<th>Test</th></tr></thead><tbody>${items.map((i) => `<tr><td>${esc(i.label)}</td><td class="muted">${esc(REGION_LABEL[i.region] || i.region || "–")}</td><td><b>${fmt(i.kg)} kg</b></td>${cmp ? `<td class="muted">${i.prevKg != null ? fmt(i.prevKg) + " kg" : "–"}</td><td>${delta(i)}</td>` : ""}<td class="muted">${fmtDate(i.at)}</td></tr>`).join("")}</tbody></table></div>
      ${cmp && p.improved + p.same + p.declined > 0 ? `<div class="muted" style="margin-top:6px">${p.improved} besser · ${p.same} gleich · ${p.declined} schwächer gegen den vorherigen Test.</div>` : '<div class="muted" style="margin-top:6px">Ein Vergleich (Vorher, Veränderung) erscheint, sobald ein Gerät mindestens zwei Tests hat.</div>'}` : ""}
    <div class="muted" style="margin-top:8px">Kraft-Einheit = Tag mit Satz-Übungen oder Geräten (Garmin-Tagesaktivität zählt nicht).</div></div></div>`;
}

/* ---------- Studien-Check der Woche ---------- */
function renderStudie(d) {
  const s = d.studie;
  $("studie-card").hidden = false; // Der Abschnitt bleibt sichtbar: Er kommt sonntags mit dem Coaching-Bericht
  if (!s) { $("studie").innerHTML = '<div class="muted">Noch kein Studien-Check diese Woche. Er kommt am Sonntag mit dem Coaching-Bericht.</div>'; return; }
  const link = s.sourceUrl ? ` · <a href="${esc(s.sourceUrl)}" target="_blank" rel="noopener noreferrer">Quelle öffnen</a>` : "";
  // Fazit nach oben: Der Absatz nennt die Konsequenz meist am Ende ("Konsequenz für Gerbert: …"); Methodik und Einschränkung
  // wandern dahinter in einen einklappbaren Teil. Ohne dieses Stichwort bleiben die ersten zwei Sätze oben.
  const text = String(s.text ?? "").trim();
  const m = text.match(/(?:^|\s)(Konsequenz(?:en)?(?: für [A-ZÄÖÜ][\wäöüß-]*)?\s*:)\s*([\s\S]*)$/i);
  let fazit, rest;
  if (m) { fazit = m[2].trim(); rest = text.slice(0, text.length - m[0].length).trim(); }
  else { const sent = text.split(/(?<=[.!?])\s+/); fazit = sent.slice(0, 2).join(" "); rest = sent.slice(2).join(" "); }
  const paras = (t) => t.split(/\n{2,}|(?<=[.!?])\s+(?=(?:Methodik|Methode|Einschränkung|Limitation|Studie|Stichprobe)\b)/).map((x) => x.trim()).filter(Boolean).map((x) => `<p>${esc(x)}</p>`).join("");
  $("studie").innerHTML = `${s.title ? `<h3>${esc(s.title)}</h3>` : ""}
    <div class="studie-fazit"><div class="muted">${m ? "Konsequenz für dich" : "Kurzfassung"}</div><div>${esc(fazit)}</div></div>
    ${rest ? `<details class="bdet" style="margin-top:8px"><summary>Methodik und Einschränkung</summary><div class="desc" style="color:var(--text)">${paras(rest)}</div></details>` : ""}
    <div class="muted" style="margin-top:8px">Quelle: ${esc(s.source)}${link}${s.week ? ` · Woche ${fmtDate(s.week)}` : ""} · übermittelt ${new Date(s.fetchedAt).toLocaleDateString("de-DE")}</div>`;
}

load();

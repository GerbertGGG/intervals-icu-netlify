"use strict";
/* Trainings-Dashboard: Laden der Daten vom Worker und Aufbau der Bereiche. Diagramme: charts.js. */
const $ = (id) => document.getElementById(id);
const { esc, fmt, fmtDate, weekday, fmtTime, pace: paceLabel, addDays, dayRange } = U;

const SPORT_LABEL = { run: "Laufen", bike: "Rad", swim: "Schwimmen", strength: "Kraft", other: "Sonstiges" };
const SPORT_ORDER = ["run", "bike", "swim", "strength", "other"];
const HISTORY_DAYS = 56;

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
  render(await r.json());
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
  const steps = [renderCockpit, renderReady, renderToday, renderHistory, renderSport, renderFitness, renderThresholds, renderWellness, renderNutrition, renderStrength, renderStudie];
  for (const step of steps) {
    try { step(d); } catch (e) { console.error(step.name, e); const n = document.createElement("div"); n.className = "notice bad"; n.textContent = `Bereich „${step.name.replace("render", "")}" konnte nicht gezeichnet werden: ${e.message}`; $("sources").append(n); }
  }
}

/* ---------- 1 · Heute ---------- */
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
    <div style="margin-top:8px">${esc([sleep, ...extra].join(" · "))}</div>
    <div class="ready-grid">${items.map((i) => `<div class="rrow"><span class="lbl">${i.label}</span><span class="strip" id="strip-${i.key}"></span><span class="badge ${i.cls}">${i.text}</span></div>`).join("")}</div>
    <div class="muted" style="margin-top:6px">Punkt = heute, graues Band = dein üblicher Bereich der letzten 8 Wochen (mind. 5 Einträge). Skala 1 = bestmöglich, links. Lücken zählen nie als „gut". Grenzen sind Standardwerte.</div></div>`;
  for (const i of items) C.strip($(`strip-${i.key}`), { label: i.label, max: i.max, band: i.band, value: i.v, cls: i.cls });
}

function renderToday(d) {
  const plan = d.planned.filter((p) => p.date === d.today), next = d.planned.find((p) => p.date > d.today);
  const meta = (p) => [p.durationMin && p.durationMin + " min", p.distanceKm && fmt(p.distanceKm, 1) + " km", p.load && "Load " + fmt(p.load)].filter(Boolean).join(" · ");
  const planHtml = plan.length
    ? plan.map((p) => `<div><b>${esc(p.name || "Einheit")}</b>${meta(p) ? `<div class="muted">${meta(p)}</div>` : ""}${p.description ? `<div class="desc">${esc(p.description)}</div>` : `<div class="muted">Kein Zweck/Beschreibung im Plan hinterlegt.</div>`}</div>`).join("<hr>")
    : `<div class="muted">Heute ist keine Einheit geplant.</div>${next ? `<div class="muted">Nächste: ${weekday(next.date)} ${fmtDate(next.date)} – ${esc(next.name || "Einheit")}</div>` : ""}`;
  $("today").innerHTML = `<div class="card"><h3>Geplante Einheit</h3>${plan.some((p) => p.steps?.length) ? '<div id="detail-workout" style="margin:6px 0;max-width:480px"></div>' : ""}${d.sources.intervalsEvents.ok ? planHtml : '<div class="muted">Plan nicht abrufbar.</div>'}</div>`;
  const withSteps = plan.find((p) => p.steps?.length);
  if (withSteps) C.workout($("detail-workout"), withSteps.steps);
}

/* ---------- Cockpit: alles Wichtige auf einen Blick ---------- */
function renderCockpit(d) {
  const S = d.summary, w = S.load, { ctl, atl, tsb, acwr, tsbCls, acwrCls } = w, g = d.goal;
  const TSB_BANDS = S.thresholds.tsb, ACWR = S.thresholds.acwr;
  const tsbText = { ok: "frisch", warn: "belastet", bad: "stark ermüdet", none: "keine Daten" }[tsbCls];
  const acwrText = { ok: "im Zielkorridor", warn: "unter Zielkorridor", bad: "über Zielkorridor", none: "keine Daten" }[acwrCls];
  const wellOk = d.sources.intervalsWellness.ok;
  const plan = d.planned.filter((p) => p.date === d.today), next = d.planned.find((p) => p.date > d.today);
  const meta = (p) => [p.durationMin && p.durationMin + " min", p.distanceKm && fmt(p.distanceKm, 1) + " km", p.load && "Load " + fmt(p.load)].filter(Boolean).join(" · ");
  const cur = d.weeks[d.weeks.length - 1];
  const days7 = dayRange(addDays(d.today, -13), d.today), map = (k) => Object.fromEntries(d.wellness.map((x) => [x.date, x[k]]));
  const last = (k) => { const m = map(k); for (let i = days7.length - 1; i >= 0; i--) if (m[days7[i]] != null) return { v: m[days7[i]], date: days7[i] }; return null; };
  const tile = (title, body, cls = "") => `<div class="tile ${cls}"><h3>${title}</h3>${body}</div>`;
  const goalPace = g.targetTimeSecs / 21.0975;

  // Training
  const shown = plan.find((p) => p.steps?.length) ?? plan[0];
  const todayTile = plan.length
    ? tile("Heute geplant", `<div class="today"><div><div class="val" style="font-size:1.1rem">${esc(plan[0].name || "Einheit")}${plan.length > 1 ? ` <small>+${plan.length - 1}</small>` : ""}</div><div class="sub">${meta(plan[0]) || "&nbsp;"}</div>${plan[0].description ? `<div class="sub clamp">${esc(plan[0].description)}</div>` : ""}</div>${shown.steps?.length ? '<div id="ck-workout" class="wprofile"></div>' : ""}</div>`, "span4")
    : tile("Heute geplant", `<div class="val" style="font-size:1.1rem">Ruhetag</div><div class="sub">${next ? `Nächste: ${weekday(next.date)} ${fmtDate(next.date)} – ${esc(next.name || "Einheit")}` : "keine Einheit geplant"}</div>`, "span4");
  const raceTile = tile(esc(g.name), `<div class="val">${g.daysToGo > 0 ? `${g.daysToGo} <small>Tag${g.daysToGo === 1 ? "" : "e"}</small>` : g.daysToGo === 0 ? "Heute!" : "vorbei"}</div><div id="ck-countdown"></div><div class="sub">${weekday(g.date)} ${fmtDate(g.date)} · Ziel ${fmtTime(g.targetTimeSecs)} (${paceLabel(goalPace)} min/km)</div>`, "span15");
  const tsbTile = tile("Frische (TSB)", `<div id="ck-tsb"></div><div><span class="badge ${tsbCls}">${tsbText}</span></div>`, "span15");
  const acwrTile = tile("Belastung (ACWR)", `<div id="ck-acwr"></div><div><span class="badge ${acwrCls}">${acwrText}</span> <span class="sub">Ziel ${fmt(ACWR.lo, 1)}–${fmt(ACWR.hi, 1)}</span></div>`, "span15");
  const wkParts = cur ? SPORT_ORDER.filter((k) => cur.bySport[k].load > 0 || cur.bySport[k].count > 0) : [];
  const wkHours = cur ? SPORT_ORDER.reduce((n, k) => n + cur.bySport[k].minutes, 0) / 60 : 0;
  const weekTile = tile("Diese Woche (alle Sportarten)", cur
    ? `<div class="val">${fmt(cur.load)} <small>Load${cur.plannedLoad != null ? ` von ${fmt(cur.plannedLoad)}` : ""}</small></div><div id="ck-wload"></div><div class="dots">${wkParts.map((k) => `<span><i style="background:var(--s-${k})"></i>${SPORT_LABEL[k]} ${fmt(cur.bySport[k].load)}</span>`).join("") || '<span class="sub">noch nichts trainiert</span>'}</div><div class="sub">${fmt(wkHours, 1)} h${cur.km ? ` · Laufen ${fmt(cur.km, 1)} km` : ""}</div>`
    : '<div class="sub">keine Daten</div>', "span15");
  // Wellness
  const R = S.readiness;
  const readyTile = wellOk ? tile("Bereit für Training?", `<div><span class="badge ${R.verdict.cls} big-badge">${R.verdict.text}</span> <span class="sub">${esc(R.verdict.sub)}</span></div><div class="dots">${R.items.map((i) => `<span title="${esc(i.text)}"><i class="${i.cls}"></i>${esc(i.label)}</span>`).join("")}</div>`, "span3") : tile("Bereit für Training?", '<div class="sub">Wellness nicht abrufbar.</div>', "span3");
  const sparkTile = (t, key, unit, dec) => { const l = last(key); return tile(t, `<div class="val">${l ? fmt(l.v, dec) : "–"} <small>${unit}${l && l.date !== d.today ? ` · ${fmtDate(l.date)}` : ""}</small></div><div id="ck-${key}"></div><div class="sub">letzte 14 Tage</div>`); };

  // Ernährung
  const kcal = map("calories"), goal = map("calorieGoal"), carbs = map("carbs"), prot = map("protein"), fat = map("fat");
  const nDay = [...days7].reverse().find((x) => kcal[x] != null || carbs[x] != null || prot[x] != null);
  const dayNote = nDay && nDay !== d.today ? " · " + fmtDate(nDay) : "";
  const nutTile = nDay && kcal[nDay] != null
    ? tile(`Kalorien${dayNote}`, `<div class="val">${fmt(kcal[nDay])} <small>kcal${goal[nDay] ? ` von ${fmt(goal[nDay])}` : ""}</small></div><div id="ck-kcal"></div><div class="sub">Tagesziel als schwarze Marke</div>`, "span3")
    : tile("Kalorien", '<div class="sub">Noch keine Yazio-Daten – erscheinen nach dem Sync.</div>', "span3");
  const macros = [["Eiweiß", prot[nDay], 4, "s-swim"], ["Kohlenhydrate", carbs[nDay], 4, "s-run"], ["Fett", fat[nDay], 9, "s-strength"]];
  const macroKcal = macros.reduce((n, m) => n + (m[1] ?? 0) * m[2], 0);
  const macroTile = tile(`Makros${dayNote}`, nDay && macroKcal > 0
    ? `<div class="val">${prot[nDay] != null ? fmt(prot[nDay]) : "–"} <small>g Eiweiß</small></div><div id="ck-macros"></div><div class="dots">${macros.map((m) => `<span><i style="background:var(--${m[3]})"></i>${m[0]}${m[1] != null ? ` ${fmt(m[1])} g (${fmt((m[1] * m[2] / macroKcal) * 100)} %)` : ""}</span>`).join("")}</div>`
    : '<div class="sub">keine Daten</div>', "span3");

  $("cockpit").innerHTML = `
    <div class="cockpit-group">Training</div>
    <div class="cockpit">${weekTile}${raceTile}${tsbTile}${acwrTile}${todayTile}</div>
    <div class="cockpit-group">Wellness</div>
    <div class="cockpit">${readyTile}${sparkTile("Schlaf", "sleepHours", "h", 1)}${sparkTile("HRV", "hrv", "ms", 0)}${sparkTile("Ruhepuls", "restingHR", "bpm", 0)}</div>
    <div class="cockpit-group">Ernährung</div>
    <div class="cockpit">${nutTile}${macroTile}</div>`;

  if (g.daysToGo >= 0) C.countdown($("ck-countdown"), { total: HISTORY_DAYS + g.daysToGo, elapsed: HISTORY_DAYS, raceLabel: `${fmtDate(g.date)} Rennen` });
  C.gauge($("ck-tsb"), { label: "TSB", min: -40, max: 30, value: tsb, ticks: [-40, -25, -10, 0, 15, 30], zones: [{ from: -40, to: TSB_BANDS.warn, cls: "bad" }, { from: TSB_BANDS.warn, to: TSB_BANDS.ok, cls: "warn" }, { from: TSB_BANDS.ok, to: 30, cls: "ok" }] });
  C.gauge($("ck-acwr"), { label: "ACWR", min: 0.4, max: 1.8, value: acwr, dec: 2, ticks: [0.4, 0.8, 1.0, 1.3, 1.8], tickLabel: (t) => fmt(t, 1), zones: [{ from: 0.4, to: ACWR.lo, cls: "warn" }, { from: ACWR.lo, to: ACWR.hi, cls: "ok" }, { from: ACWR.hi, to: 1.8, cls: "bad" }] });
  if (shown?.steps?.length && $("ck-workout")) C.workout($("ck-workout"), shown.steps);
  if (cur) C.meter($("ck-wload"), { label: "Wochenbelastung nach Sportart", segments: SPORT_ORDER.map((k) => ({ value: cur.bySport[k].load, color: `var(--s-${k})`, tip: `${SPORT_LABEL[k]}: ${fmt(cur.bySport[k].load)} Load` })), goal: cur.plannedLoad, max: Math.max(1, cur.load, cur.plannedLoad ?? 0) * 1.15 });
  for (const [key, unit, label, dec] of [["sleepHours", "h", "Schlaf", 1], ["hrv", "ms", "HRV", 0], ["restingHR", "bpm", "Ruhepuls", 0]]) C.spark($(`ck-${key}`), days7, map(key), { unit, label, dec });
  if (nDay && macroKcal > 0) C.meter($("ck-macros"), { label: "Makroverteilung", segments: macros.map((m) => ({ value: (m[1] ?? 0) * m[2], color: `var(--${m[3]})`, tip: `${m[0]}: ${fmt(m[1])} g` })), max: macroKcal });
  if (nDay && kcal[nDay] != null) C.meter($("ck-kcal"), { label: "Kalorien", value: kcal[nDay], goal: goal[nDay], max: Math.max(kcal[nDay], goal[nDay] ?? 0) * 1.15, unit: "kcal" });
}

/* ---------- 2 · Trainingsverlauf ---------- */
function renderHistory(d) {
  const items = (key, plan) => d.weeks.map((w) => ({ label: fmtDate(w.weekStart), tip: `Woche ab ${fmtDate(w.weekStart)}`, value: w[key], plan: w[plan], partial: !w.complete }));
  C.bars($("chart-km"), items("km", "plannedKm"), { unit: "km", label: "Laufumfang pro Woche", xLabel: "Woche ab (Montag) · helle Säule = laufende Woche" });
  C.bars($("chart-load"), items("load", "plannedLoad"), { unit: "Load", label: "Belastung pro Woche", xLabel: "Woche ab (Montag) · helle Säule = laufende Woche" });
  const noPlan = d.weeks.every((w) => w.plannedKm == null && w.plannedLoad == null);
  $("plan-note").textContent = noPlan ? "Für diesen Zeitraum liegen keine geplanten Workouts mit Distanz/Load vor – ein Plan-Marker wird deshalb nicht gezeigt." : "Plan-Marker nur, wenn geplante Workouts Distanz bzw. Load enthalten. Laufumfang zählt nur Läufe, Belastung alle Sportarten.";

  // Formkurve bis zum Renntag (nur wenn er in den nächsten 4 Wochen liegt)
  const TSB_BANDS = d.summary.thresholds.tsb;
  const ctl = {}, atl = {};
  for (const w of d.wellness) { ctl[w.date] = w.ctl; atl[w.date] = w.atl; }
  const first = addDays(d.today, -(HISTORY_DAYS - 1));
  const raceSoon = d.goal.daysToGo >= 0 && d.goal.daysToGo <= 28;
  const end = raceSoon ? d.goal.date : d.today;
  C.form($("form-chart"), {
    days: dayRange(first, end), ctl, atl,
    zones: [{ from: TSB_BANDS.ok, to: 200, cls: "ok" }, { from: TSB_BANDS.warn, to: TSB_BANDS.ok, cls: "warn" }, { from: -200, to: TSB_BANDS.warn, cls: "bad" }],
    marks: [{ day: d.today, label: "heute", color: "var(--muted)", anchor: raceSoon ? "end" : "middle" }, ...(raceSoon ? [{ day: d.goal.date, label: "Renntag", color: "var(--accent)", anchor: "start" }] : [])],
  });
  if (!d.wellness.some((w) => w.ctl != null)) $("form-chart").insertAdjacentHTML("beforeend", '<div class="notice">Keine CTL/ATL-Werte im Zeitraum.</div>');
  C.calendar($("calendar"), d.daily, d.today);
}

/* ---------- 2b · Wochenbericht nach Sportart ---------- */
function renderSport(d) {
  if (!d.sources.intervalsActivities.ok) { $("chart-sport").innerHTML = '<div class="muted">Aktivitäten nicht abrufbar.</div>'; $("week-report").innerHTML = ""; return; }
  const used = SPORT_ORDER.filter((k) => d.weeks.some((w) => w.bySport[k].load > 0));
  const weeks = d.weeks.map((w) => ({ weekStart: w.weekStart, label: fmtDate(w.weekStart), partial: !w.complete, by: Object.fromEntries(SPORT_ORDER.map((k) => [k, w.bySport[k].load])) }));
  C.stacked($("chart-sport"), weeks, used, "TSS pro Woche und Sportart");
  C.shares($("chart-share"), weeks, used, SPORT_LABEL);
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
  $("week-report").innerHTML = table(done, prev, "Letzte volle Woche") + '<div style="height:12px"></div>' + table(cur, done, "Laufende Woche");
}

/* ---------- 3 · Fitness ---------- */
function renderFitness(d) {
  const r = d.runalyze;
  // Decoupling: Säulen je langem Lauf
  if (!d.sources.intervalsActivities.ok) $("chart-dec").innerHTML = '<div class="muted">Aktivitäten nicht abrufbar.</div>';
  else if (!d.fitness.longRuns.length) $("chart-dec").innerHTML = '<div class="muted">Keine langen Läufe mit Decoupling-Wert im Zeitraum.</div>';
  else C.bars($("chart-dec"), d.fitness.longRuns.map((x) => ({ label: fmtDate(x.date), tip: `${fmtDate(x.date)} (${fmt(x.distanceKm, 1)} km)`, value: Math.max(0, x.decoupling) })), { unit: "Decoupling in %", label: "Decoupling je langem Lauf", valueLabels: true, dec: 1, ref: { value: 5, label: "5 % Orientierung" }, xLabel: "Datum des langen Laufs" });

  if (!r) {
    for (const id of ["vdot", "corridor", "records-chart"]) $(id).innerHTML = '<div class="muted">Noch kein Runalyze-Snapshot eingespielt – VDOT, Bestzeiten und Prognose fehlen.</div>';
    $("records-note").innerHTML = "";
    return;
  }
  const stale = (Date.now() - Date.parse(r.fetchedAt)) / 86400000 > 7;
  const staleHtml = stale ? '<div class="notice">Der Runalyze-Snapshot ist älter als 7 Tage – VDOT und Prognose können veraltet sein.</div>' : "";
  const goal = d.goal.targetTimeSecs, goalPace = goal / 21.0975;

  // VDOT und Zonen-Paces
  if (r.vdot == null) $("vdot").innerHTML = '<div class="muted">Kein VDOT im Runalyze-Snapshot – Paces fehlen.</div>';
  else {
    const toSec = (p) => { const [m, s] = p.replace("/km", "").split(":").map(Number); return m * 60 + s; };
    const zones = r.paces.map((p) => ({ label: p.label.replace(/ \(.\)$/, ""), sec: toSec(p.pace) })).sort((a, b) => a.sec - b.sec);
    $("vdot").innerHTML = `<div class="big">${fmt(r.vdot, 1)}</div><div class="muted">VDOT (effektive VO2max laut Runalyze), Stand ${new Date(r.fetchedAt).toLocaleDateString("de-DE")}</div><div id="vdot-ruler" style="margin-top:6px"></div>${staleHtml}<div class="muted">Runalyze liefert nur das VDOT, die Zonen-Paces sind daraus nach Daniels berechnet.</div>`;
    C.paceRuler($("vdot-ruler"), zones, goalPace);
  }

  // Zielkorridor Halbmarathon
  if (!r.hmEstimates.length) $("corridor").innerHTML = '<div class="muted">Keine Halbmarathon-Werte im Snapshot.</div>';
  else {
    $("corridor").innerHTML = '<div id="corridor-chart"></div><div class="muted">Gefüllte Punkte sind Rechnungen nach Daniels (VDOT-Modell) aus Bestzeiten bzw. dem VDOT, kein Halbmarathon-Ergebnis. Von kürzeren Distanzen hochgerechnet fallen sie tendenziell zu optimistisch aus. Der leere Punkt ist die Runalyze-Prognose.</div>';
    C.corridor($("corridor-chart"), { goal, estimates: r.hmEstimates });
  }

  // Bestzeiten und Prognosen als Pace
  const rows = r.rows.map((x) => ({ label: x.label, bestSeconds: x.bestSeconds, bestPace: x.bestSeconds != null ? x.bestSeconds / x.bestDistanceKm : null, progSeconds: x.prognosisSeconds, progPace: x.prognosisSeconds != null ? x.prognosisSeconds / x.distanceKm : null }));
  C.records($("records-chart"), rows, goalPace);
  const odd = r.rows.filter((x) => x.bestSeconds != null && Math.abs(x.bestDistanceKm - x.distanceKm) > 0.005).map((x) => `${x.label}: ${fmtTime(x.bestSeconds)} (${x.bestDate.split("-").reverse().join(".")}, gemessen ${fmt(x.bestDistanceKm, 2)} km)`);
  $("records-note").innerHTML = `${odd.length ? `<div class="muted">Bestzeit-Distanzen weichen leicht ab: ${odd.map(esc).join("; ")}.</div>` : ""}<div class="notice">Die Halbmarathon-Prognose ist eine reine Extrapolation aus dem Modell von Runalyze, kein Ergebnis eines Halbmarathon-Trainings oder -Rennens. Ohne Halbmarathon-Bestzeit gibt es dafür keinen Vergleichswert.</div>`;
}

/* ---------- 3b · Schwellen ---------- */
function renderThresholds(d) {
  const host = $("thresholds");
  if (!d.sources.intervalsSportSettings.ok) { host.innerHTML = '<div class="card muted">Sport-Einstellungen nicht abrufbar.</div>'; return; }
  const t = d.thresholds, val = (v, fn) => v != null ? `<b>${fn(v)}</b>` : '<span class="muted">nicht hinterlegt</span>';
  const card = (title, rows) => `<div class="card"><h3>${title}</h3>${rows.map(([l, v]) => `<div class="row"><span>${l}</span><span>${v}</span></div>`).join("")}</div>`;
  host.innerHTML =
    card("Laufen", [["Schwellenpace", val(t.run.thresholdPaceSecPerKm, (v) => paceLabel(v) + " min/km")], ["Schwellenpuls (LTHR)", val(t.run.lthr, (v) => fmt(v) + " bpm")], ["Maximalpuls", val(t.run.maxHr, (v) => fmt(v) + " bpm")]]) +
    card("Rad", [["FTP", val(t.bike.ftp, (v) => fmt(v) + " W")], ["FTP indoor", val(t.bike.indoorFtp, (v) => fmt(v) + " W")], ["Schwellenpuls (LTHR)", val(t.bike.lthr, (v) => fmt(v) + " bpm")], ["Maximalpuls", val(t.bike.maxHr, (v) => fmt(v) + " bpm")]]) +
    card("Schwimmen", [["Schwellenpace", val(t.swim.thresholdPaceSecPer100m, (v) => paceLabel(v) + " min/100 m")]]);
}

/* ---------- 4 · Wellness ---------- */
const WELLNESS = [
  { key: "mood", label: "Stimmung" }, { key: "motivation", label: "Motivation" }, { key: "fatigue", label: "Ermüdung" },
  { key: "soreness", label: "Muskelkater" }, { key: "sleepQuality", label: "Schlafqualität" },
];
function renderWellness(d) {
  if (!d.sources.intervalsWellness.ok) { $("wellness-heat").innerHTML = '<div class="muted">Wellness nicht abrufbar.</div>'; return; }
  const days = dayRange(addDays(d.today, -(HISTORY_DAYS - 1)), d.today);
  const rows = WELLNESS.map((m) => {
    const values = Object.fromEntries(d.wellness.map((w) => [w.date, w[m.key]]));
    return { label: m.label, values, max: Math.max(2, ...Object.values(values).filter((v) => v != null)) };
  });
  C.wellnessHeat($("wellness-heat"), days, rows);
  for (const [id, key, unit, label] of [["rhr", "restingHR", "bpm", "Ruhepuls"], ["sleep", "sleepHours", "h", "Schlafdauer"], ["hrv", "hrv", "ms", "HRV"]]) {
    const vals = Object.fromEntries(d.wellness.map((w) => [w.date, w[key]]));
    if (!Object.values(vals).some((v) => v != null)) $(id).innerHTML = `<div class="muted">Keine ${label}-Werte im Zeitraum.</div>`;
    else C.gapLine($(id), days, vals, { unit, label });
  }
}

/* ---------- 5 · Ernährung und Heißhunger ---------- */
function renderNutrition(d) {
  const days = dayRange(addDays(d.today, -13), d.today);
  const map = (key) => Object.fromEntries(d.wellness.map((w) => [w.date, w[key]]));
  const kcal = map("calories"), any = days.some((x) => kcal[x] != null || map("carbs")[x] != null);
  if (!d.sources.intervalsWellness.ok) $("nutrition").innerHTML = '<div class="muted">Wellness nicht abrufbar.</div>';
  else if (!any) {
    $("nutrition").innerHTML = '<div class="card" id="n-today" style="margin-bottom:12px"></div><div class="card" id="n-week" style="margin-bottom:12px"></div><div class="notice">Noch keine Ernährungsdaten aus Yazio – die Werte erscheinen, sobald das Tagebuch geführt und synchronisiert wird (Sync alle 15 Minuten). Bis dahin zeige ich bewusst nichts an, nicht 0 kcal.</div>';
    renderNutritionToday(d, map);
    renderNutritionWeek(d, map);
  } else {
    const training = Object.fromEntries(d.daily.map((x) => [x.date, x.load > 0]));
    $("nutrition").innerHTML = '<div class="card" id="n-today" style="margin-bottom:12px"></div><div class="card" id="n-week" style="margin-bottom:12px"></div><div class="grid"><div class="card"><h3>Kalorien gegen Ziel</h3><div id="n-kcal"></div></div><div class="card"><h3>Eiweiß</h3><div id="n-prot"></div></div><div class="card"><h3>Kohlenhydrate</h3><div id="n-carbs"></div></div><div class="card"><h3>Fett</h3><div id="n-fat"></div></div></div>';
    renderNutritionToday(d, map);
    renderNutritionWeek(d, map);
    const todayGoal = (v) => (v != null ? { [d.today]: v } : undefined);
    const ng = d.nutritionGoals ?? {};
    C.nutrition($("n-prot"), days, { values: map("protein"), goal: todayGoal(ng.proteinG), training, unit: "g", label: "Eiweiß je Tag", legend: "schwarze Marke = Ziel (nur heute) · Punkt = Trainingstag · schraffiert = keine Daten" });
    C.nutrition($("n-fat"), days, { values: map("fat"), goal: todayGoal(ng.fatG), training, unit: "g", label: "Fett je Tag", legend: "schwarze Marke = Ziel (nur heute) · Punkt = Trainingstag · schraffiert = keine Daten" });
    C.nutrition($("n-kcal"), days, { values: kcal, goal: map("calorieGoal"), training, unit: "kcal", label: "Kalorien je Tag", legend: "schwarze Marke = Tagesziel · Punkt = Trainingstag · schraffiert = keine Daten" });
    C.nutrition($("n-carbs"), days, { values: map("carbs"), goal: todayGoal(ng.carbsG), training, unit: "g", label: "Kohlenhydrate je Tag", legend: "schwarze Marke = Ziel (nur heute) · Punkt = Trainingstag · schraffiert = keine Daten" });
  }

  const all = d.cravings, usable = all.filter((c) => c.hour != null && c.strength != null), unreadable = all.length - usable.length;
  if (!all.length) { $("cravings-chart").innerHTML = '<div class="muted">Keine Heißhunger-Einträge (HH …) in den Wellness-Kommentaren der letzten 8 Wochen.</div>'; $("cravings-list").innerHTML = ""; return; }
  const maxS = Math.max(5, ...usable.map((c) => c.strength));
  const triggers = {};
  for (const c of all) if (c.trigger) triggers[c.trigger.toLowerCase()] = (triggers[c.trigger.toLowerCase()] ?? 0) + 1;
  const topTrig = Object.entries(triggers).sort((a, b) => b[1] - a[1])[0];
  const recent = [...all].reverse().slice(0, 6);
  $("cravings-list").innerHTML = `<div class="muted">${all.length} Einträge in 8 Wochen${topTrig ? ` · häufigster Auslöser: ${esc(topTrig[0])} (${topTrig[1]}×)` : ""}${unreadable ? ` · ${unreadable} ohne lesbare Uhrzeit oder Stärke (nicht im Diagramm)` : ""}</div>
    <ul class="runs">${recent.map((c) => `<li><b>${fmtDate(c.date)} ${c.time ?? "–"}</b> · Stärke ${c.strength ?? "–"}${c.what ? " · " + esc(c.what) : ""}${c.before ? ` · davor ${esc(c.before)}` : ""}${c.trigger ? ` · Auslöser ${esc(c.trigger)}` : ""}</li>`).join("")}</ul>`;
  if (usable.length) C.cravings($("cravings-chart"), usable, maxS); else $("cravings-chart").innerHTML = '<div class="muted">Keine Einträge mit lesbarer Uhrzeit und Stärke.</div>';
}

/* Fortschritt heute: gegessen gegen Tagesziel. Kalorienziel = Yazio-Ziel plus Trainingsverbrauch (siehe sync.js),
   Makro-Ziele direkt aus Yazio; ohne Ziel nur der Wert, kein Balken. */
function renderNutritionToday(d, map) {
  const t = d.today, g = d.nutritionGoals ?? {};
  const rows = [
    ["Kalorien", map("calories")[t], map("calorieGoal")[t] ?? g.kcal ?? null, "kcal", "var(--accent)"],
    ["Eiweiß", map("protein")[t], g.proteinG ?? null, "g", "var(--s-swim)"],
    ["Kohlenhydrate", map("carbs")[t], g.carbsG ?? null, "g", "var(--s-run)"],
    ["Fett", map("fat")[t], g.fatG ?? null, "g", "var(--s-strength)"],
  ];
  const bar = ([label, v, goal, unit, color]) => {
    const pct = v != null && goal ? Math.min(100, (v / goal) * 100) : 0;
    const over = v != null && goal && v > goal * 1.1;
    return `<div style="margin:8px 0"><div style="display:flex;justify-content:space-between;gap:8px"><b>${label}</b><span>${v != null ? fmt(v) : "–"}${goal ? ` von ${fmt(goal)}` : ""} ${unit}${v != null && goal ? ` · ${over ? "+" + fmt(v - goal) + " drüber" : "noch " + fmt(Math.max(0, goal - v))}` : ""}</span></div><div style="height:10px;border-radius:5px;background:var(--line);overflow:hidden"><div style="height:100%;width:${pct}%;background:${over ? "var(--bad)" : color}"></div></div></div>`;
  };
  $("n-today").innerHTML = `<h3>Heute · ${weekday(t)} ${fmtDate(t)}</h3>${rows.map(bar).join("")}<div class="muted">Stand des letzten Syncs (alle 15 Minuten, Yazio).</div>`;
}

/* Übersicht der letzten 7 Tage: Tageswerte gegen Ziel plus Auffälligkeiten. Heute läuft noch und
   zählt nicht in Auffälligkeiten und Schnitt. Schwellen sind grobe Faustwerte, kein Ernährungsplan. */
const NUT = { over: 1.1, under: 0.75, proteinShare: 0.2 };
function renderNutritionWeek(d, map) {
  const days = dayRange(addDays(d.today, -6), d.today);
  const kcal = map("calories"), goal = map("calorieGoal"), prot = map("protein"), carbs = map("carbs"), fat = map("fat");
  const cravByDay = {};
  for (const c of d.cravings) (cravByDay[c.date] ??= []).push(c);
  const findings = [];
  const rows = days.map((x) => {
    const k = kcal[x], g = goal[x], isToday = x === d.today;
    const diff = k != null && g != null ? k - g : null;
    const share = k ? ((prot[x] ?? 0) * 4) / k : null;
    let cls = "", flag = "";
    if (isToday) flag = k != null ? "läuft noch" : "";
    else if (k == null) { flag = "kein Tagebuch"; cls = "warn"; findings.push(`${fmtDate(x)}: nichts eingetragen – ohne Tagebuch sind Schwankungen nicht sichtbar.`); }
    else {
      if (g != null && k > g * NUT.over) { flag = "über Ziel"; cls = "bad"; findings.push(`${fmtDate(x)}: ${fmt(k - g)} kcal über dem Ziel (${fmt(k)} von ${fmt(g)}).`); }
      else if (g != null && k < g * NUT.under) { flag = "deutlich drunter"; cls = "warn"; findings.push(`${fmtDate(x)}: nur ${fmt(k)} von ${fmt(g)} kcal – starkes Defizit, das Heißhunger begünstigt${(cravByDay[addDays(x, 1)] ?? []).length ? " (am Folgetag gab es Heißhunger)" : ""}.`); }
      else if (g != null) { flag = "im Rahmen"; cls = "ok"; }
      if (share != null && share < NUT.proteinShare) findings.push(`${fmtDate(x)}: Eiweiß nur ${fmt(share * 100)} % der Kalorien (${fmt(prot[x] ?? 0)} g) – eher wenig.`);
    }
    const cr = cravByDay[x]?.length ?? 0;
    return `<tr><td>${weekday(x)} ${fmtDate(x)}</td><td>${k != null ? fmt(k) : "–"}</td><td>${g != null ? fmt(g) : "–"}</td><td>${diff != null ? (diff > 0 ? "+" : "") + fmt(diff) : "–"}</td><td>${prot[x] != null ? fmt(prot[x]) : "–"}</td><td>${carbs[x] != null ? fmt(carbs[x]) : "–"}</td><td>${fat[x] != null ? fmt(fat[x]) : "–"}</td><td>${cr ? cr + "×" : ""}</td><td>${flag ? `<span class="badge ${cls}">${flag}</span>` : ""}</td></tr>`;
  });
  const done = days.filter((x) => x !== d.today && kcal[x] != null);
  const avg = (m) => { const v = done.map((x) => m[x]).filter((v) => v != null); return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; };
  const gDone = done.filter((x) => goal[x] != null);
  const bal = gDone.length ? gDone.reduce((n, x) => n + kcal[x] - goal[x], 0) : null;
  const lateCrav = days.flatMap((x) => cravByDay[x] ?? []).filter((c) => c.hour != null && c.hour >= 20).length;
  const total = days.reduce((n, x) => n + (cravByDay[x]?.length ?? 0), 0);
  if (total) findings.push(`Heißhunger: ${total}× in 7 Tagen${lateCrav ? `, davon ${lateCrav}× ab 20 Uhr` : ""}.`);
  $("n-week").innerHTML = `<h3>Letzte 7 Tage</h3>
    <div style="overflow-x:auto"><table><thead><tr><th>Tag</th><th>kcal</th><th>Ziel</th><th>Diff</th><th>Eiweiß g</th><th>KH g</th><th>Fett g</th><th>HH</th><th></th></tr></thead><tbody>${rows.join("")}</tbody></table></div>
    <div class="muted" style="margin-top:8px">${done.length ? `Schnitt abgeschlossener Tage (${done.length}): ${fmt(avg(kcal))} kcal, ${avg(prot) != null ? fmt(avg(prot)) : "–"} g Eiweiß${bal != null ? ` · Bilanz gegen Ziel ${bal > 0 ? "+" : ""}${fmt(bal)} kcal` : ""}` : "Noch kein abgeschlossener Tag mit Daten."} · Heute zählt noch nicht mit.</div>
    ${findings.length ? `<div class="notice" style="margin-top:8px"><b>Was auffällt</b><ul class="runs">${findings.map((f) => `<li>${esc(f)}</li>`).join("")}</ul></div>` : done.length ? '<div class="notice ok" style="margin-top:8px">Nichts Auffälliges in den abgeschlossenen Tagen.</div>' : ""}
    <div class="muted" style="margin-top:6px">Faustwerte: über Ziel = mehr als ${Math.round((NUT.over - 1) * 100)} % drüber, deutlich drunter = unter ${Math.round(NUT.under * 100)} % des Ziels, wenig Eiweiß = unter ${NUT.proteinShare * 100} % der Kalorien. HH = Heißhunger-Einträge.</div>`;
}

/* ---------- 6 · Kraft und Hüfte ---------- */
function renderStrength(d) {
  if (!d.sources.intervalsActivities.ok) $("strength").innerHTML = '<div class="muted">Aktivitäten nicht abrufbar.</div>';
  else C.strength($("strength"), d.weeks.map((w) => ({ weekStart: w.weekStart, label: fmtDate(w.weekStart), minutes: w.bySport.strength.minutes, partial: !w.complete })));
  const recent = d.hipFlags.filter((f) => f.date >= addDays(d.today, -14));
  const list = (arr) => `<ul class="runs">${arr.slice(0, 5).map((f) => `<li><b>${fmtDate(f.date)}</b> · ${esc(f.source)}: „…${esc(f.snippet)}…"</li>`).join("")}</ul>`;
  if (recent.length) $("hip").innerHTML = `<div class="notice bad"><b>Hinweis auf Hüfte, Leiste oder Knie in den letzten 14 Tagen (${recent.length}×)</b> – Hüft-OP vor 2 Jahren, im Zweifel Belastung anpassen oder abklären.</div>${list(recent)}`;
  else if (d.hipFlags.length) $("hip").innerHTML = `<div class="muted">Keine Treffer in den letzten 14 Tagen. Ältere Treffer in 8 Wochen: ${d.hipFlags.length}.</div>${list(d.hipFlags)}`;
  else $("hip").innerHTML = '<div class="muted">Keine Treffer für Hüfte, Leiste oder Knie in Kommentaren und Einheiten der letzten 8 Wochen. Geprüft wird nur, was du schreibst; kein Treffer heißt nicht „beschwerdefrei".</div>';
}

/* ---------- 7 · Studien-Check ---------- */
function renderStudie(d) {
  const s = d.studie;
  if (!s) { $("studie").innerHTML = '<div class="muted">Noch kein Studien-Check übermittelt. Der Coaching-Task schickt den Sonntagsabschnitt samt Quelle an den Worker (PUT /api/studie).</div>'; return; }
  const link = s.sourceUrl ? ` · <a href="${esc(s.sourceUrl)}" target="_blank" rel="noopener noreferrer">Quelle öffnen</a>` : "";
  $("studie").innerHTML = `${s.title ? `<h3>${esc(s.title)}</h3>` : ""}<div class="desc" style="color:var(--text)">${esc(s.text)}</div>
    <div class="muted" style="margin-top:8px">Quelle: ${esc(s.source)}${link}${s.week ? ` · Woche ${fmtDate(s.week)}` : ""} · übermittelt ${new Date(s.fetchedAt).toLocaleDateString("de-DE")}</div>`;
}

load();

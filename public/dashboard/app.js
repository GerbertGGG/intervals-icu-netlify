"use strict";
/* Trainings-Dashboard: Laden der Daten vom Worker und Aufbau der Bereiche. Diagramme: charts.js. */
const $ = (id) => document.getElementById(id);
const { esc, fmt, fmtDate, weekday, fmtTime, pace: paceLabel, addDays, dayRange } = U;

const SPORT_LABEL = { run: "Laufen", bike: "Rad", swim: "Schwimmen", strength: "Kraft", other: "Sonstiges" };
const SPORT_ORDER = ["run", "bike", "swim", "strength", "other"];
const HISTORY_DAYS = 56;
const SLEEP_TARGET_H = 7.5; // Richtwert je Nacht, kein persönlich kalibrierter Wert
const isTaper = (g) => g.daysToGo >= 0 && g.daysToGo <= 14;
const hm = (secs) => (secs < 3600 ? fmtTime(secs) : fmtTime(Math.round(secs / 60) * 60).replace(/:00$/, "")); // ab 1 h als h:mm, darunter m:ss
// Schlafkonto: die zwei letzten Nächte bis heute (Schlaf eines Tages = Nacht auf diesen Morgen). Fehlende Nacht zählt nie als gut.
function sleepAccount(d) {
  const by = Object.fromEntries(d.wellness.map((w) => [w.date, w.sleepHours]));
  const nights = [d.today, addDays(d.today, -1)].map((date) => ({ date, h: by[date] ?? null }));
  const known = nights.filter((n) => n.h != null), sum = known.reduce((a, n) => a + n.h, 0);
  const target = SLEEP_TARGET_H * 2;
  const complete = known.length === 2;
  const cls = !known.length ? "none" : sum >= target - 0.5 ? "ok" : sum >= target - 2 ? "warn" : "bad";
  const text = !known.length ? "nicht erfasst" : complete ? (cls === "ok" ? "gut gefüllt" : cls === "warn" ? "leicht im Minus" : "im Minus") : "unvollständig";
  return { nights, sum, known: known.length, target, cls: complete || cls === "none" ? cls : cls === "ok" ? "warn" : cls, text };
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
  render(await r.json());
  loadEgym(token); // EGYM ist langsam und optional: die Karte kommt nach, die Seite wartet nicht darauf
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
  const steps = [renderCockpit, renderRennplan, renderReady, renderHistory, renderLongruns, renderSport, renderFitness, renderThresholds, renderWellness, renderNutrition, renderStrength, renderStudie];
  for (const step of steps) {
    try { step(d); } catch (e) { console.error(step.name, e); const n = document.createElement("div"); n.className = "notice bad"; n.textContent = `Bereich „${step.name.replace("render", "")}" konnte nicht gezeichnet werden: ${e.message}`; $("sources").append(n); }
  }
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
  const plan = d.planned.filter((p) => p.date === d.today), next = d.planned.find((p) => p.date > d.today);
  const meta = (p) => [p.durationMin && p.durationMin + " min", p.distanceKm && fmt(p.distanceKm, 1) + " km", p.load && "Load " + fmt(p.load)].filter(Boolean).join(" · ");
  const cur = d.weeks[d.weeks.length - 1];
  const days7 = dayRange(addDays(d.today, -13), d.today), map = (k) => Object.fromEntries(d.wellness.map((x) => [x.date, x[k]]));
  const last = (k) => { const m = map(k); for (let i = days7.length - 1; i >= 0; i--) if (m[days7[i]] != null) return { v: m[days7[i]], date: days7[i] }; return null; };
  const tile = (title, body, cls = "") => `<div class="tile ${cls}"><h3>${title}</h3>${body}</div>`;
  const goalPace = g.targetTimeSecs ? g.targetTimeSecs / (g.runKm ?? 21.0975) : null;

  // Training
  const shown = plan.find((p) => p.steps?.length) ?? plan[0];
  const todayTile = plan.length
    ? tile("Heute geplant", `<div class="today"><div><div class="val" style="font-size:1.1rem">${esc(plan[0].name || "Einheit")}${plan.length > 1 ? ` <small>+${plan.length - 1}</small>` : ""}</div><div class="sub">${meta(plan[0]) || "&nbsp;"}</div>${plan[0].description ? `<div class="sub clamp">${esc(plan[0].description)}</div>` : ""}</div>${shown.steps?.length ? '<div id="ck-workout" class="wprofile"></div>' : ""}</div>`, "span4")
    : tile("Heute geplant", `<div class="val" style="font-size:1.1rem">Ruhetag</div><div class="sub">${next ? `Nächste: ${weekday(next.date)} ${fmtDate(next.date)} – ${esc(next.name || "Einheit")}` : "keine Einheit geplant"}</div>`, "span4");
  const raceTile = tile(esc(g.name), `<div class="val">${g.daysToGo > 0 ? `${g.daysToGo} <small>Tag${g.daysToGo === 1 ? "" : "e"}</small>` : g.daysToGo === 0 ? "Heute!" : "vorbei"}</div><div id="ck-countdown"></div><div class="sub">${weekday(g.date)} ${fmtDate(g.date)} · ${g.triathlon ? `Schwimmen ${fmt(g.triathlon.swimKm, 2)} km · Rad ${fmt(g.triathlon.bikeKm)} km · Lauf ${fmt(g.triathlon.runKm, 1)} km${g.totalTargetSecs ? ` · Ziel ${fmtTime(g.totalTargetSecs)}` : ""}${goalPace ? ` · Lauf-Ziel ${paceLabel(goalPace)} min/km` : ""}` : `Ziel ${fmtTime(g.targetTimeSecs)} (${paceLabel(goalPace)} min/km)`}</div>`, "span15");
  const prevWk = d.weeks[d.weeks.length - 2];
  const taperTile = taper ? tile("Taper-Status", `<div class="val" style="font-size:1.1rem">Taper-Phase · noch ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "e"}</div><div class="sub">Weniger Umfang ist jetzt gewollt: Diese Woche ${cur ? fmt(cur.km, 1) : "–"} km${prevWk ? `, Vorwoche ${fmt(prevWk.km, 1)} km` : ""}. Niedriger ACWR und steigende Frische (TSB${tsb != null ? ` ${fmt(tsb)}` : ""}) gelten als Soll, nicht als Mangel.</div>`, "span4") : "";
  const tsbTile = tile("Frische (TSB)", `<div id="ck-tsb"></div><div><span class="badge ${tsbCls}">${tsbText}</span></div>`, "span15");
  const acwrTile = tile("Belastung (ACWR)", `<div id="ck-acwr"></div><div><span class="badge ${acwrShow}">${acwrText}</span> <span class="sub">${taper ? "Taper-Woche" : `Ziel ${fmt(ACWR.lo, 1)}–${fmt(ACWR.hi, 1)}`}</span></div>`, "span15");
  const wkParts = cur ? SPORT_ORDER.filter((k) => cur.bySport[k].load > 0 || cur.bySport[k].count > 0) : [];
  const wkHours = cur ? SPORT_ORDER.reduce((n, k) => n + cur.bySport[k].minutes, 0) / 60 : 0;
  const weekTile = tile("Diese Woche (alle Sportarten)", cur
    ? `<div class="val">${fmt(cur.load)} <small>Load${cur.plannedLoad != null ? ` von ${fmt(cur.plannedLoad)}` : ""}</small></div><div id="ck-wload"></div><div class="dots">${wkParts.map((k) => `<span><i style="background:var(--s-${k})"></i>${SPORT_LABEL[k]} ${fmt(cur.bySport[k].load)}</span>`).join("") || '<span class="sub">noch nichts trainiert</span>'}</div><div class="sub">${fmt(wkHours, 1)} h${cur.km ? ` · Laufen ${fmt(cur.km, 1)} km` : ""}</div>`
    : '<div class="sub">keine Daten</div>', "span15");
  // Wellness
  const R = S.readiness;
  const readyTile = wellOk ? tile("Bereit für Training?", `<div><span class="badge ${R.verdict.cls} big-badge">${R.verdict.text}</span> <span class="sub">${esc(R.verdict.sub)}</span></div>${R.verdict.reasons?.length ? `<div class="sub">${esc(R.verdict.reasons.join(" · "))}</div>` : ""}<div class="dots">${R.items.map((i) => `<span title="${esc(i.text)}"><i class="${i.cls}"></i>${esc(i.label)}</span>`).join("")}</div>`, "span3") : tile("Bereit für Training?", '<div class="sub">Wellness nicht abrufbar.</div>', "span3");
  const SA = sleepAccount(d);
  const sleepTile = tile("Schlafkonto 2 Nächte", `<div class="val">${SA.known ? fmt(SA.sum, 1) : "–"} <small>h von ${fmt(SA.target, 0)} h</small></div><div class="sub">${SA.nights.map((n) => `${weekday(n.date)} ${n.h != null ? fmt(n.h, 1) + " h" : "–"}`).join(" · ")}</div><div><span class="badge ${SA.cls}">${SA.text}</span></div>`);
  const sparkTile = (t, key, unit, dec) => { const l = last(key); return tile(t, `<div class="val">${l ? fmt(l.v, dec) : "–"} <small>${unit}${l && l.date !== d.today ? ` · ${fmtDate(l.date)}` : ""}</small></div><div id="ck-${key}"></div><div class="sub">letzte 14 Tage</div>`); };

  // Ernährung
  const kcal = map("calories"), goal = map("calorieGoal"), carbs = map("carbs"), prot = map("protein"), fat = map("fat");
  // Heute immer anzeigen: vor dem ersten Yazio-Eintrag leer (0), nicht der Stand von gestern.
  const lastData = [...days7].reverse().find((x) => kcal[x] != null || carbs[x] != null || prot[x] != null);
  const nDay = d.today;
  const hasToday = kcal[nDay] != null || carbs[nDay] != null || prot[nDay] != null;
  const kcalToday = kcal[nDay] ?? 0, kcalGoal = goal[nDay] ?? d.nutritionGoals?.kcal ?? null;
  const emptyNote = hasToday ? "" : `Heute noch nichts eingetragen${lastData ? ` (zuletzt ${fmtDate(lastData)}: ${fmt(kcal[lastData])} kcal)` : ""}`;
  const nutTile = lastData || d.nutritionGoals
    ? tile(`Kalorien · ${fmtDate(nDay)}`, `<div class="val">${fmt(kcalToday)} <small>kcal${kcalGoal ? ` von ${fmt(kcalGoal)}` : ""}</small></div><div id="ck-kcal"></div><div class="sub">${emptyNote || "Tagesziel als schwarze Marke"}</div>`, "span3")
    : tile("Kalorien", '<div class="sub">Noch keine Yazio-Daten – erscheinen nach dem Sync.</div>', "span3");
  const macros = [["Eiweiß", prot[nDay] ?? 0, 4, "s-swim"], ["Kohlenhydrate", carbs[nDay] ?? 0, 4, "s-run"], ["Fett", fat[nDay] ?? 0, 9, "s-strength"]];
  const macroKcal = macros.reduce((n, m) => n + (m[1] ?? 0) * m[2], 0);
  // Ziele direkt aus Yazio (Tagesziele ändern sich nicht pro Tag), ohne Ziel nur der Wert ohne Balken
  const ng = d.nutritionGoals ?? {}, macroGoals = [ng.proteinG, ng.carbsG, ng.fatG];
  const macroRow = (m, i) => {
    const v = m[1], goal = macroGoals[i] ?? null, over = v != null && goal && v > goal * 1.1;
    const pct = v != null && goal ? Math.min(100, (v / goal) * 100) : 0;
    const rest = v != null && goal ? (over ? `+${fmt(v - goal)} drüber` : `noch ${fmt(Math.max(0, goal - v))}`) : "";
    const share = v != null && macroKcal > 0 ? ` · ${fmt((v * m[2] / macroKcal) * 100)} % der kcal` : "";
    return `<div class="mrow"><div class="mhead"><b>${m[0]}</b><span class="mval">${v != null ? fmt(v) : "–"}${goal ? ` <small>/ ${fmt(goal)} g</small>` : " <small>g</small>"}</span></div>${goal ? `<div class="mbar"><div style="width:${pct}%;background:${over ? "var(--bad)" : `var(--${m[3]})`}"></div></div>` : ""}<div class="sub">${[rest, share.slice(3)].filter(Boolean).join(" · ")}</div></div>`;
  };
  const macroTile = tile(`Makros · ${fmtDate(nDay)}`, lastData || d.nutritionGoals ? macros.map(macroRow).join("") : '<div class="sub">keine Daten</div>', "span3");

  $("cockpit").innerHTML = `
    <div class="cockpit-group">Rennen und Training</div>
    <div class="cockpit">${weekTile}${raceTile}${tsbTile}${acwrTile}${taperTile}${todayTile}</div>
    <div class="cockpit-group">Erholung</div>
    <div class="cockpit">${readyTile}${sleepTile}${sparkTile("HRV", "hrv", "ms", 0)}${sparkTile("Ruhepuls", "restingHR", "bpm", 0)}</div>
    <div class="cockpit-group">Ernährung</div>
    <div class="cockpit">${nutTile}${macroTile}</div>`;

  if (g.daysToGo >= 0) C.countdown($("ck-countdown"), { total: HISTORY_DAYS + g.daysToGo, elapsed: HISTORY_DAYS, raceLabel: `${fmtDate(g.date)} Rennen` });
  C.gauge($("ck-tsb"), { label: "TSB", min: -40, max: 30, value: tsb, ticks: [-40, -25, -10, 0, 15, 30], zones: [{ from: -40, to: TSB_BANDS.warn, cls: "bad" }, { from: TSB_BANDS.warn, to: TSB_BANDS.ok, cls: "warn" }, { from: TSB_BANDS.ok, to: 30, cls: "ok" }] });
  C.gauge($("ck-acwr"), { label: "ACWR", min: 0.4, max: 1.8, value: acwr, dec: 2, ticks: [0.4, 0.8, 1.0, 1.3, 1.8], tickLabel: (t) => fmt(t, 1), zones: [{ from: 0.4, to: ACWR.lo, cls: taper ? "ok" : "warn" }, { from: ACWR.lo, to: ACWR.hi, cls: "ok" }, { from: ACWR.hi, to: 1.8, cls: "bad" }] });
  if (shown?.steps?.length && $("ck-workout")) C.workout($("ck-workout"), shown.steps);
  if (cur) C.meter($("ck-wload"), { label: "Wochenbelastung nach Sportart", segments: SPORT_ORDER.map((k) => ({ value: cur.bySport[k].load, color: `var(--s-${k})`, tip: `${SPORT_LABEL[k]}: ${fmt(cur.bySport[k].load)} Load` })), goal: cur.plannedLoad, max: Math.max(1, cur.load, cur.plannedLoad ?? 0) * 1.15 });
  for (const [key, unit, label, dec] of [["hrv", "ms", "HRV", 0], ["restingHR", "bpm", "Ruhepuls", 0]]) C.spark($(`ck-${key}`), days7, map(key), { unit, label, dec });
  if ($("ck-kcal")) C.meter($("ck-kcal"), { label: "Kalorien", value: kcalToday, goal: kcalGoal, max: Math.max(kcalToday, kcalGoal ?? 0, 1) * 1.15, unit: "kcal" });
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
    [`Schwimmen ${fmt(T.swimKm, 2)} km`, sw ? `<b>${hm(sw.timeSecs)}</b>${sw.timeSecs >= 3600 ? ` <small class="muted">${fmtTime(sw.timeSecs)}</small>` : ""}` : miss, sw ? `${paceLabel(sw.pacePer100m)} /100 m${tag(sw)}` : "CSS fehlt"],
    [`Rad ${fmt(T.bikeKm)} km`, b?.timeSecs ? `<b>${hm(b.timeSecs)}</b> <small class="muted">${fmtTime(b.timeSecs)}</small>` : miss, b ? [b.watts ? (b.wattsLow ? `${fmt(b.wattsLow)}–${fmt(b.wattsHigh)} W` : `${fmt(b.watts)} W`) : "", b.speedKmh ? `${fmt(b.speedKmh, 1)} km/h` : ""].filter(Boolean).join(" · ") + tag(b) : "FTP fehlt"],
    [`Laufen ${fmt(T.runKm, 1)} km`, r ? `<b>${hm(r.timeSecs)}</b> <small class="muted">${fmtTime(r.timeSecs)}</small>` : miss, r ? `${paceLabel(r.pacePerKm)} min/km${tag(r)}` : "VDOT fehlt"],
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

  const marks = (km, step, secs) => { const out = []; for (let m = step; m < km - 0.01; m += step) out.push(m); out.push(km); return out.map((m) => `<tr><td>${m === km ? fmt(m, m % 1 ? 1 : 0) : m} km</td><td>${secs ? fmtTime(Math.round(secs * (m / km))) : "–"}</td></tr>`).join(""); };
  const part = (title, km, step, secs) => `<h3 style="margin-top:8px">${title}</h3><table class="mini"><tbody>${marks(km, step, secs)}</tbody></table>`;
  const gelKm = []; if (b?.timeSecs) for (let t = 40 * 60; t < b.timeSecs - 10 * 60; t += 40 * 60) gelKm.push(fmt(T.bikeKm * (t / b.timeSecs), 0));
  $("splits").innerHTML = `<div class="scroll"><h3>Schwimmen</h3><table class="mini"><tbody>${(() => { const m = T.swimKm * 1000, out = []; for (let x = 500; x < m - 1; x += 500) out.push(x); out.push(m); return out.map((x) => `<tr><td>${fmt(x)} m</td><td>${sw ? fmtTime(Math.round(sw.pacePer100m * x / 100)) : "–"}</td></tr>`).join(""); })()}</tbody></table>
    ${b?.timeSecs ? part("Rad", T.bikeKm, T.bikeKm >= 60 ? 30 : 10, b.timeSecs) : '<h3 style="margin-top:8px">Rad</h3><div class="muted">Keine Zielzeit, daher keine Marken. Nach Leistung fahren; Zeit in die Beschreibung schreiben („Rad 3:00:00“).</div>'}${part("Laufen", T.runKm, T.runKm > 10 ? 5 : 2.5, r?.timeSecs)}</div>
    <div style="margin-top:8px"><b>Verpflegung</b> <span class="muted">(Faustwerte)</span>
    <div>Rad: ${gelKm.length ? `Kohlenhydrate etwa alle 40 min, bei km ${gelKm.join(", ")}.` : "Kohlenhydrate etwa alle 40 min."} Laufen: Gel/Getränk etwa alle 40 min, Wasser an den Stationen.</div>
    <div class="muted">Richtwert Rad 60–90 g, Laufen 30–60 g Kohlenhydrate pro Stunde; nur Bewährtes aus dem Training verwenden.</div></div>`;

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

/* ---------- 3 · Form: Verlauf ---------- */
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
  $("week-report").innerHTML = table(done, prev, "Letzte volle Woche") + '<div style="height:12px"></div>' + table(cur, done, "Laufende Woche");
}

/* ---------- 4 · Leistung ---------- */
function renderFitness(d) {
  const r = d.runalyze;
  // Decoupling: Säulen je langem Lauf
  if (!d.sources.intervalsActivities.ok) $("chart-dec").innerHTML = '<div class="muted">Aktivitäten nicht abrufbar.</div>';
  else if (!d.fitness.longRuns.length) $("chart-dec").innerHTML = '<div class="notice">Keine langen Läufe mit Decoupling-Wert in den letzten 8 Wochen – ein fehlender Nachweis, kein guter Wert.</div>';
  else C.bars($("chart-dec"), d.fitness.longRuns.map((x) => ({ label: fmtDate(x.date), tip: `${fmtDate(x.date)} (${fmt(x.distanceKm, 1)} km)`, value: Math.max(0, x.decoupling) })), { unit: "Decoupling in %", label: "Decoupling je langem Lauf", valueLabels: true, dec: 1, ref: { value: 5, label: "5 % Orientierung" }, xLabel: "Datum des langen Laufs" });

  if (!r) {
    for (const id of ["vdot", "corridor"]) $(id).innerHTML = '<div class="muted">Noch kein Runalyze-Snapshot eingespielt – VDOT und Prognose fehlen.</div>';
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
    $("vdot").innerHTML = `<div class="big">${fmt(r.vdot, 1)}</div><div class="muted">VDOT (effektive VO2max laut Runalyze), Stand ${new Date(r.fetchedAt).toLocaleDateString("de-DE")}</div><div id="vdot-ruler" style="margin-top:6px"></div>${staleHtml}<div class="muted">Runalyze liefert nur das VDOT, die Bereiche sind daraus wie in den Runalyze-Lauftabellen berechnet (Prozent der Geschwindigkeit bei vVO2max).</div>`;
    C.paceRuler($("vdot-ruler"), zones, goalPace);
  }

  // Eine konsolidierte Prognose: nur Rechnung aus VDOT und aus der 10-km-Bestzeit (5-km-Bestzeit und Runalyze-Prognose verwirren mehr).
  const est = r.hmEstimates.filter((e) => RANGE_KEYS.includes(e.key));
  if (!est.length) $("corridor").innerHTML = '<div class="muted">Keine Halbmarathon-Rechnung aus VDOT oder 10-km-Bestzeit möglich.</div>';
  else {
    $("corridor").innerHTML = '<div id="corridor-chart"></div><div class="muted">Rechnungen nach Daniels (VDOT-Modell), kein Halbmarathon-Ergebnis. Sie unterstellen Ausdauer wie über die Ausgangsdistanz und fallen von kürzeren Distanzen aus tendenziell zu optimistisch aus.</div>';
    C.corridor($("corridor-chart"), { goal, estimates: est });
  }

  // Verlauf nur zeigen, wenn es einen gibt
  if ((r.hmHistory ?? []).length > 1) { $("hm-trend-card").hidden = false; renderHmTrend(r); }
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
  const fmtD = (v) => v == null ? "–" : v === 0 ? "unverändert" : `<span style="color:var(${v < 0 ? "--ok" : "--muted"});font-weight:600">${v < 0 ? "−" : "+"}${fmtTime(Math.abs(v))}</span>`;
  const weekAgo = [...rows].reverse().find((x) => (Date.parse(last.date) - Date.parse(x.date)) / 86400000 >= 7);
  const tr = (label, key) => {
    const have = rows.filter((x) => x[key] != null);
    if (!have.length) return "";
    const now = have[have.length - 1], first = have[0], wk = weekAgo && weekAgo[key] != null ? weekAgo : null;
    return `<tr><td>${label}</td><td><b>${fmtTime(now[key])}</b></td><td>${have.length > 1 ? fmtD(now[key] - first[key]) : "–"}</td><td>${wk ? fmtD(now[key] - wk[key]) : "–"}</td></tr>`;
  };
  const table = `<table class="mini"><thead><tr><th></th><th>Prognose jetzt</th><th>seit ${fmtDate(rows[0].date)}</th><th>seit 7 Tagen</th></tr></thead><tbody>${tr("Halbmarathon", "hmProgSecs")}${tr("10 km", "p10Secs")}${tr("5 km", "p5Secs")}${tr("HM aus VDOT", "hmVdotSecs")}</tbody></table>`;
  const first = rows.length === 1 ? '<div class="muted" style="margin:6px 0">Der Vergleich beginnt heute. Mit jedem neuen Runalyze-Snapshot siehst du hier, ob die Prognose schneller wird – früher gespeicherte Werte gibt es nicht.</div>' : "";
  $("hm-trend").innerHTML = `${table}${first}<div id="hm-trend-chart" style="margin-top:8px"></div><div class="muted">Linien fallen = Prognose wird schneller. Die Prognose ist eine Rechnung von Runalyze aus deinen Läufen, kein Ergebnis.</div>`;
  if (rows.length > 1) C.deltaTrend($("hm-trend-chart"), rows, { label: "Veränderung der Prognose", series: SERIES });
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
    triathlonGoalCard(d.goal);
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
  $("sleep-account").innerHTML = `<div class="big">${SA.known ? fmt(SA.sum, 1) : "–"} <small class="muted" style="font-size:.9rem">h von ${fmt(SA.target, 0)} h</small></div><div><span class="badge ${SA.cls}">${SA.text}</span></div>
    ${SA.nights.map((n, i) => `<div class="row"><span>${i === 0 ? "Letzte Nacht (auf heute)" : "Nacht davor"} · ${weekday(n.date)} ${fmtDate(n.date)}</span><b>${n.h != null ? fmt(n.h, 1) + " h" : "nicht erfasst"}</b></div>`).join("")}
    <div class="muted">Vor dem Rennen zählt die Summe der letzten zwei Nächte mehr als eine einzelne. Richtwert ${fmt(SLEEP_TARGET_H, 1)} h pro Nacht, kein persönlich kalibrierter Wert.</div>`;
  const wv = d.wellness.filter((w) => w.weight != null);
  if (wv.length) {
    $("weight-card").hidden = false;
    const last = wv[wv.length - 1], first = wv[0], diff = last.weight - first.weight;
    C.gapLine($("weight"), days, Object.fromEntries(wv.map((w) => [w.date, w.weight])), { unit: "kg", label: "Gewicht", dec: 1 });
    $("weight").insertAdjacentHTML("afterbegin", `<div><b>${fmt(last.weight, 1)} kg</b> <span class="muted">${fmtDate(last.date)}${wv.length > 1 ? ` · ${diff > 0 ? "+" : ""}${fmt(diff, 1)} kg seit ${fmtDate(first.date)}` : ""}</span></div>`);
  }
  for (const [id, key, unit, label] of [["rhr", "restingHR", "bpm", "Ruhepuls"], ["sleep", "sleepHours", "h", "Schlafdauer"], ["hrv", "hrv", "ms", "HRV"]]) {
    const vals = Object.fromEntries(d.wellness.map((w) => [w.date, w[key]]));
    if (!Object.values(vals).some((v) => v != null)) $(id).innerHTML = `<div class="muted">Keine ${label}-Werte im Zeitraum.</div>`;
    else C.gapLine($(id), days, vals, { unit, label });
  }
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
    const todayGoal = (v) => (v != null ? { [d.today]: v } : undefined);
    const ng = d.nutritionGoals ?? {};
    C.nutrition($("n-prot"), days, { values: map("protein"), goal: todayGoal(ng.proteinG), training, unit: "g", label: "Eiweiß je Tag", legend: "schwarze Marke = Ziel (nur heute) · Punkt = Trainingstag · schraffiert = keine Daten" });
    C.nutrition($("n-fat"), days, { values: map("fat"), goal: todayGoal(ng.fatG), training, unit: "g", label: "Fett je Tag", legend: "schwarze Marke = Ziel (nur heute) · Punkt = Trainingstag · schraffiert = keine Daten" });
    C.nutrition($("n-kcal"), days, { values: kcal, goal: map("calorieGoal"), training, unit: "kcal", label: "Kalorien je Tag", legend: "schwarze Marke = Tagesziel · Punkt = Trainingstag · schraffiert = keine Daten" });
    C.nutrition($("n-carbs"), days, { values: map("carbs"), goal: todayGoal(ng.carbsG), training, unit: "g", label: "Kohlenhydrate je Tag", legend: "schwarze Marke = Ziel (nur heute) · Punkt = Trainingstag · schraffiert = keine Daten" });
  }

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
    const over = v != null && goal && v > goal;
    return `<div style="margin:8px 0"><div style="display:flex;justify-content:space-between;gap:8px"><b>${label}</b><span>${v != null ? fmt(v) : "–"}${goal ? ` von ${fmt(goal)}` : ""} ${unit}${v != null && goal ? ` · ${over ? "+" + fmt(v - goal) + " drüber" : "noch " + fmt(Math.max(0, goal - v))}` : ""}</span></div><div style="height:10px;border-radius:5px;background:var(--line);overflow:hidden"><div style="height:100%;width:${pct}%;background:${over ? "var(--warn)" : color}"></div></div></div>`;
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

/* ---------- 6 · Kraft und Hüfte: nur sichtbar, wenn es etwas zu zeigen gibt ---------- */
function renderStrength(d) {
  const has = d.sources.intervalsActivities.ok && d.weeks.some((w) => w.bySport.strength.minutes > 0);
  if (has || d.hipFlags.length) $("kraft").hidden = false;
  if (has) {
    $("strength-card").hidden = false;
    // Wochenzeilen statt Säulen: Balken gegen die Zielmarke (60 min), laufende Woche oben mit großer Zahl.
    const GOAL = 60, weeks = [...d.weeks].reverse(), cur = weeks[0];
    const max = Math.max(GOAL, ...weeks.map((w) => w.bySport.strength.minutes)) * 1.1;
    const done = d.weeks.filter((w) => w.complete && w.bySport.strength.minutes >= GOAL).length, full = d.weeks.filter((w) => w.complete).length;
    const row = (w) => { const m = w.bySport.strength.minutes, hit = m >= GOAL; return `<div style="display:grid;grid-template-columns:64px 1fr 56px;gap:8px;align-items:center;padding:3px 0;${w.complete ? "" : "opacity:.75"}"><span class="muted">${fmtDate(w.weekStart)}${w.complete ? "" : " ·&nbsp;jetzt"}</span><div class="cbar" style="height:12px"><div class="cfill" style="width:${(m / max) * 100}%;background:${hit ? "var(--ok)" : "var(--s-strength)"}"></div><i style="left:${(GOAL / max) * 100}%"></i></div><b style="text-align:right">${m ? fmt(m) + " min" : "–"}</b></div>`; };
    $("strength").innerHTML = `<div class="big">${fmt(cur.bySport.strength.minutes)} <small class="muted" style="font-size:.9rem">min diese Woche von ${GOAL}</small></div>
      <div class="muted" style="margin:2px 0 10px">Ziel erreicht in ${done} von ${full} abgeschlossenen Wochen · senkrechte Marke = ${GOAL} min</div>${weeks.map(row).join("")}`;
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
async function loadEgym(token) {
  try {
    const r = await fetch("/api/widget?view=kraft", { headers: { Authorization: "Bearer " + token }, cache: "no-store" });
    if (!r.ok) return;
    const k = await r.json();
    if (k.configured === false) return; // ohne EGYM-Zugang bleibt die Karte weg
    renderEgym(k);
  } catch (e) { console.error("egym", e); }
}

function renderEgym(k) {
  $("kraft").hidden = false;
  $("egym-card").hidden = false;
  const failed = k.sourcesFailed || [];
  if (failed.length >= 3) { $("egym").innerHTML = '<div class="notice bad"><b>EGYM nicht erreichbar.</b> Login oder Abruf ist fehlgeschlagen.</div>'; return; }
  const GOAL_SETS = 27; // 9 Geräte x 3 Sätze
  const wk = k.week, goal = wk.goalMin, min = wk.minutes, hit = min != null && min >= goal;
  const est = (src) => (src === "estimate" ? "≈" : "");
  const vol = (kg) => (kg >= 1000 ? fmt(kg / 1000, 1) + " t" : fmt(kg) + " kg");
  const max = Math.max(goal, ...k.weeks.map((w) => w.minutes || 0)) * 1.1;
  const row = (w, last) => { const m = w.minutes, ok = m != null && m >= goal; return `<div style="display:grid;grid-template-columns:64px 1fr 128px;gap:8px;align-items:center;padding:3px 0;${last ? "" : "opacity:.85"}"><span class="muted">${fmtDate(w.start)}${last ? " ·&nbsp;jetzt" : ""}</span><div class="cbar" style="height:12px"><div class="cfill" style="width:${((m || 0) / max) * 100}%;background:${ok ? "var(--ok)" : "var(--s-strength)"}${w.minutesSource === "estimate" ? ";opacity:.6" : ""}"></div><i style="left:${(goal / max) * 100}%"></i></div><span style="text-align:right;white-space:nowrap"><b>${m == null ? (w.sessions ? "Dauer ?" : "–") : m ? est(w.minutesSource) + fmt(m) + " min" : "–"}</b>${w.volumeKg ? `<span class="muted" style="font-size:.8rem"> · ${vol(w.volumeKg)}</span>` : ""}</span></div>`; };
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
  $("egym").innerHTML = `${failed.length ? `<div class="notice bad" style="margin-bottom:8px">Teilweise nicht erreichbar: ${esc(failed.join(", "))}</div>` : ""}
    <div class="big">${wk.sets || 0} <small class="muted" style="font-size:.9rem">von ${GOAL_SETS} Sätzen diese Woche</small> ${(wk.sets || 0) >= GOAL_SETS ? '<span class="badge ok">Ziel erreicht</span>' : ""}</div><div class="cbar" style="height:12px;margin:6px 0"><div class="cfill" style="width:${Math.min(100, ((wk.sets || 0) / GOAL_SETS) * 100)}%;background:var(--s-strength)"></div></div><div class="muted">${wk.sessions} ${wk.sessions === 1 ? "Einheit" : "Einheiten"} diese Woche · ${min == null ? "–" : est(wk.minutesSource) + fmt(min)} min</div>
    <div class="muted" style="margin:2px 0 10px">Ziel erreicht in ${k.weeksHit.hit} von ${k.weeksHit.of} abgeschlossenen Wochen${k.streakWeeks ? ` · Serie ${k.streakWeeks} ${k.streakWeeks === 1 ? "Woche" : "Wochen"}` : ""} · senkrechte Marke = ${goal} min${wk.goalSource === "default" ? " (Standardziel)" : ""}</div>
    ${k.weeks.slice().reverse().map((w, i) => row(w, i === 0)).join("")}
    <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:12px">${stat(wk.sessions, "Einheiten")}${stat(wk.sets || "–", "Sätze")}${stat(wk.volumeKg ? (wk.volumeKg >= 1000 ? fmt(wk.volumeKg / 1000, 1) + " t" : fmt(wk.volumeKg) + " kg") : "–", "Volumen")}${stat(since, "zuletzt")}</div>
    ${vt ? `<div class="muted" style="margin-top:8px">Volumen letzte Woche gegen die davor: ${vt.pct > 0 ? "+" : vt.pct < 0 ? "−" : ""}${fmt(Math.abs(vt.pct), 1)} % (${fmt(vt.lastKg)} kg gegen ${fmt(vt.prevKg)} kg)</div>` : ""}
    ${records}
    ${bio}
    ${items.length ? `<h3 style="margin-top:14px">Fortschritt: 1RM je Gerät</h3><div style="overflow-x:auto"><table><thead><tr><th>Gerät</th><th>Bereich</th><th>1RM</th><th>Vorher</th><th>Veränderung</th><th>Test</th></tr></thead><tbody>${items.map((i) => `<tr><td>${esc(i.label)}</td><td class="muted">${esc(REGION_LABEL[i.region] || i.region || "–")}</td><td><b>${fmt(i.kg)} kg</b></td><td class="muted">${i.prevKg != null ? fmt(i.prevKg) + " kg" : "–"}</td><td>${delta(i)}</td><td class="muted">${fmtDate(i.at)}</td></tr>`).join("")}</tbody></table></div>
      <div class="muted" style="margin-top:6px">${p.improved} besser · ${p.same} gleich · ${p.declined} schwächer gegen den vorherigen Test. Vergleich erst, wenn ein Gerät mindestens zwei Tests hat.</div>` : ""}
    <div class="muted" style="margin-top:8px">Kraft-Einheit = Tag mit Satz-Übungen oder Geräten (Garmin-Tagesaktivität zählt nicht). „≈“ und blasse Balken: Dauer nur aus der Spanne der Übungen geschätzt, EGYM lieferte keine.</div>`;
}

/* ---------- Studien-Check der Woche ---------- */
function renderStudie(d) {
  const s = d.studie;
  $("studie-card").hidden = false; // Der Abschnitt bleibt sichtbar: Er kommt sonntags mit dem Coaching-Bericht
  if (!s) { $("studie").innerHTML = '<div class="muted">Noch kein Studien-Check diese Woche. Er kommt am Sonntag mit dem Coaching-Bericht.</div>'; return; }
  const link = s.sourceUrl ? ` · <a href="${esc(s.sourceUrl)}" target="_blank" rel="noopener noreferrer">Quelle öffnen</a>` : "";
  $("studie").innerHTML = `${s.title ? `<h3>${esc(s.title)}</h3>` : ""}<div class="desc" style="color:var(--text)">${esc(s.text)}</div>
    <div class="muted" style="margin-top:8px">Quelle: ${esc(s.source)}${link}${s.week ? ` · Woche ${fmtDate(s.week)}` : ""} · übermittelt ${new Date(s.fetchedAt).toLocaleDateString("de-DE")}</div>`;
}

load();

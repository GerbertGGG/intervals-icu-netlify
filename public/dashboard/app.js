"use strict";
/* Trainings-Dashboard: Laden der Daten vom Worker und Aufbau der Bereiche. Diagramme: charts.js. */
const $ = (id) => document.getElementById(id);
const { esc, fmt, fmtDate, weekday, fmtTime, pace: paceLabel, addDays, dayRange } = U;

const SPORT_LABEL = { run: "Laufen", bike: "Rad", swim: "Schwimmen", strength: "Kraft", other: "Sonstiges" };
const SPORT_ORDER = ["run", "bike", "swim", "strength", "other"];
const HISTORY_DAYS = 56;
const FORM_DAYS = 180; // Formkurve: längerer Verlauf (siehe FORM_DAYS im Worker)
const isTaper = (g) => g.daysToGo >= 0 && g.daysToGo <= 14;
// Kommendes Rennen im Kalender? Ohne (none) keine Renn-Kacheln, Zielpaces und Prognosen: das Dashboard misst den laufenden Block.
const hasRace = (g) => Boolean(g) && !g.none && g.daysToGo >= 0;
const hm = (secs) => (secs < 3600 ? fmtTime(secs) : fmtTime(Math.round(secs / 60) * 60).replace(/:00$/, "")); // ab 1 h als h:mm, darunter m:ss
// Frische in % der Fitness (CTL), Zonen wie Intervals.icu (Worker: dashboard-summary.js)
const TSB_ZONE_TEXT = { risk: "hohes Risiko", optimal: "produktiv belastet", neutral: "neutral", fresh: "frisch", transition: "Übergang: Fitness sinkt" };
const fmtStable = (m) => (m == null ? "–" : m === 0 ? "unter 60 min" : `${m} min`);

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
  const steps = [renderCockpit, renderRennplan, renderReady, renderHistory, renderLongruns, renderIntervals, renderSport, renderFitness, renderDisciplines, renderThresholds, renderWellness, renderNutrition, renderStrength, renderStudie];
  for (const step of steps) {
    try { step(d); } catch (e) { console.error(step.name, e); const n = document.createElement("div"); n.className = "notice bad"; n.textContent = `Bereich „${step.name.replace("render", "")}" konnte nicht gezeichnet werden: ${e.message}`; $("sources").append(n); }
  }
  initNav();
}

/* Sprungleiste: der Abschnitt, in dem man gerade ist, ist hervorgehoben (leichtgewichtig über den Scroll-Stand).
   Links auf ausgeblendete Abschnitte (Rennplan ohne Rennen) verschwinden. */
function initNav() {
  for (const l of document.querySelectorAll(".jump a")) l.hidden = Boolean(document.querySelector(l.getAttribute("href"))?.closest("[hidden]"));
  const links = [...document.querySelectorAll(".jump a")], targets = links.map((l) => document.querySelector(l.getAttribute("href")));
  const mark = () => {
    let cur = 0;
    targets.forEach((t, i) => { if (t && !t.closest("[hidden]") && t.getBoundingClientRect().top <= 90) cur = i; });
    links.forEach((l, i) => l.classList.toggle("active", i === cur));
  };
  addEventListener("scroll", mark, { passive: true });
  mark();
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
// Die Einschätzungen (Bereitschaft, TSB) rechnet der Worker (dashboard-summary.js), damit Seite, Widget und Coach
// dasselbe zeigen. Hier wird nur dargestellt.
function bodyLine(R) {
  const b = R.body, f1 = (v) => fmt(v, 1);
  return [
    R.sleepScore != null ? `Schlaf-Score ${fmt(R.sleepScore)}${R.sleepHours != null ? ` (${f1(R.sleepHours)} h)` : ""}` : R.sleepHours != null ? `${f1(R.sleepHours)} h Schlaf` : "Schlaf nicht erfasst",
    R.hrv != null && `HRV ${fmt(R.hrv)}${b.hrvMedian != null ? ` · Basis ${f1(b.hrvMedian)}` : ""}`,
    R.restingHR != null && `Ruhepuls ${fmt(R.restingHR)}${b.restingMedian != null ? ` · Basis ${f1(b.restingMedian)}` : ""}`,
  ].filter(Boolean).join(" · ");
}
function renderReady(d) {
  const host = $("ready");
  if (!d.sources.intervalsWellness.ok) { host.innerHTML = '<div class="card"><h3>Erholungswerte</h3><div class="muted">Wellness nicht abrufbar.</div></div>'; return; }
  const R = d.summary.recovery, items = R.items, f1 = (v) => fmt(v, 1);
  // Vergleich mit der eigenen Basis als Fakt; die Gefühlswerte sind nur gegen den eigenen üblichen Bereich gefärbt. Die Bewertung macht der Coach.
  const row = (label, v, unit, base, extra = "") => {
    const rest = [base != null ? `Basis ${f1(base)}` : "", extra].filter(Boolean).join(" · ");
    return `<div class="row"><span>${label}</span><span>${unit ? (v != null ? `<b>${fmt(v, unit === "h" ? 1 : 0)}</b> ${unit}` : "–") : ""}${rest ? ` <span class="muted">${unit ? "· " : ""}${rest}</span>` : ""}</span></div>`;
  };
  const B = R.body, w7 = (x) => (x.mean7 != null && x.base7 != null ? `7 Tage ${f1(x.mean7)} (Wochen davor ${f1(x.base7)})` : "");
  host.innerHTML = `<div class="card"><h3>Erholungswerte</h3>
    ${row("Ruhepuls", R.restingHR, "bpm", B.restingMedian, w7(B.resting))}${row("HRV", R.hrv, "ms", B.hrvMedian, w7(B.hrv))}${row("Schlaf", R.sleepHours, "h", B.sleepMedian, R.sleepScore != null ? `Score ${fmt(R.sleepScore)}` : "")}${B.drift ? row("Langzeitbasis", null, "", null, [B.drift.hrv7 != null ? `HRV 7 Tage ${f1(B.drift.hrv7)} gegen ${f1(B.drift.hrvAnchor)} (Tag 29–90)` : null, B.drift.rhr7 != null ? `Ruhepuls 7 Tage ${f1(B.drift.rhr7)} gegen ${f1(B.drift.rhrAnchor)}` : null].filter(Boolean).join(" · ")) : ""}
    <div class="ready-grid">${items.map((i) => `<div class="rrow"><span class="lbl">${i.label}</span><span class="strip" id="strip-${i.key}"></span><span class="muted">${i.v != null ? `${i.v}${i.med != null ? ` · üblich ${f1(i.med)}` : ""}` : "fehlt"}</span></div>`).join("")}</div>
    ${R.entryDate ? `<div class="muted" style="margin-top:6px">Gefühlswerte vom ${fmtDate(R.entryDate)}, vor dem Training eingetragen.</div>` : ""}
    <details class="info"><summary>Erklärung</summary><div>Basis = Median der letzten 28 Tage ohne Krankheitstage (mind. 5 Werte), Ausreißer bleiben draußen. Gefühlswerte: Skala ab 1 = bestmöglich, das graue Band zeigt deinen üblichen Bereich der letzten 8 Wochen. Farbe nur im Vergleich dazu: über dem Bereich und 1 schlechter als üblich = gelb, 2 und mehr = rot. Ein Urteil über den Tag ist das nicht – das macht der Coach in der Morgen-Auswertung.</div></details></div>`;
  for (const i of items) C.strip($(`strip-${i.key}`), { label: i.label, max: i.max, band: i.band, value: i.v, cls: i.cls ?? "none" });
}

/* ---------- Cockpit: alles Wichtige auf einen Blick ---------- */
// Kennzahlen der letzten Einheit als kleine Kacheln (wie auf der Intervals-Karte), ohne Bewertungsfarbe: Eine einzelne Einheit
// bewertet der Coach. Puls nur, wenn der Worker ihn liefert (Pulsregel).
// Decoupling nur für die ersten 60 min (der Wert über den ganzen Lauf wächst mit der Länge), dazu stabile Dauer und HF-Deckel.
const FEEL_LABEL = { 1: "stark", 2: "gut", 3: "normal", 4: "schlecht", 5: "schwach" };
function lastStats(L) {
  const st = (label, value, cls = "") => `<div class="ls"><span class="ls-v${cls ? ` ${cls}` : ""}">${value}</span><span class="ls-l">${label}</span></div>`;
  const items = [
    L.watts != null && st("Ø Leistung", `${fmt(L.watts)} <small>W</small>`),
    L.normWatts != null && st("NP", `${fmt(L.normWatts)} <small>W</small>`),
    L.paceSecPerKm != null && st("Ø Pace", `${paceLabel(L.paceSecPerKm)} <small>/km</small>`),
    L.paceSecPer100m != null && st("Ø Pace", `${paceLabel(L.paceSecPer100m)} <small>/100 m</small>`),
    L.avgHr != null && st("Ø Puls", `${fmt(L.avgHr)} <small>bpm</small>`),
    L.overCapMin != null && st(`über HF ${fmt(L.capHr)}`, `${fmt(L.overCapMin)} <small>min</small>`),
    L.intensity != null && st("Intensität", `${fmt(L.intensity)} <small>%</small>`),
    L.dec60 != null && st("Decoupling 0–60", `${fmt(L.dec60, 1)} <small>%</small>`),
    L.stableMin != null && st("stabile Dauer", fmtStable(L.stableMin)),
    L.decoupling != null && st("Decoupling", `${fmt(L.decoupling, 1)} <small>%</small>`),
    L.rpe != null && st("RPE", fmt(L.rpe)),
    L.feel != null && FEEL_LABEL[L.feel] && st("Gefühl", FEEL_LABEL[L.feel]),
  ].filter(Boolean);
  return items.length ? `<div class="lstats">${items.join("")}</div>` : "";
}

function renderCockpit(d) {
  const S = d.summary, w = S.load, { ctl, atl, tsb } = w, g = d.goal;
  const race = hasRace(g), taper = race && isTaper(g);
  const wellOk = d.sources.intervalsWellness.ok;
  const cur = d.weeks[d.weeks.length - 1];
  const tile = (title, body, cls = "", more = null) => `<div class="tile ${cls}"><h3>${title}</h3>${body}${more ? `<a class="more" href="${more[0]}">${more[1]} →</a>` : ""}</div>`;
  const goalPace = g.targetTimeSecs ? g.targetTimeSecs / (g.runKm ?? 21.0975) : null;

  const plan = d.planned.filter((p) => p.date === d.today);
  const shown = plan.find((p) => p.steps?.length) ?? plan[0];
  const weekTile = renderWeekPreview(d, tile);
  // Renn-Kacheln nur mit kommendem Rennen im Kalender
  const raceTile = race ? tile(esc(g.name), `<div class="val">${g.daysToGo > 0 ? `${g.daysToGo} <small>Tag${g.daysToGo === 1 ? "" : "e"}</small>` : "Heute!"}</div><div id="ck-countdown"></div><div class="sub">${weekday(g.date)} ${fmtDate(g.date)}${g.triathlon ? (g.totalTargetSecs ? ` · Ziel ${fmtTime(g.totalTargetSecs)}` : "") : g.targetTimeSecs ? ` · Ziel ${fmtTime(g.targetTimeSecs)} (${paceLabel(goalPace)} min/km)` : ""}</div>`, "span15", ["#rennplan", "Zielzeiten und Plan"]) : "";
  const supportTile = (d.supportRaces ?? []).length ? tile("Vorbereitungsrennen (B)", d.supportRaces.slice(0, 3).map((r) => `<div class="mrow"><div class="mhead"><b>${esc(r.name)}</b><span class="mval">${r.daysToGo > 0 ? `${r.daysToGo} <small>Tage</small>` : r.daysToGo === 0 ? "Heute!" : "vorbei"}</span></div><div class="sub">${weekday(r.date)} ${fmtDate(r.date)}${r.runKm ? ` · Lauf ${fmt(r.runKm, 1)} km` : ""}${r.targetTimeSecs ? ` · Ziel ${fmtTime(r.targetTimeSecs)}` : ""}</div></div>`).join("") + (race ? `<div class="sub">Zwischenziel auf dem Weg zu: ${esc(g.name)}</div>` : ""), "span15") : "";
  const blockTile = renderBlock(d.seasonBlock);
  const prevWk = d.weeks[d.weeks.length - 2];
  const taperTile = taper ? tile("Taper-Status", `<div class="val">Taper-Phase · noch ${g.daysToGo} Tag${g.daysToGo === 1 ? "" : "e"}</div><div class="sub">Weniger Umfang ist jetzt gewollt: Diese Woche ${cur ? fmt(cur.runMin / 60, 1) : "–"} h Laufen${prevWk ? `, Vorwoche ${fmt(prevWk.runMin / 60, 1)} h` : ""}. Steigende Frische (TSB${tsb != null ? ` ${fmt(tsb)}` : ""}) gilt als Soll, nicht als Mangel.</div>`, "span4") : "";
  const R = S.recovery;

  // Heute oben: 1. die Aussage des Coaches (Morgen-Auswertung, save_coach) mit seiner Rückfrage, 2. was heute ansteht bzw. die
  // letzte Einheit, 3. Kennzahlen als Fakten ohne Bewertung. Das Dashboard urteilt nicht selbst: Ohne frische Coach-Aussage stehen
  // oben nur die Werte.
  const doneToday = d.daily.find((x) => x.date === d.today);
  const meta = (p) => [p.durationMin ? p.durationMin + " min" : p.distanceKm && fmt(p.distanceKm, 1) + " km", p.load && "Load " + fmt(p.load)].filter(Boolean).join(" · ");
  const L = d.lastActivity;
  const lastHtml = L ? `<div class="hb-item"><i style="background:var(--s-${L.sport})"></i><div><b>${SPORT_LABEL[L.sport] || "Einheit"}</b><span class="sub">${[L.date === d.today ? "heute" : `${weekday(L.date)} ${fmtDate(L.date)}`, meta(L), L.distanceKm && L.durationMin ? fmt(L.distanceKm, 1) + " km" : ""].filter(Boolean).join(" · ")}</span></div></div>${lastStats(L)}` : "";
  const planHtml = plan.length
    ? plan.map((p) => `<div class="hb-item"><i style="background:var(--s-${p.sport})"></i><div><b>${esc(p.name || SPORT_LABEL[p.sport] || "Einheit")}</b><span class="sub">${meta(p) || "&nbsp;"}</span></div></div>`).join("")
    : L ? `${lastHtml}${L.profile ? '<div id="ck-last" class="wprofile"></div>' : ""}`
    : '<div class="hb-item"><i style="background:var(--line-2)"></i><div><b>Keine Einheit</b><span class="sub">Noch nichts erfasst</span></div></div>';
  const doneHtml = plan.length && doneToday?.load > 0 ? `<div class="hb-done">Erledigt: Load ${fmt(doneToday.load)} ${Object.keys(doneToday.sports ?? {}).map((k) => `<i style="background:var(--s-${k})" title="${SPORT_LABEL[k]}"></i>`).join("")}</div>` : "";
  const status = `<div class="hero-status coach">${coachHead(d)}${wellOk ? factsLine(R, d.today) : '<div class="hero-why">Wellness nicht abrufbar.</div>'}
    ${wellOk ? '<a class="more" href="#erholung">Erholungswerte im Detail →</a>' : ""}</div>`;
  const kpi = (label, value, badge, cls, sub = "") => `<div class="kpi"><span class="kpi-l">${label}</span><span class="kpi-v">${value}</span>${badge ? `<span class="badge ${cls}">${badge}</span>` : ""}${sub ? `<span class="sub">${sub}</span>` : ""}</div>`;
  const BR = d.baseRuns, T = d.nextTest;
  const sleepKpi = R.sleepScore != null
    ? kpi("Schlaf-Score", fmt(R.sleepScore), "", "", R.sleepHours != null ? `${fmt(R.sleepHours, 1)} h` : "")
    : kpi("Schlaf", R.sleepHours != null ? `${fmt(R.sleepHours, 1)} <small>h</small>` : "–", "", "");
  const capKpi = BR?.cap != null
    ? kpi(`HF-Deckel ${fmt(BR.cap)}`, BR.week.runs ? `${fmt(BR.week.overMin)} <small>min drüber</small>` : "–", "", "", BR.week.runs ? `diese Woche · ${BR.week.runs} Grundlagenl${BR.week.runs === 1 ? "auf" : "äufe"}, ${fmt(BR.week.minutes)} min` : "diese Woche noch kein Grundlagenlauf")
    : kpi("Fitness CTL", ctl != null ? fmt(ctl) : "–", "", "", atl != null ? `Ermüdung ATL ${fmt(atl)}` : "");
  const lastKpi = race
    ? kpi(esc(g.name), g.daysToGo === 0 ? "Heute" : `${g.daysToGo} <small>Tag${g.daysToGo === 1 ? "" : "e"}</small>`, "", "", `${weekday(g.date)} ${fmtDate(g.date)}${taper ? " · Taper" : ""}`)
    : T ? kpi("Blocktest", T.daysToGo === 0 ? "Heute" : `${T.daysToGo} <small>Tag${T.daysToGo === 1 ? "" : "e"}</small>`, "", "", `${weekday(T.date)} ${fmtDate(T.date)}`)
    // CTL steht schon in der Deckel-Kachel, wenn es keinen HF-Deckel gibt: dann hier die Frische statt derselben Zahl zweimal
    : BR?.cap != null ? kpi("Fitness CTL", ctl != null ? fmt(ctl) : "–", "", "", atl != null ? `Ermüdung ATL ${fmt(atl)}` : "")
    : kpi("Frische TSB", tsb != null ? fmt(tsb) : "–", "", "", w.tsbPct != null ? `${w.tsbPct > 0 ? "+" : ""}${fmt(w.tsbPct)} % der CTL · ${esc(TSB_ZONE_TEXT[w.tsbZone] ?? "–")}` : "");
  // Laufen getrennt von der Gesamt-TSS (Stoßbelastung, Hüfte/Leiste/Knie): Minuten der letzten 7 Tage gegen den Wochenschnitt
  // der 4 Wochen davor, als Zahl ohne Urteil.
  const runSum = (a, b) => d.daily.filter((x) => x.date >= a && x.date <= b).reduce((n, x) => n + (x.minutes?.run ?? 0), 0);
  const run7 = runSum(addDays(d.today, -6), d.today), runRef = runSum(addDays(d.today, -34), addDays(d.today, -7)) / 4;
  const runQ = runRef > 0 ? run7 / runRef : null;
  const runKpi = kpi("Laufen 7 Tage", `${fmt(run7)} <small>min</small>`, "", "", runRef > 0 ? `Ø 4 Wochen davor ${fmt(runRef)} min · ${runQ >= 1 ? "+" : ""}${fmt((runQ - 1) * 100)} %` : "kein Vergleich");
  const kpis = `<div class="kpis">
      ${sleepKpi}
      ${runKpi}
      ${capKpi}
      ${lastKpi}
    </div>`;
  // Die Kennzahlen stehen unten in der rechten Spalte: Die Coach-Spalte ist meist länger, so bleibt dort kein Leerraum.
  const today = `<div class="hero-plan"><div class="hero-k">${plan.length ? "Heute geplant" : "Letzte Einheit"}</div>${planHtml}${doneHtml}
      ${shown?.description ? `<div class="sub clamp">${esc(shown.description)}</div>` : ""}${shown?.steps?.length ? '<div id="ck-workout" class="wprofile"></div>' : ""}
      ${plan.length && L ? `<div class="hero-k" style="margin-top:8px">Letzte Einheit</div>${lastHtml}` : ""}${kpis}</div>`;
  const heuteBand = `<div class="heute"><div class="hero">${status}${today}</div>${ideaCard(d)}</div>`;

  $("cockpit").innerHTML = `${heuteBand}
    <div class="cockpit-group">Training</div>
    ${weekTile}
    ${renderWeekReview(d, tile)}
    <div class="cockpit-group">${race ? "Rennen, Block und Form" : "Block und Form"}</div>
    ${blockTile}
    ${renderBaseTrend(d, tile)}
    ${raceTile || supportTile ? `<div class="cockpit flow" style="margin-top:var(--gap)">${raceTile}${supportTile}</div>` : ""}
    ${taperTile ? `<div class="cockpit" style="margin-top:var(--gap)">${taperTile}</div>` : ""}`;

  drawBaseTrend(d);
  wireCoachAnswer();
  if (race) C.countdown($("ck-countdown"), { total: HISTORY_DAYS + g.daysToGo, elapsed: HISTORY_DAYS, raceLabel: `${fmtDate(g.date)} Rennen` });
  if (shown?.steps?.length && $("ck-workout")) C.workout($("ck-workout"), shown.steps);
  if (!plan.length && L?.profile && $("ck-last")) C.session($("ck-last"), L.profile, { sport: L.sport });
  // Schmale Ansicht: die Wochenleiste ist wischbar; gestern und heute stehen beim Laden vorn
  const wkNow = document.querySelector(".wk-grid .wk-day.now");
  if (wkNow) { const gr = wkNow.parentElement, t = wkNow.previousElementSibling ?? wkNow; gr.scrollLeft = t.offsetLeft - gr.firstElementChild.offsetLeft; }
}

/* "Greift die Grundlage?" (Worker: base-trend.js): Frühindikator zwischen den Blocktests. EF der lockeren flachen Läufe
   (Minute 5–35) und Puls bei fester Leistung in der Rad-Vergleichseinheit, je Median der ersten 3 gegen die letzten 3. */
const TREND_DIR = { better: ["besser", "ok"], worse: ["schlechter", "warn"], flat: ["gleich (Rauschen)", "none"] };
function renderBaseTrend(d, tile) {
  const T = d.baseTrend;
  if (!T) return "";
  const f2 = (v) => fmt(v, 2);
  const line = (tr, have, unit, fv, sign) => tr
    ? `<div class="sub">Median erste ${T.n}: <b>${fv(tr.first)}</b> → letzte ${T.n}: <b>${fv(tr.last)}</b> (${tr.delta > 0 ? "+" : ""}${fmt(tr.delta, 1)}${sign}) <span class="badge ${TREND_DIR[tr.dir][1]}">${TREND_DIR[tr.dir][0]}</span></div>`
    : `<div class="sub muted">Trend ab ${2 * T.n} ${unit} (bisher ${have}).</div>`;
  const runs = T.runs.length ? `<div class="bgoal"><div class="bg-top"><span class="bg-label">Laufen: Aerobe Effizienz</span></div>
      <div class="val">${f2(T.runs[T.runs.length - 1].ef)} <small>m/min je Schlag · ${fmtDate(T.runs[T.runs.length - 1].date)}</small></div>
      <div id="bt-run" class="wprofile"></div>${line(T.runTrend, T.runs.length, "lockeren Läufen", f2, " %")}
      <div class="sub muted">Lockere Läufe auf flacher Strecke, Minute 5–35 (kürzere bis Ende). Steigt die EF bei gleichem Deckel, wirkt die Grundlage.</div></div>` : "";
  const rides = T.rides.length ? `<div class="bgoal"><div class="bg-top"><span class="bg-label">Rad: Puls bei ${T.watts ? `${fmt(T.watts)} W` : "fester Leistung"}</span></div>
      <div class="val">${fmt(T.rides[T.rides.length - 1].hr)} <small>bpm · ${fmtDate(T.rides[T.rides.length - 1].date)}${T.rides[T.rides.length - 1].noWarmup ? " · ohne Einrollen" : ""}</small></div>
      <div id="bt-ride" class="wprofile"></div>${line(T.rideTrend, T.rides.filter((r) => !r.noWarmup).length, "Vergleichseinheiten mit Einrollen", (v) => fmt(v), " bpm")}
      <div class="sub muted">Mittlerer Puls im Messfenster der Vergleichseinheit. Sinkt er bei gleicher Leistung, wirkt die Grundlage – auch wenn das Decoupling schon niedrig ist.</div></div>` : "";
  return `<div class="cockpit" style="margin-top:var(--gap)">${tile("Greift die Grundlage?", `<div class="bgoals">${runs}${rides}</div>
    <details class="info"><summary>Erklärung</summary><div>Frühindikator zwischen den Blocktests, keine Entscheidung: Das Blockziel misst nur der Blocktest. Einzelne Einheiten schwanken mit Tageszeit, Wind und Müdigkeit; verglichen wird deshalb der Median der ersten ${T.n} mit dem der letzten ${T.n} Einheiten im Block. Unter 3 % (EF) bzw. 3 Schlägen ist es Rauschen. Hitzetage zählen beim Laufen nicht mit, Fahrten ohne Einrollen nicht beim Puls.</div></details>`, "span4")}</div>`;
}
function drawBaseTrend(d) {
  const T = d.baseTrend;
  if (!T) return;
  if ($("bt-run") && T.runs.length > 1) C.spark($("bt-run"), T.runs.map((r) => r.date), Object.fromEntries(T.runs.map((r) => [r.date, r.ef])), { label: "Aerobe Effizienz je Lauf" });
  if ($("bt-ride") && T.rides.length > 1) C.spark($("bt-ride"), T.rides.map((r) => r.date), Object.fromEntries(T.rides.map((r) => [r.date, r.hr])), { label: "Puls bei fester Leistung je Vergleichseinheit" });
}

/* Woche mit Wochenzielen des Coaches (Kalender, Kategorie TARGET, jeden Montag): zwei Ringe wie auf der Uhr (ZEIT gegen
   die Zeitziele, TSS gegen die Obergrenzen mit der Untergrenze als Strich; außen Rad, Mitte Lauf, innen Schwimmen, Überschuss
   rot als zweite Runde), daneben je Sportart, was noch offen ist, Ist gegen Ziel und TSS gegen den Korridor (Untergrenze–Obergrenze); Kraft in Sätzen aus dem Block-Ziel.
   Die Tage zeigen nur noch, was gemacht wurde: Ohne festen Plan gibt es kein "Plan / Ist" mehr. */
const RING_R = { bike: 46, run: 35, swim: 24 };
// TSS-Korridor als Text: "220–275", ohne Untergrenze "≤ 275"
const tssRange = (lo, hi) => (lo > 0 ? `${fmt(lo)}–${fmt(hi)}` : `≤ ${fmt(hi)}`);
// vals: je Sportart [erledigt, Vorgabe, Untergrenze?]; die Untergrenze steht als Querstrich auf dem Ring
function weekRings(vals, warn) {
  const S = 120, c = S / 2;
  let s = `<svg width="${S}" height="${S}" viewBox="0 0 ${S} ${S}" role="img">`;
  for (const k of ["bike", "run", "swim"]) {
    const v = vals[k];
    if (!v) continue;
    const r = RING_R[k], L = 2 * Math.PI * r, f = v[0] / v[1], arc = (x, col) => `<circle cx="${c}" cy="${c}" r="${r}" fill="none" style="stroke:${col}" stroke-width="9" stroke-linecap="round" stroke-dasharray="${x * L} ${L}" transform="rotate(-90 ${c} ${c})"/>`;
    s += `<circle cx="${c}" cy="${c}" r="${r}" fill="none" style="stroke:var(--s-${k})" stroke-opacity=".18" stroke-width="9"><title>${SPORT_LABEL[k]}: ${fmt(v[0])} von ${v[2] > 0 ? tssRange(v[2], v[1]) : fmt(v[1])}</title></circle>`;
    if (f > 0) s += arc(Math.min(f, 1), `var(--s-${k})`);
    if (warn && f > 1) s += arc(Math.min(f - 1, 1), "var(--bad)");
    if (v[2] > 0 && v[2] < v[1]) {
      const a = 2 * Math.PI * (v[2] / v[1]) - Math.PI / 2, pt = (d) => [c + d * Math.cos(a), c + d * Math.sin(a)].map((n) => n.toFixed(1));
      const [x1, y1] = pt(r - 6), [x2, y2] = pt(r + 6);
      s += `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" style="stroke:var(--text)" stroke-width="2" stroke-linecap="round"><title>${SPORT_LABEL[k]}: Untergrenze ${fmt(v[2])}</title></line>`;
    }
  }
  return s + "</svg>";
}
// Tempo-Hinweis: was pro verbleibendem Tag (heute mitgezählt) noch ansteht; je Sportart nur der eigene Rest,
// Überschuss in einer Sportart gleicht keine andere aus. TSS: solange eine Sportart unter ihrer Untergrenze liegt, die Summe
// der Reste bis zu den Untergrenzen, sonst die Summe der Spielräume bis zu den Obergrenzen.
function weekPace(cur, st, left, today) {
  const ks = SPORT_ORDER.filter((k) => cur.bySport[k]);
  const restMin = ks.reduce((n, k) => n + (cur.bySport[k].targetMin > 0 ? Math.max(0, cur.bySport[k].targetMin - cur.bySport[k].minutes) : 0), 0);
  const capped = ks.filter((k) => cur.bySport[k].targetLoad > 0);
  const restTss = capped.reduce((n, k) => n + Math.max(0, cur.bySport[k].targetLoad - cur.bySport[k].load), 0);
  const restFloor = capped.reduce((n, k) => n + Math.max(0, (cur.bySport[k].targetLoadMin ?? 0) - cur.bySport[k].load), 0);
  const restSets = st?.calWeek && st.weekGoal > 0 ? Math.max(0, st.weekGoal - st.calWeek.count) : null;
  const line = (v, txt) => `<div><b>≈ ${fmt(v)}</b> ${txt}</div>`;
  const lines = [
    restMin > 0 ? line(restMin / left, "min Training") : '<div class="ok">✓ Zeitziele erreicht</div>',
    restSets ? line(restSets / (st.calWeek.end ? Math.max(1, U.dayDiff(today, st.calWeek.end) + 1) : left), `${st.countUnit ?? "Sätze"} Kraft`) : "",
    capped.length ? (restFloor > 0 ? line(restFloor / left, "TSS bis Untergrenze") : restTss > 0 ? line(restTss / left, "TSS Spielraum bis Obergrenze") : '<div class="over">TSS-Obergrenze erreicht</div>') : "",
  ];
  return `<div class="wk-pace"><span>${left <= 1 ? "Heute noch" : "Pro Resttag"}</span>${lines.join("")}</div>`;
}
// Regeln des Coaches aus dem Block-Eintrag (Abschnitt "Regeln"); sie ersetzen den Tempo-Hinweis, solange der Block läuft
function blockRules(SB, d) {
  const B = d?.baseRuns, cap = B?.cap != null && B.week.runs ? `<div class="${B.week.overMin <= 2 ? "ok" : "over"}">HF-Deckel ${fmt(B.cap)}: ${fmt(B.week.overMin)} min drüber in ${B.week.runs} Grundlagenl${B.week.runs === 1 ? "auf" : "äufen"}</div>` : "";
  return `<div class="wk-pace wk-rules"><span>Regeln · ${esc(SB.name)}</span>${cap}<ul>${SB.rules.map((r) => `<li>${esc(r)}</li>`).join("")}</ul></div>`;
}
function renderTargetWeek(d, tile, cur, monday, days) {
  const ks = ["run", "bike", "swim"].filter((k) => cur.bySport[k].targetMin > 0 || cur.bySport[k].targetLoad > 0);
  const zeit = Object.fromEntries(ks.filter((k) => cur.bySport[k].targetMin > 0).map((k) => [k, [cur.bySport[k].minutes, cur.bySport[k].targetMin]]));
  const tss = Object.fromEntries(ks.filter((k) => cur.bySport[k].targetLoad > 0).map((k) => [k, [cur.bySport[k].load, cur.bySport[k].targetLoad, cur.bySport[k].targetLoadMin ?? 0]]));
  const sum = (o, i) => Object.values(o).reduce((a, v) => a + v[i], 0);
  const left = 7 - ((new Date(d.today + "T00:00:00Z").getUTCDay() + 6) % 7);
  // TSS-Spalte: Ist gegen den Korridor; über der Obergrenze rot, unter der Untergrenze gedämpft (keine Wertung)
  const cls = (cap) => (cap.over ? " over" : cap.under ? " under" : "");
  const row = (k, open, ist, cap) => `<span class="lbl"><i style="background:var(--s-${k})"></i><span>${SPORT_LABEL[k]}<em>${open}</em></span></span><span class="o">${open}</span><span class="gap"></span><span class="n">${ist[0]}</span><span class="t">${ist[1]}</span><span class="n c${cls(cap)}">${cap.html[0]}</span><span class="t${cls(cap)}">${cap.html[1]}</span>`;
  const capOf = (b) => ({ over: b.targetLoad > 0 && b.load > b.targetLoad, under: b.targetLoadMin > 0 && b.load < b.targetLoadMin, html: b.targetLoad > 0 ? [fmt(b.load), `/ ${tssRange(b.targetLoadMin, b.targetLoad)} TSS`] : ["", ""] });
  const rows = ks.map((k) => {
    const b = cur.bySport[k], rest = b.targetMin > 0 ? b.targetMin - b.minutes : null;
    return row(k, rest == null ? "" : rest > 0 ? `noch ${fmt(rest)} min` : "✓ Ziel", [fmt(b.minutes), b.targetMin > 0 ? `/ ${fmt(b.targetMin)} min` : "min"], capOf(b));
  });
  // Kraft: Sätze der Kalenderwoche gegen das Wochenziel des Blocks (wie Uhr und Kraft-Karte), sonst Minuten
  const st = d.seasonBlock?.goals?.find((g) => g.key === "strength"), sb = cur.bySport.strength;
  if (st?.calWeek && st.weekGoal > 0) {
    const rest = st.weekGoal - st.calWeek.count, unit = st.countUnit ?? "Sätze";
    rows.push(row("strength", rest > 0 ? `noch ${fmt(rest)} ${unit}` : "✓ Ziel", [fmt(st.calWeek.count), `/ ${fmt(st.weekGoal)} ${unit}${st.periodWeeks > 1 ? ` in ${st.periodWeeks} Wo` : ""}`], capOf(sb)));
  }
  const ring = (label, vals, warn, cap) => `<div class="wk-ring"><b>${label}</b>${weekRings(vals, warn)}<div>${cap}</div></div>`;
  const over = Object.values(tss).some((v) => v[0] > v[1]), under = !over && Object.values(tss).some((v) => v[2] > 0 && v[0] < v[2]);
  const top = `<div class="wk-top">${ring("ZEIT", zeit, false, `${fmt(sum(zeit, 0))} <small>/ ${fmt(sum(zeit, 1))} min</small>`)}`
    + `${ring("TSS", tss, true, `<span${over ? ' style="color:var(--bad)"' : under ? ' style="color:var(--muted)"' : ""}>${fmt(sum(tss, 0))}</span> <small>/ ${tssRange(sum(tss, 2), sum(tss, 1))} TSS</small>`)}`
    + `<div class="wk-open"><b>Offen · ${left <= 1 ? "nur noch heute" : `noch ${left} Tage`}</b>${rows.join("")}</div>${d.seasonBlock?.status === "active" && d.seasonBlock.rules?.length ? blockRules(d.seasonBlock, d) : weekPace(cur, st, left, d.today)}</div>`;
  const actual = Object.fromEntries(d.daily.map((x) => [x.date, x]));
  const cell = (x) => {
    const a = actual[x], isToday = x === d.today, past = x < d.today;
    const ks2 = SPORT_ORDER.filter((k) => (a?.minutes?.[k] ?? 0) > 0 || (a?.sports?.[k] ?? 0) > 0);
    const body = ks2.length ? ks2.map((k) => `<div class="wk-did"><i style="background:var(--s-${k})"></i><span>${SPORT_LABEL[k]} ${a.minutes?.[k] ? `${fmt(a.minutes[k])} min` : `${fmt(a.sports[k])} TSS`}</span></div>`).join("")
      : `<span class="wk-rest">${past ? "Ruhetag" : isToday ? "heute" : ""}</span>`;
    return `<div class="wk-day${isToday ? " now" : ""}${past ? " past" : ""}"><div class="wk-h"><b>${weekday(x)}</b> <span>${fmtDate(x)}</span></div><div class="wk-plan">${body}</div></div>`;
  };
  return tile(`Woche ab ${fmtDate(monday)}`, `${top}<div class="wk-grid">${days.map(cell).join("")}</div>`, "span4", ["#form", "Wochenverlauf und Vergleich"]);
}

/* Montagsbilanz (Worker: week-review.js): Fakten zur letzten vollen Woche im Vergleich zu den bis zu 4 Wochen davor –
   Gefühlswerte (vor dem Training eingetragen), Session-RPE und Monotonie, Laufumfang, HF-Deckel, Wochenrahmen des Coaches,
   Kraft, Krankheit und Beschwerden. Keine eigene Stufe und kein Vorschlag: Die Ampel oben setzt der Coach (save_coach, Feld
   week), das Dashboard zeigt sie nur – und nur, wenn sie nach dem Ende dieser Woche gesetzt wurde. */
const WEEK_LIGHT = { gruen: ["Grün", "ok"], gelb: ["Gelb", "warn"], rot: ["Rot", "bad"] };
function renderWeekReview(d, tile) {
  const V = d.weekReview;
  if (!V) return "";
  const f1 = (v) => fmt(v, 1);
  const row = (label, value, sub = "") => `<div class="rv-row"><span class="rv-l">${label}</span><span class="rv-v">${value}</span><span class="sub">${sub}</span></div>`;
  const feel = Object.values(V.feel).map((x) => `${x.label} ${f1(x.mean)}${x.ref != null ? ` (davor ${f1(x.ref)})` : ""}`).join(" · ");
  const R = V.rpe, U = V.run, st = V.strength;
  const days = (xs) => xs.map((x) => `${weekday(x)} ${fmtDate(x)}`).join(", ");
  const wl = d.coach?.week, light = wl?.createdAt && berlinDay(wl.createdAt) > V.weekEnd ? WEEK_LIGHT[wl.light] : null;
  const rows = [
    light ? row("Coach", `<span class="badge ${light[1]}">${light[0]}</span>`, wl.text ? esc(wl.text) : "") : "",
    feel ? row("Gefühl", feel, "Wochenschnitt, 1 = bestmöglich, vor dem Training eingetragen") : "",
    // Session-RPE nur bei genug RPE-Einträgen; sonst wäre die Summe gegen den Schnitt davor ein Zufallsvergleich
    R.basis === "rpe" ? row("Session-RPE", fmt(R.srpe), [R.ref != null ? `Ø davor ${fmt(R.ref)}` : null, `RPE bei ${fmt(R.coverage * 100)} % der Einheiten`, R.monotony != null ? `Monotonie ${f1(R.monotony)}` : null].filter(Boolean).join(" · "))
      : R.monotony != null ? row("Monotonie", f1(R.monotony), `aus dem Tages-TSS${R.coverage != null ? ` (RPE nur bei ${fmt(R.coverage * 100)} % der Einheiten)` : ""}`) : "",
    row("Laufen", `${fmt(U.minutes)} min`, [U.ref != null ? `Ø 4 Wochen ${fmt(U.ref)} min${U.ratio != null ? ` (${U.ratio >= 1 ? "+" : ""}${fmt((U.ratio - 1) * 100)} %)` : ""}` : null, U.longest ? `längster ${fmt(U.longest)} min${U.longestRef ? ` (davor ${fmt(U.longestRef)})` : ""}` : null, U.capOver != null ? `${fmt(U.capOver)} min über HF-Deckel` : null].filter(Boolean).join(" · ")),
    V.frameShare != null ? row("Rahmen", `${fmt(V.frameShare * 100)} %`, V.frame.map((x) => `${SPORT_LABEL[x.sport]} ${fmt(x.minutes)}${x.targetMin ? `/${fmt(x.targetMin)}` : ""} min${x.targetLoad ? ` · ${fmt(x.load)}/${tssRange(x.targetLoadMin, x.targetLoad)} TSS` : ""}`).join(" · ")) : "",
    st ? row("Kraft", `${fmt(st.count)} <small>von ${fmt(st.goal)}</small>`, st.unit ?? "") : "",
    V.sickDays.length ? row("Krank", `${V.sickDays.length} Tag${V.sickDays.length === 1 ? "" : "e"}`, days(V.sickDays)) : "",
    V.hipDays.length ? row("Beschwerden", "Hüfte/Leiste/Knie", days(V.hipDays)) : "",
  ].join("");
  return `<div class="cockpit" style="margin-top:var(--gap)">${tile(`Montagsbilanz · Woche ab ${fmtDate(V.weekStart)}`,
    `<div class="rv-rows">${rows}</div>
    <details class="info"><summary>Erklärung</summary><div>Fakten zur letzten vollen Woche, verglichen mit den bis zu 4 vollen Wochen davor. Session-RPE = RPE × Minuten (erst ab 70 % der Einheiten mit RPE), Monotonie = Tagesschnitt ÷ Standardabweichung über 7 Tage. Rahmen = Ist gegen die Zeitziele und TSS-Korridore (Untergrenze–Obergrenze) des Coaches. Die Ampel („Coach“) ist das Urteil des Coaches zu dieser Woche, er setzt sie montags; das Dashboard berechnet sie nicht. Wie die neue Woche aussieht, entscheidet der Coach.</div></details>`, "span4")}</div>`;
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
  if (cur && (cur.targetMin > 0 || cur.targetLoad > 0)) return renderTargetWeek(d, tile, cur, monday, days);
  const cell = (x) => {
    const items = planned.filter((p) => p.date === x), isToday = x === d.today, past = x < d.today, a = actual[x];
    const planLoad = items.reduce((n, p) => n + (p.load ?? 0), 0), ist = a?.load ?? 0;
    const state = !past ? "" : planLoad > 0 ? (ist >= planLoad * WEEK_HIT ? "hit" : "miss") : ist > 0 ? "extra" : "";
    const lines = items.map((p) => `<div class="wk-item"><i style="background:var(--s-${p.sport})"></i><span>${esc(p.name || SPORT_LABEL[p.sport] || "Einheit")}<small>${p.durationMin ? p.durationMin + " min" : p.distanceKm ? fmt(p.distanceKm, 1) + " km" : ""}</small></span></div>`).join("");
    const done = Object.keys(a?.sports ?? {}).map((k) => `<i style="background:var(--s-${k})" title="${SPORT_LABEL[k]} ${fmt(a.sports[k])} Load"></i>`).join("");
    const istTxt = isToday || past ? (ist > 0 ? `<b>${fmt(ist)}</b>` : "–") : "";
    const verdict = { hit: "erfüllt", miss: planLoad > 0 && ist === 0 ? "ausgefallen" : "unter Plan", extra: "ungeplant" }[state];
    return `<div class="wk-day${isToday ? " now" : ""}${past ? " past" : ""} ${state}"><div class="wk-h"><b>${weekday(x)}</b> <span>${fmtDate(x)}</span></div><div class="wk-plan">${lines || '<span class="wk-rest">frei</span>'}</div><div class="wk-nums"><span>Plan <b>${planLoad ? fmt(planLoad) : "–"}</b></span><span>Ist ${istTxt || "–"} ${done}</span>${verdict ? `<span class="wk-v">${verdict}</span>` : ""}</div></div>`;
  };
  const planSum = cur?.plannedLoad ?? planned.reduce((n, p) => n + (p.load ?? 0), 0);
  const parts = cur ? SPORT_ORDER.filter((k) => cur.bySport[k].load > 0) : [];
  const pct = cur && planSum ? Math.min(100, (cur.load / planSum) * 100) : 0;
  const head = cur ? `<div class="wk-head"><div class="val">${fmt(cur.load)} <small>Load${planSum ? ` von ${fmt(planSum)} geplant` : ""}</small></div>${planSum ? `<div class="cbar wk-bar"><div class="cfill" style="width:${pct}%;background:var(--accent)"></div></div>` : ""}<div class="dots">${parts.map((k) => `<span><i style="background:var(--s-${k})"></i>${SPORT_LABEL[k]} ${fmt(cur.bySport[k].load)}</span>`).join("") || '<span class="sub">noch nichts trainiert</span>'}<span class="sub">${fmt(SPORT_ORDER.reduce((n, k) => n + cur.bySport[k].minutes, 0) / 60, 1)} h${cur.runMin ? ` · Laufen ${fmt(cur.runMin / 60, 1)} h` : ""}</span></div></div>` : "";
  return tile(`Woche ab ${fmtDate(monday)}`, `${head}<div class="wk-grid">${days.map(cell).join("")}</div>`, "span4", ["#form", "Wochenverlauf und Vergleich"]);
}

/* ---------- 2 · Rennplan: Zielzeiten, Splits, Verpflegung, Checkliste ---------- */
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
  $("checklist").innerHTML = items.map((t, i) => `<label class="chk"><input type="checkbox" data-i="${i}"${done[i] ? " checked" : ""}><span>${esc(t)}</span></label>`).join("");
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
  const head = total ? `<div class="big2">${hm(total)}</div><div class="muted">Gesamtziel</div>`
    : all ? `<div class="big2">${hm(sum)}</div><div class="muted">Vorschlag Summe (ohne Wechselzeiten, kein Gesamtziel hinterlegt)</div>`
    : '<div class="muted">Kein Gesamtziel hinterlegt.</div>';
  $("staffel").innerHTML = `${head}
    <div class="scroll"><table class="mini" style="margin-top:8px"><thead><tr><th></th><th>Zeit</th><th>Pace / Leistung</th></tr></thead><tbody>${rows.map((x) => `<tr><td><b>${x[0]}</b></td><td>${x[1]}</td><td>${x[2]}</td></tr>`).join("")}</tbody></table></div>
    ${all && total ? `<div class="${sum + 120 > total ? "notice" : "muted"}" style="margin-top:8px">Summe der Disziplinen ${fmtTime(sum)} ${sum > total ? `liegt ${hm(sum - total)} über dem Gesamtziel` : `plus Wechselzeiten unter dem Gesamtziel`}.</div>` : !all ? '<div class="muted" style="margin-top:8px">Für Vorschläge fehlen Schwimmschwelle, FTP oder VDOT. Alternativ in die Beschreibung des Rennens schreiben, z. B. „Schwimmen 40:00“, „Rad 3:00:00“, „Lauf 1:55:00“.</div>' : ""}
    <details class="info"><summary>Erklärung</summary><div>Vorschläge sind Modellwerte (Schwimmschwelle, FTP mit Leistungsmodell, Lauf aus VDOT plus 6 % fürs Laufen nach dem Rad), kein Trainingsplan; Einträge in der Beschreibung haben Vorrang. Wechselzeiten sind nicht eingerechnet.</div></details>`;

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
  $("staffel").innerHTML = `${hasRange ? `<div class="big2">${hm(sc.lo)}–${hm(sc.hi)}</div><div class="muted">Realistische Spanne (aus VDOT und 10-km-Bestzeit${sc.noLong ? ", Obergrenze +3 %, da kein Lauf ab 16 km in 8 Wochen" : ""})</div>` : '<div class="muted">Keine Runalyze-Werte für eine Spanne – nur das Ziel ist hinterlegt.</div>'}
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

/* ---------- 3 · Form: Grundlagenläufe ab 60 min ----------
   Aus den Zeitreihen (Worker: block-inputs.js): Dauer statt km (der lange Lauf wächst in Minuten, die Pace ist durch den
   HF-Deckel bewusst langsamer), Decoupling der ersten 60 min, stabile Dauer, Minuten über dem Deckel und DFA alpha1 (Runalyze, nur Hinweis ohne Ampel, nicht validiert).
   Blocktests sind markiert; nur sie zählen fürs Blockziel, die anderen sind Trend. */
function renderLongruns(d) {
  const host = $("longruns"), B = d.baseRuns;
  if (!d.sources.intervalsActivities.ok) { host.innerHTML = '<div class="muted">Aktivitäten nicht abrufbar.</div>'; return; }
  const decCls = (v) => (v == null ? "none" : v <= 5 ? "ok" : v <= 8 ? "warn" : "bad");
  const next = d.nextTest?.date;
  if (!B?.recent?.length) { host.innerHTML = `<div class="muted">Noch kein Grundlagenlauf ab 60 min mit Pulsdaten im Block${next ? ` – nächster Blocktest ${weekday(next)} ${fmtDate(next)}` : "."}</div>`; return; }
  const rows = B.recent.map((r) => `<tr><td>${fmtDate(r.date)}${r.test ? ' <span class="badge ok">Test</span>' : ""}</td><td>${fmt(r.durationMin)} min</td><td>${r.dec60 != null ? `<span class="badge ${decCls(r.dec60)}">${fmt(r.dec60, 1)} %</span>` : "–"}</td><td title="${esc((r.windows ?? []).map((x) => `0–${x.min}: ${fmt(x.value, 1)} %`).join(" · "))}">${fmtStable(r.stableMin)}</td><td>${r.pace60 ?? "–"}</td><td>${r.overCapMin != null ? `${fmt(r.overCapMin)} min` : "–"}</td></tr>`).join("");
  host.innerHTML = `<div class="scroll"><table class="mini tests"><thead><tr><th>Datum</th><th>Dauer</th><th>Dec. 0–60</th><th>stabil</th><th>Pace 0–60</th><th>über ${B.cap != null ? fmt(B.cap) : "Deckel"}</th></tr></thead><tbody>${rows}</tbody></table></div>
    <details class="info"><summary>Erklärung</summary><div>Decoupling 0–60: Pace je Herzschlag der ersten gegen die zweite halbe Stunde, ab 5 % driftet der Puls, ab 8 % deutlich. Stabil: längstes Fenster ab Start (0–60, 0–75 …) mit Decoupling ≤ 5 % (Fensterreihe beim Darüberfahren). Über ${B.cap != null ? fmt(B.cap) : "dem Deckel"}: Minuten über dem HF-Deckel (Hügelsprints dürfen kurz darüber). DFA α1 steht hier nicht mehr: Der Wert passt bei dir nicht (0,5 bei HF 125, obwohl du dort locker redest) und entscheidet nichts. Nur die Blocktests zählen fürs Blockziel; den Trend zwischen den Tests zeigt „Greift die Grundlage?“.</div></details>`;
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
    '<details class="info"><summary>Erklärung</summary><div>Fade: Pace der zweiten Hälfte minus erste Hälfte der Wiederholungen. Bis +3 s/km stabil, ab +8 s/km deutlicher Einbruch.</div></details>';
}

/* ---------- 3 · Form: Verlauf ---------- */
function renderHistory(d) {
  // Trainingszeit aller Sportarten gegen die Zeitziele des Coaches, wo es welche gibt, sonst gegen die geplanten Workouts
  const sumMin = (w, f) => SPORT_ORDER.reduce((a, k) => (w.bySport[k][f] != null ? (a ?? 0) + w.bySport[k][f] : a), null);
  const timePlan = (w) => w.targetMin ?? sumMin(w, "plannedMin");
  C.bars($("chart-km"), d.weeks.map((w) => ({ label: fmtDate(w.weekStart), tip: `Woche ab ${fmtDate(w.weekStart)}`, value: sumMin(w, "minutes") ?? 0, plan: timePlan(w), partial: !w.complete })), { unit: "min", label: "Trainingszeit pro Woche (Minuten)", xLabel: "Woche ab (Montag) · helle Säule = laufende Woche", valueLabels: true, dec: 0 });
  // TSS daneben: Korridor des Coaches (Summe der Unter- und Obergrenzen), in älteren Wochen der Plan aus geplanten Workouts
  const capOf = (w) => SPORT_ORDER.reduce((a, k) => (w.bySport[k].targetLoad > 0 ? (a ?? 0) + w.bySport[k].load : a), null);
  C.bars($("chart-tss"), d.weeks.map((w) => {
    const cap = w.targetLoad > 0;
    return { label: fmtDate(w.weekStart), tip: `Woche ab ${fmtDate(w.weekStart)}`, value: w.load ?? 0, plan: cap ? w.targetLoad : w.plannedLoad, cap, floor: cap && w.targetLoadMin > 0 ? w.targetLoadMin : undefined, capValue: cap ? capOf(w) ?? 0 : undefined, partial: !w.complete };
  }), { unit: "TSS", label: "TSS pro Woche", xLabel: "Woche ab (Montag) · helle Säule = laufende Woche", valueLabels: true, dec: 0 });
  const noPlan = d.weeks.every((w) => timePlan(w) == null);
  $("plan-note-km").textContent = noPlan ? "Für diesen Zeitraum gibt es weder Zeitziele noch geplante Workouts mit Dauer – deshalb keine Marke." : "Trainingszeit = Bewegungszeit aller Sportarten in Minuten. Marke = Zeitziel des Coaches (Summe der Zeitziele aller Sportarten), in älteren Wochen der Plan aus geplanten Workouts. Unter dem Datum die Abweichung: grün bis 15 %, sonst gelb.";
  $("plan-note-tss").textContent = "Marke = Summe der TSS-Obergrenzen des Coaches (in älteren Wochen der Plan), dünne Marke = Summe der Untergrenzen. Die Säule enthält alle Sportarten; „drüber“ und „unter“ zählen nur die Sportarten mit Korridor (Schwimmen hat keinen).";

  // Formkurve bis zum Renntag (nur wenn er in den nächsten 4 Wochen liegt)
  // Zonen in % der aktuellen Fitness wie Intervals.icu: unter −30 % hohes Risiko, sonst normaler Trainingsbereich
  const ctlNow = d.summary.load.ctl ?? 0, riskAt = -0.3 * ctlNow;
  const ctl = {}, atl = {};
  const series = d.formSeries?.length ? d.formSeries : d.wellness;
  for (const w of series) { ctl[w.date] = w.ctl; atl[w.date] = w.atl; }
  const first = d.formSeries?.length ? addDays(d.today, -(FORM_DAYS - 1)) : addDays(d.today, -(HISTORY_DAYS - 1));
  const raceSoon = hasRace(d.goal) && d.goal.daysToGo <= 28;
  const proj = d.formProjection ?? [], pctl = {}, patl = {};
  for (const p of proj) { pctl[p.date] = p.ctl; patl[p.date] = p.atl; }
  const end = proj.length ? proj[proj.length - 1].date : raceSoon ? d.goal.date : d.today;
  C.form($("form-chart"), {
    days: dayRange(first, end), ctl, atl, proj: proj.length ? { ctl: pctl, atl: patl } : null, projLabel: d.formProjectionSource === "target" ? "grau hinterlegt: Prognose aus den Wochenzielen (Mitte der TSS-Korridore, Woche bleibt gleich)" : null, events: formEvents(d, series, first),
    zones: [{ from: riskAt, to: 200, cls: "ok" }, { from: -200, to: riskAt, cls: "bad" }],
    marks: [{ day: d.today, label: "heute", color: "var(--muted)", anchor: raceSoon ? "end" : "middle" }, ...(raceSoon ? [{ day: d.goal.date, label: "Renntag", color: "var(--accent)", anchor: "start" }] : [])],
  });
  if (!series.some((w) => w.ctl != null)) $("form-chart").insertAdjacentHTML("beforeend", '<div class="notice">Keine CTL/ATL-Werte im Zeitraum.</div>');
  // Frische als Wochengröße (früher im Heute-Band): läuft der Erholung hinterher und fließt nicht in die Tagesampel
  const L = d.summary.load;
  $("form-now").innerHTML = L.tsb != null ? `Heute: TSB ${fmt(L.tsb)}${L.tsbPct != null ? ` (${L.tsbPct > 0 ? "+" : ""}${fmt(L.tsbPct)} % der CTL, ${esc(TSB_ZONE_TEXT[L.tsbZone] ?? "–")})` : ""} · CTL ${fmt(L.ctl)} · ATL ${fmt(L.atl)}.` : "";
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
  // Intensität: Zeitanteile locker (Z1-2), mittel (Z3-4), hart (Z5+) wie die Polarisation in Intervals
  const INT = { easy: "locker (Z1–2)", mid: "mittel (Z3–4)", hard: "hart (Z5+)" };
  const iw = d.weeks.map((w) => ({ weekStart: w.weekStart, label: fmtDate(w.weekStart), by: w.intensity || { easy: 0, mid: 0, hard: 0 } }));
  C.shares($("chart-intensity"), iw, Object.keys(INT), INT, { prefix: "i", unit: "min", label: "Intensität je Woche nach Zonenzeit", end: "100 % der Zonenzeit" });
  $("intensity-legend").innerHTML = Object.entries(INT).map(([k, t]) => `<span><i style="background:var(--i-${k})"></i>${t}</span>`).join("");
  $("sport-legend").innerHTML = used.map((k) => `<span><i style="background:var(--s-${k})"></i>${SPORT_LABEL[k]}</span>`).join("");
  const cur = d.weeks[d.weeks.length - 1], done = [...d.weeks].reverse().find((w) => w.complete), prev = done ? d.weeks[d.weeks.indexOf(done) - 1] : null;
  // Spalte "Ziel": Wochenziel des Coaches (Zeit, TSS-Obergrenze); ohne Ziel der Plan-Load der geplanten Workouts
  const goalCell = (b, hasTg) => {
    if (!hasTg) return b.plannedLoad != null ? fmt(b.plannedLoad) : "–";
    const parts = [b.targetMin > 0 ? `${fmt(b.targetMin)} min` : null, b.targetLoad > 0 ? `${tssRange(b.targetLoadMin, b.targetLoad)} TSS` : null].filter(Boolean);
    return parts.length ? parts.join(" · ") : "–";
  };
  const rows = (w, ref) => {
    const hasTg = w.targetMin != null || w.targetLoad != null;
    return SPORT_ORDER.filter((k) => w.bySport[k].count > 0 || (ref && ref.bySport[k].count > 0) || w.bySport[k].plannedLoad != null || w.bySport[k].targetMin != null || w.bySport[k].targetLoad != null).map((k) => {
      const b = w.bySport[k], r = ref ? ref.bySport[k] : null, over = b.targetLoad > 0 && b.load > b.targetLoad, under = b.targetLoadMin > 0 && b.load < b.targetLoadMin;
      return `<tr><td>${SPORT_LABEL[k]}</td><td>${b.count}</td><td>${fmt(b.minutes / 60, 1)} h</td><td><b${over ? ' style="color:var(--bad)"' : under ? ' style="color:var(--muted)"' : ""}>${fmt(b.load)}</b></td><td class="muted">${r ? fmt(r.load) : "–"}</td><td class="muted">${goalCell(b, hasTg)}</td></tr>`;
    }).join("");
  };
  const tgOf = (w) => w.targetMin != null || w.targetLoad != null;
  const table = (w, ref, title) => {
    if (!w) return "";
    const sum = (x) => SPORT_ORDER.reduce((a, k) => a + x.bySport[k].load, 0), body = rows(w, ref);
    return `<h3>${title} <span class="muted">ab ${fmtDate(w.weekStart)}</span></h3>` + (body ? `<table><thead><tr><th>Sport</th><th>Einh.</th><th>Zeit</th><th>TSS</th><th>Vorwoche</th><th>${tgOf(w) ? "Ziel" : "Plan"}</th></tr></thead><tbody>${body}<tr><td><b>Gesamt</b></td><td></td><td><b>${fmt(SPORT_ORDER.reduce((a, k) => a + w.bySport[k].minutes, 0) / 60, 1)} h</b></td><td><b>${fmt(sum(w))}</b></td><td class="muted">${ref ? fmt(sum(ref)) : "–"}</td><td class="muted">${tgOf(w) ? (w.targetMin ? `${fmt(w.targetMin)} min` : "–") : w.plannedLoad != null ? fmt(w.plannedLoad) : "–"}</td></tr></tbody></table>` : '<div class="muted">Keine Einheiten in dieser Woche.</div>');
  };
  // Laufende Woche ohne Einheit: eine Zeile statt einer Tabelle voller Nullen
  const curEmpty = cur && cur !== done && SPORT_ORDER.every((k) => cur.bySport[k].count === 0);
  const curHtml = curEmpty
    ? `<h3>Laufende Woche <span class="muted">ab ${fmtDate(cur.weekStart)}</span></h3><div class="muted">Woche läuft – noch keine Einheit${cur.targetMin ? `, Ziel ${fmt(cur.targetMin)} min` : cur.plannedLoad != null ? `, ${fmt(cur.plannedLoad)} TSS geplant` : ""}.</div>`
    : table(cur, done, "Laufende Woche");
  $("week-report").innerHTML = table(done, prev, "Letzte volle Woche") + '<div style="height:12px"></div>' + curHtml;
}

/* ---------- 4 · Leistung ---------- */
function renderFitness(d) {
  const r = d.runalyze;
  // VDOT, Daniels-Paces und Prognosen nur mit kommendem Rennen: im Grundlagenblock widersprechen die Paces dem HF-Deckel
  const show = hasRace(d.goal);
  $("vdot-card").hidden = !show;
  if (!show) { for (const id of ["race-pred-card", "pace-zones-card", "hm-trend-card"]) $(id).hidden = true; return; }
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
    $("race-pred").innerHTML = `<table class="mini"><tbody>${est}</tbody></table><details class="info"><summary>Erklärung</summary><div>Von/bis = schnellste und langsamste Schätzung aus Runalyze-Prognose, aktuellem VDOT und 10-km-Bestzeit. Das ist eine Rechnung nach Daniels, kein Ergebnis.</div></details>`;
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
  $("hm-trend").innerHTML = `${pair}${table}${first}<div id="hm-trend-chart" style="margin-top:8px"></div><details class="info"><summary>Erklärung</summary><div>Linien fallen = Prognose wird schneller. Die Prognose ist eine Rechnung von Runalyze aus deinen Läufen, kein Ergebnis.</div></details>`;
  if (rows.length > 1) C.deltaTrend($("hm-trend-chart"), rows, { label: "Veränderung der Prognose", series: SERIES });
}

/* ---------- 4 · Leistung: Disziplinen (Rad, Schwimmen, Laufen) ----------
   Je Disziplin Umfang, letzte Einheit und Entwicklung der letzten Wochen. Nur sichtbar, wenn ein Triathlon-Ziel
   besteht oder in der Disziplin trainiert wurde. Datenbasis sind die Wochen- und Tageswerte der Seite. */
const DISC = { bike: { label: "Rad" }, swim: { label: "Schwimmen" }, run: { label: "Laufen" } };
function renderDisciplines(d) {
  const host = $("disciplines"), tri = d.goal.triathlon, used = Object.keys(DISC).filter((k) => tri || d.weeks.some((w) => w.bySport[k].count > 0));
  $("disciplines-head").hidden = host.hidden = !used.length;
  if (!used.length || !d.sources.intervalsActivities.ok) { host.innerHTML = ""; return; }
  const t = d.thresholds, tg = tri?.targets ?? {};
  const sum = (ws, k, f) => ws.reduce((n, w) => n + w.bySport[k][f], 0);
  host.innerHTML = used.map((k) => {
    const cfg = DISC[k], recent = d.weeks.slice(-4);
    const minutes4 = sum(recent, k, "minutes"), n4 = sum(recent, k, "count");
    const lastDay = [...d.daily].reverse().find((x) => x.sports[k] != null);
    const ago = lastDay ? U.dayDiff(lastDay.date, d.today) : null;
    const thr = k === "bike" ? (t.bike.ftp != null ? `FTP ${fmt(t.bike.ftp)} W` : "FTP fehlt")
      : k === "run" ? (t.run.thresholdPaceSecPerKm != null ? `Schwellenpace ${paceLabel(t.run.thresholdPaceSecPerKm)} min/km${d.runalyze?.vdot != null ? ` · VDOT ${fmt(d.runalyze.vdot, 1)}` : ""}` : "Schwellenpace fehlt")
      : t.swim.thresholdPaceSecPer100m != null ? `Schwellenpace ${paceLabel(t.swim.thresholdPaceSecPer100m)} min/100 m` : "Schwellenpace fehlt";
    const raceKm = { bike: tri?.bikeKm, swim: tri?.swimKm, run: tri?.runKm }[k];
    const tgt = tg[k]?.timeSecs ? `Rennziel ${fmtTime(tg[k].timeSecs)} über ${fmt(raceKm, k === "swim" ? 2 : k === "run" ? 1 : 0)} km` : null;
    const lastTxt = lastDay ? `${weekday(lastDay.date)} ${fmtDate(lastDay.date)} · ${ago === 0 ? "heute" : `vor ${ago} Tag${ago === 1 ? "" : "en"}`} · ${fmt(lastDay.sports[k])} TSS` : "keine Einheit in den letzten 8 Wochen";
    const stale = tri && (ago == null || ago > 14);
    return `<div class="card"><h3>${cfg.label}</h3>
      <div class="disc-stats"><div><div class="muted">4 Wochen</div><b class="v">${fmt(minutes4 / 60, 1)} h</b><div class="muted">${n4} Einh.</div></div>
      <div><div class="muted">Zuletzt</div><b class="v">${lastDay ? (ago === 0 ? "heute" : `vor ${ago} T`) : "–"}</b><div class="muted">${stale ? '<span style="color:var(--warn)">lange her</span>' : "&nbsp;"}</div></div></div>
      <div class="discbars" id="disc-${k}"></div>
      <div class="muted">Letzte Einheit: ${lastTxt}</div><div class="muted">${thr}${tgt ? ` · ${tgt}` : ""}</div></div>`;
  }).join("");
  for (const k of used) C.miniBars($(`disc-${k}`), d.weeks.map((w) => ({ label: fmtDate(w.weekStart), value: w.bySport[k].minutes / 60, partial: !w.complete })), { label: `${DISC[k].label}: Stunden je Woche`, color: `var(--s-${k})`, unit: "h", dec: 1 });
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
  rows.push(row(`<i style="background:var(--s-swim)"></i>Schwimmen · ${fmt(g.triathlon.swimKm, 2)} km`, sw, sw ? fmtTime(sw.timeSecs) : "–", sw ? `${paceLabel(sw.pacePer100m)} min/100 m` : missing("Schwellenpace fehlt")));
  const b = tg.bike;
  const bikeDetail = !b ? missing("FTP fehlt")
    : [b.watts ? `${b.wattsLow ? `${fmt(b.wattsLow)}–${fmt(b.wattsHigh)}` : fmt(b.watts)} W` : "", b.pctFtp ? `${fmt(b.pctFtp * 100)} % FTP` : "", b.speedKmh ? `${fmt(b.speedKmh, 1)} km/h` : ""].filter(Boolean).join(" · ");
  rows.push(row(`<i style="background:var(--s-bike)"></i>Rad · ${fmt(g.triathlon.bikeKm)} km`, b, b?.timeSecs ? fmtTime(b.timeSecs) : "–", bikeDetail));
  const r = tg.run;
  rows.push(row(`<i style="background:var(--s-run)"></i>Laufen · ${fmt(g.triathlon.runKm, 1)} km`, null, r ? fmtTime(r.timeSecs) : "–", r ? `${paceLabel(r.pacePerKm)} min/km` : missing("in der Beschreibung ergänzen: „Lauf 1:55:00“")));
  if (tg.totalTargetSecs) rows.push(row("Gesamtziel", null, fmtTime(tg.totalTargetSecs), "", "tri-total"));
  return `<div class="card"><h3>Triathlon-Ziele · ${esc(g.name.replace("Triathlon ", ""))}</h3>${rows.join("")}<details class="info"><summary>Erklärung</summary><div>Vorschläge aus FTP und Schwimmschwelle (Faustwerte). Eigene Ziele in die Beschreibung des Rennens schreiben, z. B. „Schwimmen 40:00“, „Rad 215 W“ oder „Rad 3:00:00“, „Lauf 1:55:00“.</div></details></div>`;
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
  // Band = mittlere 50 % der eigenen Werte im Zeitraum (wie das graue Band der Bereitschaftskarte); letzter Wert als Zahl
  const quart = (arr, q) => { const a = [...arr].sort((x, y) => x - y), i = (a.length - 1) * q, lo = Math.floor(i); return a[lo] + (a[Math.min(a.length - 1, lo + 1)] - a[lo]) * (i - lo); };
  for (const [id, key, unit, label, dec] of [["rhr", "restingHR", "bpm", "Ruhepuls", 0], ["sleep", "sleepHours", "h", "Schlafdauer", 1], ["hrv", "hrv", "ms", "HRV", 0]]) {
    const vals = Object.fromEntries(d.wellness.map((w) => [w.date, w[key]]));
    const have = days.filter((x) => vals[x] != null);
    if (!have.length) { $(id).innerHTML = `<div class="muted">Keine ${label}-Werte im Zeitraum.</div>`; continue; }
    const lastDay = have[have.length - 1], nums = have.map((x) => vals[x]), q1 = quart(nums, 0.25), q3 = quart(nums, 0.75);
    C.gapLine($(id), days, vals, { unit, label, dec, last: { date: lastDay, value: vals[lastDay] }, band: nums.length >= 5 ? { lo: q1, hi: q3, label: "üblicher Bereich", tip: `Mittlere 50 % deiner Werte: ${fmt(q1, dec)}–${fmt(q3, dec)} ${unit}` } : null });
    $(id).insertAdjacentHTML("afterbegin", `<div><b class="num-l">${fmt(vals[lastDay], dec)}</b> <span class="muted">${unit} · ${lastDay === d.today ? "heute" : fmtDate(lastDay)}${nums.length >= 5 ? ` · üblich ${fmt(q1, dec)}${fmt(q1, dec) === fmt(q3, dec) ? "" : "–" + fmt(q3, dec)}` : ""}</span></div>`);
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
  $("weight").insertAdjacentHTML("afterbegin", `<div class="wt-top"><div><div class="muted">7-Tage-Schnitt</div><b class="num-l">${fmt(avg7[lastDay], 1)} kg</b></div>
    <div><div class="muted">Gewicht, ${dKg != null ? `seit ${fmtDate(before)}` : "Verlauf"}</div><b class="num-l">${dKg != null ? sgn(dKg, 1) + " kg" : "–"}</b></div>
    <div><div class="muted">Kalorien gegen Ziel${both.length ? `, ${both.length} Tage` : ""}</div><b class="num-l">${bal != null ? sgn(bal) + " kcal" : "–"}</b></div></div>`);
  $("weight").insertAdjacentHTML("beforeend", '<details class="info"><summary>Erklärung</summary><div>Dünne Linie = Tageswert, dicke Linie = 7-Tage-Schnitt. Kalorien gegen das Yazio-Ziel nur an Tagen mit Eintrag und ohne heute; das Ziel ist kein Verbrauch, die Zahl also keine exakte Energiebilanz.</div></details>');
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
  // Noch nichts gegessen: ein ruhiger Hinweis mit den Tageszielen statt vier leerer Balken
  if (rows.every((r) => r[1] == null)) {
    const goals = rows.filter((r) => r[2]).map(([label, , goal, unit]) => `${label} ${fmt(goal)} ${unit}`).join(" · ");
    $("n-today").innerHTML = `<h3>Heute · ${weekday(t)} ${fmtDate(t)}</h3><div class="nempty">Heute noch nichts eingetragen.${goals ? `<br><small>Tagesziele: ${goals}</small>` : ""}</div><div class="muted">Stand des letzten Syncs (alle 15 Minuten, Yazio).</div>`;
    return;
  }
  $("n-today").innerHTML = `<h3>Heute · ${weekday(t)} ${fmtDate(t)}</h3>${rows.map(bar).join("")}<details class="info"><summary>Erklärung</summary><div>Stand des letzten Syncs (alle 15 Minuten, Yazio). Schwarze Marke = Tagesziel, gestrichelte Marke = Soll bis jetzt (linear von ${NUT_DAY.from} bis ${NUT_DAY.to} Uhr, grobe Orientierung).</div></details>`;
}

/* Übersicht der letzten 7 Tage: Tageswerte gegen das Yazio-Ziel als Zahlen, ohne Bewertung (die macht der Coach).
   Heute läuft noch und zählt nicht in den Schnitt. */
function renderNutritionWeek(d, map) {
  const days = dayRange(addDays(d.today, -6), d.today);
  const kcal = map("calories"), goal = map("calorieGoal"), prot = map("protein"), carbs = map("carbs"), fat = map("fat");
  const cravByDay = {}, ng = d.nutritionGoals ?? {};
  for (const c of d.cravings) (cravByDay[c.date] ??= []).push(c);
  const hasCrav = d.cravings.length > 0; // HH-Spalte nur, wenn überhaupt Einträge gepflegt werden
  const rows = days.map((x) => {
    const k = kcal[x], g = goal[x], isToday = x === d.today;
    const diff = k != null && g != null ? k - g : null;
    const flag = isToday ? (k != null ? "läuft noch" : "") : k == null ? "kein Tagebuch" : "";
    const cr = cravByDay[x]?.length ?? 0;
    return `<tr><td>${weekday(x)} ${fmtDate(x)}</td><td>${k != null ? fmt(k) : "–"}</td><td>${g != null ? fmt(g) : "–"}</td><td>${diff != null ? (diff > 0 ? "+" : "") + fmt(diff) : "–"}</td><td>${prot[x] != null ? fmt(prot[x]) : "–"}</td><td>${carbs[x] != null ? fmt(carbs[x]) : "–"}</td><td>${fat[x] != null ? fmt(fat[x]) : "–"}</td>${hasCrav ? `<td>${cr ? cr + "×" : ""}</td>` : ""}<td>${flag ? `<span class="muted">${flag}</span>` : ""}</td></tr>`;
  });
  const done = days.filter((x) => x !== d.today && kcal[x] != null);
  const avg = (m) => { const v = done.map((x) => m[x]).filter((v) => v != null); return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; };
  const gDone = done.filter((x) => goal[x] != null);
  const bal = gDone.length ? gDone.reduce((n, x) => n + kcal[x] - goal[x], 0) : null;
  $("n-week").innerHTML = `<h3>Letzte 7 Tage</h3>
    <div style="overflow-x:auto"><table><thead><tr><th>Tag</th><th>kcal</th><th>Ziel</th><th>Diff</th><th>Eiweiß g${ng.proteinG ? `<small class="muted"> Ziel ${fmt(ng.proteinG)}</small>` : ""}</th><th>KH g${ng.carbsG ? `<small class="muted"> Ziel ${fmt(ng.carbsG)}</small>` : ""}</th><th>Fett g${ng.fatG ? `<small class="muted"> Ziel ${fmt(ng.fatG)}</small>` : ""}</th>${hasCrav ? "<th>HH</th>" : ""}<th></th></tr></thead><tbody>${rows.join("")}</tbody></table></div>
    <div class="muted" style="margin-top:8px">${done.length ? `Schnitt abgeschlossener Tage (${done.length}): ${fmt(avg(kcal))} kcal, ${avg(prot) != null ? fmt(avg(prot)) : "–"} g Eiweiß${bal != null ? ` · Bilanz gegen Ziel ${bal > 0 ? "+" : ""}${fmt(bal)} kcal` : ""}` : "Noch kein abgeschlossener Tag mit Daten."} · Heute zählt noch nicht mit.</div>
    ${hasCrav ? '<div class="muted">HH = Heißhunger-Einträge.</div>' : ""}`;
}


/* ---------- Trainingsblock: Zeitleiste und Fortschritt der Ziele ---------- */
function renderBlock(SB) {
  if (!SB) return "";
  const stLabel = { ok: "im Soll", warn: "knapp", bad: "unter Soll", base: "Basis", none: "keine Daten" };
  const head = SB.status === "active" ? `Woche ${SB.weekNo}${SB.weeks ? ` von ${SB.weeks}` : ""}` : SB.status === "upcoming" ? `Start in ${SB.daysToStart} Tag${SB.daysToStart === 1 ? "" : "en"}` : "beendet";
  const timeline = SB.weeks ? `<div class="btl">${Array.from({ length: SB.weeks }, (_, i) => `<i class="${SB.status === "active" ? (i + 1 < SB.weekNo ? "past" : i + 1 === SB.weekNo ? "now" : "") : SB.status === "done" ? "past" : ""}${(i + 1) % 4 === 0 && SB.goals?.some((g) => g.group) ? " rec" : ""}" title="Woche ${i + 1}${(i + 1) % 4 === 0 && SB.goals?.some((g) => g.group) ? " · Entlastung" : ""}"></i>`).join("")}</div>` : "";
  // Saeulen mit Ziellinie (durchgezogen) und optional Muss-Linie (gestrichelt, grau)
  const bars = (pts, lim, must) => {
    if (!pts.length) return "";
    const max = Math.max((must ?? lim) * 1.6, ...pts.map((x) => x.v), 1);
    const col = (v) => (v <= lim ? "ok" : must != null && v <= must ? "warn" : "bad");
    return `<div class="bbars">${pts.map((x) => `<div title="${esc(x.t)}" style="height:${Math.max(6, (Math.max(0, x.v) / max) * 100)}%;background:var(--${x.muted ? "line" : col(x.v)})"></div>`).join("")}<u${must != null && must > lim ? ' class="aim"' : ""} style="bottom:${(lim / max) * 100}%"></u>${must != null && must > lim ? `<u class="must" style="bottom:${(must / max) * 100}%"></u>` : ""}</div>`;
  };
  const valOf = (g) => {
    if (g.value == null) return "–";
    if (g.key === "strength" && g.periodWeeks > 1) return `${g.value}<small> von ${g.of} Abschnitten mit Ziel</small>`;
    if (g.key === "strength" || g.key === "intensity") return `${g.value}<small> von ${g.of} ${g.group ? "Belastungswochen" : "Wochen"} mit Ziel</small>`;
    if (g.key === "swimTest") return `${g.value > 0 ? "+" : ""}${fmt(g.value, 1)}<small> s/100 m</small>`;
    if (g.key === "ramp") return `${fmt(g.value, 1)}<small> max./Woche</small>`;
    if (g.key === "pause") return `${g.value}<small> Tag${g.value === 1 ? "" : "e"} längste Lücke</small>`;
    if (g.key === "runStable") return `${g.value === 0 ? "unter 60" : g.value}<small> min${g.baseValue != null && g.count > 1 ? ` · Basistest ${fmtStable(g.baseValue)}` : ""}</small>`;
    return `${fmt(g.value, g.key === "acwr" ? 0 : 1)}<small> %</small>`;
  };
  // Verlauf (Worker: goalCourse in season-block.js): erster Messwert → letzter und Richtung, steht direkt in der Zielkarte.
  // Nur der Trend, keine Entscheidung (die trifft der Coach); ohne Vergleich nur die nächste Messung.
  const DIR = { better: "besser", worse: "schlechter", flat: "gleich" };
  const cv = (v, u) => (v == null ? "–" : typeof v === "string" ? esc(v) : u === "min" ? fmtStable(v) : u === "%" ? `${fmt(v, 1)}&nbsp;%` : `${v > 0 ? "+" : ""}${fmt(v, 1)}&nbsp;s`);
  const cap = (t) => t.charAt(0).toUpperCase() + t.slice(1);
  const courseOf = (g) => {
    const C = g.course;
    if (!C) return "";
    if (C.open) return "";
    const main = C.main ? `<div class="sub">${esc(C.main)}${C.mainDir ? ` (${DIR[C.mainDir]})` : ""}</div>` : "";
    return `<div class="bcrs" title="Trend, keine Entscheidung${C.baseline ? " · vor dem Blockstart: Ausgangswerte" : ""}">Verlauf: ${cv(C.from, C.unit)} → ${cv(C.to, C.unit)}${C.dir ? ` (${DIR[C.dir]})` : ""}</div>${main}`;
  };
  const nextOf = (g) => (g.course?.nextText && g.value != null ? `<div class="sub muted">${esc(cap(g.course.nextText))}</div>` : "");
  const goalCard = (g) => {
    let vis = "";
    if (g.key === "decoupling") {
      vis = bars(g.series.slice(-10).map((x) => ({ v: x.value, t: `${fmtDate(x.date)}: ${fmt(x.value, 1)} %` })), g.max, g.max + 2);
    } else if (g.key === "runDecoupling" && g.tests) {
      // Blocktests: eine Zeile je Test (Decoupling 0–60, Pace 0–60, stabile Dauer, Fensterreihe beim Darüberfahren)
      vis = bars(g.series.map((x) => ({ v: x.value, t: `${fmtDate(x.date)}: ${fmt(x.value, 1)} %${x.pace ? ` · ${x.pace} /km` : ""}` })), g.max, g.must)
        + (g.tests.length ? `<div class="scroll"><table class="mini tests" style="margin-top:6px"><thead><tr><th>Test</th><th>0–60</th><th>Pace</th><th>stabil</th></tr></thead><tbody>${g.tests.map((t) => `<tr title="${esc(`${t.label}, ${fmt(t.durationMin)} min${t.date !== t.planned ? `, geplant ${fmtDate(t.planned)}` : ""} · ${t.windows.map((x) => `0–${x.min}: ${fmt(x.value, 1)} %`).join(" · ")}`)}"><td>${fmtDate(t.date)}</td><td>${fmt(t.dec60, 1)}&nbsp;%</td><td>${t.pace60 ?? "–"}</td><td>${t.stableMin === 0 ? "&lt;&nbsp;60" : `${fmt(t.stableMin)}&nbsp;min`}</td></tr>`).join("")}</tbody></table></div>` : "");
    } else if (g.key === "runStable") {
      vis = g.tests.length ? `<div class="sub">${g.tests.map((t) => `${esc(t.label)} ${fmtDate(t.date)}: ${fmtStable(t.stableMin)}`).join(" · ")}</div>` : "";
    } else if (g.key === "runDecoupling" || g.key === "bikeDecoupling") {
      vis = bars(g.series.map((x) => ({ v: x.value, t: `${fmtDate(x.date)}: ${fmt(x.value, 1)} %${x.pace ? ` · ${x.pace} /km` : ""}${x.watts ? ` · ${x.watts} W` : ""}${x.noWarmup ? " · ohne Einrollen" : ""}` })), g.max, g.must);
    } else if (g.key === "swimTest") {
      vis = g.tests.length ? bars(g.tests.map((x) => ({ v: x.slowerPer100, t: `${fmtDate(x.date)}: ${x.t1} / ${x.t2}${x.pauseSec != null ? ` · Pause ${x.pauseSec} s` : ""}` })), g.maxSlowerPer100) : "";
    } else if (g.key === "ramp") {
      vis = bars(g.weeks.filter((w) => w.ramp != null).map((w) => ({ v: w.ramp, muted: !w.load, t: `ab ${fmtDate(w.start)}: ${w.ramp > 0 ? "+" : ""}${fmt(w.ramp, 1)}${w.load ? "" : " (Entlastung)"}${w.done ? "" : " (läuft)"}` })), g.max);
    } else if (g.key === "pause") {
      // Auch im Ausgangswert nach der Grenze färben (wie die Säulen der anderen Karten); Badge-Text bleibt „Basis“, Farbe rot über der Grenze
      vis = `<div class="cbar"><div class="cfill" style="width:${Math.min(100, (g.value / (g.max * 1.5)) * 100)}%;background:var(--${g.status === "base" ? (g.value > g.max ? "bad" : "ok") : g.status === "none" ? "muted" : g.status})"></div><i style="left:${(1 / 1.5) * 100}%"></i></div>`;
    } else if (g.key === "strength") {
      vis = `<div class="bsq">${g.weeks.map((w) => `<i class="${w.load === false ? "rec" : w.done ? (w.hit ? "ok" : g.status === "base" ? "off" : "bad") : "open"}" title="${w.end ? `${fmtDate(w.start)}–${fmtDate(w.end)}` : `ab ${fmtDate(w.start)}`}: ${w.count}${g.goalSets ? " Sätze" : "× Kraft"}${w.load === false ? " (Entlastungswoche, zählt nicht)" : ""}"></i>`).join("")}</div>`;
    } else if (g.key === "intensity") {
      const kd = { bikeHiit: "Rad-HIIT", hillSprints: "Hügelsprints" };
      vis = `<div class="bsq">${g.weeks.map((w) => `<i class="${w.load === false ? "rec" : !w.done ? "open" : w.hit ? "ok" : w.sick ? "rec" : g.status === "base" ? "off" : "bad"}" title="ab ${fmtDate(w.start)}: ${g.kinds.map((k) => `${kd[k]} ${w.has[k] ?? 0}×`).join(", ")}${w.load === false ? " (zählt nicht)" : w.sick ? " (krank)" : ""}"></i>`).join("")}</div>`;
    } else if (g.key === "acwr") {
      vis = g.value != null ? `<div class="cbar"><div class="cfill" style="width:${Math.min(100, g.value)}%;background:var(--${g.status === "base" ? "muted" : g.status === "none" ? "muted" : g.status})"></div><i style="left:80%"></i></div>` : "";
    }
    const legend = g.key === "intensity" ? "Jedes Kästchen = eine Woche, grün = beides erledigt, hell = zählt nicht (Woche 1, Entlastung oder krank)" : g.key === "strength" ? `Jedes Kästchen = ${g.periodWeeks > 1 ? `ein Abschnitt von ${g.periodWeeks * 7} Tagen` : "eine Woche"}, grün = ${g.goalSets ? `mind. ${g.goalSets} Sätze` : "Ziel erreicht"}${g.group && !(g.periodWeeks > 1) ? ", hell = Entlastungswoche" : ""} · Verlauf im Bereich Kraft` : g.must != null && g.must > g.max ? `durchgezogen = Ziel ${g.max} %, gestrichelt = Muss ${g.must} %` : null;
    const sub = [g.note, g.key === "strength" && !g.egym ? "ohne EGYM-Daten" : null, g.basis ? (g.status === "base" ? "Ausgangswert: " : "") + g.basis : null].filter(Boolean).map(esc).join(" · ");
    const lines = [`Ziel: ${esc(g.target)}`, vis && legend ? esc(legend) : null, sub || null].filter(Boolean);
    return `<div class="bgoal"><div class="bg-top"><span class="bg-label" style="min-width:0">${esc(g.label)}</span><span class="badge ${g.key === "pause" && g.status === "base" && g.value > g.max ? "bad" : g.status === "base" || g.status === "none" ? "none" : g.status}">${stLabel[g.status]}</span></div>${g.value != null ? `<div class="val">${valOf(g)}</div>` : ""}${vis}${courseOf(g)}${lines.map((t) => `<div class="sub">${t}</div>`).join("")}${(g.hints ?? []).map((h) => `<div class="sub muted">${esc(h)}</div>`).join("")}${nextOf(g)}</div>`;
  };
  const list = SB.goals ?? [];
  const grouped = list.some((g) => g.group);
  const section = (title, gs) => (gs.length ? `${title ? `<h4 class="bgh">${title}</h4>` : ""}<div class="bgoals">${gs.map(goalCard).join("")}</div>` : "");
  const goals = grouped
    ? section("Abschlussziele", list.filter((g) => g.group === "ziel")) + section("Leitplanken", list.filter((g) => g.group === "leitplanke")) + section("", list.filter((g) => !g.group))
    : section("", list);
  const text = [SB.goal, ...(SB.notes ?? [])].filter(Boolean);
  // Was aus dem Block-Eintrag gelesen wurde (Deckel, Testband, wie Blocktests erkannt werden) und Widersprüche darin – ein Tippfehler im Text
  // verfälscht sonst still die Auswertung
  const CK = SB.check;
  const check = CK ? `<div class="sub muted" style="margin-top:6px">Gelesen aus dem Block-Eintrag: HF-Deckel ${CK.hrCap ?? "–"} · Testband ${CK.band ? CK.band.join("–") : "–"} · Blocktests ${CK.testsByName ? "= Läufe mit „Blocktest“/„Basistest“ im Namen (du legst sie fest)" : "keine"}</div>${CK.hints.map((h) => `<div class="notice" style="margin-top:6px">${esc(h)}</div>`).join("")}` : "";
  return `<div class="cockpit" id="block-card"><div class="tile block"><div class="bhead"><div><h3>Trainingsblock</h3><div class="val">${esc(SB.name)}</div></div><div class="bmeta"><span class="badge ok">${head}</span><div class="sub">${fmtDate(SB.start)}${SB.end ? ` – ${fmtDate(SB.end)}` : ""}${SB.daysLeft != null ? ` · noch ${SB.daysLeft} Tag${SB.daysLeft === 1 ? "" : "e"}` : ""}</div></div></div>${timeline}${check}${goals}${text.length ? `<details class="bdet"><summary>Ziel und Checkpoint</summary>${text.map((t) => `<p class="sub" style="white-space:pre-line">${esc(t)}</p>`).join("")}</details>` : ""}</div></div>`;
}

/* ---------- 6 · Kraft und Hüfte: nur sichtbar, wenn es etwas zu zeigen gibt ---------- */
/* Kraft-Karte in drei Stufen: oben der laufende Zeitraum gegen das Satz-Ziel (die eine Zahl, auf die es ankommt), darunter die
   Woche in Zahlen, dann die früheren Zeiträume als Balken mit Zielmarke. Quelle ist EGYM live (renderEgym); solange das fehlt
   oder nicht eingerichtet ist, die Kraft-Leitplanke des Blocks. m: { goal, unit, goalText, cur: { count, label }, past: [{ range,
   count, hit, rec?, tip? }], pastLabel, stats?: [[Wert, Beschriftung]], statsLabel?, foot?: [Zeilen], info: Erklärtext } */
function strengthCard(m) {
  const hit = m.cur.count >= m.goal;
  const max = Math.max(m.goal * 1.25, ...m.past.map((p) => p.count));
  const done = m.past.filter((p) => !p.rec), hits = done.filter((p) => p.hit).length;
  const row = (p) => `<div class="kr-row${p.rec ? " rec" : ""}"${p.tip ? ` title="${esc(p.tip)}"` : ""}><span class="muted">${p.range}</span><div class="cbar"><div class="cfill" style="width:${(p.count / max) * 100}%;background:var(--${p.rec ? "line-2" : p.hit ? "ok" : "s-strength"})"></div><i style="left:${(m.goal / max) * 100}%"></i></div><b>${p.rec ? '<span class="muted">Entl.</span> ' : p.hit ? '<span class="kr-ok">✓</span> ' : ""}${fmt(p.count)}</b></div>`;
  return `<div class="kh"><h3>Krafttraining</h3><span class="muted">${esc(m.goalText)}</span></div>
    <div class="kr-now"><span class="big">${fmt(m.cur.count)} <small>/ ${fmt(m.goal)} ${m.unit}</small></span>${hit ? '<span class="badge ok">Ziel erreicht</span>' : ""}</div>
    <div class="cbar kr-bar"><div class="cfill" style="width:${Math.min(100, (m.cur.count / m.goal) * 100)}%;background:var(--${hit ? "ok" : "s-strength"})"></div></div>
    <div class="muted">${m.cur.label}</div>
    ${m.stats?.length ? `<div class="bgh kr-h">${m.statsLabel}</div><div class="lstats">${m.stats.map(([v, l]) => `<div class="ls"><span class="ls-v">${v}</span><span class="ls-l">${l}</span></div>`).join("")}</div>` : ""}
    ${m.past.length ? `<div class="bgh kr-h">${m.pastLabel}${done.length ? ` · Ziel in ${hits} von ${done.length} erreicht` : ""}</div>${[...m.past].reverse().map(row).join("")}` : ""}
    ${(m.foot ?? []).map((t) => `<div class="muted" style="margin-top:6px">${t}</div>`).join("")}
    <details class="info"><summary>Erklärung</summary><div>${m.info}</div></details>`;
}
const spanTxt = (a, b) => `${fmtDate(a)}–${fmtDate(b)}`;
const daysLeftTo = (end, today) => U.dayDiff(today, end) + 1;
const leftTxt = (n) => (n <= 1 ? "letzter Tag" : `noch ${n} Tage`);

function renderStrength(d) {
  const has = d.sources.intervalsActivities.ok && d.weeks.some((w) => w.bySport.strength.minutes > 0);
  if (has || d.hipFlags.length) $("kraft").hidden = false;
  // Bis EGYM nachkommt (oder ohne EGYM): Sätze bzw. Kraft-Tage aus der Kraft-Leitplanke des Blocks
  const bg = d.seasonBlock?.goals?.find((x) => x.key === "strength");
  if (bg?.weeks?.length && bg.calWeek) {
    $("strength-card").hidden = false; $("kraft").hidden = false;
    const P = bg.periodWeeks > 1 ? bg.periodWeeks : 1, cw = bg.calWeek;
    const unit = bg.goalSets ? "Sätze" : "Kraft-Tage";
    $("strength").innerHTML = strengthCard({
      goal: bg.weekGoal, unit, goalText: `Ziel ${bg.target}`,
      cur: { count: cw.count, label: P > 1 ? `Abschnitt ${spanTxt(cw.start, cw.end)} · ${leftTxt(daysLeftTo(cw.end, d.today))}` : `Woche ab Mo ${fmtDate(cw.start)} · ${leftTxt(daysLeftTo(addDays(cw.start, 6), d.today))}` },
      past: bg.weeks.filter((w) => w.done).map((w) => ({ range: spanTxt(w.start, w.end ?? addDays(w.start, 6)), count: w.count, hit: w.hit, rec: w.load === false })),
      pastLabel: P > 1 ? `Abschnitte im Block (je ${7 * P} Tage)` : "Wochen im Block",
      info: `Gezählt aus dem Trainingsblock${bg.egym ? " (EGYM-Sätze)" : ", ohne EGYM-Daten: ein Tag mit Kraft-Einheit ab 20 min zählt einmal"}. Grün mit ✓ = Ziel erreicht, Strich = Ziel${bg.group ? ", grau = Entlastungswoche (zählt nicht)" : ""}.`,
    });
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
    const nf = (k.sourcesFailed || []).length, ne = (k.sourcesFailed || []).filter((x) => x.startsWith("egym")).length, at = new Date(k.generatedAt).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
    setSource("egym", "EGYM", ne >= 3 ? "bad" : nf ? "warn" : "ok", ne >= 3 ? "nicht erreichbar" : nf ? "teilweise · " + at : "live · " + at, nf ? `Nicht erreichbar: ${(k.sourcesFailed || []).join(", ")}` : "EGYM wird beim Öffnen abgefragt.");
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
  if (failed.filter((x) => x.startsWith("egym")).length >= 3) { $("egym").innerHTML = '<div class="notice bad"><b>EGYM nicht erreichbar.</b> Login oder Abruf ist fehlgeschlagen.</div>'; return; }
  // Karte "Krafttraining": laufender Zeitraum gegen das Satz-Ziel; Ziel über mehrere Wochen ("≥90 Sätze in 2 Wochen") in festen Abschnitten ab Blockstart
  const wk = k.week, goal = wk.goalSets, P = wk.periodWeeks > 1 && k.periods ? wk.periodWeeks : 1;
  const est = (src) => (src === "estimate" ? "≈" : "");
  const vol = (kg) => (kg >= 1000 ? `${fmt(kg / 1000, 1)} <small>t</small>` : `${fmt(kg)} <small>kg</small>`);
  const since = k.daysSince == null ? "–" : k.daysSince === 0 ? "heute" : k.daysSince === 1 ? "gestern" : `vor ${k.daysSince} Tagen`;
  const src = wk.goalSource === "block" ? "aus dem Blocktext" : failed.includes("intervalsBlock") ? "Standardziel, Blocktext aus Intervals nicht abrufbar" : wk.goalSource === "config" ? "Einstellung im Worker" : "Standardziel";
  const curEnd = P > 1 ? wk.periodEnd : addDays(wk.start, 6);
  const past = P > 1
    ? k.periods.slice(0, -1).map((p) => ({ range: spanTxt(p.start, p.end), count: p.sets || 0, hit: (p.sets || 0) >= goal, tip: p.weekSets.map((n, j) => `Woche ${j + 1}: ${n} Sätze`).join(" · ") }))
    : k.weeks.slice(0, -1).map((w) => ({ range: spanTxt(w.start, addDays(w.start, 6)), count: w.sets || 0, hit: (w.sets || 0) >= goal, tip: [`${w.sessions} Einheiten`, w.minutes ? `${est(w.minutesSource)}${fmt(w.minutes)} min` : "", w.volumeKg ? `${fmt(w.volumeKg)} kg` : ""].filter(Boolean).join(" · ") }));
  const vt = k.volumeTrend;
  $("strength-card").hidden = false;
  $("strength").innerHTML = strengthCard({
    goal, unit: "Sätze", goalText: `Ziel ≥ ${goal} Sätze ${P > 1 ? `je ${7 * P} Tage` : "pro Woche"} · ${src}`,
    cur: { count: (P > 1 ? wk.windowSets ?? wk.sets : wk.sets) || 0, label: `${P > 1 ? `Abschnitt ${spanTxt(wk.periodStart, wk.periodEnd)}` : `Woche ab Mo ${fmtDate(wk.start)}`} · ${leftTxt(daysLeftTo(curEnd, k.today))}` },
    statsLabel: `Diese Woche · ab Mo ${fmtDate(wk.start)}`,
    // Sätze der Woche nur bei Abschnitten: im Wochenmodus stehen sie schon oben
    stats: [[fmt(wk.sessions), wk.sessions === 1 ? "Einheit" : "Einheiten"], P > 1 ? [fmt(wk.sets || 0), "Sätze"] : null, [wk.minutes == null ? "–" : `${est(wk.minutesSource)}${fmt(wk.minutes)} <small>min</small>`, "Dauer"], [wk.volumeKg ? vol(wk.volumeKg) : "–", "Volumen"], [since, "zuletzt"]].filter(Boolean),
    past, pastLabel: P > 1 ? `Frühere Abschnitte (je ${7 * P} Tage)` : "Frühere Wochen",
    foot: [vt ? `Volumen Vorwoche ${vt.pct > 0 ? "+" : vt.pct < 0 ? "−" : "±"}${fmt(Math.abs(vt.pct), 1)} % gegen die Woche davor (${fmt(vt.lastKg)} kg gegen ${fmt(vt.prevKg)} kg)` : null,
      `<button id="egym-sync" type="button" class="lnk">EGYM jetzt nach Intervals schreiben</button> <span id="egym-sync-msg"></span>`].filter(Boolean),
    info: `Sätze aus EGYM. Eine Kraft-Einheit ist ein Tag mit Satz-Übungen oder Geräten, die Garmin-Tagesaktivität zählt nicht. ${P > 1 ? `Das Ziel gilt je fester Abschnitt von ${7 * P} Tagen ab Blockstart; beim Darüberfahren steht die Aufteilung auf die Wochen.` : "Beim Darüberfahren stehen Einheiten, Dauer und Volumen der Woche."} Grün mit ✓ = Ziel erreicht, Strich = Ziel. „≈“ = Dauer nur aus der Spanne der Übungen geschätzt, EGYM lieferte keine.`,
  });
  const syncBtn = $("egym-sync");
  if (syncBtn) syncBtn.onclick = async () => {
    const msg = $("egym-sync-msg");
    msg.textContent = "schreibe …";
    try {
      const r = await fetch("/api/egym-sync", { method: "POST", headers: { Authorization: "Bearer " + localStorage.getItem("dashboard-token") } });
      const j = await r.json();
      const mins = j.minutes ? " · Dauer " + Object.entries(j.minutes).map(([d, m]) => `${fmtDate(d)} ${m} min`).join(", ") : "";
      const extra = j.reconcileError ? ` · Nachziehen fehlgeschlagen: ${j.reconcileError}` : ` · ${j.updated ?? 0} korrigiert, ${j.paired ?? 0} gekoppelt${mins}`;
      msg.textContent = j.ok ? (j.skipped ? "übersprungen: EGYM- oder Intervals-Zugang fehlt im Worker" : `${j.count} Einheit(en) übertragen${j.count ? extra : ""}`) : "Fehler: " + j.error;
    } catch (e) { msg.textContent = "Fehler: " + (e.message || e); }
  };

  // Karte "Kraftwerte": Muskelalter, Krafttest je Gerät (EGYM-Messung) und Trainings-Bestwerte der Woche (geschätzter 1RM)
  const p = k.progress, items = p ? p.items : [];
  const delta = (v, unit = "kg") => (v == null ? '<span class="muted">–</span>' : `<span class="badge ${v > 0 ? "ok" : v < 0 ? "bad" : "none"}">${v > 0 ? "+" : v < 0 ? "−" : "±"}${fmt(Math.abs(v), 1)} ${unit}</span>`);
  const b = k.bioAge, rg = k.regions;
  const regionRows = ["UPPER", "CORE", "LOWER"].map((key) => ({ label: REGION_LABEL[key], age: b ? { UPPER: b.upper, CORE: b.core, LOWER: b.lower }[key] : null, share: rg ? rg[key] : null }));
  const worstAge = Math.max(...regionRows.map((r) => r.age ?? 0));
  // Muskelalter mit Skala: Balken von 20 bis 60 Jahren; der älteste Bereich ist beschriftet, nicht rot (kein Urteil)
  const AGE = { lo: 20, hi: 60 }, ageW = (a) => `${Math.max(0, Math.min(100, ((a - AGE.lo) / (AGE.hi - AGE.lo)) * 100))}%`;
  const bio = b && b.muscle != null ? `<h3>Muskelalter</h3><div class="big">${b.muscle} <small>Jahre${b.total != null ? ` · EGYM-Gesamtalter ${b.total}` : ""} · niedriger ist besser</small></div>
    ${regionRows.map((r) => `<div class="kr-age"><span>${r.label}${r.age === worstAge && r.age ? ' <small class="muted">ältester</small>' : ""}</span><div class="cbar">${r.age != null ? `<div class="cfill" style="width:${ageW(r.age)};background:var(--s-strength)"></div>` : ""}</div><b>${r.age ?? "–"}</b></div>`).join("")}
    <div class="agescale"><span>${AGE.lo}</span><span>${(AGE.lo + AGE.hi) / 2}</span><span>${AGE.hi} Jahre</span></div>
    ${rg ? `<div class="muted" style="margin-top:6px">Volumenanteil 4 Wochen: ${regionRows.map((r) => `${r.label} ${r.share} %`).join(" · ")}</div>` : ""}` : "";
  // Tabelle nur mit Spalten, die Inhalt haben (Vorher/Veränderung erst ab dem zweiten Test je Gerät)
  const cmp = items.some((i) => i.prevKg != null);
  const tests = items.length ? `<h3>Krafttest je Gerät <span class="muted">(EGYM-Messung)</span></h3><div class="scroll"><table class="mini"><thead><tr><th>Gerät</th><th>1RM</th>${cmp ? "<th>Veränderung</th>" : ""}<th>Test</th></tr></thead><tbody>${items.map((i) => `<tr><td>${esc(i.label)} <small class="muted">${esc(REGION_LABEL[i.region] || "")}</small></td><td class="nw"><b>${fmt(i.kg)} kg</b></td>${cmp ? `<td title="${i.prevKg != null ? `vorher ${fmt(i.prevKg)} kg am ${fmtDate(i.prevAt)}` : "erster Test"}">${delta(i.diffKg)}</td>` : ""}<td class="muted">${fmtDate(i.at)}</td></tr>`).join("")}</tbody></table></div>
    <div class="muted" style="margin-top:6px">${cmp && p.improved + p.same + p.declined > 0 ? `${p.improved} besser · ${p.same} gleich · ${p.declined} schwächer gegen den vorherigen Test` : "Veränderung erscheint, sobald ein Gerät zwei Tests hat"}</div>` : "";
  const recs = k.records || [];
  const records = `<h3>Trainings-Bestwerte diese Woche <span class="muted">(geschätzter 1RM)</span></h3>${recs.length ? `<div class="scroll"><table class="mini"><thead><tr><th>Gerät</th><th>bester Satz</th><th>1RM geschätzt</th><th>gegen vorher</th></tr></thead><tbody>${recs.map((r) => `<tr><td>${esc(r.label)}</td><td class="nw">${fmt(r.kg, r.kg % 1 ? 1 : 0)} kg × ${r.reps}</td><td><b class="nw">${fmt(r.e1rm, 1)} kg</b> <small class="muted">vorher ${fmt(r.prevE1rm, 1)}</small></td><td>${delta(r.diffKg)} <small class="muted">+${fmt(r.pct, 1)} %</small></td></tr>`).join("")}</tbody></table></div>` : '<div class="muted">Diese Woche noch kein Bestwert.</div>'}`;
  $("egym").innerHTML = `${failed.length ? `<div class="notice bad" style="margin-bottom:8px">Teilweise nicht erreichbar: ${esc(failed.join(", "))}</div>` : ""}
    <div class="egym-cols"><div>${bio}</div><div>${tests}</div></div>
    <div style="margin-top:16px">${records}</div>
    <details class="info"><summary>Erklärung</summary><div>Muskelalter und Krafttest sind EGYM-Messungen (Muskelalter: Balken auf einer Skala von ${AGE.lo} bis ${AGE.hi} Jahren; das Gesamtalter ist ein eigener EGYM-Wert und wird hier nicht nachgerechnet). Bestwerte kommen aus dem Training: Ein Bestwert ist ein Satz, dessen geschätzter 1RM (Epley, höchstens 12 Wiederholungen angesetzt) höher ist als in allen früheren Einheiten der letzten 12 Wochen am selben Gerät.</div></details>`;
}

/* ---------- Coach: Tagesaussage des Coaching-Tasks (das Widget zeigt nur den Hinweis, solange er frisch ist) ---------- */
/* Coach im Heute-Kopf: Aussage der Morgen-Auswertung (save_coach) und seine Rückfrage mit Antwortfeld.
   Frisch heißt: von heute (Berlin). Ältere Aussagen stehen nur als Hinweis da, nie als Urteil für heute. */
const berlinDay = (iso) => new Date(iso).toLocaleDateString("sv-SE", { timeZone: "Europe/Berlin" });
const clock = (iso) => new Date(iso).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit", timeZone: "Europe/Berlin" });
const GO_LABEL = { hart: ["Hart erlaubt", "ok"], locker: ["Nur locker", "warn"], frei: ["Frei", "bad"] };
function coachHead(d) {
  const c = d.coach, fresh = c && berlinDay(c.fetchedAt) === d.today;
  const k = `<div class="hero-k">Coach · ${weekday(d.today)} ${fmtDate(d.today)}</div>`;
  if (!fresh) {
    return `${k}<div class="verdict none"><span>Heute noch keine Coach-Auswertung</span></div>
      <p class="hero-sub">Die Morgen-Auswertung läuft um 8:30. Bis dahin stehen hier nur die Werte, kein Urteil.</p>
      ${c ? `<div class="hero-why">Letzte Aussage (${weekday(berlinDay(c.fetchedAt))} ${fmtDate(berlinDay(c.fetchedAt))}): ${esc(c.note)}</div>` : ""}`;
  }
  const paras = String(c.text ?? "").split(/\n{2,}/).map((x) => x.trim()).filter(Boolean).map((x) => `<p>${esc(x)}</p>`).join("");
  const q = c.question ? `<div class="coach-q"><b>Frage an dich</b><p>${esc(c.question)}</p>
      ${c.answer ? `<div class="sub">Deine Antwort (${clock(c.answeredAt)}): ${esc(c.answer)}</div>` : ""}
      <textarea id="coach-answer" rows="2" maxlength="1000" placeholder="Antwort in ein, zwei Sätzen">${c.answer ? esc(c.answer) : ""}</textarea>
      <div class="coach-send"><button type="button" id="coach-send">${c.answer ? "Antwort ändern" : "Antworten"}</button><span class="sub" id="coach-msg">Der Coach liest sie morgen früh. Sofort reagiert er, wenn du im Chat der Morgen-Auswertung antwortest.</span></div></div>` : "";
  // Urteil des Coaches (go): darf ich heute draufhauen? Dazu seine Blocker. Beides kommt nur von ihm.
  const go = GO_LABEL[c.go];
  const goHtml = go || c.blockers?.length ? `<div class="coach-go">${go ? `<span class="badge ${go[1]}">${go[0]}</span>` : ""}${(c.blockers ?? []).map((b) => `<span class="coach-bl">${esc(b)}</span>`).join("")}</div>` : "";
  return `${k}${goHtml}<div class="verdict coach"><span>${esc(c.headline ?? c.note)}</span></div>
    ${c.headline ? `<p class="hero-sub">${esc(c.note)}</p>` : ""}
    ${q}
    ${paras ? `<details class="bdet"><summary>Ganzer Bericht</summary><div class="desc" style="color:var(--text)">${paras}</div></details>` : ""}
    <div class="hero-why">Stand ${clock(c.fetchedAt)} Uhr</div>`;
}
/* Idee der Woche: freiwillige Anregung des Coaches (montags per save_coach, Feld idea). Ruhig unter der Coach-Karte, ohne
   Abhaken und ohne Status. Älter als 8 Tage (Montag ausgelassen) wird sie ausgeblendet. */
const IDEA_ICON = {
  lauf: '<path d="M2.5 13.5V8l2.5 1 2-3.5h2.5l1.5 3 5 1.5a2 2 0 0 1 1.5 2v1.5zM2.5 16h15M9 8.5l1.5-.5M9.8 10l1.5-.5"/>',
  rad: '<circle cx="5" cy="13" r="3.2"/><circle cx="15" cy="13" r="3.2"/><path d="M5 13l3-6h5l2 6M8 7h-2M10 13l3-6"/>',
  schwimmen: '<path d="M2 8c2-1.5 3-1.5 4 0s2 1.5 4 0 3-1.5 4 0 2 1.5 4 0M2 13c2-1.5 3-1.5 4 0s2 1.5 4 0 3-1.5 4 0 2 1.5 4 0"/>',
  kraft: '<path d="M6 6v8M14 6v8M3 8v4M17 8v4M6 10h8"/>',
  alle: '<path d="M10 2.5a5 5 0 0 0-3 9V14h6v-2.5a5 5 0 0 0-3-9zM8 17h4"/>',
};
const IDEA_SPORT_LABEL = { lauf: "Laufen", rad: "Rad", schwimmen: "Schwimmen", kraft: "Kraft", alle: "Alle Sportarten" };
function ideaCard(d) {
  const i = d.coach?.idea;
  if (!i?.text || !i.createdAt || berlinDay(i.createdAt) < addDays(d.today, -8)) return "";
  const icon = `<svg class="idea-ic" viewBox="0 0 20 20" aria-hidden="true">${IDEA_ICON[i.sport] ?? IDEA_ICON.alle}</svg>`;
  return `<div class="idea" title="${IDEA_SPORT_LABEL[i.sport] ?? "Idee"}">${icon}<div><div class="hero-k">Idee der Woche</div><p>${esc(i.text)}</p></div></div>`;
}
// Werte von heute früh als Fakten: Körperwerte gegen die eigene Basis, Gefühlswerte (vor dem Training eingetragen) gegen das Übliche
function factsLine(R, today) {
  const f1 = (v) => fmt(v, 1);
  const feel = R.items.filter((i) => i.v != null).map((i) => `${i.label} ${i.v}${i.med != null ? ` (üblich ${f1(i.med)})` : ""}`);
  const when = R.entryDate == null ? "" : R.entryDate === today ? "" : ` · Eintrag vom ${fmtDate(R.entryDate)}`;
  return `<div class="hero-facts">${R.sick ? '<div><span class="hero-k">Kalender</span> Krankheit eingetragen</div>' : ""}<div><span class="hero-k">Heute früh</span> ${esc(bodyLine(R))}</div>
    <div><span class="hero-k">Gefühl</span> ${feel.length ? esc(feel.join(" · ")) + when : "noch nicht eingetragen"}</div></div>`;
}
function wireCoachAnswer() {
  const btn = $("coach-send");
  if (!btn) return;
  btn.onclick = async () => {
    const answer = $("coach-answer").value.trim(), msg = $("coach-msg");
    if (!answer) { msg.textContent = "Bitte erst eine Antwort schreiben."; return; }
    btn.disabled = true; msg.textContent = "Wird gespeichert …";
    let token = null;
    try { token = localStorage.getItem("dashboard-token"); } catch {}
    try {
      const r = await fetch("/api/coach/answer", { method: "POST", headers: { Authorization: "Bearer " + token, "content-type": "application/json" }, body: JSON.stringify({ answer }) });
      const j = await r.json().catch(() => ({}));
      msg.textContent = r.ok ? `Gespeichert (${clock(j.answeredAt)}). Der Coach liest sie morgen früh.` : `Nicht gespeichert: ${j.error ?? r.status}`;
      if (r.ok) btn.textContent = "Antwort ändern";
    } catch (e) { msg.textContent = "Nicht gespeichert: " + e.message; }
    btn.disabled = false;
  };
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

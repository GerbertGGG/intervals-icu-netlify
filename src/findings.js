// Auffälligkeiten (Training und Ernährung) als gebündelte Muster mit Konsequenz, für das Widget.
// Dieselben Regeln und Schwellen wie "Was auffällt" auf der Seite (public/dashboard/app.js: sportFindings und
// renderNutritionWeek): Wer dort etwas ändert, ändert es hier mit. Faustwerte, kein Trainings- oder Ernährungsplan.
//
// Befund: { sev, n, short, text, act }
//   sev   2 = Warnung, 1 = Hinweis
//   n     Anzahl betroffener Tage oder Wochen (für die Reihenfolge bei gleichem sev)
//   short kurze Überschrift für das Widget, text der volle Satz, act die Konsequenz

const TRN = { jump: 1.3, drop: 0.6, planUnder: 0.75, planOver: 1.15, hardShare: 0.2, midShare: 0.25, streak: 6, restDays: 4 };
const NUT = { over: 1.1, under: 0.75, proteinShare: 0.2 };
const SPORT_LABEL = { run: "Laufen", bike: "Rad", swim: "Schwimmen", strength: "Kraft", other: "Sonstiges" };
const SPORT_ORDER = ["run", "bike", "swim", "strength", "other"];

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const pct = (x) => Math.round(x * 100);
const num = (v, d = 0) => (v == null ? "–" : Number(v).toLocaleString("de-DE", { minimumFractionDigits: d, maximumFractionDigits: d }));
const sortFindings = (list) => [...list].sort((a, b) => b.sev - a.sev || (b.n ?? 0) - (a.n ?? 0));

// Nur abgeschlossene Wochen zählen (die laufende ist unvollständig), Vergleich gegen den Schnitt der bis zu 4 Wochen davor.
export function trainingFindings(d) {
  const full = d.weeks.filter((w) => w.complete), last = full[full.length - 1];
  if (!last) return [];
  const out = [], base = full.slice(-5, -1).filter((w) => w.load > 0);
  const avg = (key) => (base.length ? base.reduce((n, w) => n + w[key], 0) / base.length : null);
  const aLoad = avg("load"), aKm = avg("km");
  const add = (sev, short, text, act, n = 1) => out.push({ sev, n, short, text, act });
  if (aLoad && last.load > aLoad * TRN.jump) add(2, `Belastung letzte Woche +${pct(last.load / aLoad - 1)} %`, `Belastung ${num(last.load)} TSS, ${pct(last.load / aLoad - 1)} % mehr als dein Schnitt (${num(aLoad)}) – steiler Anstieg.`, "Die nächste Woche nicht noch steigern, 1–2 lockere Tage fest einplanen (Verletzungsrisiko).");
  else if (aLoad && last.load < aLoad * TRN.drop) add(1, `Belastung letzte Woche nur ${pct(last.load / aLoad)} %`, `Belastung ${num(last.load)} TSS, nur ${pct(last.load / aLoad)} % deines Schnitts (${num(aLoad)}) – deutlich weniger Training.`, "Wenn das kein gewollter Entlastungsblock war: die Ursache klären und die Last schrittweise zurückholen.");
  if (aKm && last.km > aKm * TRN.jump) add(2, `Laufumfang letzte Woche +${pct(last.km / aKm - 1)} %`, `Laufumfang ${num(last.km, 1)} km, ${pct(last.km / aKm - 1)} % mehr als dein Schnitt (${num(aKm, 1)} km).`, "Laufumfang in der Folgewoche auf den Schnitt oder knapp darüber begrenzen.");
  if (last.plannedLoad) {
    if (last.load < last.plannedLoad * TRN.planUnder) add(1, `Plan nur zu ${pct(last.load / last.plannedLoad)} % geschafft`, `nur ${num(last.load)} von ${num(last.plannedLoad)} TSS des Plans geschafft.`, "Verpasste Einheiten nicht nachholen, sondern den Plan der laufenden Woche halten.");
    else if (last.load > last.plannedLoad * TRN.planOver) add(1, `${num(last.load - last.plannedLoad)} TSS über dem Plan`, `${num(last.load - last.plannedLoad)} TSS über dem Plan (${num(last.load)} von ${num(last.plannedLoad)}).`, "Mehr als geplant bringt vor dem Rennen selten etwas: lockere Einheiten wirklich locker laufen.");
  }
  const skipped = SPORT_ORDER.filter((k) => last.bySport[k].plannedLoad > 0 && last.bySport[k].count === 0);
  if (skipped.length) add(1, `Geplant, aber ausgelassen: ${skipped.map((k) => SPORT_LABEL[k]).join(", ")}`, `${skipped.map((k) => `${SPORT_LABEL[k]} (${num(last.bySport[k].plannedLoad)} TSS)`).join(", ")} war geplant, aber nichts absolviert.`, "Die ausgelassene Sportart gezielt wieder einplanen, damit sie nicht ganz verschwindet.", skipped.length);
  const z = last.intensity ?? { easy: 0, mid: 0, hard: 0 }, zt = z.easy + z.mid + z.hard;
  if (zt >= 60) {
    if (z.hard / zt > TRN.hardShare) add(1, `Viel Intensität: ${pct(z.hard / zt)} % hart`, `${pct(z.hard / zt)} % der Zonenzeit hart (Z5+) – viel Intensität.`, "Nächste Woche mehr lockere Zeit, harte Einheiten auf höchstens zwei begrenzen.");
    if (z.mid / zt > TRN.midShare) add(1, `Viel Grauzone: ${pct(z.mid / zt)} % mittel`, `${pct(z.mid / zt)} % der Zonenzeit im mittleren Bereich (Z3–4).`, "Lockere Einheiten langsamer, harte gezielt härter: weniger Grauzone.");
  }
  // Tage in Folge ohne Ruhetag bzw. lange Pause bis heute (heute zählt nur, wenn schon trainiert)
  const load = Object.fromEntries(d.daily.map((x) => [x.date, x.load]));
  let streak = 0, pause = 0;
  for (let x = d.today; load[x] != null; x = addDays(x, -1)) { if (load[x] > 0) streak++; else if (x !== d.today) break; }
  for (let x = addDays(d.today, -1); load[x] != null && !(load[x] > 0); x = addDays(x, -1)) pause++;
  if (streak >= TRN.streak) out.push({ sev: 2, n: streak, short: `${streak} Tage ohne Ruhetag`, text: `${streak} Trainingstage in Folge ohne Ruhetag.`, act: "Heute oder morgen einen echten Ruhetag setzen." });
  if (load[d.today] === 0 && pause >= TRN.restDays) out.push({ sev: 1, n: pause, short: `Seit ${pause} Tagen kein Training`, text: `Seit ${pause} Tagen kein Training.`, act: "Mit einer lockeren Einheit wieder einsteigen, nicht mit dem Plantempo." });
  return sortFindings(out);
}

// Letzte 7 Tage (ohne heute): fehlendes Tagebuch, deutlich drunter, über Ziel, wenig Eiweiß, Heißhunger.
export function nutritionFindings(d) {
  const days = Array.from({ length: 7 }, (_, i) => addDays(d.today, -6 + i));
  const by = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const cravByDay = {};
  for (const c of d.cravings ?? []) (cravByDay[c.date] ??= []).push(c);
  const past = days.filter((x) => x !== d.today), miss = [], over = [], under = [], lowProt = [];
  for (const x of past) {
    const w = by[x] ?? {}, k = w.calories, g = w.calorieGoal;
    if (k == null) { miss.push(x); continue; }
    if (g != null && k > g * NUT.over) over.push({ x, by: k - g });
    else if (g != null && k < g * NUT.under) under.push({ x, by: g - k });
    const share = k ? ((w.protein ?? 0) * 4) / k : null;
    if (share != null && share < NUT.proteinShare) lowProt.push({ x, share });
  }
  const done = past.length - miss.length;
  if (!done) return []; // Tagebuch nicht in Benutzung (oder Yazio nicht verbunden): kein Muster, die Seite zeigt dann nur den Hinweis
  const mean = (xs, key) => xs.reduce((n, e) => n + e[key], 0) / xs.length;
  const out = [];
  if (miss.length) out.push({ sev: miss.length * 2 >= past.length ? 2 : 1, n: miss.length, short: `Kein Tagebuch an ${miss.length} von ${past.length} Tagen`, text: `Kein Tagebuch an ${miss.length} von ${past.length} Tagen.`, act: "Ohne Einträge sind Schwankungen nicht sichtbar – auch grobe Einträge genügen." });
  if (under.length) out.push({ sev: 2, n: under.length, short: `Deutlich unter dem Kalorienziel an ${under.length} von ${done} Tagen`, text: `Deutlich unter dem Kalorienziel an ${under.length} von ${done} Tagen, im Schnitt ${num(mean(under, "by"))} kcal.`, act: "Ein starkes Defizit begünstigt Heißhunger: Kalorien über den Tag verteilen und abends nicht nachholen müssen." });
  if (over.length) out.push({ sev: over.length >= 2 ? 2 : 1, n: over.length, short: `Über dem Kalorienziel an ${over.length} von ${done} Tagen`, text: `Über dem Kalorienziel an ${over.length} von ${done} Tagen, im Schnitt +${num(mean(over, "by"))} kcal.`, act: "Den größten Posten des Tages (meist abends) kleiner planen, statt überall ein bisschen zu sparen." });
  if (lowProt.length) out.push({ sev: lowProt.length * 2 >= done ? 2 : 1, n: lowProt.length, short: `Eiweiß an ${lowProt.length} von ${done} Tagen unter ${NUT.proteinShare * 100} %`, text: `Eiweiß an ${lowProt.length} von ${done} Tagen unter ${NUT.proteinShare * 100} % der Kalorien (Schnitt ${pct(mean(lowProt, "share"))} %).`, act: "Eiweiß zu jeder Mahlzeit einplanen, besonders zum Frühstück und an Trainingstagen." });
  const total = days.reduce((n, x) => n + (cravByDay[x]?.length ?? 0), 0);
  const late = days.flatMap((x) => cravByDay[x] ?? []).filter((c) => c.hour != null && c.hour >= 20).length;
  if (total) out.push({ sev: late >= 2 ? 2 : 1, n: total, short: `Heißhunger ${total}× in 7 Tagen`, text: `Heißhunger ${total}× in 7 Tagen${late ? `, davon ${late}× ab 20 Uhr` : ""}.`, act: late ? "Das Abendessen sättigender planen (Eiweiß, Ballaststoffe) und tagsüber nicht zu knapp essen." : null });
  return sortFindings(out);
}

// Der wichtigste Befund fürs Widget: nur Warnungen (sev 2), damit der Platz nicht jeden Tag belegt ist.
// Training vor Ernährung, weil die Trainingswoche meist die größere Stellschraube ist; je Bereich der gewichtigste Befund.
export function topFinding(d) {
  const best = [trainingFindings(d), nutritionFindings(d)].map((l) => l.find((f) => f.sev >= 2 && f.act)).find(Boolean) ?? null;
  return best ? { short: best.short, act: best.act } : null;
}

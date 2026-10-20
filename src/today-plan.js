// Tagesentscheidung fürs Heute-Band: Erholungsampel (dashboard-summary.js) plus die Regeln aus dem Block-Eintrag im
// Kalender (nie zwei Lauftage hintereinander, keine Intensität vor Woche X, kein HIIT kurz vor dem Blocktest, Ruhetag pro Woche).
// Ergebnis ist eine Aussage "Qualität möglich / locker und kürzer / frei / kein Training" mit Umfang, Laufen je eigener
// Bremse (Hüfte/Leiste/Knie, Muskelkater) und Begründung.
// Eine Regel greift nur, wenn sie im Block-Eintrag steht; ohne Block bleibt nur die Ampel.

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const dm = (iso) => `${iso.slice(8, 10)}.${iso.slice(5, 7)}.`;
const WD = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"];
const wd = (iso) => WD[new Date(iso + "T00:00:00Z").getUTCDay()];
const KIND_DE = { bikeHiit: "Rad-HIIT", hillSprints: "Hügelsprints" };
const RECOVERY_EVERY = 4;
const TEST_SOON_DAYS = 7; // Blocktest im Band ankündigen

// Regeln aus dem Block-Text (Stichworte, wie sie der Nutzer schreibt)
export function parseDayRules(text) {
  const t = String(text ?? "");
  const pre = t.match(/(\d)\s*tage\w*\s*vor\s*(?:einem|dem)\s*blocktest/i);
  return {
    noRunBackToBack: /zwei\s*tage\w*\s*hintereinander/i.test(t) && /lauf/i.test(t),
    restDay: /ruhetag/i.test(t),
    preTestDays: pre ? Number(pre[1]) : null,
  };
}

// input: { today, verdict, block: {status, weekNo, start, rulesText}|null, testDates:[iso], band:[lo,hi]|null, hrCap,
//   intensity: {fromWeek, kinds}|null, intensitySessions:[{date,kind}], daily:[{date,load,minutes}] }
export function buildTodayPlan(input) {
  const { today, verdict } = input;
  const B = input.block?.status === "active" ? input.block : null;
  const rules = parseDayRules(B?.rulesText);
  const daily = Object.fromEntries((input.daily ?? []).map((x) => [x.date, x]));
  const trained = (d) => (daily[d]?.load ?? 0) > 0 || Object.values(daily[d]?.minutes ?? {}).some((m) => m > 0);
  const ran = (d) => (daily[d]?.minutes?.run ?? 0) > 0;
  const lines = [];
  const add = (cls, text) => lines.push({ cls, text });

  const tests = (input.testDates ?? []).filter((x) => x >= today).sort();
  const testToday = tests[0] === today;
  const nextTest = tests.find((x) => x > today) ?? null;
  const daysTo = nextTest ? Math.round((Date.parse(nextTest) - Date.parse(today)) / 86400000) : null;
  const dow = (new Date(today + "T00:00:00Z").getUTCDay() + 6) % 7, monday = addDays(today, -dow);
  const week = Array.from({ length: 7 }, (_, i) => addDays(monday, i));
  const deload = B?.weekNo != null && B.weekNo % RECOVERY_EVERY === 0;
  const preTest = rules.preTestDays != null && daysTo != null && daysTo <= rules.preTestDays;

  // 0. Umfang: die Ampel sagt auch, wie viel (im Grundlagenblock ist ohnehin fast alles locker)
  if (verdict.dose && !verdict.sick && verdict.cls !== "none") add(verdict.cls, `Umfang: ${verdict.dose.text}.`);

  // 1. Laufen (eigene Bremsen aus der Ampel: Hüfte/Leiste/Knie, Muskelkater)
  const runV = verdict.sports?.run ?? null;
  const runOff = runV?.cls === "bad" && verdict.cls !== "bad";
  if (B && rules.noRunBackToBack && ran(addDays(today, -1))) add("bad", "Kein Lauf heute – gestern gelaufen (nie zwei Tage hintereinander).");
  else if (ran(today)) add("none", "Heute schon gelaufen.");
  else if (runOff) add("bad", `Laufen: ${runV.text} – Rad oder Schwimmen locker.`);
  else if (B && input.hrCap) add(runV?.cls === "warn" ? "warn" : "ok", `Laufen möglich: locker unter HF ${input.hrCap}, Gehpausen erlaubt${runV?.cls === "warn" ? ` – ${runV.text}` : ""}.`);
  else if (runV?.cls === "warn" && verdict.cls === "ok") add("warn", `Laufen ${runV.text}.`);

  // 2. Intensität (nur über Pace/Watt, RPE und Gefühl gesteuert)
  let qualityOpen = false;
  if (B && input.intensity) {
    const I = input.intensity;
    if ((B.weekNo ?? 1) < I.fromWeek) add("none", `Keine Intensität in Woche ${B.weekNo} (Rad-HIIT und Hügelsprints ab Woche ${I.fromWeek}).`);
    else if (preTest) add("warn", `Kein HIIT, keine Sprints: Blocktest am ${wd(nextTest)} ${dm(nextTest)}.`);
    else {
      const done = Object.fromEntries(I.kinds.map((k) => [k, (input.intensitySessions ?? []).some((x) => x.kind === k && x.date >= monday && x.date <= today)]));
      const kinds = deload ? I.kinds.filter((k) => k === "bikeHiit") : I.kinds;
      for (const k of kinds) {
        const label = deload && k === "bikeHiit" ? "Rad-HIIT (halbiert, Entlastungswoche)" : KIND_DE[k] ?? k;
        if (done[k]) add("ok", `${label} diese Woche erledigt.`);
        else {
          qualityOpen = true;
          const stop = verdict.cls === "warn" || verdict.cls === "bad";
          add(verdict.cls === "ok" ? "ok" : stop ? "warn" : "none", `${label} noch offen${stop ? " – heute nicht (Ampel)" : ""}${k === "hillSprints" && ((rules.noRunBackToBack && ran(addDays(today, -1))) || runOff) ? " – heute kein Lauf" : ""}.`);
        }
      }
      if (deload) add("none", "Entlastungswoche: keine Hügelsprints.");
    }
  }

  // 3. Ruhetag (mindestens einer pro Woche, Lage frei)
  if (B && rules.restDay) {
    const rest = week.filter((d) => d < today && !trained(d));
    const left = 7 - dow; // heute mitgezählt
    if (rest.length) add("ok", `Ruhetag diese Woche: ${rest.map((d) => `${wd(d)} ${dm(d)}`).join(", ")}`);
    else if (left <= 2) add("warn", `Noch kein Ruhetag diese Woche – ${left === 1 ? "heute ist der letzte Tag" : "heute oder morgen"}.`);
    else add("none", `Ruhetag diese Woche noch offen (noch ${left} Tage).`);
  }

  // 4. Blocktest
  const band = input.band ? `HF-Band ${input.band[0]}–${input.band[1]}` : "nach Protokoll";
  if (testToday) {
    if (verdict.cls === "bad") add("bad", "Blocktest heute: Ampel rot – um 1–2 Tage verschieben.");
    else add(verdict.cls === "warn" ? "warn" : "ok", `Blocktest heute: volle geplante Länge, ${band}, ruhig anlaufen, gleiche Runde und Bedingungen${verdict.cls === "warn" ? " – Ampel gelb notieren" : ""}.`);
  } else if (nextTest && daysTo <= TEST_SOON_DAYS) add("none", `Blocktest ${wd(nextTest)} ${dm(nextTest)} (in ${daysTo} Tag${daysTo === 1 ? "" : "en"}): ${band}, flache Runde, gleiche Tageszeit, Schuhe und Mahlzeit.`);

  let title;
  if (verdict.sick) title = "Kein Training";
  else if (testToday) title = verdict.cls === "bad" ? "Blocktest verschieben" : "Heute Blocktest";
  else if (verdict.cls === "bad") title = "Frei – oder kurz und locker";
  else if (verdict.missing) title = "Erst Gefühlswerte eintragen";
  else if (verdict.cls === "warn") title = "Heute locker und kürzer";
  else if (verdict.cls === "ok") title = qualityOpen ? "Qualität möglich" : "Locker nach Wochenrahmen";
  else title = "Keine Einschätzung";
  return { cls: verdict.cls, title, sub: verdict.sub, dose: verdict.dose ?? null, sports: verdict.sports ?? null, reasons: verdict.reasons ?? [], note: [verdict.note, verdict.watch, verdict.thin].filter(Boolean).join(" · ") || null, lines, nextTest, daysToTest: daysTo };
}

// CTL/ATL für "heute" an EINER Stelle (Kacheln, Tachos, Formkurve, Widget, Formcheck).
//
// Der heutige Wellness-Eintrag von Intervals.icu enthält schon die GEPLANTE Einheit (die Projektion rechnet
// mit dem Plan weiter), und Wellness enthält auch Zukunftstage. Deshalb: letzter Eintrag <= gestern als Basis,
// dann mit dem tatsächlich absolvierten Load exponentiell weiterrechnen, wie Intervals es selbst tut:
//   ctl_heute = ctl_gestern + (load - ctl_gestern) * (1 - e^(-1/42)),  ATL analog mit 1/7.
import { activityDay, activityLoad } from "./activity-utils.js";

const CTL_DAYS = 42;
const ATL_DAYS = 7;

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

function decay(prev, load, days) {
  return prev + (load - prev) * (1 - Math.exp(-1 / days));
}

// Summe des tatsächlich absolvierten Loads eines Tages (nur Aktivitäten, nie Planung).
export function todayActualLoad(activities, dayIso) {
  let sum = 0;
  for (const a of activities ?? []) if (activityDay(a) === dayIso) sum += activityLoad(a);
  return sum;
}

// Liefert die Wellness-Liste mit korrigierten CTL/ATL für Tage nach dem letzten gesicherten Eintrag bis heute.
// Einträge nach heute werden verworfen. Basis = letzter Eintrag mit CTL/ATL vor heute; Tage danach (auch Lücken)
// werden mit ihrem Aktivitäts-Load gerechnet. Ohne gesicherte CTL/ATL bleibt die Liste unverändert (minus Zukunft).
export function withActualToday(wellness, activities, todayIso) {
  const list = (wellness ?? []).filter((w) => w.date <= todayIso);
  const base = [...list].reverse().find((w) => w.date < todayIso && w.ctl != null && w.atl != null);
  if (!base) return list;
  const byDate = new Map(list.map((w) => [w.date, w]));
  let ctl = base.ctl;
  let atl = base.atl;
  const out = list.filter((w) => w.date <= base.date);
  for (let d = addDays(base.date, 1); d <= todayIso; d = addDays(d, 1)) {
    const load = todayActualLoad(activities, d);
    ctl = decay(ctl, load, CTL_DAYS);
    atl = decay(atl, load, ATL_DAYS);
    out.push({ ...(byDate.get(d) ?? {}), date: d, ctl, atl });
  }
  return out;
}

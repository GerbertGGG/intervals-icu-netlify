export function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

export function isoDateBerlin(d = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(d);
}

export function isIsoDate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s));
}

export function diffDays(a, b) {
  const da = new Date(a + "T00:00:00Z").getTime();
  const db = new Date(b + "T00:00:00Z").getTime();
  return Math.round((db - da) / 86400000);
}

export function parseISODateSafe(iso) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(iso))) return null;
  const [y, m, d] = String(iso).split("-").map((v) => Number(v));
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
  const date = new Date(Date.UTC(y, m - 1, d));
  if (Number.isNaN(date.getTime())) return null;
  if (date.getUTCFullYear() !== y || date.getUTCMonth() + 1 !== m || date.getUTCDate() !== d) return null;
  return date;
}

export function weeksBetween(dateAISO, dateBISO) {
  const a = parseISODateSafe(dateAISO);
  const b = parseISODateSafe(dateBISO);
  if (!a || !b) return NaN;
  return (b.getTime() - a.getTime()) / (7 * 86400000);
}

export function daysBetween(dateAISO, dateBISO) {
  const a = parseISODateSafe(dateAISO);
  const b = parseISODateSafe(dateBISO);
  if (!a || !b) return NaN;
  return (b.getTime() - a.getTime()) / 86400000;
}

export function clampStartDate(startISO, todayISO, maxAgeDays = 180) {
  const start = parseISODateSafe(startISO);
  const today = parseISODateSafe(todayISO);
  if (!start || !today) return null;
  if (start.getTime() > today.getTime()) return null;
  const ageDays = (today.getTime() - start.getTime()) / 86400000;
  if (ageDays > maxAgeDays) return null;
  return isoDate(start);
}

export function listIsoDaysInclusive(oldest, newest) {
  const out = [];
  const start = new Date(oldest + "T00:00:00Z").getTime();
  const end = new Date(newest + "T00:00:00Z").getTime();
  for (let t = start; t <= end; t += 86400000) out.push(isoDate(new Date(t)));
  return out;
}

export function isMondayIso(dayIso) {
  const d = new Date(dayIso + "T00:00:00Z");
  return d.getUTCDay() === 1;
}

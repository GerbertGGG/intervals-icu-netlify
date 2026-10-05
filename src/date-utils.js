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

export function listIsoDaysInclusive(oldest, newest) {
  const out = [];
  const start = new Date(oldest + "T00:00:00Z").getTime();
  const end = new Date(newest + "T00:00:00Z").getTime();
  for (let t = start; t <= end; t += 86400000) out.push(isoDate(new Date(t)));
  return out;
}

// Monday of the ISO week containing dayIso (dayIso itself if it's already a Monday).
export function mondayOnOrBefore(dayIso) {
  const d = new Date(dayIso + "T00:00:00Z");
  if (Number.isNaN(d.getTime())) return null;
  const dow = d.getUTCDay(); // 0=Sun..6=Sat
  const diffDaysToMonday = dow === 0 ? 6 : dow - 1;
  return isoDate(new Date(d.getTime() - diffDaysToMonday * 86400000));
}

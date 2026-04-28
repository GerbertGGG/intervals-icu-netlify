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

export function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

export function round(x, n) {
  const p = 10 ** n;
  return Math.round(x * p) / p;
}

export function avg(arr) {
  const v = arr.filter((x) => x != null && Number.isFinite(x));
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

export function median(arr) {
  const v = arr.filter((x) => x != null && Number.isFinite(x)).sort((a, b) => a - b);
  if (!v.length) return null;
  const m = Math.floor(v.length / 2);
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}

export function pearsonCorrelation(pairs) {
  const clean = (pairs || []).filter((p) => Array.isArray(p) && Number.isFinite(p[0]) && Number.isFinite(p[1]));
  if (clean.length < 2) return null;
  const xs = clean.map((p) => p[0]);
  const ys = clean.map((p) => p[1]);
  const mx = avg(xs);
  const my = avg(ys);
  if (!Number.isFinite(mx) || !Number.isFinite(my)) return null;
  let num = 0;
  let denX = 0;
  let denY = 0;
  for (let i = 0; i < clean.length; i++) {
    const dx = xs[i] - mx;
    const dy = ys[i] - my;
    num += dx * dy;
    denX += dx * dx;
    denY += dy * dy;
  }
  if (!(denX > 0) || !(denY > 0)) return null;
  return num / Math.sqrt(denX * denY);
}

export function sum(arr) {
  let s = 0;
  for (const x of arr) s += Number(x) || 0;
  return s;
}

export function std(arr) {
  const v = arr.filter((x) => x != null && Number.isFinite(x));
  if (v.length < 2) return null;
  const m = v.reduce((a, b) => a + b, 0) / v.length;
  const vv = v.reduce((a, b) => a + (b - m) * (b - m), 0) / (v.length - 1);
  return Math.sqrt(vv);
}

export function uniq(arr) {
  const out = [];
  const seen = new Set();
  for (const x of arr) {
    const k = String(x);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(x);
  }
  return out;
}

export function countBy(arr) {
  const m = {};
  for (const x of arr) {
    const k = String(x);
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

export function isMondayIso(dayIso) {
  const d = new Date(dayIso + "T00:00:00Z");
  return d.getUTCDay() === 1;
}

export function safeRound(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n) : 0;
}

export function bucketLoadsByDay(runs) {
  const m = {};
  for (const r of runs) {
    const d = r.date;
    if (!d) continue;
    m[d] = (m[d] || 0) + (Number(r.load) || 0);
  }
  return m;
}

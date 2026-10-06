"use strict";
/* Kleine SVG-Diagrammbibliothek für das Dashboard (kein Build, keine Fremdbibliothek).
   Farben nur über CSS-Variablen: Ampel (--ok/--warn/--bad) ausschließlich mit Bedeutung,
   --accent für Werte, --s-* für Sportarten. Fehlende Werte werden nie als 0 gezeichnet. */
(() => {
  const NS = "http://www.w3.org/2000/svg";

  /* ---------- Hilfsfunktionen ---------- */
  const U = {
    esc: (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
    fmt: (n, d = 0) => (n == null || !Number.isFinite(Number(n)) ? "–" : Number(n).toLocaleString("de-DE", { minimumFractionDigits: d, maximumFractionDigits: d })),
    fmtDate: (iso) => { const [, m, d] = iso.split("-"); return `${d}.${m}.`; },
    weekday: (iso) => new Date(iso + "T12:00:00").toLocaleDateString("de-DE", { weekday: "short" }),
    fmtTime: (secs) => { const h = Math.floor(secs / 3600), m = Math.floor((secs % 3600) / 60), s = Math.round(secs % 60); return h ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}` : `${m}:${String(s).padStart(2, "0")}`; },
    pace: (sec) => `${Math.floor(sec / 60)}:${String(Math.round(sec % 60)).padStart(2, "0")}`,
    addDays: (iso, n) => new Date(new Date(iso + "T00:00:00Z").getTime() + n * 86400000).toISOString().slice(0, 10),
    dayDiff: (a, b) => Math.round((new Date(b + "T00:00:00Z") - new Date(a + "T00:00:00Z")) / 86400000),
    dayRange: (from, to) => { const out = []; for (let d = from; d <= to; d = U.addDays(d, 1)) out.push(d); return out; },
    median: (a) => { const s = [...a].sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; },
    quantile: (a, q) => { const s = [...a].sort((x, y) => x - y); const p = (s.length - 1) * q, lo = Math.floor(p), hi = Math.ceil(p); return s[lo] + (s[hi] - s[lo]) * (p - lo); },
    niceMax: (v) => { if (v <= 0) return 1; const p = 10 ** Math.floor(Math.log10(v)); for (const m of [1, 2, 4, 6, 8, 10]) if (m * p >= v) return m * p; return 10 * p; },
  };

  function el(tag, attrs = {}, text) {
    const e = document.createElementNS(NS, tag);
    for (const [k, v] of Object.entries(attrs)) if (v != null) e.setAttribute(k, v);
    if (text != null) e.textContent = text;
    return e;
  }
  function svg(W, H, label) { return el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": label }); }
  function title(node, text) { node.append(el("title", {}, text)); return node; }
  function mount(host, node) { host.replaceChildren(node); }
  // Schraffur für "nicht eingetragen": klar unterscheidbar von jedem Wert, auch von "gut".
  function hatch(s) {
    const defs = el("defs");
    const p = el("pattern", { id: "hatch", width: 5, height: 5, patternUnits: "userSpaceOnUse", patternTransform: "rotate(45)" });
    p.append(el("rect", { width: 5, height: 5, fill: "none" }), el("line", { x1: 0, y1: 0, x2: 0, y2: 5, stroke: "var(--muted)", "stroke-width": 1, opacity: 0.45 }));
    defs.append(p); s.append(defs);
  }
  const ZONE = { ok: "var(--ok)", warn: "var(--warn)", bad: "var(--bad)" };

  /* ---------- Skala mit Ampelzonen (TSB, ACWR) ---------- */
  function gauge(host, o) {
    const W = 320, H = 78, L = 12, R = 12, Y = 30, BH = 14;
    const s = svg(W, H, o.label);
    const x = (v) => L + (W - L - R) * ((Math.min(o.max, Math.max(o.min, v)) - o.min) / (o.max - o.min));
    for (const z of o.zones) s.append(el("rect", { x: x(z.from), y: Y, width: Math.max(0, x(z.to) - x(z.from)), height: BH, fill: ZONE[z.cls], "fill-opacity": 0.32 }));
    s.append(el("rect", { x: L, y: Y, width: W - L - R, height: BH, fill: "none", stroke: "var(--line)" }));
    for (const t of o.ticks) s.append(el("line", { x1: x(t), x2: x(t), y1: Y + BH, y2: Y + BH + 4, class: "axis" }), el("text", { x: x(t), y: Y + BH + 16, "text-anchor": "middle" }, o.tickLabel ? o.tickLabel(t) : U.fmt(t)));
    if (o.value == null) s.append(el("text", { x: W / 2, y: Y - 8, "text-anchor": "middle" }, "keine Daten"));
    else {
      const vx = x(o.value);
      s.append(el("path", { d: `M${vx},${Y - 2} l-6,-9 h12 z`, fill: "var(--text)" }));
      s.append(el("text", { x: Math.min(W - 24, Math.max(24, vx)), y: Y - 14, "text-anchor": "middle", style: "fill:var(--text);font-weight:700;font-size:12px" }, U.fmt(o.value, o.dec ?? 1)));
    }
    mount(host, s);
  }

  /* ---------- Punkt auf Skala mit eigenem Normalbereich (Bereitschaft) ---------- */
  function strip(host, o) {
    const W = 230, H = 34, L = 10, R = 10, Y = 14;
    const s = svg(W, H, o.label);
    const x = (v) => L + (W - L - R) * ((v - 1) / Math.max(1, o.max - 1));
    s.append(el("line", { x1: L, x2: W - R, y1: Y, y2: Y, class: "axis", "stroke-width": 2 }));
    if (o.band) s.append(title(el("rect", { x: x(o.band[0]) - 4, y: Y - 8, width: x(o.band[1]) - x(o.band[0]) + 8, height: 16, rx: 8, fill: "var(--muted)", "fill-opacity": 0.22 }), "üblicher Bereich der letzten 8 Wochen"));
    for (let v = 1; v <= o.max; v++) s.append(el("text", { x: x(v), y: 32, "text-anchor": "middle" }, v));
    if (o.value != null) s.append(title(el("circle", { cx: x(o.value), cy: Y, r: 6, fill: o.cls === "none" ? "var(--muted)" : ZONE[o.cls] }), `heute: ${o.value}`));
    mount(host, s);
  }

  /* ---------- Balken (Woche): Ist mit Plan-Marke ---------- */
  function bars(host, items, o) {
    const W = 480, H = 230, L = 44, R = 8, T = 24, B = 42;
    const max = U.niceMax(Math.max(1, ...items.flatMap((w) => [w.value, w.plan]).filter((v) => v != null)));
    const s = svg(W, H, o.label);
    const y = (v) => T + (H - T - B) * (1 - v / max);
    for (let i = 0; i <= 4; i++) { const v = (max / 4) * i; s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: i ? "grid-l" : "axis" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, U.fmt(v, max < 10 ? 1 : 0))); }
    s.append(el("text", { x: 4, y: 11 }, o.unit));
    const slot = (W - L - R) / items.length, bw = Math.min(slot * 0.55, 46);
    items.forEach((w, i) => {
      const x = L + slot * i + (slot - bw) / 2;
      if (w.value != null) s.append(title(el("rect", { x, y: y(w.value), width: bw, height: Math.max(0, y(0) - y(w.value)), fill: "var(--accent)", opacity: w.partial ? 0.55 : 1 }), `${w.tip ?? w.label}: ${U.fmt(w.value, max < 10 ? 1 : 0)} ${o.unit}${w.plan != null ? ` (Plan ${U.fmt(w.plan)})` : ""}${w.partial ? " – laufende Woche" : ""}`));
      if (w.plan != null) s.append(el("line", { x1: x - 3, x2: x + bw + 3, y1: y(w.plan), y2: y(w.plan), stroke: "var(--plan)", "stroke-width": 3 }));
      if (o.valueLabels && w.value != null) s.append(el("text", { x: x + bw / 2, y: y(w.value) - 4, "text-anchor": "middle" }, U.fmt(w.value, o.dec ?? 1)));
      s.append(el("text", { x: x + bw / 2, y: H - 22, "text-anchor": "middle" }, w.label));
    });
    if (o.ref) s.append(el("line", { x1: L, x2: W - R, y1: y(o.ref.value), y2: y(o.ref.value), stroke: "var(--warn)", "stroke-width": 1.5, "stroke-dasharray": "5 3" }), el("text", { x: W - R, y: y(o.ref.value) - 4, "text-anchor": "end" }, o.ref.label));
    s.append(el("text", { x: (L + W - R) / 2, y: H - 6, "text-anchor": "middle" }, o.xLabel));
    mount(host, s);
  }

  /* ---------- Gestapelte Balken (TSS je Sportart) ---------- */
  function stacked(host, weeks, sports, label) {
    const W = 480, H = 230, L = 44, R = 8, T = 24, B = 42;
    const totals = weeks.map((w) => sports.reduce((a, k) => a + w.by[k], 0));
    const max = U.niceMax(Math.max(1, ...totals));
    const s = svg(W, H, label);
    const y = (v) => T + (H - T - B) * (1 - v / max);
    for (let i = 0; i <= 4; i++) { const v = (max / 4) * i; s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: i ? "grid-l" : "axis" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, U.fmt(v, max < 10 ? 1 : 0))); }
    s.append(el("text", { x: 4, y: 11 }, "TSS"));
    const slot = (W - L - R) / weeks.length, bw = Math.min(slot * 0.55, 46);
    weeks.forEach((w, i) => {
      const x = L + slot * i + (slot - bw) / 2;
      let acc = 0;
      for (const k of sports) {
        const v = w.by[k.key ?? k];
        if (!v) continue;
        s.append(title(el("rect", { x, y: y(acc + v), width: bw, height: Math.max(0, y(acc) - y(acc + v)), fill: `var(--s-${k})`, opacity: w.partial ? 0.6 : 1 }), `Woche ab ${U.fmtDate(w.weekStart)} · ${k}: ${U.fmt(v)} TSS`));
        acc += v;
      }
      s.append(el("text", { x: x + bw / 2, y: H - 22, "text-anchor": "middle" }, w.label));
    });
    s.append(el("text", { x: (L + W - R) / 2, y: H - 6, "text-anchor": "middle" }, "Woche ab (Montag) · helle Säule = laufende Woche"));
    mount(host, s);
  }

  /* ---------- Anteilsleisten je Woche ---------- */
  function shares(host, weeks, sports, names, o = {}) {
    const unit = o.unit ?? "TSS";
    const W = 480, rowH = 20, L = 64, R = 8, H = weeks.length * rowH + 22;
    const s = svg(W, H, o.label ?? "Anteil der Sportarten an der Wochenbelastung");
    weeks.forEach((w, i) => {
      const total = sports.reduce((a, k) => a + w.by[k], 0), y = 4 + i * rowH;
      s.append(el("text", { x: L - 6, y: y + 12, "text-anchor": "end" }, w.label));
      if (!total) { s.append(el("rect", { x: L, y, width: W - L - R, height: 14, fill: "url(#hatch)", stroke: "var(--line)" })); return; }
      let acc = 0;
      for (const k of sports) {
        const v = w.by[k]; if (!v) continue;
        const bw = ((W - L - R) * v) / total;
        s.append(title(el("rect", { x: L + ((W - L - R) * acc) / total, y, width: bw, height: 14, fill: `var(--${o.prefix ?? "s"}-${k})` }), `${names[k]}: ${Math.round((100 * v) / total)} % (${U.fmt(v)} ${unit})`));
        const pct = Math.round((100 * v) / total);
        if (bw >= 26) s.append(el("text", { x: L + ((W - L - R) * acc) / total + bw / 2, y: y + 11, "text-anchor": "middle", style: "fill:#fff;font-weight:700;font-size:10px;pointer-events:none" }, `${pct} %`));
        acc += v;
      }
    });
    hatch(s);
    s.append(el("text", { x: L, y: H - 6 }, "0 %"), el("text", { x: W - R, y: H - 6, "text-anchor": "end" }, o.end ?? "100 % der Wochen-TSS"));
    mount(host, s);
  }

  /* ---------- Formkurve: Fitness (CTL), Ermüdung (ATL) und Frische (TSB) bis zum Renntag ---------- */
  function form(host, o) {
    const W = 880, H = 450, L = 34, R = 40, T1 = 40, B1 = 236, T2 = 316, B2 = 408;
    const CTL = "var(--accent)", ATL = "#e8683f";
    const s = svg(W, H, "Fitness, Ermüdung und Frische bis zum Renntag");
    const days = o.days, first = days[0], last = days[days.length - 1];
    const span = Math.max(1, U.dayDiff(first, last));
    const x = (d) => L + (W - L - R) * (U.dayDiff(first, d) / span);
    const today = (o.marks.find((m) => m.label === "heute") ?? { day: last }).day;
    // Glatte Kurve: monotone kubische Interpolation (Fritsch-Carlson), überschwingt nie über die Datenpunkte
    const smooth = (pts) => {
      const n = pts.length, f = (p) => `${p[0].toFixed(1)},${p[1].toFixed(1)}`;
      if (n < 3) return pts.map((p, i) => `${i ? "L" : "M"}${f(p)}`).join("");
      const dx = [], m = [], t = [];
      for (let i = 0; i < n - 1; i++) { dx[i] = pts[i + 1][0] - pts[i][0] || 1e-6; m[i] = (pts[i + 1][1] - pts[i][1]) / dx[i]; }
      t[0] = m[0]; t[n - 1] = m[n - 2];
      for (let i = 1; i < n - 1; i++) t[i] = m[i - 1] * m[i] <= 0 ? 0 : (m[i - 1] + m[i]) / 2;
      for (let i = 0; i < n - 1; i++) {
        if (m[i] === 0) { t[i] = t[i + 1] = 0; continue; }
        const a = t[i] / m[i], b = t[i + 1] / m[i], h = Math.hypot(a, b);
        if (h > 3) { t[i] = (3 * a / h) * m[i]; t[i + 1] = (3 * b / h) * m[i]; }
      }
      let d = `M${f(pts[0])}`;
      for (let i = 0; i < n - 1; i++) d += `C${(pts[i][0] + dx[i] / 3).toFixed(1)},${(pts[i][1] + t[i] * dx[i] / 3).toFixed(1)} ${(pts[i + 1][0] - dx[i] / 3).toFixed(1)},${(pts[i + 1][1] - t[i + 1] * dx[i] / 3).toFixed(1)} ${f(pts[i + 1])}`;
      return d;
    };
    const lastOf = (map) => { let r = null; for (const d of days) if (d <= today && map[d] != null) r = { day: d, v: map[d] }; return r; };
    const cNow = lastOf(o.ctl), aNow = lastOf(o.atl);
    // Legende oben mit den heutigen Werten
    const legend = (items, y, x0 = L) => {
      let lx = x0;
      for (const it of items) {
        s.append(it.round ? el("rect", { x: lx, y: y - 5, width: 14, height: 4, rx: 2, fill: it.color }) : el("rect", { x: lx, y: y - 9, width: 11, height: 11, rx: 3, fill: it.color }));
        const label = el("text", { x: lx + (it.round ? 20 : 17), y: y + 1, style: "font-size:13px" }, it.text);
        s.append(label);
        let w = it.text.length * 7.2 + 6;
        if (it.value != null) { s.append(el("text", { x: lx + (it.round ? 20 : 17) + w, y: y + 1, style: "font-size:13px;font-weight:700;fill:var(--text)" }, U.fmt(it.value))); w += String(Math.round(it.value)).length * 8 + 4; }
        lx += (it.round ? 20 : 17) + w + 18;
      }
    };
    legend([{ color: CTL, text: "Fitness (CTL)", value: cNow?.v, round: true }, { color: ATL, text: "Ermüdung (ATL)", value: aNow?.v, round: true }], 14);
    // Y-Achse eng an die Daten legen, damit die Kurven nicht als flache Linie am Boden kleben
    const cvals = days.flatMap((d) => [o.ctl[d], o.atl[d]]).filter((v) => v != null);
    const dMin = Math.min(...cvals, 10), dMax = Math.max(...cvals, 10), rough = Math.max(4, dMax - dMin) / 4;
    const stp = [1, 2, 5, 10, 20, 50].find((q) => q >= rough) ?? 100;
    const lo1 = Math.max(0, Math.floor((dMin - rough * 0.3) / stp) * stp), hi1 = Math.ceil((dMax + rough * 0.3) / stp) * stp;
    const y1 = (v) => T1 + (B1 - T1) * (1 - (v - lo1) / (hi1 - lo1));
    for (let v = lo1; v <= hi1 + 1e-9; v += stp) s.append(el("line", { x1: L, x2: W - R, y1: y1(v), y2: y1(v), class: "grid-l" }), el("text", { x: L - 8, y: y1(v) + 4, "text-anchor": "end" }, U.fmt(v)));
    const curve = (map, color, fill, sharp) => {
      const runs = []; let cur = null;
      for (const day of days) { const v = map[day]; if (v == null) { cur = null; continue; } if (!cur) runs.push(cur = []); cur.push([x(day), y1(v)]); }
      for (const pts of runs) {
        const d = sharp ? pts.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join("") : smooth(pts);
        if (fill) s.append(el("path", { d: `${d}L${pts[pts.length - 1][0].toFixed(1)},${B1}L${pts[0][0].toFixed(1)},${B1}Z`, fill: color, "fill-opacity": 0.12, stroke: "none" }));
        s.append(el("path", { d, fill: "none", stroke: color, "stroke-width": fill ? 2.4 : 1.4, "stroke-linejoin": "round", "stroke-linecap": "round", "vector-effect": "non-scaling-stroke" }));
      }
    };
    curve(o.ctl, CTL, true, true); curve(o.atl, ATL, false, true);
    if (cNow) s.append(el("circle", { cx: x(cNow.day), cy: y1(cNow.v), r: 4.5, fill: CTL, stroke: "var(--card)", "stroke-width": 1.5 }));
    // Tooltip je Tag über beide Kurven
    const hw = (W - L - R) / (span + 1);
    for (const d of days) if (o.ctl[d] != null || o.atl[d] != null) s.append(title(el("rect", { x: x(d) - hw / 2, y: T1, width: hw, height: B1 - T1, fill: "transparent" }), `${U.fmtDate(d)}: Fitness ${U.fmt(o.ctl[d], 1)} · Ermüdung ${U.fmt(o.atl[d], 1)}`));

    // TSB als Säulen um die Nulllinie: frisch / belastet / stark ermüdet nach den Schwellen der Seite
    const tsb = days.map((d) => (o.ctl[d] != null && o.atl[d] != null ? { d, v: o.ctl[d] - o.atl[d] } : null)).filter(Boolean);
    const tv = tsb.map((p) => p.v);
    const hi = Math.max(5, Math.ceil(Math.max(5, ...tv) / 5) * 5), lo = Math.min(-5, Math.floor(Math.min(-5, ...tv) / 5) * 5);
    const y2 = (v) => T2 + (B2 - T2) * (1 - (v - lo) / (hi - lo));
    const cls = (v) => (v >= o.zones[0].from ? "ok" : v >= o.zones[1].from ? "warn" : "bad");
    const TSB_COL = { ok: "var(--ok)", warn: "var(--plan)", bad: "var(--bad)" };
    legend([{ color: TSB_COL.ok, text: "frisch" }, { color: TSB_COL.warn, text: "belastet" }, { color: TSB_COL.bad, text: "stark ermüdet" }], T2 - 24, L + 112);
    s.append(el("text", { x: L, y: T2 - 23, style: "font-size:13px;font-weight:700;fill:var(--text)" }, "Frische (TSB)"));
    for (const v of [hi, 0, lo]) s.append(el("line", { x1: L, x2: W - R, y1: y2(v), y2: y2(v), class: v === 0 ? "axis" : "grid-l" }), el("text", { x: L - 8, y: y2(v) + 4, "text-anchor": "end" }, U.fmt(v)));
    const bw = Math.max(2, hw * 0.62);
    for (const p of tsb) s.append(title(el("rect", { x: x(p.d) - bw / 2, y: Math.min(y2(p.v), y2(0)), width: bw, height: Math.max(1, Math.abs(y2(p.v) - y2(0))), rx: 1.5, fill: TSB_COL[cls(p.v)] }), `${U.fmtDate(p.d)}: TSB ${U.fmt(p.v, 1)}`));
    // heute und Renntag
    for (const m of o.marks) {
      if (m.day < first || m.day > last) continue;
      s.append(el("line", { x1: x(m.day), x2: x(m.day), y1: T1 - 4, y2: B2, stroke: m.color, "stroke-width": 1.3, "stroke-dasharray": "4 3" }), el("text", { x: x(m.day) + (m.anchor === "end" ? 2 : m.anchor === "start" ? 4 : 0), y: B2 + 22, "text-anchor": m.anchor === "end" ? "middle" : m.anchor ?? "middle", style: `fill:${m.color};font-weight:700` }, m.label));
    }
    // Datumsachse wöchentlich ab dem ersten Tag; Beschriftung nur, wenn sie nicht auf eine Marke fällt
    const mx = o.marks.map((m) => x(m.day));
    for (let i = 0; i <= span; i += 7) { const d = U.addDays(first, i); if (mx.some((m) => Math.abs(m - x(d)) < 34)) continue; s.append(el("text", { x: x(d), y: T1 + 0 + (B1 - T1) + 20, "text-anchor": "middle" }, U.fmtDate(d)), el("text", { x: x(d), y: B2 + 22, "text-anchor": "middle" }, U.fmtDate(d))); }
    mount(host, s);
  }

  /* ---------- Kalender-Heatmap (Tag x Woche) ---------- */
  function calendar(host, daily, today) {
    const CW = 76, CH = 46, L = 66, T = 22, cols = 7;
    const first = daily[0].date, dow = (d) => (new Date(d + "T12:00:00").getDay() + 6) % 7;
    const start = U.addDays(first, -dow(first));
    const weeks = Math.ceil((U.dayDiff(start, today) + 1) / 7);
    const W = L + cols * CW + 8, H = T + weeks * CH + 26;
    const s = svg(W, H, "Belastung je Tag");
    const max = Math.max(1, ...daily.map((d) => d.load));
    ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"].forEach((n, i) => s.append(el("text", { x: L + i * CW + CW / 2, y: 14, "text-anchor": "middle" }, n)));
    for (let w = 0; w < weeks; w++) {
      const ws = U.addDays(start, w * 7);
      s.append(el("text", { x: L - 8, y: T + w * CH + CH / 2 + 4, "text-anchor": "end" }, U.fmtDate(ws)));
      for (let i = 0; i < 7; i++) {
        const date = U.addDays(ws, i);
        if (date < first || date > today) continue;
        const day = daily.find((d) => d.date === date), x = L + i * CW + 2, y = T + w * CH + 2;
        const sports = Object.entries(day.sports).map(([k, v]) => `${k} ${U.fmt(v)}`).join(", ");
        const cell = title(el("rect", { x, y, width: CW - 4, height: CH - 4, rx: 4, fill: day.load ? "var(--accent)" : "none", "fill-opacity": day.load ? 0.18 + 0.82 * (day.load / max) : 1, stroke: date === today ? "var(--text)" : "var(--line)", "stroke-width": date === today ? 2 : 1 }), `${U.weekday(date)} ${U.fmtDate(date)}: ${day.load ? `${U.fmt(day.load)} TSS (${sports})` : "keine Belastung / Ruhetag"}`);
        s.append(cell);
        if (day.load) s.append(el("text", { x: x + (CW - 4) / 2, y: y + (CH - 4) / 2 + 4, "text-anchor": "middle", style: `fill:${day.load / max > 0.55 ? "#fff" : "var(--text)"};font-size:10px` }, U.fmt(day.load)));
      }
    }
    s.append(el("text", { x: L, y: H - 6 }, "Zahl = TSS · dunkler = mehr · leer = Ruhetag"));
    mount(host, s);
  }

  /* Beschriftungen als "Fähnchen" in Zeilen über der Achse: Text steht rechts neben seiner Hilfslinie (am rechten Rand links),
     von rechts nach links vergeben, damit keine Hilfslinie durch einen fremden Text läuft */
  function labelLanes(items, W, cw = 5.8) {
    const placed = [];
    for (const it of [...items].sort((a, b) => b.x - a.x)) {
      const w = it.text.length * cw, end = it.x + w + 8 > W - 2;
      const l = end ? it.x - w - 8 : it.x - 2, r = end ? it.x + 2 : it.x + w + 8;
      const hit = placed.filter((p) => p.l < r && l < p.r);
      placed.push({ ...it, anchor: end ? "end" : "start", tx: end ? it.x - 5 : it.x + 5, l, r, lane: hit.length ? Math.max(...hit.map((p) => p.lane)) + 1 : 0 });
    }
    return placed;
  }

  /* ---------- Zielkorridor Halbmarathon: Zeilen statt Zahlenstrahl, Abstand zum Ziel als Text ---------- */
  function corridor(host, o) {
    const rows = [...o.estimates].sort((a, b) => a.seconds - b.seconds);
    const hasGoal = o.goal != null, ref = hasGoal ? o.goal : rows[0].seconds;
    const max = Math.max(ref, ...rows.map((e) => e.seconds)) * 1.04, min = Math.min(ref, ...rows.map((e) => e.seconds)) * 0.8;
    const pos = (v) => ((v - min) / (max - min)) * 100;
    const gap = (sec) => { if (!hasGoal) return ""; const d = Math.round(sec - o.goal); return d === 0 ? "genau Ziel" : `${U.fmtTime(Math.abs(d))} ${d < 0 ? "schneller" : "langsamer"}`; };
    host.innerHTML = (hasGoal ? `<div class="crow chead"><span>Ziel</span><b>${U.fmtTime(o.goal)}</b></div>` : "") + rows.map((e) => {
      const fast = e.seconds <= o.goal, hollow = e.kind === "prognosis";
      return `<div class="crow"><div class="ctop"><span>${U.esc(e.label)}</span><span><b>${U.fmtTime(e.seconds)}</b> <small class="${fast ? "ok" : "bad"}">${gap(e.seconds)}</small></span></div><div class="cbar"><div class="cfill${hollow ? " hollow" : ""}" style="width:${pos(e.seconds)}%"></div>${hasGoal ? `<i style="left:${pos(o.goal)}%"></i>` : ""}</div></div>`;
    }).join("") + `<div class="sub">Balken = Zeit${hasGoal ? ", Strich = Ziel" : ""}. Kürzerer Balken = schneller.</div>`;
  }

  /* ---------- Veraenderung der Prognose gegenueber dem ersten Eintrag je Serie. Null-Linie = unveraendert,
     tiefer = schneller. Eine Linie je Distanz, direkt am Linienende beschriftet. ---------- */
  function deltaTrend(host, rows, o) {
    const W = 480, H = 210, L = 52, R = 56, T = 22, B = 30;
    const fmtD = (v) => (v === 0 ? "0:00" : `${v < 0 ? "−" : "+"}${U.fmtTime(Math.abs(v))}`);
    const lines = o.series.map((sr) => {
      const pts = rows.filter((r) => r[sr.key] != null);
      if (!pts.length) return null;
      return { ...sr, pts: pts.map((r) => ({ date: r.date, v: r[sr.key] - pts[0][sr.key], abs: r[sr.key] })) };
    }).filter(Boolean);
    // Mindestens ±5 Min, damit Schwankungen von unter einer Minute nicht wie ein Einbruch aussehen
    const maxAbs = Math.max(300, ...lines.flatMap((l) => l.pts.map((p) => Math.abs(p.v))));
    const lim = [300, 600, 900, 1800].find((n) => n >= maxAbs) ?? Math.ceil(maxAbs / 600) * 600;
    const t0 = Date.parse(rows[0].date), t1 = Date.parse(rows[rows.length - 1].date);
    const x = (d) => L + (W - L - R) * (t1 === t0 ? 0.5 : (Date.parse(d) - t0) / (t1 - t0));
    const y = (v) => T + (H - T - B) * (0.5 - v / (2 * lim));
    const s = svg(W, H, o.label);
    for (const v of [-lim, -lim / 2, lim / 2, lim]) s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: "grid-l" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, fmtD(Math.round(v))));
    s.append(el("line", { x1: L, x2: W - R, y1: y(0), y2: y(0), class: "axis", "stroke-width": 1.5 }), el("text", { x: L - 6, y: y(0) + 4, "text-anchor": "end", style: "font-weight:700" }, "0:00"));
    s.append(el("text", { x: 4, y: 12 }, "Prognose gegenüber dem ersten Eintrag"));
    s.append(el("text", { x: W - R, y: H - 16, "text-anchor": "end", style: "fill:var(--ok)" }, "↓ schneller"), el("text", { x: W - R, y: T - 8, "text-anchor": "end", style: "fill:var(--muted)" }, "↑ langsamer"));
    const ends = [];
    for (const l of lines) {
      const col = l.color;
      const d = l.pts.map((p, i) => `${i ? "L" : "M"}${x(p.date).toFixed(1)},${y(p.v).toFixed(1)}`).join("");
      if (l.pts.length > 1) s.append(el("path", { d, fill: "none", stroke: col, "stroke-width": l.bold ? 2.5 : 1.8, "stroke-dasharray": l.dash ?? null }));
      for (const p of l.pts) s.append(title(el("circle", { cx: x(p.date), cy: y(p.v), r: l.bold ? 4.5 : 3.5, fill: col }), `${U.fmtDate(p.date)}: ${l.label} ${U.fmtTime(p.abs)} (${fmtD(p.v)})`));
      ends.push({ l, py: y(l.pts[l.pts.length - 1].v) });
    }
    ends.sort((a, b) => a.py - b.py);
    let prev = -99;
    for (const e of ends) { const ty = Math.max(e.py + 4, prev + 13); prev = ty; s.append(el("text", { x: W - R + 6, y: ty, style: `fill:${e.l.color};font-weight:700` }, e.l.label)); }
    const n = Math.min(rows.length, 5);
    for (let i = 0; i < n; i++) { const r = rows[n === 1 ? 0 : Math.round((i * (rows.length - 1)) / (n - 1))]; s.append(el("text", { x: x(r.date), y: H - 2, "text-anchor": i === n - 1 && n > 1 ? "end" : i === 0 && n > 1 ? "start" : "middle" }, U.fmtDate(r.date))); }
    mount(host, s);
  }

  /* ---------- Trainingspaces (VDOT-Zonen) als sortierte Liste, Ziel-Pace an passender Stelle ---------- */
  function paceRuler(host, zones, goalPace) {
    const items = [...zones.map((z) => ({ label: z.label, sec: z.sec, fast: z.fast, slow: z.slow })), ...(goalPace ? [{ label: "Ziel-Pace", sec: goalPace, goal: true }] : [])].sort((a, b) => a.sec - b.sec);
    host.innerHTML = items.map((z) => `<div class="prow${z.goal ? " goal" : ""}"><span>${U.esc(z.label)}</span><b>${z.fast != null ? `${U.pace(z.fast)}–${U.pace(z.slow)}` : U.pace(z.sec)} <small>min/km</small></b></div>`).join("");
  }

  /* ---------- Heatmap Wellness (Wert x Tag), Lücken schraffiert ---------- */
  function wellnessHeat(host, days, rows) {
    const L = 92, R = 8, T = 22, RH = 26, W = 880, H = T + rows.length * RH + 30;
    const s = svg(W, H, "Wellness-Werte je Tag");
    const cw = (W - L - R) / days.length;
    hatch(s);
    rows.forEach((r, i) => {
      const y = T + i * RH;
      s.append(el("text", { x: L - 8, y: y + RH / 2 + 3, "text-anchor": "end", style: "fill:var(--text)" }, r.label));
      days.forEach((d, j) => {
        const v = r.values[d], x = L + j * cw;
        if (v == null) { s.append(title(el("rect", { x, y: y + 1, width: Math.max(1, cw - 1), height: RH - 4, fill: "url(#hatch)" }), `${U.fmtDate(d)}: nicht eingetragen`)); return; }
        // Neutrale Farbskala (keine Ampel): Die Bedeutung der Stufen ist nicht aus den Daten lesbar.
        const t = (v - 1) / Math.max(1, r.max - 1);
        s.append(title(el("rect", { x, y: y + 1, width: Math.max(1, cw - 1), height: RH - 4, fill: "var(--accent)", "fill-opacity": 0.14 + 0.86 * t }), `${U.fmtDate(d)}: ${r.label} ${v} (1 = bestmöglich)`));
      });
    });
    const step = Math.max(1, Math.round(days.length / 8));
    for (let j = 0; j < days.length; j += step) s.append(el("text", { x: L + j * cw + cw / 2, y: T + rows.length * RH + 14, "text-anchor": "middle" }, U.fmtDate(days[j])));
    mount(host, s);
  }

  /* ---------- Linie mit Lücken (Ruhepuls) ---------- */
  function gapLine(host, days, values, o) {
    const W = 480, H = 190, L = 40, R = 10, T = 24, B = 34;
    const v = days.map((d) => values[d]).filter((x) => x != null);
    const lo = Math.floor(Math.min(...v) - 2), hi = Math.ceil(Math.max(...v) + 2);
    const s = svg(W, H, o.label);
    const x = (i) => L + (W - L - R) * (i / Math.max(1, days.length - 1)), y = (val) => T + (H - T - B) * (1 - (val - lo) / (hi - lo));
    for (const t of [lo, (lo + hi) / 2, hi]) s.append(el("line", { x1: L, x2: W - R, y1: y(t), y2: y(t), class: "grid-l" }), el("text", { x: L - 6, y: y(t) + 4, "text-anchor": "end" }, U.fmt(t)));
    s.append(el("text", { x: 4, y: 12 }, o.unit));
    let d = "", pen = false;
    days.forEach((day, i) => { const val = values[day]; if (val == null) { pen = false; return; } d += `${pen ? "L" : "M"}${x(i).toFixed(1)},${y(val).toFixed(1)}`; pen = true; });
    s.append(el("path", { d, fill: "none", stroke: "var(--accent)", "stroke-width": 2 }));
    days.forEach((day, i) => { const val = values[day]; if (val != null) s.append(title(el("circle", { cx: x(i), cy: y(val), r: 2.5, fill: "var(--accent)" }), `${U.fmtDate(day)}: ${U.fmt(val)} ${o.unit}`)); });
    const step = Math.max(1, Math.round(days.length / 6));
    for (let i = 0; i < days.length; i += step) s.append(el("text", { x: x(i), y: H - 16, "text-anchor": "middle" }, U.fmtDate(days[i])));
    mount(host, s);
  }

  /* ---------- Krafttraining je Woche in Minuten mit Zielmarke 60 min ---------- */
  function strength(host, weeks) {
    const GOAL = 60, W = 480, H = 200, L = 40, R = 8, T = 22, B = 40;
    const max = Math.max(90, Math.ceil(Math.max(...weeks.map((w) => w.minutes)) / 30) * 30);
    const s = svg(W, H, "Krafttraining pro Woche in Minuten");
    const y = (v) => T + (H - T - B) * (1 - v / max);
    for (let v = 0; v <= max; v += 30) s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: v ? "grid-l" : "axis" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, v));
    s.append(el("text", { x: 4, y: 12 }, "Minuten"));
    const slot = (W - L - R) / weeks.length, bw = slot * 0.6;
    weeks.forEach((w, i) => {
      const cx = L + slot * i + slot / 2, m = Math.round(w.minutes);
      if (m > 0) s.append(title(el("rect", { x: cx - bw / 2, y: y(m), width: bw, height: y(0) - y(m), rx: 3, fill: "var(--s-strength)", opacity: w.partial ? 0.55 : 1 }), `Woche ab ${U.fmtDate(w.weekStart)}: ${m} min Krafttraining${w.partial ? " – laufende Woche" : ""}`));
      s.append(el("text", { x: cx, y: (m > 0 ? y(m) : y(0)) - 6, "text-anchor": "middle" }, m));
      s.append(el("text", { x: cx, y: H - 22, "text-anchor": "middle" }, w.label));
    });
    s.append(title(el("line", { x1: L, x2: W - R, y1: y(GOAL), y2: y(GOAL), stroke: "var(--ok)", "stroke-width": 1.5, "stroke-dasharray": "5 4" }), "Ziel: 60 Minuten pro Woche"), el("text", { x: W - R, y: y(GOAL) - 4, "text-anchor": "end", style: "fill:var(--ok)" }, "Ziel 1 h"));
    s.append(el("text", { x: (L + W - R) / 2, y: H - 6, "text-anchor": "middle" }, "Woche ab (Montag) · helle Balken = laufende Woche"));
    mount(host, s);
  }

  /* ---------- Ernährung: Tage mit Ziel-Marke, Lücken schraffiert ---------- */
  function nutrition(host, days, o) {
    const W = 480, H = 236, L = 44, R = 8, T = 24, B = 60;
    const vals = days.flatMap((d) => [o.values[d], o.goal?.[d]]).filter((v) => v != null);
    const max = U.niceMax(Math.max(1, ...vals));
    const s = svg(W, H, o.label);
    hatch(s);
    const y = (v) => T + (H - T - B) * (1 - v / max);
    for (let i = 0; i <= 4; i++) { const v = (max / 4) * i; s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: i ? "grid-l" : "axis" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, U.fmt(v))); }
    s.append(el("text", { x: 4, y: 12 }, o.unit));
    const slot = (W - L - R) / days.length, bw = slot * 0.62;
    days.forEach((d, i) => {
      const x = L + slot * i + (slot - bw) / 2, v = o.values[d];
      if (v == null) s.append(title(el("rect", { x, y: T, width: bw, height: H - T - B, fill: "url(#hatch)" }), `${U.fmtDate(d)}: noch keine Daten`));
      else s.append(title(el("rect", { x, y: y(v), width: bw, height: Math.max(0, y(0) - y(v)), fill: "var(--accent)" }), `${U.fmtDate(d)}: ${U.fmt(v)} ${o.unit}${o.goal?.[d] != null ? ` (Ziel ${U.fmt(o.goal[d])})` : ""}`));
      const g = o.goal?.[d];
      if (g != null) s.append(el("line", { x1: x - 2, x2: x + bw + 2, y1: y(g), y2: y(g), stroke: "var(--text)", "stroke-width": 2.5 }));
      if (o.training?.[d]) s.append(title(el("circle", { cx: x + bw / 2, cy: H - B + 12, r: 4, fill: "var(--s-run)" }), "Trainingstag"));
      if (i % 2 === 0 || days.length <= 8) s.append(el("text", { x: x + bw / 2, y: H - 26, "text-anchor": "middle" }, U.fmtDate(d)));
    });
    s.append(el("text", { x: L, y: H - 6 }, o.legend));
    mount(host, s);
  }

  /* ---------- Heißhunger: Uhrzeit x Stärke ---------- */
  function cravings(host, entries, maxStrength) {
    const W = 480, H = 230, L = 44, R = 12, T = 24, B = 42;
    const s = svg(W, H, "Heißhunger nach Uhrzeit und Stärke");
    const x = (h) => L + (W - L - R) * ((h - 6) / 18), y = (v) => T + (H - T - B) * (1 - (v - 0.5) / maxStrength);
    for (let v = 1; v <= maxStrength; v++) s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: "grid-l" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, v));
    for (let h = 6; h <= 24; h += 3) s.append(el("line", { x1: x(h), x2: x(h), y1: T, y2: H - B, class: "grid-l" }), el("text", { x: x(h), y: H - 26, "text-anchor": "middle" }, `${h % 24}:00`));
    s.append(el("text", { x: 4, y: 12 }, "Stärke (wie eingetragen)"), el("text", { x: (L + W - R) / 2, y: H - 8, "text-anchor": "middle" }, "Uhrzeit"));
    for (const e of entries) s.append(title(el("circle", { cx: x(Math.min(24, Math.max(6, e.hour))), cy: y(e.strength), r: 7, fill: "var(--accent)", "fill-opacity": 0.6, stroke: "var(--accent)" }), `${U.fmtDate(e.date)} ${e.time} · Stärke ${e.strength}${e.what ? " · " + e.what : ""}${e.trigger ? " · Auslöser " + e.trigger : ""}${e.before ? " · davor " + e.before : ""}`));
    mount(host, s);
  }

  /* ---------- Fortschrittsbalken bis zum Renntag ---------- */
  function countdown(host, o) {
    const W = 320, H = 54, L = 8, R = 8, Y = 20;
    const s = svg(W, H, "Zeit bis zum Rennen");
    const x = (i) => L + (W - L - R) * (i / o.total);
    s.append(el("rect", { x: L, y: Y, width: W - L - R, height: 10, rx: 5, fill: "var(--line)" }), el("rect", { x: L, y: Y, width: Math.max(0, x(Math.min(o.total, o.elapsed)) - L), height: 10, rx: 5, fill: "var(--accent)" }));
    for (let i = 0; i <= o.total; i += 7) s.append(el("line", { x1: x(i), x2: x(i), y1: Y + 10, y2: Y + 15, class: "axis" }));
    s.append(el("text", { x: L, y: 12 }, "vor 8 Wochen"), el("text", { x: W - R, y: 12, "text-anchor": "end", style: "fill:var(--text);font-weight:700" }, o.raceLabel), el("text", { x: x(Math.min(o.total, o.elapsed)), y: 46, "text-anchor": "middle" }, "heute"));
    mount(host, s);
  }

  /* ---------- Cockpit: Mini-Verlauf (Lücken bleiben Lücken) und Fortschrittsbalken ---------- */
  function spark(host, days, vals, o) {
    const W = 220, H = 56, L = 6, R = 6, T = 8, B = 8;
    const s = svg(W, H, o.label);
    const pts = days.map((d, i) => [i, vals[d]]).filter(([, v]) => v != null);
    if (!pts.length) { s.append(el("text", { x: W / 2, y: H / 2, "text-anchor": "middle" }, "keine Daten")); return mount(host, s); }
    const lo = Math.min(...pts.map((p) => p[1])), hi = Math.max(...pts.map((p) => p[1])), pad = (hi - lo || Math.abs(hi) * 0.1 || 1) * 0.15;
    const x = (i) => L + (W - L - R) * (i / Math.max(1, days.length - 1)), y = (v) => T + (H - T - B) * (1 - (v - (lo - pad)) / (hi - lo + 2 * pad));
    s.append(el("line", { x1: L, x2: W - R, y1: H - B + 2, y2: H - B + 2, class: "axis" }));
    let run = [];
    const flush = () => { if (run.length > 1) s.append(el("polyline", { points: run.map(([i, v]) => `${x(i)},${y(v)}`).join(" "), fill: "none", stroke: "var(--accent)", "stroke-width": 2, "stroke-linejoin": "round" })); run = []; };
    days.forEach((d, i) => { if (vals[d] == null) flush(); else run.push([i, vals[d]]); });
    flush();
    for (const [i, v] of pts) s.append(title(el("circle", { cx: x(i), cy: y(v), r: i === pts[pts.length - 1][0] ? 4 : 2.5, fill: "var(--accent)" }), `${U.fmtDate(days[i])}: ${U.fmt(v, o.dec ?? 0)} ${o.unit}`));
    mount(host, s);
  }
  function meter(host, o) {
    const pct = (v) => Math.min(100, Math.max(0, (v / o.max) * 100));
    const bar = document.createElement("div");
    bar.setAttribute("role", "img"); bar.setAttribute("aria-label", o.label);
    bar.style.cssText = "position:relative;height:12px;border-radius:6px;background:var(--line);margin:8px 0;overflow:visible;display:flex";
    const segs = o.segments ?? (o.value != null ? [{ value: o.value, color: o.cls ? ZONE[o.cls] : "var(--accent)", tip: `${U.fmt(o.value)} ${o.unit ?? ""}` }] : []);
    segs.filter((g) => g.value > 0).forEach((g, i, arr) => {
      const s = document.createElement("div");
      s.style.cssText = `width:${pct(g.value)}%;min-width:2px;background:${g.color};${i === 0 ? "border-radius:6px 0 0 6px;" : ""}${i === arr.length - 1 ? "border-radius:0 6px 6px 0;" : ""}${arr.length === 1 ? "border-radius:6px;" : ""}`;
      s.title = g.tip; bar.append(s);
    });
    if (o.goal != null) { const m = document.createElement("div"); m.style.cssText = `position:absolute;left:${pct(o.goal)}%;top:-4px;bottom:-4px;width:2px;background:var(--text)`; bar.append(m); }
    host.replaceChildren(bar);
  }

  /* ---------- Workout-Profil: Breite = Dauer, Höhe = Intensität (% der Schwelle) ---------- */
  function workout(host, steps) {
    const W = 480, H = 84, L = 2, R = 2, T = 4, B = 16;
    const total = steps.reduce((n, b) => n + b.secs, 0);
    const s = svg(W, H, "Aufbau der geplanten Einheit");
    const lvl = (p) => (p == null ? "–" : p < 60 ? "locker" : p < 80 ? "Grundlage" : p < 95 ? "zügig" : p <= 105 ? "Schwelle" : "hart");
    const col = (p) => (p == null ? "var(--plan)" : p < 60 ? "var(--plan)" : p < 80 ? "var(--s-swim)" : p < 95 ? "var(--accent)" : p <= 105 ? "var(--s-strength)" : "var(--s-bike)");
    const x = (secs) => L + (W - L - R) * (secs / total);
    let t = 0;
    for (const b of steps) {
      const h = Math.max(0.15, Math.min(1, ((b.pct ?? 70) - 40) / 90)) * (H - T - B);
      const x0 = x(t), w = Math.max(1, x(t + b.secs) - x0 - 1);
      s.append(title(el("rect", { x: x0, y: H - B - h, width: w, height: h, rx: 2, fill: col(b.pct) }), `${U.fmt(b.secs / 60, b.secs % 60 ? 1 : 0)} min${b.pct != null ? ` · ${U.fmt(b.pct)} % (${lvl(b.pct)})` : ""}`));
      t += b.secs;
    }
    s.append(el("line", { x1: L, x2: W - R, y1: H - B, y2: H - B, class: "axis" }), el("text", { x: L, y: H - 3 }, "0"), el("text", { x: W - R, y: H - 3, "text-anchor": "end" }, `${U.fmt(total / 60)} min`));
    mount(host, s);
  }

  window.U = U;
  window.C = { gauge, strip, bars, stacked, shares, form, calendar, corridor, deltaTrend, paceRuler, wellnessHeat, gapLine, strength, nutrition, cravings, countdown, hatch, spark, meter, workout };
})();

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
  function shares(host, weeks, sports, names) {
    const W = 480, rowH = 20, L = 64, R = 8, H = weeks.length * rowH + 22;
    const s = svg(W, H, "Anteil der Sportarten an der Wochenbelastung");
    weeks.forEach((w, i) => {
      const total = sports.reduce((a, k) => a + w.by[k], 0), y = 4 + i * rowH;
      s.append(el("text", { x: L - 6, y: y + 12, "text-anchor": "end" }, w.label));
      if (!total) { s.append(el("rect", { x: L, y, width: W - L - R, height: 14, fill: "url(#hatch)", stroke: "var(--line)" })); return; }
      let acc = 0;
      for (const k of sports) {
        const v = w.by[k]; if (!v) continue;
        const bw = ((W - L - R) * v) / total;
        s.append(title(el("rect", { x: L + ((W - L - R) * acc) / total, y, width: bw, height: 14, fill: `var(--s-${k})` }), `${names[k]}: ${Math.round((100 * v) / total)} % (${U.fmt(v)} TSS)`));
        acc += v;
      }
    });
    hatch(s);
    s.append(el("text", { x: L, y: H - 6 }, "0 %"), el("text", { x: W - R, y: H - 6, "text-anchor": "end" }, "100 % der Wochen-TSS"));
    mount(host, s);
  }

  /* ---------- Formkurve: CTL/ATL und TSB bis zum Renntag ---------- */
  function form(host, o) {
    const W = 880, H = 300, L = 40, R = 74, T1 = 22, B1 = 158, T2 = 182, B2 = 268;
    const s = svg(W, H, "Fitness, Ermüdung und Frische bis zum Renntag");
    const days = o.days, first = days[0];
    const span = Math.max(1, U.dayDiff(first, days[days.length - 1]));
    const x = (d) => L + (W - L - R) * (U.dayDiff(first, d) / span);
    const cvals = days.flatMap((d) => [o.ctl[d], o.atl[d]]).filter((v) => v != null);
    const max = U.niceMax(Math.max(10, ...cvals)), y1 = (v) => T1 + (B1 - T1) * (1 - v / max);
    for (let i = 0; i <= 4; i++) { const v = (max / 4) * i; s.append(el("line", { x1: L, x2: W - R, y1: y1(v), y2: y1(v), class: i ? "grid-l" : "axis" }), el("text", { x: L - 6, y: y1(v) + 4, "text-anchor": "end" }, U.fmt(v, max < 10 ? 1 : 0))); }
    s.append(el("text", { x: 4, y: 12 }, "CTL / ATL"));
    const line = (map, color, name) => {
      let d = "", pen = false, last = null;
      for (const day of days) { const v = map[day]; if (v == null) { pen = false; continue; } d += `${pen ? "L" : "M"}${x(day).toFixed(1)},${y1(v).toFixed(1)}`; pen = true; last = { day, v }; }
      s.append(el("path", { d, fill: "none", stroke: color, "stroke-width": 2 }));
      if (last) s.append(el("text", { x: x(last.day) + 6, y: y1(last.v) + 4, style: `fill:${color};font-weight:700` }, name));
    };
    line(o.ctl, "var(--accent)", "CTL"); line(o.atl, "var(--muted)", "ATL");
    // TSB als Säulen um die Nulllinie, Ampelfarben nach den Schwellen der Seite
    const tsb = days.map((d) => (o.ctl[d] != null && o.atl[d] != null ? { d, v: o.ctl[d] - o.atl[d] } : null));
    const tv = tsb.filter(Boolean).map((p) => p.v);
    const lo = Math.min(-30, ...tv), hi = Math.max(15, ...tv), y2 = (v) => T2 + (B2 - T2) * (1 - (v - lo) / (hi - lo));
    for (const z of o.zones) s.append(el("rect", { x: L, y: y2(Math.min(hi, z.to)), width: W - L - R, height: Math.max(0, y2(Math.max(lo, z.from)) - y2(Math.min(hi, z.to))), fill: ZONE[z.cls], "fill-opacity": 0.1 }));
    s.append(el("line", { x1: L, x2: W - R, y1: y2(0), y2: y2(0), class: "axis" }), el("text", { x: L - 6, y: y2(0) + 4, "text-anchor": "end" }, "0"), el("text", { x: L - 6, y: y2(hi) + 10, "text-anchor": "end" }, U.fmt(hi)), el("text", { x: L - 6, y: y2(lo), "text-anchor": "end" }, U.fmt(lo)));
    s.append(el("text", { x: 4, y: T2 - 6 }, "TSB = CTL − ATL"));
    const bw = Math.max(2, ((W - L - R) / (span + 1)) * 0.7);
    for (const p of tsb) if (p) s.append(title(el("rect", { x: x(p.d) - bw / 2, y: Math.min(y2(p.v), y2(0)), width: bw, height: Math.abs(y2(p.v) - y2(0)), fill: ZONE[p.v >= o.zones[0].from ? "ok" : p.v >= o.zones[1].from ? "warn" : "bad"] }), `${U.fmtDate(p.d)}: TSB ${U.fmt(p.v, 1)}`));
    // heute und Renntag
    for (const m of o.marks) {
      if (m.day < first || m.day > days[days.length - 1]) continue;
      s.append(el("line", { x1: x(m.day), x2: x(m.day), y1: T1 - 4, y2: B2, stroke: m.color, "stroke-width": 1.5, "stroke-dasharray": "4 3" }), el("text", { x: x(m.day) + (m.anchor === "end" ? -4 : m.anchor === "start" ? 4 : 0), y: B2 + 26, "text-anchor": m.anchor ?? "middle", style: `fill:${m.color};font-weight:700` }, m.label));
    }
    const step = Math.max(1, Math.round(span / 8));
    for (let i = 0; i <= span; i += step) { const d = U.addDays(first, i); s.append(el("text", { x: x(d), y: B2 + 14, "text-anchor": "middle" }, U.fmtDate(d))); }
    mount(host, s);
  }

  /* ---------- Kalender-Heatmap (Tag x Woche) ---------- */
  function calendar(host, daily, today) {
    const CW = 46, CH = 30, L = 66, T = 22, cols = 7;
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

  /* ---------- Zielkorridor Halbmarathon ---------- */
  function corridor(host, o) {
    const W = 480, H = 170, L = 30, R = 30, AY = 96;
    const all = [o.goal, ...o.estimates.map((e) => e.seconds)];
    const lo = Math.floor((Math.min(...all) - 240) / 300) * 300, hi = Math.ceil((Math.max(...all) + 240) / 300) * 300;
    const x = (v) => L + (W - L - R) * ((v - lo) / (hi - lo));
    const s = svg(W, H, "Halbmarathon-Zeiten im Vergleich zum Ziel");
    s.append(el("rect", { x: L, y: AY - 30, width: x(o.goal) - L, height: 60, fill: "var(--ok)", "fill-opacity": 0.1 }), el("text", { x: L + 4, y: AY + 44 }, "schneller als Ziel"));
    s.append(el("line", { x1: L, x2: W - R, y1: AY, y2: AY, class: "axis", "stroke-width": 2 }));
    for (let t = lo; t <= hi; t += 300) s.append(el("line", { x1: x(t), x2: x(t), y1: AY - 4, y2: AY + 4, class: "axis" }), el("text", { x: x(t), y: AY + 18, "text-anchor": "middle" }, `${Math.floor(t / 3600)}:${String(Math.floor((t % 3600) / 60)).padStart(2, "0")}`));
    s.append(el("line", { x1: x(o.goal), x2: x(o.goal), y1: AY - 36, y2: AY + 30, stroke: "var(--accent)", "stroke-width": 2 }), el("text", { x: x(o.goal), y: AY + 60, "text-anchor": "middle", style: "fill:var(--accent);font-weight:700;font-size:12px" }, `Ziel ${U.fmtTime(o.goal)}`));
    [...o.estimates].sort((a, b) => a.seconds - b.seconds).forEach((e, i) => {
      const up = 22 + (i % 3) * 20;
      s.append(el("line", { x1: x(e.seconds), x2: x(e.seconds), y1: AY - up + 4, y2: AY, stroke: "var(--muted)" }));
      s.append(title(el("circle", { cx: x(e.seconds), cy: AY, r: 5, fill: e.kind === "prognosis" ? "var(--card)" : "var(--muted)", stroke: "var(--muted)", "stroke-width": 2 }), `${e.label}: ${U.fmtTime(e.seconds)}`));
      s.append(el("text", { x: x(e.seconds), y: AY - up, "text-anchor": "middle" }, `${e.label} ${U.fmtTime(e.seconds)}`));
    });
    mount(host, s);
  }

  /* ---------- Bestzeiten und Prognosen als Pace je Distanz ---------- */
  function records(host, rows, goalPaceSec) {
    const W = 480, H = 250, L = 48, R = 16, T = 26, B = 40;
    const paces = rows.flatMap((r) => [r.bestPace, r.progPace]).filter((v) => v != null).concat(goalPaceSec);
    const lo = Math.floor((Math.min(...paces) - 15) / 15) * 15, hi = Math.ceil((Math.max(...paces) + 15) / 15) * 15;
    const y = (v) => T + (H - T - B) * ((v - lo) / (hi - lo)); // schneller = oben
    const s = svg(W, H, "Bestzeiten und Prognosen als Pace");
    for (let v = Math.ceil(lo / 30) * 30; v <= hi; v += 30) s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: "grid-l" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, U.pace(v)));
    s.append(el("text", { x: 4, y: 12 }, "min/km (schneller = oben)"));
    s.append(el("line", { x1: L, x2: W - R, y1: y(goalPaceSec), y2: y(goalPaceSec), stroke: "var(--accent)", "stroke-width": 1.5, "stroke-dasharray": "5 3" }), el("text", { x: W - R, y: y(goalPaceSec) - 5, "text-anchor": "end", style: "fill:var(--accent);font-weight:700" }, `Ziel-Pace ${U.pace(goalPaceSec)}`));
    const slot = (W - L - R) / rows.length;
    rows.forEach((r, i) => {
      const cx = L + slot * i + slot / 2;
      s.append(el("text", { x: cx, y: H - 20, "text-anchor": "middle", style: "fill:var(--text)" }, r.label));
      if (r.bestPace != null) { s.append(title(el("circle", { cx: cx - 18, cy: y(r.bestPace), r: 7, fill: "var(--accent)" }), `Bestzeit ${U.fmtTime(r.bestSeconds)}`), el("text", { x: cx - 18, y: y(r.bestPace) + 20, "text-anchor": "middle", style: "fill:var(--text)" }, U.fmtTime(r.bestSeconds))); }
      if (r.progPace != null) { s.append(title(el("circle", { cx: cx + 18, cy: y(r.progPace), r: 7, fill: "var(--card)", stroke: "var(--muted)", "stroke-width": 2.5 }), `Prognose ${U.fmtTime(r.progSeconds)}`), el("text", { x: cx + 18, y: y(r.progPace) - 12, "text-anchor": "middle" }, U.fmtTime(r.progSeconds))); }
      if (r.bestPace == null && r.progPace == null) s.append(el("text", { x: cx, y: (T + H - B) / 2, "text-anchor": "middle" }, "keine Daten"));
    });
    s.append(el("circle", { cx: L + 8, cy: H - 6, r: 5, fill: "var(--accent)" }), el("text", { x: L + 18, y: H - 2 }, "Bestzeit"), el("circle", { cx: L + 88, cy: H - 6, r: 5, fill: "var(--card)", stroke: "var(--muted)", "stroke-width": 2 }), el("text", { x: L + 98, y: H - 2 }, "Runalyze-Prognose"));
    mount(host, s);
  }

  /* ---------- Pace-Skala (VDOT-Zonen und Ziel-Pace) ---------- */
  function paceRuler(host, zones, goalPace) {
    const W = 480, H = 150, L = 20, R = 20, AY = 70;
    const lo = Math.floor((Math.min(goalPace, zones[0].sec) - 15) / 15) * 15, hi = Math.ceil((Math.max(goalPace, zones[zones.length - 1].sec) + 15) / 15) * 15;
    const x = (v) => L + (W - L - R) * ((v - lo) / (hi - lo));
    const s = svg(W, H, "Trainingspaces und Ziel-Pace");
    s.append(el("line", { x1: L, x2: W - R, y1: AY, y2: AY, class: "axis", "stroke-width": 2 }));
    for (let v = Math.ceil(lo / 30) * 30; v <= hi; v += 30) s.append(el("line", { x1: x(v), x2: x(v), y1: AY - 4, y2: AY + 4, class: "axis" }), el("text", { x: x(v), y: AY + 18, "text-anchor": "middle" }, U.pace(v)));
    zones.forEach((z, i) => { const up = i % 2 === 0 ? 24 : 48; s.append(el("line", { x1: x(z.sec), x2: x(z.sec), y1: AY - up + 4, y2: AY, stroke: "var(--muted)" }), el("circle", { cx: x(z.sec), cy: AY, r: 4, fill: "var(--muted)" }), el("text", { x: x(z.sec), y: AY - up, "text-anchor": "middle" }, `${z.label} ${U.pace(z.sec)}`)); });
    s.append(el("path", { d: `M${x(goalPace)},${AY + 6} l-6,10 h12 z`, fill: "var(--accent)" }), el("text", { x: x(goalPace), y: AY + 42, "text-anchor": "middle", style: "fill:var(--text);font-weight:700;font-size:12px" }, `Ziel ${U.pace(goalPace)}`), el("text", { x: W / 2, y: H - 4, "text-anchor": "middle" }, "Pace in min/km (links schneller)"));
    mount(host, s);
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

  /* ---------- Krafteinheiten je Woche mit Zielband 2–3 ---------- */
  function strength(host, weeks) {
    const W = 480, H = 200, L = 40, R = 8, T = 22, B = 40, max = Math.max(4, ...weeks.map((w) => w.count));
    const s = svg(W, H, "Krafteinheiten pro Woche");
    const y = (v) => T + (H - T - B) * (1 - v / (max + 0.5));
    s.append(title(el("rect", { x: L, y: y(3.5), width: W - L - R, height: y(1.5) - y(3.5), fill: "var(--ok)", "fill-opacity": 0.12 }), "Ziel: 2 bis 3 Einheiten pro Woche"), el("text", { x: W - R, y: y(3.5) + 12, "text-anchor": "end", style: "fill:var(--ok)" }, "Ziel 2–3"));
    for (let v = 0; v <= max; v++) s.append(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), class: v ? "grid-l" : "axis" }), el("text", { x: L - 6, y: y(v) + 4, "text-anchor": "end" }, v));
    s.append(el("text", { x: 4, y: 12 }, "Einheiten"));
    const slot = (W - L - R) / weeks.length;
    weeks.forEach((w, i) => {
      const cx = L + slot * i + slot / 2;
      for (let k = 1; k <= w.count; k++) s.append(title(el("circle", { cx, cy: y(k), r: 8, fill: "var(--s-strength)", opacity: w.partial ? 0.55 : 1 }), `Woche ab ${U.fmtDate(w.weekStart)}: ${w.count} Krafteinheit(en)${w.partial ? " – laufende Woche" : ""}`));
      if (!w.count) s.append(el("text", { x: cx, y: y(0) - 6, "text-anchor": "middle" }, "0"));
      s.append(el("text", { x: cx, y: H - 22, "text-anchor": "middle" }, w.label));
    });
    s.append(el("text", { x: (L + W - R) / 2, y: H - 6, "text-anchor": "middle" }, "Woche ab (Montag) · helle Punkte = laufende Woche"));
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

  window.U = U;
  window.C = { gauge, strip, bars, stacked, shares, form, calendar, corridor, records, paceRuler, wellnessHeat, gapLine, strength, nutrition, cravings, countdown, hatch };
})();

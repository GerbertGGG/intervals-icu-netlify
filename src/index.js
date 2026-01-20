// src/index.js
// Cloudflare Worker – Run only
// Writes metrics + explanatory comments into Intervals Wellness
//
// Required Secret:
// INTERVALS_API_KEY
//
// Wellness custom numeric fields (exact codes):
// VDOT, Drift, EF, TTT, Score
//
// Logic summary:
// - Only Run activities
// - GA: VDOT_like + Drift
// - Key: EF + TTT
// - Score = execution quality (not progress)
// - Progress = VDOT + Drift trends (28d vs prev 28d)
// - Minimum stimulus = 100 Run-TSS / 7 days (comment only)

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (url.pathname === "/") {
      return new Response("ok");
    }

    if (url.pathname === "/sync") {
      const write = url.searchParams.get("write") === "true";
      const debug = url.searchParams.get("debug") === "true";

      const date = url.searchParams.get("date");
      const days = Number(url.searchParams.get("days") || 14);

      let from, to;
      if (date) {
        from = date;
        to = date;
      } else {
        to = isoDate(new Date());
        from = isoDate(new Date(Date.now() - days * 86400000));
      }

      const res = await syncRange(env, from, to, write, debug);
      return json(res);
    }

    return new Response("not found", { status: 404 });
  },
};

// ================= CORE =================

async function syncRange(env, from, to, write, debug) {
  const acts = await fetchActivities(env, from, to);

  const byDay = new Map();
  const debugOut = {};

  for (const a of acts) {
    if (!isRun(a)) continue;

    const day = a.start_date_local.slice(0, 10);
    if (!byDay.has(day)) byDay.set(day, []);
    byDay.get(day).push(a);
  }

  const patches = {};
  let daysWritten = 0;

  for (const [day, runs] of byDay.entries()) {
    const patch = {};
    const infos = [];

    for (const a of runs) {
      const isKey = hasKeyTag(a);
      const ga = !isKey && a.moving_time >= 1800;

      const ef = efFromSummary(a);
      const drift = a.decoupling ?? null;
      const ttt = a.compliance ?? null;

      // ----- Score (execution only) -----
      const score = computeScore({
        ga,
        isKey,
        drift,
        ttt,
        load: a.icu_training_load,
      });

      if (ga) {
        if (ef) patch.VDOT = round(ef * 1200, 1);
        if (drift != null) patch.Drift = round(drift, 1);
      } else {
        if (ef) patch.EF = round(ef, 5);
      }

      if (ttt != null) patch.TTT = round(ttt, 1);
      patch.Score = score;

      infos.push({
        ga,
        isKey,
        drift,
        ttt,
        score,
      });
    }

    // ----- Progress Trends (28d GA only) -----
    const trend = await computeTrends(env, day);

    // ----- Minimum stimulus (Run-TSS only) -----
    const runLoad7 = await runLoadLast7Days(env, day);
    const minOk = runLoad7 >= 100;

    // ----- Comment -----
    patch.comments = renderComment({
      infos,
      trend,
      minOk,
      runLoad7,
    });

    patches[day] = patch;

    if (write) {
      await putWellness(env, day, patch);
      daysWritten++;
    }
  }

  return {
    ok: true,
    from,
    to,
    daysComputed: Object.keys(patches).length,
    daysWritten,
    patches: debug ? patches : undefined,
  };
}

// ================= METRICS =================

function computeScore({ ga, isKey, drift, ttt, load }) {
  const C = Math.min(load || 0, 70);

  let Q = 65;
  if (ga && drift != null) Q = drift <= 6 ? 90 : drift <= 10 ? 70 : 45;
  if (isKey && ttt != null) Q = ttt >= 95 ? 95 : ttt >= 90 ? 85 : ttt >= 80 ? 65 : 40;

  return round(0.75 * Q + 0.25 * C, 1);
}

function efFromSummary(a) {
  if (!a.average_speed || !a.average_heartrate) return null;
  return a.average_speed / a.average_heartrate;
}

// ================= COMMENT =================

function renderComment({ infos, trend, minOk, runLoad7 }) {
  const avgScore = Math.round(
    infos.reduce((s, x) => s + x.score, 0) / infos.length
  );

  const lines = [];

  // Score
  lines.push(`${scoreEmoji(avgScore)} Trainingausführung`);
  lines.push(`Score Ø: ${avgScore}/100`);

  // Trend
  lines.push("");
  lines.push(trend.text);

  // Minimum
  lines.push("");
  if (minOk) {
    lines.push("ℹ️ Mindest-Laufreiz erreicht");
    lines.push(`7-Tage Lauf-Load ≥ 100 (${Math.round(runLoad7)})`);
  } else {
    lines.push("ℹ️ Mindest-Laufreiz unterschritten");
    lines.push(`7-Tage Lauf-Load < 100 (${Math.round(runLoad7)})`);
    lines.push("➡️ Kurzfristig ok – langfristig kein Aufbau.");
  }

  return lines.join("\n");
}

function scoreEmoji(s) {
  if (s >= 85) return "🟢";
  if (s >= 70) return "🟡";
  if (s >= 55) return "🟠";
  return "🔴";
}

// ================= TRENDS =================

async function computeTrends(env, day) {
  const end = new Date(day);
  const mid = new Date(end.getTime() - 28 * 86400000);
  const start = new Date(end.getTime() - 56 * 86400000);

  const acts = await fetchActivities(
    env,
    isoDate(start),
    isoDate(end)
  );

  const ga = acts.filter(
    (a) =>
      isRun(a) &&
      !hasKeyTag(a) &&
      a.moving_time >= 1800 &&
      a.decoupling != null
  );

  const recent = ga.filter((a) => new Date(a.start_date_local) >= mid);
  const prev = ga.filter((a) => new Date(a.start_date_local) < mid);

  const v1 = avg(recent.map((a) => efFromSummary(a)));
  const v0 = avg(prev.map((a) => efFromSummary(a)));

  const d1 = median(recent.map((a) => a.decoupling));
  const d0 = median(prev.map((a) => a.decoupling));

  const dv = v0 ? ((v1 - v0) / v0) * 100 : 0;
  const dd = d1 - d0;

  let emoji = "🟡";
  let verdict = "Gemischtes Signal";

  if (dv > 1.5 && dd <= 0) {
    emoji = "🟢";
    verdict = "Aerober Fortschritt";
  } else if (dv < -1.5 && dd > 1) {
    emoji = "🔴";
    verdict = "Warnsignal";
  }

  return {
    dv,
    dd,
    text: `${emoji} ${verdict}\nVDOT-Trend: ${dv.toFixed(
      1
    )}% | Drift-Trend: ${dd > 0 ? "↑" : "↓"} ${Math.abs(dd).toFixed(1)}%`,
  };
}

// ================= LOAD =================

async function runLoadLast7Days(env, day) {
  const end = new Date(day);
  const start = new Date(end.getTime() - 7 * 86400000);

  const acts = await fetchActivities(
    env,
    isoDate(start),
    isoDate(end)
  );

  return acts
    .filter((a) => isRun(a))
    .reduce((s, a) => s + (a.icu_training_load || 0), 0);
}

// ================= API =================

async function fetchActivities(env, from, to) {
  const url = `https://intervals.icu/api/v1/athlete/0/activities?oldest=${from}&newest=${to}`;
  const r = await fetch(url, { headers: auth(env) });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function putWellness(env, day, body) {
  const url = `https://intervals.icu/api/v1/athlete/0/wellness/${day}`;
  const r = await fetch(url, {
    method: "PUT",
    headers: { ...auth(env), "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
}

function auth(env) {
  return {
    Authorization: "Basic " + btoa(`API_KEY:${env.INTERVALS_API_KEY}`),
  };
}

// ================= HELPERS =================

function isRun(a) {
  return String(a.type).toLowerCase().includes("run");
}

function hasKeyTag(a) {
  return (a.tags || []).some((t) => t.startsWith("key:"));
}

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function avg(arr) {
  const v = arr.filter((x) => x != null);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

function median(arr) {
  const v = arr.filter((x) => x != null).sort((a, b) => a - b);
  if (!v.length) return null;
  const m = Math.floor(v.length / 2);
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}

function round(x, n) {
  const p = 10 ** n;
  return Math.round(x * p) / p;
}

function json(o) {
  return new Response(JSON.stringify(o, null, 2), {
    headers: { "content-type": "application/json" },
  });
}

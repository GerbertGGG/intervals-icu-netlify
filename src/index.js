// src/index.js
// Cloudflare Worker
// - Berechnet VDOT_like + Drift (GA), EF (sonstige), TTT (Intervall)
// - Zusätzlich: Overall Score (0..100) pro Tag (Qualität-dominiert + Consistency aus icu_training_load)
// - Schreibt Werte + einen kurzen erklärenden Text ins Wellness-"comments" Feld
// - Schreiben NUR wenn ?write=true
// - Debug-Ausgabe NUR wenn ?debug=true
//
// Required secret:
// - INTERVALS_API_KEY
//
// Wellness Custom Fields (numeric) in Intervals anlegen (Codes exakt so):
// - VDOT, Drift, EF, TTT, Score

export default {
  async scheduled(event, env, ctx) {
    // Cron schreibt IMMER (letzte 14 Tage)
    ctx.waitUntil(sync(env, 14, true));
  },

  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (url.pathname === "/") return new Response("ok");

    if (url.pathname === "/sync") {
      const write = (url.searchParams.get("write") || "").toLowerCase() === "true";
      const debug = (url.searchParams.get("debug") || "").toLowerCase() === "true";

      const date = url.searchParams.get("date"); // YYYY-MM-DD
      const from = url.searchParams.get("from"); // YYYY-MM-DD
      const to = url.searchParams.get("to");     // YYYY-MM-DD
      const days = clampInt(url.searchParams.get("days") ?? "14", 1, 31);

      let oldest, newest;

      if (date) {
        oldest = date;
        newest = date;
      } else if (from && to) {
        oldest = from;
        newest = to;
      } else {
        newest = isoDate(new Date());
        oldest = isoDate(new Date(Date.now() - days * 86400000));
      }

      // Safety rails
      if (!isIsoDate(oldest) || !isIsoDate(newest)) {
        return json({ ok: false, error: "Invalid date format (YYYY-MM-DD)" }, 400);
      }
      if (newest < oldest) {
        return json({ ok: false, error: "`to` must be >= `from`" }, 400);
      }
      const rangeDays = diffDays(oldest, newest);
      if (rangeDays > 31) {
        return json({ ok: false, error: "Max range is 31 days" }, 400);
      }
      const oldestAllowed = isoDate(new Date(Date.now() - 365 * 86400000));
      if (oldest < oldestAllowed) {
        return json({ ok: false, error: "Date too old (max 365 days back)" }, 400);
      }

      if (debug) {
        const result = await syncRange(env, oldest, newest, write, true);
        return json(result);
      } else {
        ctx.waitUntil(syncRange(env, oldest, newest, write, false));
        return json({ ok: true, oldest, newest, write });
      }
    }

    return new Response("Not found", { status: 404 });
  },
};

// ================= CONFIG =================
const GA_TAGS = ["GA", "Z2", "Easy"];

const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_EF = "EF";
const FIELD_TTT = "TTT";
const FIELD_SCORE = "Score";

// ================= SYNC =================
async function sync(env, days, write) {
  const newest = isoDate(new Date());
  const oldest = isoDate(new Date(Date.now() - days * 86400000));
  return syncRange(env, oldest, newest, write, false);
}

async function syncRange(env, oldest, newest, write, debug = false) {
  const acts = await fetchIntervalsActivities(env, oldest, newest);
  acts.sort((a, b) => String(a.start_date || "").localeCompare(String(b.start_date || "")));

  const dayPatch = new Map();
  const dayMeta = new Map(); // collects per-day info for comment
  const debugOut = debug ? {} : null;

  let activitiesSeen = 0;
  let activitiesUsed = 0;

  for (const a of acts) {
    activitiesSeen++;

    if (!isRun(a)) {
      if (debug) addDebug(debugOut, a, null, null, null, null, null, "skip:not_run");
      continue;
    }

    const day = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!day) {
      if (debug) addDebug(debugOut, a, null, null, null, null, null, "skip:no_day");
      continue;
    }

    const patch = dayPatch.get(day) || {};

    // Summary fallbacks
    const efSummary = extractActivityEF(a);
    const tttSummary = extractActivityTTT(a);          // compliance (%)
    const driftSummary = extractActivityDecoupling(a); // optional
    const load = extractActivityLoad(a);               // icu_training_load

    // Streams only if we might need them
    let streams = null;
    let qStreams = null;
    let tttStreams = null;

    const mightNeedDrift = isProbablyGA(a) && (driftSummary == null);
    const mightNeedTTT = (tttSummary == null);
    const mightNeedEF = (efSummary == null);

    if (mightNeedDrift || mightNeedTTT || mightNeedEF) {
      try {
        streams = await fetchIntervalsStreams(env, a.id, [
          "heartrate",
          "velocity_smooth",
          "velocity",
          "pace",
          "time",
          "distance",
        ]);
      } catch {
        streams = null;
      }

      if (streams) {
        qStreams = calcEfAndDriftFromStreams(streams);
        tttStreams = calcTTTFromStreamsFlexible(streams);
      }
    }

    // Final metric selection
    const ef = qStreams?.ef_overall ?? efSummary ?? null;
    const drift = driftSummary ?? qStreams?.drift_pct ?? null;
    const ttt = tttSummary ?? (tttStreams?.isIntervalWorkout ? tttStreams.ttt_pct : null);

    // Classification
    const ga = isGrundlageWithOptionalDrift(a, drift);
    const isKey = hasKeyTag(a);

    // Write base fields
    let wrote = false;

    if (ga) {
      if (ef != null) {
        patch[FIELD_VDOT] = round(vdotLikeFromEf(ef), 1);
        wrote = true;
      }
      if (drift != null) {
        patch[FIELD_DRIFT] = round(drift, 1);
        wrote = true;
      }
    } else {
      if (ef != null) {
        patch[FIELD_EF] = round(ef, 5);
        wrote = true;
      }
    }

    if (ttt != null) {
      patch[FIELD_TTT] = round(ttt, 1);
      wrote = true;
    }

    // ===== Score =====
    const C = scoreConsistency(load);
    const AQ = (drift != null) ? scoreAerobicQuality(drift) : 70;
    const IQ = (ttt != null) ? scoreIntervalQuality(ttt) : 60;

    let overall;
    if (isKey) overall = 0.75 * IQ + 0.25 * C;
    else if (ga) overall = 0.75 * AQ + 0.25 * C;
    else {
      const Q = (ttt != null) ? IQ : (drift != null ? AQ : 65);
      overall = 0.75 * Q + 0.25 * C;
    }

    patch[FIELD_SCORE] = round(clamp(overall, 0, 100), 1);
    wrote = true;

    if (!wrote) {
      if (debug) addDebug(debugOut, a, ef, drift, ttt, load, null, streams ? "skip:no_metrics" : "skip:no_metrics_no_streams");
      continue;
    }

    // Store patch
    dayPatch.set(day, patch);
    activitiesUsed++;

    // Collect per-day meta for comment (aggregate)
    const m = dayMeta.get(day) || {
      day,
      hasKey: false,
      hasGA: false,
      loadSum: 0,
      drift: null,
      ttt: null,
      ef: null,
      score: null,
      scoreC: null,
      scoreAQ: null,
      scoreIQ: null,
      activities: 0,
    };

    m.activities += 1;
    m.hasKey = m.hasKey || isKey;
    m.hasGA = m.hasGA || ga;
    m.loadSum += (Number.isFinite(load) ? load : 0);

    // keep "best" available signals
    if (m.drift == null && drift != null) m.drift = drift;
    if (m.ttt == null && ttt != null) m.ttt = ttt;
    if (m.ef == null && ef != null) m.ef = ef;

    // prefer the score we just computed (latest), but if multiple activities, keep max
    const s = Number(patch[FIELD_SCORE]);
    if (Number.isFinite(s)) {
      m.score = (m.score == null) ? s : Math.max(m.score, s);
      m.scoreC = scoreConsistency(m.loadSum);
      m.scoreAQ = (m.drift != null) ? scoreAerobicQuality(m.drift) : 70;
      m.scoreIQ = (m.ttt != null) ? scoreIntervalQuality(m.ttt) : 60;
    }

    dayMeta.set(day, m);

    if (debug) addDebug(debugOut, a, ef, drift, ttt, load, patch[FIELD_SCORE], streams ? "ok" : "ok:summary_only");
  }

  // ===== Build & attach wellness comments per day =====
  for (const [day, patch] of dayPatch.entries()) {
    const m = dayMeta.get(day);
    if (!m) continue;
    patch.comments = renderWellnessComment(m); // <-- Intervals wellness comment field "comments"  [oai_citation:2‡Intervals.icu Forum](https://forum.intervals.icu/t/api-access-to-intervals-icu/609?page=11&utm_source=chatgpt.com)
  }

  let daysWritten = 0;
  if (write) {
    for (const [day, patch] of dayPatch.entries()) {
      await putWellnessDay(env, day, patch);
      daysWritten++;
    }
  }

  return {
    ok: true,
    oldest,
    newest,
    write,
    activitiesSeen,
    activitiesUsed,
    daysComputed: dayPatch.size,
    daysWritten: write ? daysWritten : 0,
    patches: debug ? Object.fromEntries(dayPatch.entries()) : undefined,
    debug: debug ? debugOut : undefined,
  };
}

// ================= COMMENT RENDERING =================
function renderWellnessComment(m) {
  // pick main mode for the day:
  // - if any key activity -> key day
  // - else if any GA -> GA day
  // - else mixed day
  const mode = m.hasKey ? "KEY" : (m.hasGA ? "GA" : "MIX");
  const score = (m.score != null) ? round(m.score, 0) : null;
  const load = round(clamp(m.loadSum, 0, 999), 0);

  const emoji = scoreEmoji(score);
  const verdict = scoreVerdict(score);

  const lines = [];
  lines.push(`${emoji} ${verdict}`);
  if (score != null) lines.push(`Score: ${score}/100 | Load: ${load}`);
  else lines.push(`Load: ${load}`);

  if (mode === "GA") {
    // explain drift
    if (m.drift != null) {
      lines.push(`GA-Qualität: Drift ${round(m.drift, 1)}% (${driftLabel(m.drift)})`);
    } else {
      lines.push(`GA-Qualität: Drift n/a (nur Basis-Heuristik)`);
    }
    lines.push(`➡️ Ziel: stabil & ökonomisch. Niedriger Drift = gut.`);
  } else if (mode === "KEY") {
    // explain TTT
    if (m.ttt != null) {
      lines.push(`Intervall-Qualität: TTT ${round(m.ttt, 1)}% (${tttLabel(m.ttt)})`);
    } else {
      lines.push(`Intervall-Qualität: TTT n/a`);
    }
    lines.push(`➡️ Ziel: sauber treffen. Hoher TTT = gute Umsetzung.`);
  } else {
    // mixed / sonstige
    if (m.ttt != null) lines.push(`TTT: ${round(m.ttt, 1)}%`);
    if (m.drift != null) lines.push(`Drift: ${round(m.drift, 1)}%`);
    if (m.ef != null) lines.push(`EF: ${round(m.ef, 5)}`);
    lines.push(`➡️ Gemischt: Score bewertet v.a. Qualität + (gedeckelte) Belastung.`);
  }

  // lightweight guidance
  const tip = nextStepTip(mode, score);
  if (tip) lines.push(tip);

  return lines.join("\n");
}

function scoreEmoji(score) {
  if (score == null) return "ℹ️";
  if (score >= 85) return "🟢";
  if (score >= 70) return "🟡";
  if (score >= 55) return "🟠";
  return "🔴";
}

function scoreVerdict(score) {
  if (score == null) return "Training bewertet";
  if (score >= 85) return "Sehr guter Tag";
  if (score >= 70) return "Solider Tag";
  if (score >= 55) return "Grenzwertig (Ermüdung möglich)";
  return "Warnsignal (Qualität niedrig)";
}

function driftLabel(d) {
  if (d <= 3) return "sehr stabil";
  if (d <= 6) return "stabil";
  if (d <= 10) return "ok";
  if (d <= 15) return "hoch";
  return "sehr hoch";
}

function tttLabel(t) {
  if (t >= 95) return "sehr sauber";
  if (t >= 90) return "gut";
  if (t >= 80) return "teilweise";
  return "verfehlt";
}

function nextStepTip(mode, score) {
  if (score == null) return null;
  if (score >= 85) return "➡️ Morgen normal weiter (Plan beibehalten).";
  if (score >= 70) return "➡️ Morgen ok – bei Bedarf etwas lockerer.";
  if (score >= 55) return "➡️ Morgen eher locker/kürzer oder ohne Intensität.";
  return mode === "KEY"
    ? "➡️ Nächste Intensität verschieben oder reduzieren."
    : "➡️ Fokus auf Erholung (locker, kurz, stabil).";
}

// ================= DEBUG =================
function addDebug(debugOut, a, ef, drift, ttt, load, score, status) {
  if (!debugOut) return;
  const day = String(a.start_date_local || a.start_date || "").slice(0, 10) || "unknown-day";
  debugOut[day] ??= [];
  debugOut[day].push({
    activityId: a.id ?? null,
    start: a.start_date ?? null,
    start_local: a.start_date_local ?? null,
    type: a.type ?? a.activity_type ?? null,
    tags: a.tags ?? [],
    stream_types: a.stream_types ?? [],
    status,
    has_heartrate: a.has_heartrate ?? null,
    average_speed: a.average_speed ?? null,
    average_heartrate: a.average_heartrate ?? null,
    compliance: a.compliance ?? null,
    icu_training_load: a.icu_training_load ?? null,
    ga: isGrundlageWithOptionalDrift(a, drift),
    isKey: hasKeyTag(a),
    ef,
    drift,
    ttt,
    score,
  });
}

// ================= CLASSIFICATION =================
function isRun(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t === "run" || t === "running" || t.includes("run") || t.includes("laufen");
}

function hasKeyTag(a) {
  return (a?.tags || []).some((t) => String(t).toLowerCase().startsWith("key:"));
}

function isProbablyGA(a) {
  const tags = (a.tags || []).map(String);
  if (tags.some((t) => GA_TAGS.includes(t))) return true;

  const dur = Number(a.moving_time || a.elapsed_time || 0);
  if (dur < 30 * 60) return false;

  if (hasKeyTag(a)) return false;
  return true;
}

function isGrundlageWithOptionalDrift(a, driftMaybe) {
  const tags = (a.tags || []).map(String);
  if (tags.some((t) => GA_TAGS.includes(t))) return true;

  if (hasKeyTag(a)) return false;

  const dur = Number(a.moving_time || a.elapsed_time || 0);
  if (dur < 30 * 60) return false;

  if (driftMaybe != null) return driftMaybe <= 10;
  return true;
}

// ================= SUMMARY FALLBACK EXTRACTORS =================
function extractActivityEF(a) {
  const sp = Number(a?.average_speed);
  const hr = Number(a?.average_heartrate);
  if (Number.isFinite(sp) && sp > 0 && Number.isFinite(hr) && hr > 0) return sp / hr;
  return null;
}

function extractActivityTTT(a) {
  const c = Number(a?.compliance);
  if (Number.isFinite(c) && c > 0) return c; // percent 0..100
  return null;
}

function extractActivityDecoupling(a) {
  const v1 = Number(a?.pahr_decoupling);
  if (Number.isFinite(v1) && v1 > 0) return v1;

  const v2 = Number(a?.pwhr_decoupling);
  if (Number.isFinite(v2) && v2 > 0) return v2;

  const v3 = Number(a?.decoupling);
  if (Number.isFinite(v3) && v3 > 0) return v3;

  return null;
}

function extractActivityLoad(a) {
  const l1 = Number(a?.icu_training_load);
  if (Number.isFinite(l1) && l1 >= 0) return l1;
  const l2 = Number(a?.hr_load);
  if (Number.isFinite(l2) && l2 >= 0) return l2;
  return 0;
}

// ================= SCORE FUNCTIONS (0..100) =================
function scoreConsistency(load) {
  if (!Number.isFinite(load) || load <= 0) return 0;
  return clamp(load, 0, 70); // cap
}

function scoreAerobicQuality(d) {
  if (!Number.isFinite(d)) return 70;
  if (d <= 3) return 100;
  if (d <= 6) return lerp(100, 90, (d - 3) / 3);
  if (d <= 10) return lerp(90, 70, (d - 6) / 4);
  if (d <= 15) return lerp(70, 45, (d - 10) / 5);
  if (d <= 20) return lerp(45, 20, (d - 15) / 5);
  return 20;
}

function scoreIntervalQuality(t) {
  if (!Number.isFinite(t)) return 60;
  if (t >= 95) return clamp(95 + (t - 95) * 1, 95, 100);
  if (t >= 90) return lerp(80, 95, (t - 90) / 5);
  if (t >= 80) return lerp(50, 80, (t - 80) / 10);
  if (t <= 60) return 0;
  return lerp(0, 50, (t - 60) / 20);
}

// ================= METRICS FROM STREAMS =================
function calcEfAndDriftFromStreams(streams) {
  const hr = streams.heartrate;
  const speed = pickSpeedFromStreams(streams); // m/s
  if (!hr || !speed) return null;

  const n = Math.min(hr.length, speed.length);
  if (n < 300) return null;

  const half = Math.floor(n / 2);

  const ef = (a, b) => {
    let s = 0, c = 0;
    for (let i = a; i < b; i++) {
      const h = hr[i];
      const sp = speed[i];
      if (!h || h < 40) continue;
      if (!sp || sp <= 0) continue;
      s += sp / h;
      c++;
    }
    return c ? s / c : null;
  };

  const ef1 = ef(0, half);
  const ef2 = ef(half, n);
  if (ef1 == null || ef2 == null) return null;

  return { ef_overall: (ef1 + ef2) / 2, drift_pct: ((ef2 - ef1) / ef1) * 100 };
}

function calcTTTFromStreamsFlexible(streams) {
  const speed = pickSpeedFromStreams(streams);
  if (!speed) return null;
  return calcTTTFromSpeed(speed);
}

function pickSpeedFromStreams(streams) {
  if (!streams) return null;

  if (Array.isArray(streams.velocity_smooth) && streams.velocity_smooth.length) return streams.velocity_smooth;
  if (Array.isArray(streams.velocity) && streams.velocity.length) return streams.velocity;

  if (Array.isArray(streams.pace) && streams.pace.length) {
    const p = streams.pace;
    const p50 = percentile(p.filter((x) => typeof x === "number" && x > 0), 50);
    if (p50 && p50 > 20) return p.map((secPerKm) => (secPerKm > 0 ? 1000 / secPerKm : 0));
    return p;
  }

  return null;
}

function vdotLikeFromEf(ef) {
  return ef * 1200;
}

// ================= TTT (from speed array) =================
function calcTTTFromSpeed(speed) {
  if (!speed || speed.length < 600) return null;
  const v = speed.filter((x) => typeof x === "number" && x > 0);
  if (v.length < 600) return null;

  const p50 = percentile(v, 50);
  const p90 = percentile(v, 90);
  if (!p50 || !p90 || p90 <= p50 * 1.08) return { isIntervalWorkout: false };

  const workThr = (p50 + p90) / 2;
  const segs = detectSegments(speed, workThr, 60, 25);
  if (segs.length < 3) return { isIntervalWorkout: false };

  const center = meanSegmentSpeed(speed, segs.slice(0, 2));
  if (!center) return { isIntervalWorkout: false };

  const low = center * 0.97;
  const high = center * 1.03;

  let planned = 0, hit = 0;
  for (const [a, b] of segs) {
    planned += b - a;
    for (let i = a; i < b; i++) {
      const s = speed[i];
      if (s >= low && s <= high) hit++;
    }
  }
  if (!planned) return { isIntervalWorkout: false };
  return { isIntervalWorkout: true, ttt_pct: (hit / planned) * 100 };
}

function detectSegments(speed, thr, minLen, maxDrop) {
  const segs = [];
  let i = 0;

  while (i < speed.length) {
    while (i < speed.length && speed[i] < thr) i++;
    if (i >= speed.length) break;

    const start = i;
    let below = 0;

    while (i < speed.length) {
      if (speed[i] >= thr) below = 0;
      else below++;
      if (below > maxDrop) break;
      i++;
    }

    const end = i - below;
    if (end - start >= minLen) segs.push([start, end]);
  }
  return segs;
}

function meanSegmentSpeed(speed, segs) {
  let s = 0, c = 0;
  for (const [a, b] of segs) {
    for (let i = a; i < b; i++) {
      const sp = speed[i];
      if (typeof sp === "number" && sp > 0) {
        s += sp;
        c++;
      }
    }
  }
  return c ? s / c : null;
}

function percentile(arr, p) {
  const clean = arr.filter((x) => typeof x === "number" && Number.isFinite(x));
  if (!clean.length) return null;
  const a = [...clean].sort((x, y) => x - y);
  const i = (p / 100) * (a.length - 1);
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? a[lo] : a[lo] + (a[hi] - a[lo]) * (i - lo);
}

// ================= INTERVALS API =================
async function fetchIntervalsActivities(env, oldest, newest) {
  const url = `https://intervals.icu/api/v1/athlete/0/activities?oldest=${oldest}&newest=${newest}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(`activities ${r.status}: ${await r.text()}`);
  return r.json();
}

async function fetchIntervalsStreams(env, id, types) {
  const url = `https://intervals.icu/api/v1/activity/${id}/streams?types=${encodeURIComponent(types.join(","))}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(`streams ${r.status}: ${await r.text()}`);
  const raw = await r.json();
  return normalizeStreams(raw);
}

function normalizeStreams(raw) {
  if (!raw) return null;

  // direct object
  if (raw.heartrate || raw.velocity_smooth || raw.velocity || raw.pace || raw.time || raw.distance) return raw;

  // wrapper
  if (raw.streams && (raw.streams.heartrate || raw.streams.velocity_smooth || raw.streams.velocity || raw.streams.pace)) return raw.streams;
  if (raw.data && (raw.data.heartrate || raw.data.velocity_smooth || raw.data.velocity || raw.data.pace)) return raw.data;

  // array format
  if (Array.isArray(raw)) {
    const out = {};
    for (const item of raw) {
      const type = item?.type ?? item?.name ?? item?.key;
      const data = item?.data ?? item?.values ?? item?.stream;
      if (type && Array.isArray(data)) out[String(type)] = data;
    }
    return out;
  }

  return raw;
}

async function putWellnessDay(env, day, patch) {
  const url = `https://intervals.icu/api/v1/athlete/0/wellness/${day}`;
  const r = await fetch(url, {
    method: "PUT",
    headers: { Authorization: auth(env), "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (!r.ok) throw new Error(`wellness PUT ${day} ${r.status}: ${await r.text()}`);
}

function auth(env) {
  return "Basic " + btoa(`API_KEY:${env.INTERVALS_API_KEY}`);
}

// ================= HELPERS =================
function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function round(x, n) {
  const p = 10 ** n;
  return Math.round(x * p) / p;
}

function clampInt(x, min, max) {
  const n = Number(x);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
}

function isIsoDate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s));
}

function diffDays(a, b) {
  const da = new Date(a + "T00:00:00Z").getTime();
  const db = new Date(b + "T00:00:00Z").getTime();
  return Math.round((db - da) / 86400000);
}

function json(o, status = 200) {
  return new Response(JSON.stringify(o), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

function lerp(a, b, t) {
  return a + (b - a) * clamp(t, 0, 1);
}
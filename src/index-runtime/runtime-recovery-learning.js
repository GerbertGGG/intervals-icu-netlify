export default `

const RECOVERY_PROFILE_KV_PREFIX = "recovery_profile:";
const RECOVERY_PROFILE_EWMA_ALPHA = 0.15;
const RECOVERY_PROFILE_MIN_SAMPLES = 5;

function recoveryProfileKvKey(env) {
  return \`\${RECOVERY_PROFILE_KV_PREFIX}\${mustEnv(env, "ATHLETE_ID")}\`;
}

async function loadRecoveryProfile(env) {
  if (!hasKv(env)) return null;
  return readKvJson(env, recoveryProfileKvKey(env));
}

async function saveRecoveryProfile(env, profile) {
  if (!hasKv(env)) return;
  await writeKvJson(env, recoveryProfileKvKey(env), profile);
}

function ewmaUpdate(current, newVal, alpha) {
  if (!Number.isFinite(newVal)) return current;
  if (current == null || !Number.isFinite(current)) return newVal;
  return current * (1 - alpha) + newVal * alpha;
}

function recomputeTippingPoint(samples) {
  if (!Array.isArray(samples) || samples.length < 10) return null;
  const sorted = [...samples].sort((a, b) => a.load - b.load);
  const splitIdx = Math.floor(sorted.length * 0.7);
  const low = sorted.slice(0, splitIdx);
  const high = sorted.slice(splitIdx);
  if (!low.length || !high.length) return null;
  const avgFatigueLow = low.reduce((s, x) => s + x.ft1, 0) / low.length;
  const avgFatigueHigh = high.reduce((s, x) => s + x.ft1, 0) / high.length;
  if (avgFatigueHigh <= avgFatigueLow * 1.25) return null;
  return {
    threshold: Math.round(sorted[splitIdx].load),
    avgFatigueBelow: round(avgFatigueLow, 1),
    avgFatigueAbove: round(avgFatigueHigh, 1),
    confidence: sorted.length >= 25 ? "high" : "medium",
    n: sorted.length,
  };
}

function updateRecoveryProfile(profile, { keyType, load, fatigueT1, sorenessT1, fatigueT2, sorenessT2 }) {
  if (!profile) {
    profile = {
      byType: {},
      tippingSamples: [],
      tippingPoint: null,
      personalBaselines: { avgFatigue: null, avgSoreness: null, n: 0 },
      updatedAt: null,
    };
  }
  if (!profile.byType) profile.byType = {};
  if (!Array.isArray(profile.tippingSamples)) profile.tippingSamples = [];
  if (!profile.personalBaselines) profile.personalBaselines = { avgFatigue: null, avgSoreness: null, n: 0 };

  const alpha = RECOVERY_PROFILE_EWMA_ALPHA;

  if (keyType && Number.isFinite(fatigueT1) && fatigueT1 > 0) {
    if (!profile.byType[keyType]) {
      profile.byType[keyType] = { n: 0, ft1: null, st1: null, ft2: null, st2: null };
    }
    const t = profile.byType[keyType];
    t.n++;
    t.ft1 = ewmaUpdate(t.ft1, fatigueT1, alpha);
    if (Number.isFinite(sorenessT1) && sorenessT1 > 0) t.st1 = ewmaUpdate(t.st1, sorenessT1, alpha);
    if (Number.isFinite(fatigueT2) && fatigueT2 > 0) t.ft2 = ewmaUpdate(t.ft2, fatigueT2, alpha);
    if (Number.isFinite(sorenessT2) && sorenessT2 > 0) t.st2 = ewmaUpdate(t.st2, sorenessT2, alpha);
  }

  if (Number.isFinite(fatigueT1) && fatigueT1 > 0) {
    profile.personalBaselines.avgFatigue = ewmaUpdate(profile.personalBaselines.avgFatigue, fatigueT1, alpha);
    profile.personalBaselines.n = (profile.personalBaselines.n || 0) + 1;
  }
  if (Number.isFinite(sorenessT1) && sorenessT1 > 0) {
    profile.personalBaselines.avgSoreness = ewmaUpdate(profile.personalBaselines.avgSoreness, sorenessT1, alpha);
  }

  if (Number.isFinite(load) && load > 0 && Number.isFinite(fatigueT1) && fatigueT1 > 0) {
    profile.tippingSamples.push({ load: Math.round(load), ft1: fatigueT1 });
    if (profile.tippingSamples.length > 80) profile.tippingSamples = profile.tippingSamples.slice(-80);
    profile.tippingPoint = recomputeTippingPoint(profile.tippingSamples);
  }

  profile.updatedAt = isoDate(new Date());
  return profile;
}

function getPersonalWellnessThresholds(recoveryProfile) {
  const b = recoveryProfile?.personalBaselines;
  if (!b || b.n < RECOVERY_PROFILE_MIN_SAMPLES) return null;
  const { avgFatigue, avgSoreness } = b;
  if (!Number.isFinite(avgFatigue)) return null;
  return {
    fatigueLow: Math.max(4, Math.round(avgFatigue * 1.35)),
    fatigueHigh: Math.max(6, Math.round(avgFatigue * 1.75)),
    sorenessLow: Number.isFinite(avgSoreness) && avgSoreness > 0 ? Math.max(3, Math.round(avgSoreness * 1.4)) : null,
    sorenessHigh: Number.isFinite(avgSoreness) && avgSoreness > 0 ? Math.max(5, Math.round(avgSoreness * 1.8)) : null,
  };
}

async function handleBackfillRecoveryProfileRequest(url, env) {
  if (!hasKv(env)) return json({ ok: false, error: "KV not available" }, 500);
  const weeksRaw = parseInt(url.searchParams.get("weeks") || "24", 10);
  const weeks = Math.min(Math.max(isNaN(weeksRaw) ? 24 : weeksRaw, 1), 52);
  const today = isoDate(new Date());
  const oldest = isoDate(new Date(Date.now() - weeks * 7 * 86400000));
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const auth = authHeader(env);

  let activities;
  try {
    activities = await fetchIntervalsActivities(env, oldest, today, false);
  } catch (e) {
    return json({ ok: false, error: "Failed to fetch activities: " + String(e?.message || e) }, 500);
  }

  const trainDays = [];
  for (const a of activities) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < oldest || d > today) continue;
    const load = extractLoad(a);
    if (!Number.isFinite(load) || load < 20) continue;
    const isKey = hasKeyTag(a);
    const rawKeyType = isKey ? getKeyType(a) : null;
    const keyType = rawKeyType
      ? normalizeKeyType(rawKeyType, { activity: a, movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0) })
      : (isRun(a) ? "steady" : null);
    if (!keyType) continue;
    const existing = trainDays.find((x) => x.day === d);
    if (existing) {
      existing.load += load;
      if (!existing.isKey && isKey) { existing.isKey = true; existing.keyType = keyType; }
    } else {
      trainDays.push({ day: d, load, keyType, isKey });
    }
  }

  trainDays.sort((a, b) => (a.day < b.day ? -1 : a.day > b.day ? 1 : 0));

  const wellnessCache = new Map();
  async function fetchWellnessDirect(dayIso) {
    if (wellnessCache.has(dayIso)) return wellnessCache.get(dayIso);
    const p = fetch(\`\${BASE_URL}/athlete/\${athleteId}/wellness/\${dayIso}\`, {
      headers: { Authorization: auth },
    }).then((r) => (r.ok ? r.json() : null)).catch(() => null);
    wellnessCache.set(dayIso, p);
    return p;
  }

  let profile = null;
  let processed = 0;

  for (const { day, load, keyType } of trainDays) {
    const dayT1 = isoDate(new Date(new Date(day + "T00:00:00Z").getTime() + 86400000));
    const dayT2 = isoDate(new Date(new Date(day + "T00:00:00Z").getTime() + 2 * 86400000));
    const [w1, w2] = await Promise.all([fetchWellnessDirect(dayT1), fetchWellnessDirect(dayT2)]);
    const fatigueT1 = Number(w1?.fatigue);
    if (!Number.isFinite(fatigueT1) || fatigueT1 <= 0) continue;
    profile = updateRecoveryProfile(profile, {
      keyType,
      load,
      fatigueT1,
      sorenessT1: Number.isFinite(Number(w1?.soreness)) && Number(w1?.soreness) > 0 ? Number(w1.soreness) : null,
      fatigueT2: Number.isFinite(Number(w2?.fatigue)) && Number(w2?.fatigue) > 0 ? Number(w2.fatigue) : null,
      sorenessT2: Number.isFinite(Number(w2?.soreness)) && Number(w2?.soreness) > 0 ? Number(w2.soreness) : null,
    });
    processed++;
  }

  if (profile) await saveRecoveryProfile(env, profile);
  return json({ ok: true, weeks, trainDaysFound: trainDays.length, processed, profile });
}

`;

import { readKvJson, writeKvJson, hasKv } from "./kv.js";

// Yazio has no official public API. This talks to the same internal endpoints the
// Yazio mobile app uses (reverse-engineered, documented at
// https://github.com/saganos/yazio_public_api and https://github.com/juriadams/yazio).
// client_id/client_secret below are the app's own public OAuth client credentials
// (not a per-user secret) - every third-party client using this API reuses them.
const BASE_URL = "https://yzapi.yazio.com/v15";
const CLIENT_ID = "1_4hiybetvfksgw40o0sog4s884kwc840wwso8go4k8c04goo4c";
const CLIENT_SECRET = "6rok2m65xuskgkgogw40wkkk8sw0osg84s8cggsc4woos4s8o";

const TOKEN_KV_KEY = "yazio:token";
const PRODUCT_KV_PREFIX = "yazio:product:";
const TOKEN_REFRESH_BUFFER_MS = 60_000;

export function hasYazioCredentials(env) {
  return Boolean(env?.YAZIO_USERNAME && env?.YAZIO_PASSWORD);
}

async function requestToken(formFields) {
  const r = await fetch(`${BASE_URL}/oauth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams(formFields),
  });
  if (!r.ok) throw new Error(`yazio oauth ${r.status}: ${await r.text()}`);
  return r.json();
}

async function persistToken(env, tokenResp) {
  const expiresAt = Date.now() + Number(tokenResp?.expires_in || 0) * 1000;
  await writeKvJson(env, TOKEN_KV_KEY, {
    access_token: tokenResp.access_token,
    refresh_token: tokenResp.refresh_token,
    expires_at: expiresAt,
  });
  return tokenResp.access_token;
}

// Reuses the cached access token until shortly before it expires, refreshes via
// refresh_token when possible, and only falls back to a full password login
// (which Yazio appears to rate-limit) when there is no usable refresh token.
async function getAccessToken(env) {
  const cached = hasKv(env) ? await readKvJson(env, TOKEN_KV_KEY) : null;
  if (cached?.access_token && Date.now() < Number(cached.expires_at || 0) - TOKEN_REFRESH_BUFFER_MS) {
    return cached.access_token;
  }

  if (cached?.refresh_token) {
    try {
      const refreshed = await requestToken({
        grant_type: "refresh_token",
        client_id: CLIENT_ID,
        client_secret: CLIENT_SECRET,
        refresh_token: cached.refresh_token,
      });
      return persistToken(env, refreshed);
    } catch (e) {
      console.warn("yazio token refresh failed, falling back to password login", String(e?.message ?? e));
    }
  }

  const loggedIn = await requestToken({
    grant_type: "password",
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
    username: String(env.YAZIO_USERNAME),
    password: String(env.YAZIO_PASSWORD),
  });
  return persistToken(env, loggedIn);
}

async function yazioGet(env, path) {
  const token = await getAccessToken(env);
  const r = await fetch(`${BASE_URL}${path}`, { headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) throw new Error(`yazio GET ${path} ${r.status}: ${await r.text()}`);
  return r.json();
}

async function fetchConsumedItems(env, dateIso) {
  const data = await yazioGet(env, `/user/consumed-items?date=${dateIso}`);
  const items = Array.isArray(data) ? data : [];
  return { items, raw: data };
}

// Debug-only description of what the API actually returned, so an unexpectedly
// empty diary (e.g. wrong response shape, wrong date semantics) can be told apart
// from a genuinely empty one without guessing. Includes one sample item per
// category (if the response turns out to be the {products, recipe_portions,
// simple_products} shape) so the real field names can be inspected directly
// instead of relying on possibly-stale third-party API docs.
function describeRawShape(raw) {
  if (Array.isArray(raw)) return { shape: `array(length=${raw.length})` };
  if (raw && typeof raw === "object") {
    const samples = {};
    for (const key of Object.keys(raw)) {
      if (Array.isArray(raw[key]) && raw[key].length > 0) samples[key] = raw[key][0];
    }
    return { shape: `object(keys=${Object.keys(raw).join(",")})`, samples };
  }
  return { shape: typeof raw };
}

// The athlete's own diet goal from the Yazio app (e.g. a deficit for weight loss).
// Used as the floor for CalorieGoal - see fetchYazioDailyNutrition's caller in sync.js.
export async function fetchYazioDailyGoalKcal(env, dateIso) {
  const summary = await yazioGet(env, `/user/widgets/daily-summary?date=${dateIso}`);
  const goal = Number(summary?.goals?.["energy.energy"]);
  return Number.isFinite(goal) && goal > 0 ? goal : null;
}

// Product nutrients (kcal/protein/fat/carb per gram) are static reference data, so
// they're cached in KV indefinitely rather than re-fetched on every sync.
async function fetchProductNutrientsPerGram(env, productId) {
  const kvKey = `${PRODUCT_KV_PREFIX}${productId}`;
  const cached = hasKv(env) ? await readKvJson(env, kvKey) : null;
  if (cached) return cached;

  const product = await yazioGet(env, `/products/${encodeURIComponent(productId)}`);
  const nutrients = {
    energyKcalPerG: Number(product?.nutrients?.["energy.energy"] || 0),
    proteinPerG: Number(product?.nutrients?.["nutrient.protein"] || 0),
    fatPerG: Number(product?.nutrients?.["nutrient.fat"] || 0),
    carbPerG: Number(product?.nutrients?.["nutrient.carb"] || 0),
  };
  await writeKvJson(env, kvKey, nutrients);
  return nutrients;
}

// Sums the day's diary entries into total calories/macros. Amounts already come back
// in grams from /user/consumed-items, so total = amount * per-gram nutrient value.
export async function fetchYazioDailyNutrition(env, dateIso) {
  const { items, raw } = await fetchConsumedItems(env, dateIso);
  if (items.length === 0) {
    return { energyKcal: 0, proteinG: 0, fatG: 0, carbG: 0, itemCount: 0, failedProductIds: [], rawShape: describeRawShape(raw) };
  }

  const uniqueProductIds = [...new Set(items.map((i) => i.product_id).filter(Boolean))];
  const nutrientsByProduct = new Map();
  const failedProductIds = [];
  for (const productId of uniqueProductIds) {
    // One bad/unavailable product (404 on a custom recipe, transient rate-limit, ...)
    // must not blow up the whole day's totals - skip it and keep summing the rest,
    // same as items that already have no product_id at all (see loop below).
    try {
      nutrientsByProduct.set(productId, await fetchProductNutrientsPerGram(env, productId));
    } catch (e) {
      failedProductIds.push(productId);
      console.warn("yazio product lookup failed, skipping item", { productId, error: String(e?.message ?? e) });
    }
  }

  const totals = { energyKcal: 0, proteinG: 0, fatG: 0, carbG: 0, itemCount: items.length, failedProductIds };
  for (const item of items) {
    const nutrients = nutrientsByProduct.get(item.product_id);
    const amount = Number(item?.amount || 0);
    if (!nutrients || !amount) continue;
    totals.energyKcal += amount * nutrients.energyKcalPerG;
    totals.proteinG += amount * nutrients.proteinPerG;
    totals.fatG += amount * nutrients.fatPerG;
    totals.carbG += amount * nutrients.carbPerG;
  }
  return totals;
}

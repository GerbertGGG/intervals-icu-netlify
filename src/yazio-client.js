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

// The API returns a bare array on some accounts, but (confirmed against a real
// account) normally returns {products, recipe_portions, simple_products} - three
// differently-shaped categories of diary entries, not one flat list. Flatten
// them here so the rest of the module only ever deals with a single item array.
async function fetchConsumedItems(env, dateIso) {
  const data = await yazioGet(env, `/user/consumed-items?date=${dateIso}`);
  const items = Array.isArray(data)
    ? data
    : data && typeof data === "object"
      ? [...(data.products || []), ...(data.recipe_portions || []), ...(data.simple_products || [])]
      : [];
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

// Product nutrients (kcal/protein/fat/carb per gram) plus display name are static
// reference data, so they're cached in KV indefinitely rather than re-fetched on
// every sync.
async function fetchProductInfo(env, productId) {
  const kvKey = `${PRODUCT_KV_PREFIX}${productId}`;
  const cached = hasKv(env) ? await readKvJson(env, kvKey) : null;
  if (cached) return cached;

  const product = await yazioGet(env, `/products/${encodeURIComponent(productId)}`);
  const info = {
    name: product?.name || product?.title || null,
    energyKcalPerG: Number(product?.nutrients?.["energy.energy"] || 0),
    proteinPerG: Number(product?.nutrients?.["nutrient.protein"] || 0),
    fatPerG: Number(product?.nutrients?.["nutrient.fat"] || 0),
    carbPerG: Number(product?.nutrients?.["nutrient.carb"] || 0),
  };
  await writeKvJson(env, kvKey, info);
  return info;
}

// name/daytime field names for simple_product and recipe_portion entries are a
// best-effort guess (not confirmed against a real sample the way the
// {products, recipe_portions, simple_products} split itself was - see
// describeRawShape/rawShape) - fall back to a generic label rather than crash if
// an account's response shapes those fields differently.
function dishNameFromInlineItem(item) {
  return item.name || item.title || item?.recipe?.name || item?.product?.name || "Unbenannter Eintrag";
}

// Sums the day's diary entries into total calories/macros, and builds a per-item
// dish list alongside the totals so a caller (e.g. the MCP get_nutrition tool) can
// show what was actually eaten, not just the daily sum. Two item shapes exist (see
// fetchConsumedItems): "product" entries carry a product_id + amount in grams and
// need a per-gram nutrient lookup; "simple_product" (AI-logged quick-adds) and
// (assumed, no confirmed sample yet) recipe portions carry their own absolute
// nutrients for that entry directly, no lookup or multiplication needed.
export async function fetchYazioDailyNutrition(env, dateIso) {
  const { items, raw } = await fetchConsumedItems(env, dateIso);
  if (items.length === 0) {
    return { energyKcal: 0, proteinG: 0, fatG: 0, carbG: 0, itemCount: 0, skippedItems: [], rawShape: describeRawShape(raw), items: [] };
  }

  const productItems = items.filter((i) => !i.nutrients && i.product_id);
  const uniqueProductIds = [...new Set(productItems.map((i) => i.product_id))];
  const infoByProduct = new Map();
  const skippedItems = [];
  for (const productId of uniqueProductIds) {
    // One bad/unavailable product (404 on a custom recipe, transient rate-limit, ...)
    // must not blow up the whole day's totals - skip it and keep summing the rest.
    try {
      infoByProduct.set(productId, await fetchProductInfo(env, productId));
    } catch (e) {
      skippedItems.push({ productId, reason: String(e?.message ?? e) });
      console.warn("yazio product lookup failed, skipping item", { productId, error: String(e?.message ?? e) });
    }
  }

  const totals = { energyKcal: 0, proteinG: 0, fatG: 0, carbG: 0, itemCount: items.length, skippedItems, items: [] };
  for (const item of items) {
    if (item.nutrients) {
      // simple_product / recipe_portion: nutrients are already the entry's total, not per-gram.
      const energyKcal = Number(item.nutrients["energy.energy"]) || 0;
      const proteinG = Number(item.nutrients["nutrient.protein"]) || 0;
      const fatG = Number(item.nutrients["nutrient.fat"]) || 0;
      const carbG = Number(item.nutrients["nutrient.carb"]) || 0;
      totals.energyKcal += energyKcal;
      totals.proteinG += proteinG;
      totals.fatG += fatG;
      totals.carbG += carbG;
      totals.items.push({ name: dishNameFromInlineItem(item), daytime: item.daytime || null, energyKcal, proteinG, fatG, carbG });
      continue;
    }
    if (!item.product_id) {
      // Neither an inline nutrients total nor a product_id to look up - unknown item
      // shape (e.g. a differently-structured recipe_portion). Skip and record it
      // instead of guessing, so a debug run can reveal what it actually looks like.
      skippedItems.push({ id: item.id, type: item.type, reason: "unrecognized item shape (no nutrients, no product_id)" });
      continue;
    }
    const info = infoByProduct.get(item.product_id);
    const amount = Number(item?.amount || 0);
    if (!info || !amount) {
      skippedItems.push({ id: item.id, type: item.type, reason: "no product_id match or zero amount" });
      continue;
    }
    const energyKcal = amount * info.energyKcalPerG;
    const proteinG = amount * info.proteinPerG;
    const fatG = amount * info.fatPerG;
    const carbG = amount * info.carbPerG;
    totals.energyKcal += energyKcal;
    totals.proteinG += proteinG;
    totals.fatG += fatG;
    totals.carbG += carbG;
    totals.items.push({
      name: info.name || `Produkt ${item.product_id}`,
      daytime: item.daytime || null,
      amountG: amount,
      energyKcal,
      proteinG,
      fatG,
      carbG,
    });
  }
  return totals;
}

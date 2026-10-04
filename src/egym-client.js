import { readKvJson, writeKvJson } from "./kv.js";

// EGYM hat keine oeffentliche API fuer Mitglieder. Dieser Client spricht dieselben internen Endpunkte an wie die
// EGYM-Fitness-App (Netpulse-Backend, reverse-engineered, siehe https://github.com/togabrennan/egym-personal-viewer
// und https://github.com/marcelpoelstra/python-egym). Die Feldnamen sind aus diesen Projekten uebernommen und noch
// nicht gegen echte Daten dieses Accounts geprueft: /api/egym-debug liefert die Rohdaten zum Abgleichen.
//
// Worker-Variablen (Secrets): EGYM_BRAND (Studio-Kennung, Host ist <brand>.netpulse.com), EGYM_USERNAME, EGYM_PASSWORD.
const MOBILE_API = "https://mobile-api.int.api.egym.com";
const SESSION_KV_KEY = "egym:session";
// Die Sitzung (Cookie) wird wiederverwendet; bei 401/403 gibt es einen neuen Login.
const SESSION_MAX_AGE_MS = 6 * 60 * 60 * 1000;

// Header wie die aktuelle EGYM-Fitness-App (iOS), siehe python-egym; die alte Kennung (NetpulseFitness 3.11) lehnt der Server teils mit 403 ab.
const APP_VERSION = "3.91";
const APP_BUILD = "1190";
const deviceUid = (env) => {
  const h = [...String(env.EGYM_USERNAME)].reduce((a, c) => (a * 31 + c.charCodeAt(0)) >>> 0, 7).toString(16).padStart(8, "0");
  return `${h}-0000-4000-8000-${h}0000`.toUpperCase().slice(0, 36);
};
const headersFor = (env) => ({
  Accept: "application/json,text/plain",
  "Accept-Language": "de-DE",
  "X-NP-API-Version": "1.5",
  "X-NP-APP-Version": APP_VERSION,
  "X-NP-User-Agent": `clientType=MOBILE_DEVICE; devicePlatform=IOS; deviceUid=${deviceUid(env)}; applicationName=EGYM Fitness; applicationVersion=${APP_VERSION}; applicationVersionCode=${APP_BUILD}; containerName=NetpulseFitness;`,
  "User-Agent": `NetpulseFitness/${APP_VERSION} (com.netpulse.netpulsefitness; build:${APP_BUILD}; iOS 17.0.0) Alamofire/5.9.1`,
});

export function hasEgymCredentials(env) {
  return Boolean(env?.EGYM_BRAND && env?.EGYM_USERNAME && env?.EGYM_PASSWORD);
}

const baseUrl = (env) => `https://${String(env.EGYM_BRAND).trim().toLowerCase()}.netpulse.com`;

async function login(env) {
  const r = await fetch(`${baseUrl(env)}/np/exerciser/login`, {
    method: "POST",
    headers: { ...headersFor(env), "Content-Type": "application/x-www-form-urlencoded; charset=utf-8" },
    body: new URLSearchParams({ username: String(env.EGYM_USERNAME), password: String(env.EGYM_PASSWORD) }),
  });
  // Antworttext (gekuerzt) und Host mitgeben, damit ein 403 (falsche Kennung, Sperre, falsches Passwort) unterscheidbar ist; Zugangsdaten stehen nie darin.
  if (!r.ok) throw new Error(`egym login ${r.status} bei ${new URL(baseUrl(env)).host}: ${(await r.text()).replace(/\s+/g, " ").slice(0, 200)}`);
  const body = await r.json();
  const cookies = typeof r.headers.getSetCookie === "function" ? r.headers.getSetCookie() : [r.headers.get("set-cookie") ?? ""];
  const cookie = cookies.map((c) => c.split(";", 1)[0]).filter(Boolean).join("; ");
  if (!body?.uuid || !cookie) throw new Error("egym login: keine uuid oder kein Cookie in der Antwort");
  const session = { uuid: body.uuid, cookie, at: Date.now() };
  await writeKvJson(env, SESSION_KV_KEY, session).catch(() => {});
  return session;
}

async function getSession(env, force = false) {
  if (!force) {
    const s = await readKvJson(env, SESSION_KV_KEY).catch(() => null);
    if (s?.uuid && s?.cookie && Date.now() - s.at < SESSION_MAX_AGE_MS) return s;
  }
  return login(env);
}

async function egymGet(env, buildUrl) {
  for (const force of [false, true]) {
    const session = await getSession(env, force);
    const r = await fetch(buildUrl(session.uuid), { headers: { ...headersFor(env), Cookie: session.cookie } });
    if ((r.status === 401 || r.status === 403) && !force) continue;
    if (!r.ok) throw new Error(`egym GET ${r.status}: ${(await r.text()).slice(0, 300)}`);
    return r.json();
  }
  throw new Error("egym: Sitzung abgelehnt");
}

// Netpulse verlangt RFC3339 ohne Millisekunden und ohne URL-Encoding der Doppelpunkte.
const rfc3339 = (d) => d.toISOString().replace(/\.\d{3}Z$/, "Z");

// Workouts mit Uebungen, Saetzen (Wiederholungen und Gewicht) und Aktivitaetspunkten im Zeitraum (ISO-Daten, Ende eingeschlossen).
export function fetchEgymWorkouts(env, fromIso, toIso) {
  const after = rfc3339(new Date(`${fromIso}T00:00:00Z`));
  const before = rfc3339(new Date(Date.parse(`${toIso}T00:00:00Z`) + 86400000 - 1000));
  return egymGet(env, (uid) => `${baseUrl(env)}/workouts/api/workouts/v2.3/exercisers/${uid}/workouts?completedAfter=${after}&completedBefore=${before}`);
}

// Kraft-Tests (1RM je Geraet) im Zeitraum; hier sind die Daten LocalDate (YYYY-MM-DD).
export function fetchEgymStrength(env, fromIso, toIso) {
  return egymGet(env, (uid) => `${MOBILE_API}/measurements/api/v1.0/exercisers/${uid}/strength?startDate=${fromIso}&endDate=${toIso}`);
}

export function fetchEgymBioAge(env) {
  return egymGet(env, (uid) => `${MOBILE_API}/analysis/api/v1.0/exercisers/${uid}/bioage`);
}

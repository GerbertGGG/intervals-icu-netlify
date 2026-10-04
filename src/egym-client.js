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

const HEADERS_BASE = {
  "x-np-user-agent": "clientType=MOBILE_DEVICE; devicePlatform=IOS; deviceUid=intervals-worker; applicationName=NetpulseFitness; applicationVersion=3.11",
  "user-agent": "NetpulseFitness/3.11",
  "x-np-app-version": "3.11",
  Accept: "application/json",
};

export function hasEgymCredentials(env) {
  return Boolean(env?.EGYM_BRAND && env?.EGYM_USERNAME && env?.EGYM_PASSWORD);
}

const baseUrl = (env) => `https://${String(env.EGYM_BRAND).trim().toLowerCase()}.netpulse.com`;

async function login(env) {
  const r = await fetch(`${baseUrl(env)}/np/exerciser/login`, {
    method: "POST",
    headers: { ...HEADERS_BASE, "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ username: String(env.EGYM_USERNAME), password: String(env.EGYM_PASSWORD), relogin: "false" }),
  });
  if (!r.ok) throw new Error(`egym login ${r.status}`);
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
    const r = await fetch(buildUrl(session.uuid), { headers: { ...HEADERS_BASE, Cookie: session.cookie } });
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

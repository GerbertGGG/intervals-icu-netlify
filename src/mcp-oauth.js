// Minimal single-tenant OAuth 2.1 authorization server, just enough to satisfy the
// MCP remote-server auth spec (RFC 8414 metadata + RFC 9728 protected-resource
// metadata + PKCE-only authorization_code/refresh_token grants) so this Worker can
// be added as a Claude custom connector. There is exactly one athlete and one
// client ("claude"), so this intentionally skips things a multi-tenant OAuth
// server would need (dynamic client registration, per-user consent storage, scopes).
import { json } from "./http-helpers.js";
import { mustEnv } from "./kv.js";

export const MCP_CLIENT_ID = "claude";

const CODE_TTL_SECONDS = 300;
const ACCESS_TOKEN_TTL_SECONDS = 3600;
const REFRESH_TOKEN_TTL_SECONDS = 60 * 24 * 60 * 60;

function randomToken() {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return base64url(bytes);
}

function base64url(bytes) {
  const str = btoa(String.fromCharCode(...bytes));
  return str.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function sha256Base64Url(input) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input));
  return base64url(new Uint8Array(digest));
}

// Claude.ai's custom-connector flow calls back to a fixed claude.ai URL; Claude
// Code / Desktop instead run a local loopback listener on an arbitrary port. Both
// need to be accepted here since either can be the OAuth client for this server.
function isAllowedRedirectUri(redirectUri) {
  if (redirectUri === "https://claude.ai/api/mcp/auth_callback") return true;
  try {
    const u = new URL(redirectUri);
    return (u.hostname === "localhost" || u.hostname === "127.0.0.1") && u.pathname === "/callback";
  } catch {
    return false;
  }
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

function renderAuthorizeForm({ redirectUri, state, codeChallenge, error }) {
  return `<!doctype html>
<html><head><meta charset="utf-8"><title>Intervals.icu MCP - Anmelden</title></head>
<body style="font-family: sans-serif; max-width: 420px; margin: 60px auto;">
  <h2>Zugriff bestätigen</h2>
  <p>Ein Client möchte auf deine Intervals.icu-Trainingsdaten (geplante Workouts, Aktivitäten, Wellness) und dein Yazio-Ernährungstagebuch zugreifen.</p>
  ${error ? `<p style="color:red;">${escapeHtml(error)}</p>` : ""}
  <form method="POST" action="/authorize">
    <input type="hidden" name="response_type" value="code">
    <input type="hidden" name="client_id" value="${escapeHtml(MCP_CLIENT_ID)}">
    <input type="hidden" name="redirect_uri" value="${escapeHtml(redirectUri)}">
    <input type="hidden" name="state" value="${escapeHtml(state)}">
    <input type="hidden" name="code_challenge" value="${escapeHtml(codeChallenge)}">
    <input type="hidden" name="code_challenge_method" value="S256">
    <label>Passcode:<br><input type="password" name="passcode" autofocus></label><br><br>
    <button type="submit">Freigeben</button>
  </form>
</body></html>`;
}

async function readAuthParams(req, url) {
  if (req.method === "POST") {
    const form = await req.formData();
    return {
      responseType: form.get("response_type"),
      clientId: form.get("client_id"),
      redirectUri: form.get("redirect_uri"),
      state: form.get("state") || "",
      codeChallenge: form.get("code_challenge"),
      codeChallengeMethod: form.get("code_challenge_method"),
      passcode: form.get("passcode"),
    };
  }
  const params = url.searchParams;
  return {
    responseType: params.get("response_type"),
    clientId: params.get("client_id"),
    redirectUri: params.get("redirect_uri"),
    state: params.get("state") || "",
    codeChallenge: params.get("code_challenge"),
    codeChallengeMethod: params.get("code_challenge_method"),
    passcode: null,
  };
}

export async function handleAuthorizeRequest(req, url, env) {
  const p = await readAuthParams(req, url);

  if (p.responseType !== "code" || p.clientId !== MCP_CLIENT_ID || !p.redirectUri || !isAllowedRedirectUri(p.redirectUri) || !p.codeChallenge || p.codeChallengeMethod !== "S256") {
    return new Response("Invalid authorization request", { status: 400 });
  }

  if (req.method === "POST") {
    const expected = mustEnv(env, "MCP_OWNER_PASSCODE");
    if (p.passcode !== expected) {
      return new Response(renderAuthorizeForm({ ...p, error: "Falscher Passcode." }), {
        status: 401,
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    const code = randomToken();
    await env.KV.put(`mcp:code:${code}`, JSON.stringify({ redirectUri: p.redirectUri, codeChallenge: p.codeChallenge }), {
      expirationTtl: CODE_TTL_SECONDS,
    });

    const redirect = new URL(p.redirectUri);
    redirect.searchParams.set("code", code);
    if (p.state) redirect.searchParams.set("state", p.state);
    return Response.redirect(redirect.toString(), 302);
  }

  return new Response(renderAuthorizeForm(p), { headers: { "content-type": "text/html; charset=utf-8" } });
}

function oauthError(error, description, status) {
  return json({ error, error_description: description }, status);
}

async function issueTokenResponse(env) {
  const accessToken = randomToken();
  const refreshToken = randomToken();
  await env.KV.put(`mcp:token:${accessToken}`, "1", { expirationTtl: ACCESS_TOKEN_TTL_SECONDS });
  await env.KV.put(`mcp:refresh:${refreshToken}`, "1", { expirationTtl: REFRESH_TOKEN_TTL_SECONDS });
  return json({
    access_token: accessToken,
    token_type: "Bearer",
    expires_in: ACCESS_TOKEN_TTL_SECONDS,
    refresh_token: refreshToken,
  });
}

export async function handleTokenRequest(req, env) {
  let form;
  try {
    form = await req.formData();
  } catch {
    return oauthError("invalid_request", "Malformed request body", 400);
  }

  const grantType = form.get("grant_type");
  const clientId = form.get("client_id");
  const clientSecret = form.get("client_secret");
  const expectedSecret = mustEnv(env, "MCP_CLIENT_SECRET");
  if (clientId !== MCP_CLIENT_ID || clientSecret !== expectedSecret) {
    return oauthError("invalid_client", "Unknown client", 401);
  }

  if (grantType === "authorization_code") {
    const code = form.get("code");
    const redirectUri = form.get("redirect_uri");
    const codeVerifier = form.get("code_verifier");
    if (!code || !redirectUri || !codeVerifier) return oauthError("invalid_request", "Missing parameters", 400);

    const raw = await env.KV.get(`mcp:code:${code}`);
    if (!raw) return oauthError("invalid_grant", "Unknown or expired code", 400);
    await env.KV.delete(`mcp:code:${code}`);

    const stored = JSON.parse(raw);
    if (stored.redirectUri !== redirectUri) return oauthError("invalid_grant", "redirect_uri mismatch", 400);

    const computedChallenge = await sha256Base64Url(codeVerifier);
    if (computedChallenge !== stored.codeChallenge) return oauthError("invalid_grant", "PKCE verification failed", 400);

    return issueTokenResponse(env);
  }

  if (grantType === "refresh_token") {
    const refreshToken = form.get("refresh_token");
    if (!refreshToken) return oauthError("invalid_request", "Missing refresh_token", 400);
    const exists = await env.KV.get(`mcp:refresh:${refreshToken}`);
    if (!exists) return oauthError("invalid_grant", "Unknown or expired refresh token", 400);
    await env.KV.delete(`mcp:refresh:${refreshToken}`);
    return issueTokenResponse(env);
  }

  return oauthError("unsupported_grant_type", `Unsupported grant_type: ${grantType}`, 400);
}

export function handleAuthServerMetadata(url) {
  const issuer = url.origin;
  return json({
    issuer,
    authorization_endpoint: `${issuer}/authorize`,
    token_endpoint: `${issuer}/token`,
    response_types_supported: ["code"],
    grant_types_supported: ["authorization_code", "refresh_token"],
    code_challenge_methods_supported: ["S256"],
    token_endpoint_auth_methods_supported: ["client_secret_post"],
  });
}

export function handleProtectedResourceMetadata(url) {
  const issuer = url.origin;
  return json({
    resource: `${issuer}/mcp`,
    authorization_servers: [issuer],
  });
}

export async function isValidAccessToken(env, token) {
  if (!token) return false;
  return Boolean(await env.KV.get(`mcp:token:${token}`));
}

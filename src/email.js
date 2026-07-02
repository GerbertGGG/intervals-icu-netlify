import { mustEnv } from "./kv.js";
import { buildRecentFormAnalysis } from "./form-analysis.js";

const RESEND_API_URL = "https://api.resend.com/emails";
const DEFAULT_FROM = "Intervals.icu Report <onboarding@resend.dev>";

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

// Sends a raw-data report email via the Resend REST API (no SDK needed, works
// from a Workers fetch() call). Intentionally has no fallback/retry: the caller
// wraps this in its own try/catch so a failure here never blocks the rest of
// the scheduled job.
export async function sendJsonReportEmail(env, { subject, introText, data }) {
  const apiKey = mustEnv(env, "RESEND_API_KEY");
  const to = mustEnv(env, "REPORT_EMAIL_TO");
  const from = env?.RESEND_FROM_EMAIL || DEFAULT_FROM;

  const prettyJson = JSON.stringify(data, null, 2);
  const html = `<p>${escapeHtml(introText)}</p><pre style="white-space: pre-wrap; font-family: monospace;">${escapeHtml(prettyJson)}</pre>`;

  const res = await fetch(RESEND_API_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ from, to, subject, html }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Resend request failed: ${res.status} ${body}`);
  }

  return res.json();
}

// Shared by the Monday cron job and the manual /report-email debug route, so
// both trigger paths build and mail the exact same recent-form snapshot.
export async function sendRecentFormReportEmail(env, todayIso, { days = 28 } = {}) {
  const data = await buildRecentFormAnalysis(env, todayIso, { days, includeWeather: true });
  return sendJsonReportEmail(env, {
    subject: `Trainingsdaten Woche ${todayIso}`,
    introText: "Rohdaten der letzten 4 Wochen, zur manuellen Analyse.",
    data,
  });
}

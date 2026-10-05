export function json(o, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(o, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...extraHeaders },
  });
}

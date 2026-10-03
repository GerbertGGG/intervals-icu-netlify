# Claude Code – Projekthinweise

## Berechtigungen

- PRs in diesem Repo dürfen ohne Rückfrage gemergt werden (squash merge).
- Die Netlify-Checks ("Pages changed", "Header rules", "Redirect rules") sind dauerhaft irrelevant – dieses Projekt ist ein Cloudflare Worker, kein Netlify-Projekt. Nur "Workers Builds: intervals-icu-netlify" zählt.

## Wellness-Gefühlswerte

- Muskelkater, Motivation, Stimmung usw. werden immer VOR dem Lauf eingetragen. Sie beschreiben den Ausgangszustand und nicht die Reaktion auf den Lauf. Das gilt besonders für die Ready-Auswertung (`computeReadiness`).
- Eine andere Einschätzung nach dem Lauf ist ein eigener Hinweis, den der Nutzer extra nennt.

## Nach jedem Merge

Nach dem Merge von Änderungen am Athlete-Profil: `/backfill-profile?weeks=24` ausführen, damit das KV-Profil aktuell ist.

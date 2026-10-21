# Claude Code – Projekthinweise

## Berechtigungen

- PRs in diesem Repo dürfen ohne Rückfrage gemergt werden (squash merge).
- Die Netlify-Checks ("Pages changed", "Header rules", "Redirect rules") sind dauerhaft irrelevant – dieses Projekt ist ein Cloudflare Worker, kein Netlify-Projekt. Nur "Workers Builds: intervals-icu-netlify" zählt.

## Grundsatz: Das Dashboard zeigt, der Coach entscheidet

- Die Steuerung kommt vom Coach (Skill `triathlon-coach`, jeden Morgen per geplantem Task, Aussage über `save_coach`). Dashboard, Widget und Uhr machen seine Aussage und die Daten sichtbar.
- Keine neuen Bewertungsregeln einbauen: keine Ampeln, keine Tagesentscheidung, keine Steigern/Halten-Stufen, keine „Was auffällt“-Befunde mit Konsequenz, keine Vorschläge für Umfang oder Intensität. Werte, Vergleiche mit der eigenen Basis und Soll/Ist gegen Ziele, die der Coach selbst gesetzt hat, sind in Ordnung.
- Wenn eine Auswertung nötig scheint, gehört sie in den Coach-Skill, nicht ins Dashboard.

## Wellness-Gefühlswerte

- Muskelkater, Motivation, Stimmung usw. werden immer VOR dem Lauf eingetragen. Sie beschreiben den Ausgangszustand und nicht die Reaktion auf den Lauf. Das gilt für die Erholungswerte (`computeRecovery`) und für die Auswertung des Coaches.
- Eine andere Einschätzung nach dem Lauf ist ein eigener Hinweis, den der Nutzer extra nennt.

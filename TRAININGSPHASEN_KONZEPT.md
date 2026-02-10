# Konzept: Trainingsvorschläge auf Block-Maximum + Progression ausrichten

## Zielbild
- Jede Phase (`BASE`, `BUILD`, `RACE`) erhält pro Distanz (5k/10k/HM/M) ein klares **Block-Maximum** (Zeit pro Schlüsselformat).
- Der Tagesvorschlag führt Athlet:innen mit **progressiver Belastung** in Richtung dieses Maximums.
- Progression wird über **Umfang/Zeit** gesteuert, nicht über gleichzeitige Pace-Erhöhung.

## Fachliche Leitplanken (Ausdauer-Trainingslehre)
- Periodisierung: Base → Build → Race (mit Deload/Taper).
- Progressive Overload: Wochenweise dosierte Steigerung, dann Entlastung.
- Polarität in Base: überwiegend locker, nur kurze neuromuskuläre/VO2-Reize.
- Race/Taper: Intensität halten, Volumen senken.

## Umsetzung im Code (Status)
Bereits eingebaut in `src/index.js`:
1. **Phase-Maxima je Distanz** (`PHASE_MAX_MINUTES`):
   - z. B. BUILD-10k `schwelle=30`, `vo2_touch=32`, `racepace=40`, `longrun=120`.
2. **Primärreiz je Phase** (`resolvePrimaryKeyType`):
   - bevorzugt aus bestehenden Key-Rules; fallback je Phase.
3. **Progressions-Engine** (`computeProgressionTarget`):
   - BASE: 75% → 85% → 95% → 100%
   - BUILD: 80% → 90% → 100% → 65% (Deload)
   - RACE: 90% → 75% → 60% je Eventnähe
4. **Coach-Hinweis im Vorschlag** (`buildProgressionSuggestion`):
   - ergänzt den Key-Vorschlag um Wochenziel in Minuten + Block-Maximum.

## Beispielausgabe
`Nächster Key: schwelle (...) Schwelle: diese Woche ~27′ (Block-Maximum 30′). Progression über Zeit/Umfang – Pace nicht parallel anheben.`

## Wie wir es als Nächstes weiter einarbeiten sollten
1. **Maxima in Konfiguration auslagern** (KV/JSON), damit Coaches ohne Deploy anpassen können.
2. **Workout-Templates mappen** (z. B. `3x10`, `2x20`) statt nur Minuten-Ziel.
3. **Athleten-Level** einführen (Einsteiger/Fortgeschritten/Elite) als Multiplikator auf Maxima.
4. **Safety-Rails**: bei Fatigue-Override Progression auf „halten/entlasten“ fixieren.
5. **Validierung**: Replays über 12 Wochen Historie und Prüfung auf monotone Lastanstiege.

## Erkannte Risiken / offene Probleme
- `PHASE_MAX_MINUTES` ist aktuell statisch im Code; Coaching-Feintuning braucht Deploy.
- Bei uneindeutigen Tags (`key:*`) kann Primärreiz falsch erkannt werden.
- Für einzelne Distanz/Phase-Kombis fehlen noch feinere Unterformen (z. B. HM-Rhythmus vs HM-Pace).
- RACE-Progression nutzt Eventnähe, aber noch keine Wettkampfdichte (mehrere Events).

## Offene Fragen für die nächste Iteration
1. Soll das Block-Maximum **pro Woche** oder **pro Einheit** hart begrenzt werden?
2. Wollen wir für Marathon die Struktur-Longruns (`3x15@M`) explizit als eigenes Muster führen?
3. Sollen wir Bike-Primärphasen mit eigener Progression (analog Run) ergänzen?
4. Soll Deload immer Woche 4 sein oder adaptiv anhand Fatigue/ACWR ausgelöst werden?

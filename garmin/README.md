# Garmin-Watchface "Intervals"

Zifferblatt für runde Garmin-Uhren (Connect IQ, Monkey C) im Apple-Stil: Schwarz und Weiß, Farbe nur, wo sie etwas bedeutet. Von oben nach unten:

- **Woche** Mo bis So am oberen Rand: gefüllt in Sportfarbe (Laufen lila, Rad grün, Schwimmen blau, Kraft gelb) = erledigt, Ring = geplant, dunkel = frei, weißer Ring = heute. Zwei Sportarten an einem Tag: geteilter Kreis, links die erste (erledigt vor geplant, sonst nach Belastung), rechts die zweite.
- **Nächste Einheit**: Sportpunkt, Tag, Einheit mit Dauer ("HEUTE Lauf · 60′").
- **Uhrzeit**, groß.
- **Bereitschaft** wie im Dashboard (Bereit, Mit Vorsicht, Ruhig angehen, Ohne Eintrag) mit Ampelpunkt. Dahinter eine **Warnung** (gelbes Dreieck), wenn der ACWR über dem Korridor liegt (mit Wert) oder "Was auffällt" eine Warnung hat (mit Stichwort, z. B. "Eiweiß", "Kalorien", "Tagebuch", "Heißhunger", "Ruhetag", "Last", "Laufumfang").
- **Vier Komplikationen**: Essen (kcal gegen Tagesziel), TSS der Woche gegen Plan (ohne Plan gegen `WEEKLY_TSS_GOAL`, grün, sobald erreicht), Kraft (Sätze gegen das Ziel aus dem Trainingsblock, knapp/verfehlt in Gelb/Rot), Wetter (Temperatur, Regenwahrscheinlichkeit als Ring).
- **Unterer Rand**: Renn-Countdown (bis 100 Tage vorher) und Blockwoche als gebogene Schrift.

Farben: `COLORFUL` in `IntervalsFaceView.mc` (false = alle Ringe weiß wie im Mockup, true = jede Komplikation in eigener dezenter Farbe).

Die Daten holt die Uhr alle 5 Minuten von `GET /api/widget?view=watch` (siehe `buildWidgetWatch` in `src/widget.js`). Wetter und Akku kommen von der Uhr selbst.

Stand: noch nicht auf einer Uhr getestet. Der Code ist nach dem Mockup geschrieben und wurde nicht mit dem Connect-IQ-Compiler gebaut. Beim ersten Build sind kleine Anpassungen möglich (z. B. Berechtigungen im Manifest, Typhinweise).

## 1. Worker

Ein eigenes Token, das nur die Ansicht `watch` öffnet (das Dashboard-Token bleibt privat):

```
npx wrangler secret put WATCH_TOKEN
```

Optional `WEEKLY_TSS_GOAL` für das Wochenziel (siehe `wrangler.toml`). Der Plan der Woche kommt aus dem Intervals-Kalender.

## 2. Zugangsdaten für die Uhr

In `garmin/source/Config.mc` die Adresse des Workers (`https://intervals-icu-netlify.<konto>.workers.dev`) und das `WATCH_TOKEN` eintragen. Mit leeren Werten baut die App, holt aber keine Daten. Die eingetragene Datei nicht einchecken.

Sideloaded Watchfaces haben in Garmin Connect keine Einstellungen. Wird die App später im Connect-IQ-Store veröffentlicht (auch privat/Beta), überschreiben die Felder "Worker-Adresse" und "Uhr-Token" in den App-Einstellungen die Werte aus `Config.mc`.

## 3. Bauen und installieren

1. Connect IQ SDK und die VS-Code-Erweiterung "Monkey C" installieren, einen Developer Key erzeugen (`Monkey C: Generate a Developer Key`).
2. Ordner `garmin/` in VS Code öffnen, `Monkey C: Build for Device`, Gerät wählen.
3. Die erzeugte `.prg` aus `bin/` per USB nach `GARMIN/APPS/` auf der Uhr kopieren, dann das Zifferblatt auf der Uhr auswählen. Zum Ausprobieren ohne Uhr gibt es den Simulator (`Run`).

Gebaut wird für die Forerunner 265 (416 × 416 Pixel, AMOLED, rund; `manifest.xml`). Die Maße im Code sind auf 454 Pixel ausgelegt und werden auf die Displaygröße skaliert. Weitere runde Modelle lassen sich in `manifest.xml` ergänzen.

## Hinweise

- Schrift: Apples SF Pro darf nicht auf die Uhr. Alle Schriften sind Bitmap-Schriften aus **Barlow** (SIL Open Font License) in `resources/fonts/`, die Uhrzeit nur aus Ziffern und gleich breit. Die Größen sind für 416 Pixel (Forerunner 265) erzeugt, Bitmap-Schriften skalieren nicht mit. Neu erzeugen: siehe Kopf von `tools/make_font.py` (braucht Pillow und die Barlow-Dateien von Google Fonts).
- Always-on-Modus: Es wird nur die Uhrzeit gezeichnet, grau in der eingebauten Schrift (AMOLED schonen).
- Ist der letzte Abruf älter als eine Stunde, wird die Bereitschaft grau und bekommt ein `!`.
- Die gebogene Zeile unten nutzt `drawRadialText`. Das geht nur mit einer Vektorschrift der Uhr (Roboto Condensed, Barlow gibt es nur als Bitmap). Hat die Uhr keine, steht die Zeile gerade.

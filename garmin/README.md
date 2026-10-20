# Garmin-Watchface "Intervals"

Zifferblatt für runde Garmin-Uhren (Connect IQ, Monkey C) im Apple-Stil: Schwarz und Weiß, Farbe nur, wo sie etwas bedeutet. Von oben nach unten:

- **Datum und Akku** ganz oben als kleine Zeile („DI 20. OKT“ und Akkusymbol mit Prozent): weiß, ab 20 % orange, ab 10 % rot, beim Laden grün. Kommt von der Uhr, steht also auch ohne Daten vom Worker.
- **Woche** Mo bis So oben als gerade Reihe abgerundeter Quadrate: gefüllt in Sportfarbe (Laufen gelb, Rad grün, Schwimmen blau, Kraft orange) = erledigt, gedimmt gefüllt = geplant, dunkel = frei, weißer Rahmen = heute. Zwei Sportarten an einem Tag: geteiltes Feld, links die erste (erledigt vor geplant, sonst nach Belastung), rechts die zweite.
- **Nächste Einheit**: Sportpunkt, Tag, Einheit mit Dauer ("HEUTE Lauf · 60′"). Heute bleibt eine geplante Einheit stehen, solange ihre Sportart heute noch nicht erledigt ist (Rad erledigt, Kraft offen → Kraft).
- **Oben mit Wochenzielen des Coaches**: statt Wochenreihe und nächster Einheit je eine Kapsel mit dem, was diese Woche noch offen ist: Minuten bis zum Zeitziel für Lauf, Rad und Schwimmen, offene Sätze für Kraft (dunkel mit Zahl in Sportfarbe = offen, voll mit Haken = erreicht), darunter „OFFEN · NOCH N TAGE“ bis Sonntag.
- **Uhrzeit**, groß.
- **Bereitschaft** wie im Dashboard (Bereit, Mit Vorsicht, Ruhig angehen, Ohne Eintrag) mit Ampel in Farbe und Form: Kreis = bereit, Dreieck = mit Vorsicht, Quadrat mit Kreuz = ruhig angehen. Dahinter eine **Warnung** (gelbes Dreieck), wenn der ACWR über dem Korridor liegt (mit Wert) oder "Was auffällt" eine Warnung hat (mit Stichwort, z. B. "Eiweiß", "Kalorien", "Tagebuch", "Heißhunger", "Ruhetag", "Last", "Laufumfang").
- **Vier Komplikationen**: Essen (in der Mitte die übrigen kcal bis zum Tagesziel, darüber „+N“; der Ring füllt sich mit dem Gegessenen), mit Wochenzielen des Coaches zwei gleiche Kreise im Apple-Stil, „ZEIT“ (Minuten gegen das Zeitziel) und „TSS“ (gegen die Obergrenze), je ein Ring pro Sportart: außen Rad, Mitte Lauf, innen Schwimmen (ohne TSS-Grenze kein Ring); in der Mitte die Summe; über der TSS-Grenze läuft der Überschuss orange als zweite Runde weiter und die Zahl wird orange. Ohne Wochenziele stattdessen TSS der Woche gegen Plan (ohne Plan gegen `WEEKLY_TSS_GOAL`) und Kraft (Sätze gegen das Ziel aus dem Trainingsblock, knapp/verfehlt in Gelb/Rot und zusätzlich mit Dreieck/Quadrat links neben „KRAFT“), Wetter (Temperatur und höchste Regenwahrscheinlichkeit der nächsten 12 Stunden, unter 30 % stattdessen die Uhrzeit des nächsten Sonnenauf- oder -untergangs; der Ring ist ein 12-Stunden-Zifferblatt: die hellen Stunden zwischen Sonnenauf- und -untergang in gedämpftem Gold, darüber jedes Stundensegment ab 30 % Regenwahrscheinlichkeit in Blau, je wahrscheinlicher, desto dunkler: sehr hell 30–49 %, hell 50–69 %, kräftig 70–84 %, dunkel ab 85 %; die Prozentzahl steht im Blau ihrer Stufe; ein weißer Strich markiert die aktuelle Uhrzeit).
- **Unterer Rand**: Renn-Countdown (bis 100 Tage vorher) und Blockwoche als gerade Zeile.

Farben: `COLORFUL` in `IntervalsFaceView.mc` (false = alle Ringe weiß wie im Mockup, true = jede Komplikation in eigener dezenter Farbe).

Die Daten holt die Uhr alle 5 Minuten von `GET /api/widget?view=watch` (siehe `buildWidgetWatch` in `src/widget.js`). Wetter und Akku kommen von der Uhr selbst.

Ist der letzte Stand älter als eine Stunde, steht hinter der Bereitschaft ein „!“ und, wenn der letzte Abruf gescheitert ist, dessen Fehlercode: `-104` keine Bluetooth-Verbindung zum Handy, `-2`/`-300` Zeitüberschreitung, `401` falsches Uhr-Token, `404` falsche Worker-Adresse, `-400`/`-402`/`-403` Antwort unlesbar oder zu groß, `5xx` Fehler im Worker.

Stand: noch nicht auf einer Uhr getestet. Der Code ist nach dem Mockup geschrieben und wurde nicht mit dem Connect-IQ-Compiler gebaut. Beim ersten Build sind kleine Anpassungen möglich (z. B. Berechtigungen im Manifest, Typhinweise).

## 1. Worker

Ein eigenes Token, das nur die Ansicht `watch` öffnet (das Dashboard-Token bleibt privat):

```
npx wrangler secret put WATCH_TOKEN
```

Optional `WEEKLY_TSS_GOAL` für das Wochenziel (siehe `wrangler.toml`). Der Plan der Woche kommt aus dem Intervals-Kalender; die Wochenziele sind dort Einträge der Kategorie „Target“ (Weekly) je Sportart mit Zeit und Load.

## 2. Zugangsdaten für die Uhr

In `garmin/source/Config.mc` die Adresse des Workers (`https://intervals-icu-netlify.<konto>.workers.dev`) und das `WATCH_TOKEN` eintragen. Mit leeren Werten baut die App, holt aber keine Daten. Die eingetragene Datei nicht einchecken.

Sideloaded Watchfaces haben in Garmin Connect keine Einstellungen. Wird die App später im Connect-IQ-Store veröffentlicht (auch privat/Beta), überschreiben die Felder "Worker-Adresse" und "Uhr-Token" in den App-Einstellungen die Werte aus `Config.mc`.

## 3. Bauen und installieren

1. Connect IQ SDK und die VS-Code-Erweiterung "Monkey C" installieren, einen Developer Key erzeugen (`Monkey C: Generate a Developer Key`).
2. Ordner `garmin/` in VS Code öffnen, `Monkey C: Build for Device`, Gerät wählen.
3. Die erzeugte `.prg` aus `bin/` per USB nach `GARMIN/APPS/` auf der Uhr kopieren, dann das Zifferblatt auf der Uhr auswählen. Zum Ausprobieren ohne Uhr gibt es den Simulator (`Run`).

Gebaut wird für die Forerunner 265 (416 × 416 Pixel, AMOLED, rund; `manifest.xml`). Die Maße im Code sind auf 454 Pixel ausgelegt und werden auf die Displaygröße skaliert. Weitere runde Modelle lassen sich in `manifest.xml` ergänzen.

## Hinweise

- Schrift: Apples SF Pro darf nicht auf die Uhr. Alle Schriften sind Bitmap-Schriften aus **Nunito** (SIL Open Font License) in `resources/fonts/`, die Uhrzeit nur aus Ziffern und gleich breit. Die Größen sind für 416 Pixel (Forerunner 265) erzeugt, Bitmap-Schriften skalieren nicht mit. Neu erzeugen: siehe Kopf von `tools/make_font.py` (braucht Pillow und die Nunito-Dateien von Google Fonts).
- Always-on-Modus: Es wird nur die Uhrzeit gezeichnet, grau in der eingebauten Schrift (AMOLED schonen).
- Ist der letzte Abruf älter als eine Stunde, wird die Bereitschaft grau und bekommt ein `!`.
- Die Zeile unten steht in der großen Schrift, wenn sie in die Breite des Kreises passt, sonst in der kleineren.

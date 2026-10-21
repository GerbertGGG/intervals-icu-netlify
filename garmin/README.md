# Garmin-Watchface "Intervals"

Zifferblatt für runde Garmin-Uhren (Connect IQ, Monkey C) im Apple-Stil: Schwarz und Weiß, Farbe nur, wo sie etwas bedeutet, und möglichst ohne Text, dafür große Zahlen. Von oben nach unten:

- **Akku** ganz oben: Akkusymbol mit Prozent, weiß, ab 20 % orange, ab 10 % rot, beim Laden grün. Kommt von der Uhr.
- **Oben mit Wochenzielen des Coaches**: je eine Kapsel mit dem, was diese Woche noch offen ist: Minuten bis zum Zeitziel für Lauf, Rad und Schwimmen, offene Sätze für Kraft (dunkel mit Zahl in Sportfarbe = offen, voll mit Haken = erreicht). Sportfarben: Laufen gelb, Rad grün, Schwimmen blau, Kraft orange.
- **Ohne Wochenziele** stattdessen die **Woche** Mo bis So als gerade Reihe abgerundeter Quadrate (gefüllt in Sportfarbe = erledigt, gedimmt = geplant, dunkel = frei, weißer Rahmen = heute; zwei Sportarten an einem Tag: geteiltes Feld) und darunter die **nächste Einheit** ("HEUTE Lauf · 60′").
- **Uhrzeit**, groß. Der **Doppelpunkt** trägt das Urteil des Coaches für heute (Feld `go` in `save_coach`): grüne Kreise = hart erlaubt, orange Dreiecke = nur locker, rote Quadrate = frei, blaue Kreise = Coach hat geschrieben, aber kein Urteil, grau = noch nichts. Eine eigene Ampel hat die Uhr nicht.
- **Datum** unter der Uhrzeit („DI 21. OKT“), dahinter ein oranges Dreieck mit der Zahl der Blocker des Coaches.
- **Vier Ringe im Bogen**, ohne Überschriften, in der Mitte jeweils die Zahl groß:
  - links außen **Essen**: übrige kcal bis zum Tagesziel (darüber „+N“), der weiße Ring füllt sich mit dem Gegessenen, innen türkis das Eiweiß gegen das Yazio-Ziel. Orange bei mehr als 150 kcal über dem Ziel bzw. ab 18 Uhr unter 80 % Eiweiß.
  - innen links **Zeit**: Minuten der Woche gegen die Summe der Zeitziele, ein Ring mit einem Abschnitt je Sportart in Sportfarbe.
  - innen rechts **TSS**: gegen die Summe der TSS-Grenzen, ebenso je Sportart; die Zahl wird orange, wenn eine Sportart über ihrer Grenze liegt. Ohne Wochenziele stehen innen TSS der Woche gegen Plan (ohne Plan gegen `WEEKLY_TSS_GOAL`) und Kraft (Sätze gegen das Ziel aus dem Trainingsblock, knapp/verfehlt orange/rot).
  - rechts außen **Wetter**: Temperatur, darunter die höchste Regenwahrscheinlichkeit der nächsten 12 Stunden (unter 30 % die Uhrzeit des nächsten Sonnenauf- oder -untergangs). Der Ring ist ein 12-Stunden-Zifferblatt: helle Stunden in gedämpftem Gold, jedes Stundensegment ab 30 % Regen in Blau, je wahrscheinlicher, desto dunkler (30–49 %, 50–69 %, 70–84 %, ab 85 %); ein weißer Strich markiert die aktuelle Uhrzeit.

Renn-Countdown und Blockwoche stehen nicht mehr auf der Uhr (Widget und Dashboard zeigen sie).

Die Daten holt die Uhr alle 5 Minuten von `GET /api/widget?view=watch` (siehe `buildWidgetWatch` in `src/widget.js`). Wetter und Akku kommen von der Uhr selbst.

Ist der letzte Stand älter als eine Stunde, wird das Datum grau, der Doppelpunkt grau, und hinter dem Datum steht ein „!“ und, wenn der letzte Abruf gescheitert ist, dessen Fehlercode: `-104` keine Bluetooth-Verbindung zum Handy, `-2`/`-300` Zeitüberschreitung, `401` falsches Uhr-Token, `404` falsche Worker-Adresse, `-400`/`-402`/`-403` Antwort unlesbar oder zu groß, `5xx` Fehler im Worker.

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
- Ist der letzte Abruf älter als eine Stunde, werden Datum und Doppelpunkt grau und das Datum bekommt ein `!`.

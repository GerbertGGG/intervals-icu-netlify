# Garmin-Watchface "Intervals"

Zifferblatt für runde Garmin-Uhren (Connect IQ, Monkey C), Entwurf „Ultra“ nach Apples Modular Ultra: schwarz, große Uhrzeit in einer Zeile, Skalen am Rand, eine Akzentfarbe (Cyan) für die Woche. Sportfarben nur in den runden Anzeigen: Laufen gelb, Rad grün, Schwimmen blau.

Fest von oben nach unten:

- **Kopf**: Datum wie bei Apple („SO. 25. OKT.“), darunter das Urteil des Coaches für heute als Wort in einer Kapsel seiner Farbe (Feld `go` in `save_coach`): HART grün, LOCKER orange, FREI rot, COACH blau = Coach hat geschrieben, aber kein Urteil, ohne Aussage keine Kapsel. Eine eigene Ampel hat die Uhr nicht.
- **Uhrzeit**, groß und schlank, ohne führende Null.
- **Skalen am Rand** wie beim Ultra (je 25 Striche): links die Ausdauer der Woche gegen das Zeitziel des Coaches (`wm`), von unten in Cyan gefüllt; rechts der Akku (grau, ab 20 % orange, ab 10 % rot, beim Laden grün).
- **Offene Wochenminuten** je Sportart bis zum Zeitziel des Coaches (Einträge der Kategorie „Target“ im Intervals-Kalender) als drei runde Anzeigen: in der Mitte die Minuten in Sportfarbe, außen ein Ring mit Lücke unten, der sich zum Ziel füllt. Ohne Beschriftung: von links Lauf, Rad, Schwimmen. Ziel erreicht: Haken, ohne Ziel „--“.
- **Ganz unten** die nächste Einheit („Heute Lauf 45'“, „Morgen Rad 60'“).

Der **Bereich unter der Uhrzeit** wechselt:

- **Morgens bis 10 Uhr**: „ERHOLUNG“ mit Schlaf, HRV und Ruhepuls von heute, jeder Wert in der Farbe seines Vergleichs mit dem eigenen üblichen Bereich der Basistage (grün im Bereich oder besser, orange schlechter, rot schlechter als 9 von 10 Tagen), darunter die Basis (Ø). Eine Gesamtampel gibt es nicht, das Urteil ist das Wort des Coaches oben. Den Text des Coaches zeigt die Uhr nicht (zu klein und immer abgeschnitten), der steht in Widget und Dashboard.
- **Bis 3 Stunden nach einer Einheit** (von heute): Sportart mit Startzeit in Sportfarbe, daneben „LÄNGSTE SEIT N WO.“, wenn die letzte mindestens so lange Einheit derselben Sportart zwei Wochen oder länger zurückliegt. Darunter nebeneinander Strecke, Zeit (h:mm), Pace oder Watt und Puls (nur, wenn das Dashboard ihn für die Einheit zeigt).
- **Sonst (tagsüber)**: oben die Ausdauer der Woche gegen das Ziel des Coaches („4:05 / 6:00 WOCHE“), darunter die Woche Mo bis So als Kurve wie die Gezeitenkurve des Ultra: bis heute die erledigten Minuten je Tag als Fläche mit Linie, ab heute die geplanten gestrichelt, ein Punkt auf heute.

Essen, Wetter, TSS und Kraft stehen nicht auf der Uhr (Widget und Dashboard zeigen sie). Der Worker schickt ältere Felder weiter, die Uhr nutzt `rd`, `co`, `rz`, `next`, `ti`, `rv`, `wd`, `wm` und `la`.

Die Daten holt die Uhr alle 5 Minuten von `GET /api/widget?view=watch` (siehe `buildWidgetWatch` in `src/widget.js`). Der Worker antwortet dabei sofort aus dem Cache von heute, auch wenn er älter als 5 Minuten ist, und baut ihn im Hintergrund neu; der Cron hält ihn alle 15 Minuten warm. Ein voller Neuaufbau aus allen Quellen dauert länger, als die Uhr wartet (`-300`). Der Akku kommt von der Uhr selbst.

Ist der letzte Stand älter als eine Stunde, steht in der Kopfzeile statt des Coach-Urteils ein oranges „!“ und, wenn der letzte Abruf gescheitert ist, dessen Fehlercode: `-104` keine Bluetooth-Verbindung zum Handy, `-2`/`-300` Zeitüberschreitung, `401` falsches Uhr-Token, `404` falsche Worker-Adresse, `-400`/`-402`/`-403` Antwort unlesbar oder zu groß, `5xx` Fehler im Worker.

Stand: noch nicht auf einer Uhr getestet. Der Code ist nach dem Mockup „Ultra“ geschrieben (Layout mit den Bitmap-Schriften in Python nachgestellt) und wurde nicht mit dem Connect-IQ-Compiler gebaut. Beim ersten Build sind kleine Anpassungen möglich (z. B. Berechtigungen im Manifest, Typhinweise).

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

- Schrift: Apples SF Pro darf nicht auf die Uhr. Alle Schriften sind Bitmap-Schriften aus **Inter** (SIL Open Font License; kommt SF Pro am nächsten; Inter Tight fett lief auf dem AMOLED zusammen) in `resources/fonts/`: `time` (Uhrzeit, nur Ziffern und gleich breit, normale Stärke wie die schlanke Zeit des Ultra), `val` (Werte, Medium), `cap` (Wochenstand, Zahlen in den runden Anzeigen, Halbfett), `tag` (Datum, Kapsel, Beschriftungen, Zeile unten; Halbfett und leicht gesperrt wie Apples Beschriftungen). Die Größen sind für 416 Pixel (Forerunner 265) erzeugt (Beschriftungen 17, `cap` 21, Werte 30, Uhrzeit 100 Pixel; kleiner ist auf der Uhr schlecht lesbar), die Deckkraft steht in allen vier Kanälen des PNG (nur im Alpha wurden die Kanten auf der Uhr gezackt), Bitmap-Schriften skalieren nicht mit. Neu erzeugen: siehe Kopf von `tools/make_font.py` (braucht Pillow und die variable Inter von Google Fonts).
- Always-on-Modus: Es wird nur die Uhrzeit gezeichnet, grau in der eingebauten Schrift (AMOLED schonen).
- Ist der letzte Abruf älter als eine Stunde, steht in der Kopfzeile ein `!` statt des Coach-Urteils.
